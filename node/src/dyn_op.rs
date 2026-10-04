//! One Node Controller task will be spawned on each physical nodes.
use op_lib_manager::OpLibrary;
use reactor_actor::ControlReq;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use tokio::sync::mpsc::{Sender, UnboundedSender, channel, unbounded_channel};
use tokio::sync::oneshot;
use tokio::task::AbortHandle;
use tracing::info;

use code_gen::CodeGenerator;

use crate::ActorAddr;
use crate::JobControllerReq;
use crate::LocalActor;
use crate::RemoteActor;
use crate::code_gen;
use crate::handle_actor_req;
use crate::lib_builder::{BuildError, LibBuilder};
use crate::op_lib_manager;
use crate::rpc;
use rpc::webserver;

/// The libraries being built, by name. A build runs as a task, so the controller keeps serving
/// meanwhile; it reports back on `done_tx`.
struct Builds {
    running: HashMap<String, Build>,
    done_tx: UnboundedSender<BuildDone>,
}

struct Build {
    args_json: String,
    resp_tx: oneshot::Sender<Result<bool, BuildError>>,
    /// Aborting the task kills the build.
    task: AbortHandle,
}

struct BuildDone {
    lib_name: String,
    result: Result<libloading::Library, BuildError>,
}

impl Builds {
    /// Cancels the builds of `lib_name`, or all of them; returns the names of the cancelled.
    fn cancel(&mut self, lib_name: Option<&str>) -> Vec<String> {
        let names: Vec<String> = self
            .running
            .keys()
            .filter(|n| lib_name.is_none_or(|l| l == n.as_str()))
            .cloned()
            .collect();
        for name in &names {
            let build = self.running.remove(name).expect("listed above");
            build.task.abort();
            info!("[Node] Cancelled the build of {name}");
            let _ = build.resp_tx.send(Err(BuildError::Cancelled(name.clone())));
        }
        names
    }

    /// Applies a finished build: loads the library and answers its requester.
    fn finish(&mut self, done: BuildDone, op_lib: &mut OpLibrary) {
        // a build cancelled just as it finished has no entry: its library is not used
        let Some(build) = self.running.remove(&done.lib_name) else {
            return;
        };
        let result = done.result.map(|lib| {
            op_lib.add_compiled(done.lib_name.clone(), lib, build.args_json);
            true
        });
        info!(
            lib = done.lib_name,
            ok = result.is_ok(),
            "[Node] Build finished"
        );
        let _ = build.resp_tx.send(result);
    }
}

#[tracing::instrument(skip_all)]
#[allow(clippy::too_many_arguments)]
async fn handle_job_req<CG: CodeGenerator + Send + Sync + 'static>(
    req: JobControllerReq,
    op_lib: &mut OpLibrary,
    builds: &mut Builds,
    local_actors: &mut HashMap<String, LocalActor>,
    remote_actors: &mut HashMap<ActorAddr, RemoteActor>,
    actor_contrl_tx: &Sender<ControlReq>,
    data_addr: SocketAddr,
    code_gen: &CG,
) {
    match req {
        JobControllerReq::ActorLifeCycle(lc) => {
            crate::handle_actor_lc(
                lc,
                op_lib,
                actor_contrl_tx,
                remote_actors,
                local_actors,
                data_addr,
            )
            .await
        }
        #[cfg(feature = "chaos")]
        JobControllerReq::ChaosMsg { msg, resp_tx } => {
            let found = crate::handle_chaos(msg, local_actors).await;
            let _ = resp_tx.send(found);
        }
        JobControllerReq::CompileOps {
            lib_name,
            args,
            resp_tx,
        } => {
            info!("[Node] Registering Op from lib: {lib_name}");
            let args_json = serde_json::to_string(&args).unwrap_or_default();

            // A loaded library is never replaced: actors may be running its code.
            match op_lib.loaded_args(&lib_name) {
                Some(loaded) if loaded == args_json => {
                    info!("[Node] Library {lib_name} already compiled with same args, skipping");
                    let _ = resp_tx.send(Ok(false));
                    return;
                }
                Some(_) => {
                    let _ = resp_tx.send(Err(BuildError::LoadedWithOtherArgs(lib_name)));
                    return;
                }
                None => {}
            }
            if builds.running.contains_key(&lib_name) {
                let _ = resp_tx.send(Err(BuildError::AlreadyBuilding(lib_name)));
                return;
            }

            // A code generator may panic on unexpected arguments; contain it so the node
            // controller keeps serving.
            let generated: Result<(String, String), String> =
                match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    code_gen.generate(&lib_name, args)
                })) {
                    Ok(generated) => generated.map_err(|e| e.to_string()),
                    Err(panic) => Err(format!(
                        "the code generator panicked: {}",
                        reactor_actor::panic_message(panic.as_ref())
                    )),
                };
            let (code, cargo_toml) = match generated {
                Ok(generated) => generated,
                Err(e) => {
                    let _ = resp_tx.send(Err(BuildError::CodegenFailed(e)));
                    return;
                }
            };

            // A stable build directory per library, so rebuilds reuse cargo's build cache.
            let build_dir = crate::lib_builder::build_root().join(&lib_name);
            let done_tx = builds.done_tx.clone();
            let name = lib_name.clone();
            let task = tokio::spawn(async move {
                let result = LibBuilder::build_named_async(code, cargo_toml, &build_dir).await;
                let _ = done_tx.send(BuildDone {
                    lib_name: name,
                    result,
                });
            });
            builds.running.insert(
                lib_name,
                Build {
                    args_json,
                    resp_tx,
                    task: task.abort_handle(),
                },
            );
        }
        JobControllerReq::CancelBuilds { lib_name, resp_tx } => {
            let _ = resp_tx.send(builds.cancel(lib_name.as_deref()));
        }
        JobControllerReq::ClearBuildCache { resp_tx } => {
            let result = if !local_actors.is_empty() {
                Err(BuildError::ActorsRunning(local_actors.len()))
            } else if !builds.running.is_empty() {
                let mut names: Vec<&str> = builds.running.keys().map(String::as_str).collect();
                names.sort();
                Err(BuildError::Building(names.join(", ")))
            } else {
                let unloaded = op_lib.forget_compiled();
                crate::lib_builder::clear_build_root()
                    .map(|freed_bytes| crate::ClearedBuilds {
                        unloaded,
                        freed_bytes,
                    })
                    .map_err(BuildError::Io)
            };
            info!(?result, "[Node] Cleared the compile cache");
            let _ = resp_tx.send(result);
        }
    }
}

pub async fn node_controller_cg<CG: CodeGenerator + Send + Sync + 'static>(
    operator_dir: PathBuf,
    code_gen: CG,
    config: crate::NodeConfig,
    extension: crate::NodeExtension,
) -> std::io::Result<()> {
    use tracing::info_span;

    let listener = rpc::listen(&config).await?;
    let bind = config.bind;
    let span = info_span!("init_node_controller");
    let mut ops = span.in_scope(|| crate::load_ops(operator_dir));

    let (job_control_tx, mut job_control_rx) = unbounded_channel();

    let server_handle = tokio::spawn(webserver(job_control_tx, listener, config, extension));
    info!(parent: &span, msg="spawned_http_server");

    let control_loop = tokio::spawn(async move {
        let (done_tx, mut done_rx) = unbounded_channel();
        let mut builds = Builds {
            running: HashMap::new(),
            done_tx,
        };
        let mut local_actors: HashMap<ActorAddr, LocalActor> = HashMap::new();
        let mut remote_actors: HashMap<ActorAddr, RemoteActor> = HashMap::new();
        let (actor_control_tx, mut actor_control_rx) = channel(20);
        let mut actor_port = 6000;
        loop {
            tokio::select! {
                req = actor_control_rx.recv() => {
                    match req {
                        Some(req) => {
                            handle_actor_req(req, &local_actors, &remote_actors).await;
                        },
                        None => break,
                    }
                }
                Some(done) = done_rx.recv() => builds.finish(done, &mut ops),
                req = job_control_rx.recv() => {
                    match req {
                        Some(req) => {
                            handle_job_req(
                                req, &mut ops, &mut builds, &mut local_actors,
                                &mut remote_actors, &actor_control_tx,
                                SocketAddr::new(bind, actor_port), &code_gen
                            ).await;
                            actor_port += 1;
                        },
                        None => break,
                    }
                }
            }
        }
    });
    server_handle.await.unwrap();
    control_loop.await.unwrap();

    log::info!("[Node] Controller Ended");
    Ok(())
}
