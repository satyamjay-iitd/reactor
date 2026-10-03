//! One Node Controller task will be spawned on each physical nodes.
use op_lib_manager::OpLibrary;
use reactor_actor::ControlReq;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use tokio::sync::mpsc::{Sender, channel, unbounded_channel};
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

#[tracing::instrument(skip_all)]
async fn handle_job_req<CG: CodeGenerator + Send + Sync + 'static>(
    req: JobControllerReq,
    op_lib: &mut OpLibrary,
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

            // Use a stable, named build directory so the metadata and the .so
            // always live at the same path across invocations.
            let build_dir = std::env::temp_dir()
                .join("streamary_builds")
                .join(&lib_name);
            let meta_path = build_dir.join("metadata.json");
            let args_json = serde_json::to_string(&args).unwrap_or_default();

            let already_compiled = op_lib.has_lib(&lib_name)
                && std::fs::read_to_string(&meta_path)
                    .map(|cached| cached == args_json)
                    .unwrap_or(false);

            if already_compiled {
                info!("[Node] Library {lib_name} already compiled with same args, skipping");
                let _ = resp_tx.send(Ok(false));
            } else {
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
                let result = generated
                    .map_err(BuildError::CodegenFailed)
                    .and_then(|(code, cargo_toml)| {
                        LibBuilder::build_named(code, cargo_toml, &build_dir).map(|lib| {
                            op_lib.add_lib(lib_name.clone(), lib);
                            let _ = std::fs::write(&meta_path, &args_json);
                        })
                    });
                let _ = resp_tx.send(result.map(|_| true));
            }
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
                req = job_control_rx.recv() => {
                    match req {
                        Some(req) => {
                            handle_job_req(req, &mut ops, &mut local_actors, &mut remote_actors, &actor_control_tx, SocketAddr::new(bind, actor_port), &code_gen).await;
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
