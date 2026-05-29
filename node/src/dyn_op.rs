//! One Node Controller task will be spawned on each physical nodes.
use op_lib_manager::OpLibrary;
use reactor_actor::ControlReq;
use std::collections::HashMap;
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
    port: u16,
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
                port,
            )
            .await
        }
        #[cfg(feature = "chaos")]
        JobControllerReq::ChaosMsg(msg) => crate::handle_chaos(msg, local_actors).await,
        JobControllerReq::CompileOps {
            lib_name,
            args,
            resp_tx,
        } => {
            info!("[Node] Registering Op from lib: {lib_name}");
            let result = code_gen
                .generate(args)
                .map_err(|e| BuildError::CodegenFailed(e.to_string()))
                .and_then(|(code, cargo_toml)| {
                    LibBuilder::build(code, cargo_toml).map(|lib| {
                        op_lib.add_lib(lib_name, lib);
                    })
                });
            resp_tx.send(result).unwrap();
        }
    }
}

pub async fn node_controller_cg<CG: CodeGenerator + Send + Sync + 'static>(
    operator_dir: PathBuf,
    code_gen: CG,
    node_port: u16,
    extension: crate::NodeExtension,
) {
    use tracing::info_span;

    let span = info_span!("init_node_controller");
    let mut ops = span.in_scope(|| crate::load_ops(operator_dir));

    let (job_control_tx, mut job_control_rx) = unbounded_channel();

    let server_handle = tokio::spawn(webserver(job_control_tx, node_port, extension.into()));
    info!(parent: &span, msg="spawned_http_server", port=node_port);

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
                            handle_job_req(req, &mut ops, &mut local_actors, &mut remote_actors, &actor_control_tx, actor_port, &code_gen).await;
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
}
