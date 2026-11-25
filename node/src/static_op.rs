use crate::rpc::webserver;
use crate::{
    ActorAddr, JobControllerReq, LocalActor, RemoteActor, handle_actor_lc, handle_actor_req,
    op_lib_manager,
};
use op_lib_manager::OpLibrary;
use reactor_actor::ControlReq;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use tokio::sync::mpsc::{Sender, channel, unbounded_channel};
use tracing::info;

pub async fn node_controller(
    config: crate::NodeConfig,
    operator_dir: PathBuf,
    extension: impl Into<crate::NodeExtension>,
) -> std::io::Result<()> {
    use tracing::info_span;

    let listener = crate::rpc::listen(&config).await?;
    let bind = config.bind;
    let span = info_span!("init_node_controller");
    let mut ops = span.in_scope(|| crate::load_ops(operator_dir));

    let (job_control_tx, mut job_control_rx) = unbounded_channel();

    let server_handle = tokio::spawn(webserver(
        job_control_tx,
        listener,
        config,
        extension.into(),
    ));
    info!(parent: &span, msg="spawned_http_server");

    let actor_control_loop = tokio::spawn(async move {
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
                            handle_job_req(req, &mut ops, &mut local_actors, &mut remote_actors, &actor_control_tx, SocketAddr::new(bind, actor_port)).await;
                            actor_port += 1;
                        },
                        None => break,
                    }
                }
            }
        }
    });

    drop(span);

    server_handle.await.unwrap();
    actor_control_loop.await.unwrap();
    Ok(())
}

#[tracing::instrument(skip_all)]
pub(crate) async fn handle_job_req(
    req: JobControllerReq,
    op_lib: &mut OpLibrary,
    local_actors: &mut HashMap<ActorAddr, LocalActor>,
    remote_actors: &mut HashMap<ActorAddr, RemoteActor>,
    actor_contrl_tx: &Sender<ControlReq>,
    data_addr: SocketAddr,
) {
    match req {
        JobControllerReq::ActorLifeCycle(lc) => {
            handle_actor_lc(
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
        #[cfg(feature = "dynop")]
        JobControllerReq::CancelBuilds { resp_tx, .. } => {
            let _ = resp_tx.send(Vec::new());
        }
        #[cfg(feature = "dynop")]
        JobControllerReq::ClearBuildCache { resp_tx } => {
            let _ = resp_tx.send(Err(crate::lib_builder::BuildError::CompilationNotSupported));
        }
        #[cfg(feature = "dynop")]
        JobControllerReq::CompileOps { resp_tx, .. } => {
            let _ = resp_tx.send(Err(crate::lib_builder::BuildError::CompilationNotSupported));
        }
    }
}
