//! One Node Controller task will be spawned on each physical nodes.
use op_lib_manager::OpLibrary;
use reactor_actor::{Connection, ControlInst, ControlReq, NodeComm, RuntimeCtx};
use std::net::SocketAddr;
use std::{collections::HashMap, path::PathBuf};
use tokio::sync::{
    mpsc::{self, Sender, channel},
    oneshot,
};
use tracing::{error, info};
use tracing_shared::SharedLogger;

use serde_json::Value;

#[cfg(feature = "dynop")]
pub mod code_gen;
#[cfg(feature = "dynop")]
pub mod lib_builder;

mod static_op;
pub use static_op::node_controller;

pub use axum::Router;

/// Bundles extra routes and an OpenAPI spec to merge into the node's built-in swagger docs.
pub struct NodeExtension {
    pub router: Router,
    #[cfg(feature = "swagger")]
    pub openapi: utoipa::openapi::OpenApi,
}

impl NodeExtension {
    #[cfg(not(feature = "swagger"))]
    pub fn new(router: Router) -> Self {
        Self { router }
    }
    #[cfg(feature = "swagger")]
    pub fn new(router: Router, openapi: utoipa::openapi::OpenApi) -> Self {
        Self { router, openapi }
    }

    #[cfg(not(feature = "swagger"))]
    pub fn empty() -> Self {
        Self {
            router: Router::new(),
        }
    }
    #[cfg(feature = "swagger")]
    pub fn empty() -> Self {
        Self {
            router: Router::new(),
            openapi: utoipa::openapi::OpenApiBuilder::new().build(),
        }
    }
}

#[cfg(feature = "dynop")]
mod dyn_op;
#[cfg(feature = "dynop")]
pub use dyn_op::node_controller_cg;

mod op_lib_manager;
mod rpc;

pub type NodeAddr = &'static str;
// pub type ActorSpawnCB = fn(RuntimeCtx, HashMap<String, serde_json::Value>);

pub type SetupSharedLogger = fn(SharedLogger);

type ActorAddr = String;
type LibName = String;

#[derive(Debug)]
pub(crate) struct SpawnResult {
    port: u16,
}

#[derive(Debug)]
pub(crate) struct NodeStatus {
    actors: Vec<String>,
    loaded_libs: HashMap<String, Vec<String>>,
}

pub(crate) struct SpawnActor {
    pub(crate) addr: ActorAddr,
    pub(crate) lib_name: String,
    pub(crate) op_name: String,
    pub(crate) resp_tx: oneshot::Sender<Option<SpawnResult>>,
    pub(crate) payload: HashMap<String, Value>,
}

pub(crate) enum ActorLifeCycle {
    SpawnActor(SpawnActor),
    RemoteActorAdded {
        addr: ActorAddr,
        sock_addr: SocketAddr,
    },
    StopActor {
        addr: ActorAddr,
    },
    StopAllActors,
    GetStatus {
        resp_tx: oneshot::Sender<NodeStatus>,
    },
}

#[cfg(feature = "chaos")]
pub(crate) enum ChaosMsg {
    MsgLoss {
        actor_name: ActorAddr,
        probability: f32,
    },
    MsgDuplication {
        actor_name: ActorAddr,
        factor: u32,
        probability: f32,
    },
    MsgDelay {
        actor_name: ActorAddr,
        delay_range_ms: (u64, u64),
        senders: Vec<String>,
    },
    DisableMsgLoss {
        actor_name: ActorAddr,
    },
    DisableMsgDuplication {
        actor_name: ActorAddr,
    },
    DisableMsgDelay {
        actor_name: ActorAddr,
        senders: Vec<String>,
    },
}

/// Global Controller
pub(crate) enum JobControllerReq {
    #[cfg(feature = "dynop")]
    CompileOps {
        lib_name: String,
        args: HashMap<String, Value>,
        resp_tx: oneshot::Sender<Result<(), crate::lib_builder::BuildError>>,
    },
    ActorLifeCycle(ActorLifeCycle),
    #[cfg(feature = "chaos")]
    ChaosMsg(ChaosMsg),
}

struct LocalActor {
    handle: Sender<ControlInst>,
}
struct RemoteActor {
    remote_actor_addr: SocketAddr,
}

#[tracing::instrument(skip(local_actors, remote_actors, req))]
pub(crate) async fn handle_actor_req(
    req: ControlReq,
    local_actors: &HashMap<ActorAddr, LocalActor>,
    remote_actors: &HashMap<ActorAddr, RemoteActor>,
) {
    match req {
        ControlReq::Resolve { addr, resp_tx } => {
            info!(target: "serving resolve addr", addr);
            if let Some(local) = local_actors.get(&addr) {
                info!(target: "resolved", addr="local");
                let (write_half, read_half) = mpsc::channel(1 << 10);
                local
                    .handle
                    .send(ControlInst::StartLocalRecv(read_half))
                    .await
                    .unwrap();
                resp_tx.send(Connection::Local(write_half)).unwrap();
            } else if let Some(local) = remote_actors.get(&addr) {
                info!(target: "resolved", addr=?local.remote_actor_addr);
                resp_tx
                    .send(Connection::Remote(local.remote_actor_addr))
                    .unwrap();
            } else {
                let _ = resp_tx.send(Connection::CouldntResolve);
            }
        }
    }
}

pub(crate) async fn handle_spawnactor(
    req: SpawnActor,
    op_lib: &OpLibrary,
    actor_control_tx: &Sender<ControlReq>,
    local_actors: &mut HashMap<ActorAddr, LocalActor>,
    port: u16,
) {
    let SpawnActor {
        addr,
        lib_name,
        op_name,
        resp_tx,
        payload,
    } = req;
    info!(target: "serving spawn actor", addr, op_name, lib_name, ?payload);
    let (control_tx, control_rx) = channel(20);

    use reactor_actor::ActorSpawnCB;

    let op: libloading::Symbol<ActorSpawnCB> = op_lib.get_op(lib_name, op_name);
    op(
        RuntimeCtx::new(
            addr.clone().leak(),
            NodeComm::new(control_rx, actor_control_tx.clone()),
        ),
        payload,
    );
    resp_tx.send(Some(SpawnResult { port })).unwrap();
    control_tx
        .send(ControlInst::StartTcpRecv(port))
        .await
        .unwrap();
    info!(target: "actor spawned", port);
    local_actors.insert(addr, LocalActor { handle: control_tx });
}

#[cfg(feature = "chaos")]
async fn handle_chaos(msg: ChaosMsg, local_actors: &mut HashMap<ActorAddr, LocalActor>) {
    match msg {
        ChaosMsg::MsgDuplication {
            actor_name,
            factor,
            probability,
        } => {
            if let Some(actor) = local_actors.get(&actor_name) {
                info!(target: "setting msg duplication", actor_name, factor, probability);
                actor
                    .handle
                    .send(ControlInst::SetMsgDuplication {
                        factor,
                        probability,
                    })
                    .await
                    .unwrap();
            }
        }
        ChaosMsg::MsgLoss {
            actor_name,
            probability,
        } => {
            if let Some(actor) = local_actors.get(&actor_name) {
                info!(target: "setting msg loss", actor_name, probability);
                actor
                    .handle
                    .send(ControlInst::SetMsgLoss { probability })
                    .await
                    .unwrap();
            }
        }
        ChaosMsg::MsgDelay {
            actor_name,
            delay_range_ms,
            senders,
        } => {
            if let Some(actor) = local_actors.get(&actor_name) {
                info!(target: "setting msg delay", actor_name, ?senders);
                actor
                    .handle
                    .send(ControlInst::SetMsgDelay {
                        delay_range_ms,
                        senders,
                    })
                    .await
                    .unwrap();
            }
        }
        ChaosMsg::DisableMsgLoss { actor_name } => {
            if let Some(actor) = local_actors.get(&actor_name) {
                info!(target: "disabling msg loss", actor_name);
                actor.handle.send(ControlInst::UnsetMsgLoss).await.unwrap();
            }
        }
        ChaosMsg::DisableMsgDuplication { actor_name } => {
            if let Some(actor) = local_actors.get(&actor_name) {
                info!(target: "disabling msg duplication", actor_name);
                actor
                    .handle
                    .send(ControlInst::UnsetMsgDuplication)
                    .await
                    .unwrap();
            }
        }
        ChaosMsg::DisableMsgDelay {
            actor_name,
            senders,
        } => {
            if let Some(actor) = local_actors.get(&actor_name) {
                info!(target: "disabling msg delay", actor_name);
                actor
                    .handle
                    .send(ControlInst::UnsetMsgDelay { senders })
                    .await
                    .unwrap();
            }
        }
    }
}

async fn handle_actor_lc(
    lc: ActorLifeCycle,
    op_lib: &OpLibrary,
    actor_control_tx: &Sender<ControlReq>,
    remote_actors: &mut HashMap<ActorAddr, RemoteActor>,
    local_actors: &mut HashMap<ActorAddr, LocalActor>,
    port: u16,
) {
    match lc {
        ActorLifeCycle::SpawnActor(req) => {
            handle_spawnactor(req, op_lib, actor_control_tx, local_actors, port).await;
        }
        ActorLifeCycle::RemoteActorAdded { addr, sock_addr } => {
            info!(target: "serving remote actor added", addr, ?sock_addr);
            remote_actors.insert(
                addr,
                RemoteActor {
                    remote_actor_addr: sock_addr,
                },
            );
        }
        ActorLifeCycle::StopActor { addr } => {
            if let Some(actor) = local_actors.remove(&addr) {
                info!(target: "stopping actor", addr);
                actor.handle.send(ControlInst::Stop).await.unwrap();
            }
        }
        ActorLifeCycle::StopAllActors => {
            info!(target: "serving stop all actors", total_actors = local_actors.len());
            for (name, actor) in local_actors.drain() {
                info!(target: "stopping actor", name);
                actor.handle.send(ControlInst::Stop).await.unwrap();
            }
        }
        ActorLifeCycle::GetStatus { resp_tx } => {
            use tracing::{Level, event};
            event!(
                target: "serving::get_status",
                Level::INFO,
                total_actors = local_actors.len(),
                "serving get status"
            );
            resp_tx
                .send(NodeStatus {
                    actors: local_actors.keys().cloned().collect(),
                    loaded_libs: op_lib.lib_names(),
                })
                .unwrap();
        }
    }
}

#[tracing::instrument(fields(operator_dir = ?operator_dir, loaded_lib = tracing::field::Empty))]
fn load_ops(operator_dir: PathBuf) -> OpLibrary {
    use std::ffi::OsStr;
    use std::fs;

    use libloading::Library;

    let mut op_libs = OpLibrary::default();

    if operator_dir.is_dir() {
        for entry in fs::read_dir(operator_dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.extension() == Some(OsStr::new("so"))
                || path.extension() == Some(OsStr::new("dylib"))
            {
                let file_stem = path.file_stem().unwrap().to_string_lossy().to_string();
                let lib_name: String = file_stem
                    .strip_prefix("lib")
                    .unwrap_or(&file_stem)
                    .to_string();
                info!(target: "loaded_lib", lib_name);
                unsafe {
                    let lib = Library::new(&path).unwrap();
                    op_libs.add_lib(lib_name, lib);
                }
            }
        }
    } else {
        error!("Path is not a directory");
    }
    if op_libs.num_libs() == 0 {
        error!("Did not load any library");
    }
    op_libs
}
