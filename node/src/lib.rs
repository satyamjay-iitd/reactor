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

mod config;
mod op_lib_manager;
mod rpc;

#[cfg(feature = "cli")]
pub use config::NodeArgs;
pub use config::NodeConfig;

pub type NodeAddr = &'static str;
// pub type ActorSpawnCB = fn(RuntimeCtx, HashMap<String, serde_json::Value>);

pub type SetupSharedLogger = fn(SharedLogger);

type ActorAddr = String;
type LibName = String;

#[derive(Debug)]
pub(crate) struct SpawnResult {
    port: u16,
}

/// Why an actor could not be started.
#[derive(Debug, thiserror::Error)]
pub(crate) enum SpawnError {
    #[error("library '{0}' is not loaded")]
    LibraryNotFound(String),
    #[error("library '{lib}' has no operator '{op}'")]
    OperatorNotFound { lib: String, op: String },
    #[error("an actor named '{0}' already exists")]
    ActorExists(String),
    /// The operator panicked while starting, typically because of its payload.
    #[error("operator '{op}' failed to start: {message}")]
    OperatorFailed { op: String, message: String },
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
    pub(crate) resp_tx: oneshot::Sender<Result<SpawnResult, SpawnError>>,
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
        /// `false` if there is no such actor.
        resp_tx: oneshot::Sender<bool>,
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

#[cfg(feature = "dynop")]
#[derive(Debug)]
pub(crate) struct ClearedBuilds {
    pub unloaded: Vec<LibName>,
    pub freed_bytes: u64,
}

/// Global Controller
pub(crate) enum JobControllerReq {
    #[cfg(feature = "dynop")]
    CompileOps {
        lib_name: String,
        args: HashMap<String, Value>,
        /// `true` = freshly compiled, `false` = already up-to-date (skipped)
        resp_tx: oneshot::Sender<Result<bool, crate::lib_builder::BuildError>>,
    },
    /// Cancels the builds of a library (`None`: all); replies with the cancelled libraries.
    #[cfg(feature = "dynop")]
    CancelBuilds {
        lib_name: Option<String>,
        resp_tx: oneshot::Sender<Vec<String>>,
    },
    #[cfg(feature = "dynop")]
    /// Unloads the compiled libraries and deletes their build directories. Refused while actors
    /// run.
    ClearBuildCache {
        resp_tx: oneshot::Sender<Result<ClearedBuilds, crate::lib_builder::BuildError>>,
    },
    ActorLifeCycle(ActorLifeCycle),
    #[cfg(feature = "chaos")]
    ChaosMsg {
        msg: ChaosMsg,
        /// `false` if there is no such actor.
        resp_tx: oneshot::Sender<bool>,
    },
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
                let connection = match local
                    .handle
                    .send(ControlInst::StartLocalRecv(read_half))
                    .await
                {
                    Ok(()) => Connection::Local(write_half),
                    // the actor stopped in the meantime
                    Err(_) => Connection::CouldntResolve,
                };
                let _ = resp_tx.send(connection);
            } else if let Some(local) = remote_actors.get(&addr) {
                info!(target: "resolved", addr=?local.remote_actor_addr);
                let _ = resp_tx.send(Connection::Remote(local.remote_actor_addr));
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
    data_addr: SocketAddr,
) {
    let SpawnActor {
        addr,
        lib_name,
        op_name,
        resp_tx,
        payload,
    } = req;
    info!(target: "serving spawn actor", addr, op_name, lib_name, ?payload);
    let result = spawn_actor(
        &addr,
        &lib_name,
        &op_name,
        payload,
        op_lib,
        actor_control_tx,
        local_actors,
        data_addr,
    )
    .await;
    match &result {
        Ok(_) => info!(target: "actor spawned", %data_addr),
        Err(e) => error!(target: "spawn actor failed", addr, error = %e),
    }
    // the requester may have gone away
    let _ = resp_tx.send(result);
}

#[allow(clippy::too_many_arguments)]
async fn spawn_actor(
    addr: &str,
    lib_name: &str,
    op_name: &str,
    payload: HashMap<String, Value>,
    op_lib: &OpLibrary,
    actor_control_tx: &Sender<ControlReq>,
    local_actors: &mut HashMap<ActorAddr, LocalActor>,
    data_addr: SocketAddr,
) -> Result<SpawnResult, SpawnError> {
    if local_actors.contains_key(addr) {
        return Err(SpawnError::ActorExists(addr.to_string()));
    }
    let op = op_lib.get_op(lib_name, op_name)?;
    let (control_tx, control_rx) = channel(20);
    let ctx = RuntimeCtx::new(
        addr.to_string().leak(),
        NodeComm::new(control_rx, actor_control_tx.clone()),
    );
    op(ctx, payload).map_err(|message| SpawnError::OperatorFailed {
        op: op_name.to_string(),
        message,
    })?;
    control_tx
        .send(ControlInst::StartTcpRecv(data_addr))
        .await
        .map_err(|_| SpawnError::OperatorFailed {
            op: op_name.to_string(),
            message: "the actor exited while starting".to_string(),
        })?;
    local_actors.insert(addr.to_string(), LocalActor { handle: control_tx });
    Ok(SpawnResult {
        port: data_addr.port(),
    })
}

/// Applies a fault-injection setting to a local actor; `false` if there is no such actor.
#[cfg(feature = "chaos")]
async fn handle_chaos(msg: ChaosMsg, local_actors: &mut HashMap<ActorAddr, LocalActor>) -> bool {
    let (actor_name, inst) = match msg {
        ChaosMsg::MsgDuplication {
            actor_name,
            factor,
            probability,
        } => (
            actor_name,
            ControlInst::SetMsgDuplication {
                factor,
                probability,
            },
        ),
        ChaosMsg::MsgLoss {
            actor_name,
            probability,
        } => (actor_name, ControlInst::SetMsgLoss { probability }),
        ChaosMsg::MsgDelay {
            actor_name,
            delay_range_ms,
            senders,
        } => (
            actor_name,
            ControlInst::SetMsgDelay {
                delay_range_ms,
                senders,
            },
        ),
        ChaosMsg::DisableMsgLoss { actor_name } => (actor_name, ControlInst::UnsetMsgLoss),
        ChaosMsg::DisableMsgDuplication { actor_name } => {
            (actor_name, ControlInst::UnsetMsgDuplication)
        }
        ChaosMsg::DisableMsgDelay {
            actor_name,
            senders,
        } => (actor_name, ControlInst::UnsetMsgDelay { senders }),
    };
    match local_actors.get(&actor_name) {
        Some(actor) => {
            info!(target: "applying chaos setting", actor_name);
            // best effort: the actor may be stopping
            let _ = actor.handle.send(inst).await;
            true
        }
        None => false,
    }
}

async fn handle_actor_lc(
    lc: ActorLifeCycle,
    op_lib: &OpLibrary,
    actor_control_tx: &Sender<ControlReq>,
    remote_actors: &mut HashMap<ActorAddr, RemoteActor>,
    local_actors: &mut HashMap<ActorAddr, LocalActor>,
    data_addr: SocketAddr,
) {
    match lc {
        ActorLifeCycle::SpawnActor(req) => {
            handle_spawnactor(req, op_lib, actor_control_tx, local_actors, data_addr).await;
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
        ActorLifeCycle::StopActor { addr, resp_tx } => {
            let found = match local_actors.remove(&addr) {
                Some(actor) => {
                    info!(target: "stopping actor", addr);
                    // the actor may already have exited
                    let _ = actor.handle.send(ControlInst::Stop).await;
                    true
                }
                None => false,
            };
            let _ = resp_tx.send(found);
        }
        ActorLifeCycle::StopAllActors => {
            info!(target: "serving stop all actors", total_actors = local_actors.len());
            for (name, actor) in local_actors.drain() {
                info!(target: "stopping actor", name);
                let _ = actor.handle.send(ControlInst::Stop).await;
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
            let _ = resp_tx.send(NodeStatus {
                actors: local_actors.keys().cloned().collect(),
                loaded_libs: op_lib.lib_names(),
            });
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

#[cfg(test)]
mod tests {
    use super::*;

    fn local_actor() -> LocalActor {
        let (handle, _rx) = channel(1);
        LocalActor { handle }
    }

    async fn spawn(
        local_actors: &mut HashMap<ActorAddr, LocalActor>,
        addr: &str,
        lib_name: &str,
    ) -> Result<SpawnResult, SpawnError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let (actor_control_tx, _actor_control_rx) = channel(1);
        let req = SpawnActor {
            addr: addr.to_string(),
            lib_name: lib_name.to_string(),
            op_name: "op".to_string(),
            resp_tx,
            payload: HashMap::new(),
        };
        handle_spawnactor(
            req,
            &OpLibrary::default(),
            &actor_control_tx,
            local_actors,
            "127.0.0.1:6000".parse().unwrap(),
        )
        .await;
        resp_rx.await.expect("the controller replies")
    }

    #[tokio::test]
    async fn spawning_from_an_unknown_library_is_an_error() {
        let mut local_actors = HashMap::new();
        let err = spawn(&mut local_actors, "a", "nope").await.unwrap_err();
        assert!(
            matches!(err, SpawnError::LibraryNotFound(ref lib) if lib == "nope"),
            "{err}"
        );
        assert!(local_actors.is_empty());
    }

    #[tokio::test]
    async fn spawning_a_duplicate_actor_is_an_error() {
        let mut local_actors = HashMap::from([("a".to_string(), local_actor())]);
        let err = spawn(&mut local_actors, "a", "nope").await.unwrap_err();
        assert!(
            matches!(err, SpawnError::ActorExists(ref name) if name == "a"),
            "{err}"
        );
        assert_eq!(local_actors.len(), 1);
    }

    #[tokio::test]
    async fn stopping_reports_whether_the_actor_existed() {
        let mut local_actors = HashMap::from([("a".to_string(), local_actor())]);
        let mut remote_actors = HashMap::new();
        let (actor_control_tx, _rx) = channel(1);
        for (name, expected) in [("ghost", false), ("a", true), ("a", false)] {
            let (resp_tx, resp_rx) = oneshot::channel();
            let lc = ActorLifeCycle::StopActor {
                addr: name.to_string(),
                resp_tx,
            };
            handle_actor_lc(
                lc,
                &OpLibrary::default(),
                &actor_control_tx,
                &mut remote_actors,
                &mut local_actors,
                "127.0.0.1:6000".parse().unwrap(),
            )
            .await;
            assert_eq!(resp_rx.await.unwrap(), expected, "stop {name}");
        }
        assert!(local_actors.is_empty());
    }

    #[cfg(feature = "chaos")]
    #[tokio::test]
    async fn chaos_settings_report_unknown_actors() {
        let mut local_actors = HashMap::from([("a".to_string(), local_actor())]);
        let known = ChaosMsg::MsgLoss {
            actor_name: "a".to_string(),
            probability: 0.5,
        };
        let unknown = ChaosMsg::MsgLoss {
            actor_name: "ghost".to_string(),
            probability: 0.5,
        };
        assert!(handle_chaos(known, &mut local_actors).await);
        assert!(!handle_chaos(unknown, &mut local_actors).await);
    }

    #[test]
    fn catch_spawn_reports_panics() {
        let hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        assert_eq!(reactor_actor::catch_spawn(|| ()), Ok(()));
        assert_eq!(
            reactor_actor::catch_spawn(|| panic!("static")),
            Err("static".to_string())
        );
        assert_eq!(
            reactor_actor::catch_spawn(|| panic!("formatted {}", 1)),
            Err("formatted 1".to_string())
        );
        std::panic::set_hook(hook);
    }
}
