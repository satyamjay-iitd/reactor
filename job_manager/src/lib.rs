use std::collections::{BTreeMap, HashMap};

use futures::future::join_all;
use std::time::{Duration, Instant};
use tokio::time::sleep;

use placement::{
    ChaosOp, CrashOp, Hostname, LibInfo, LogicalOp, MsgDuplicationOp, MsgLossOp, PhysicalOp,
    PlacementManager,
};
use reactor_client::{
    self,
    apis::configuration::Configuration,
    models::{
        DisableMsgDelayRequest, MsgDelayRequest, MsgDuplicationRequest, MsgLossRequest,
        RemoteActorInfo, SpawnArgs,
    },
};

use crate::placement::MsgDelayOp;

pub mod placement;

impl ChaosOp {
    async fn apply(&self, client_config: &Configuration, actor_name: String) {
        match self {
            ChaosOp::Crash(CrashOp { .. }) => {
                reactor_client::apis::default_api::stop_actor(client_config, &actor_name)
                    .await
                    .unwrap();
            }

            ChaosOp::MsgLoss(MsgLossOp { probability, .. }) => {
                let msg_loss_request = MsgLossRequest {
                    actor_name,
                    probability: probability.0,
                };
                reactor_client::apis::default_api::set_msg_loss(client_config, msg_loss_request)
                    .await
                    .unwrap();
            }

            ChaosOp::MsgDuplication(MsgDuplicationOp {
                factor,
                probability,
                ..
            }) => {
                let msg_duplication_request = MsgDuplicationRequest {
                    actor_name,
                    factor: factor.0,
                    probability: probability.0,
                };
                reactor_client::apis::default_api::set_duplication(
                    client_config,
                    msg_duplication_request,
                )
                .await
                .unwrap();
            }
            ChaosOp::MsgDelay(MsgDelayOp {
                delay_range_ms,
                senders,
                ..
            }) => {
                let msg_delay_request = MsgDelayRequest {
                    actor_name,
                    delay_range_start: delay_range_ms.0 as i64,
                    delay_range_end: delay_range_ms.1 as i64,
                    senders: senders.clone(),
                };
                reactor_client::apis::default_api::set_msg_delay(client_config, msg_delay_request)
                    .await
                    .unwrap();
            }
        }
    }

    async fn revert(
        &self,
        client_config: &Configuration,
        actor_name: String,
        spawn_args: &SpawnArgs,
    ) {
        match self {
            ChaosOp::Crash(CrashOp { .. }) => {
                reactor_client::apis::default_api::start_actor(client_config, spawn_args.clone())
                    .await
                    .unwrap();
            }

            ChaosOp::MsgLoss(MsgLossOp { .. }) => {
                reactor_client::apis::default_api::unset_msg_loss(client_config, &actor_name)
                    .await
                    .unwrap();
            }

            ChaosOp::MsgDuplication(MsgDuplicationOp { .. }) => {
                reactor_client::apis::default_api::unset_msg_duplication(
                    client_config,
                    &actor_name,
                )
                .await
                .unwrap();
            }
            ChaosOp::MsgDelay(MsgDelayOp { senders, .. }) => {
                let disable_delay_request = DisableMsgDelayRequest {
                    actor_name,
                    senders: senders.clone(),
                };
                reactor_client::apis::default_api::unset_msg_delay(
                    client_config,
                    disable_delay_request,
                )
                .await
                .unwrap();
            }
        }
    }
}

#[derive(Clone)]
enum ChaosActionKind {
    Apply(ChaosOp),
    Revert(ChaosOp),
}

#[derive(Clone)]
struct ChaosEvent {
    actor_name: String,
    event_time: Instant,
    action: ChaosActionKind,
}

struct NodeHandle {
    hostname: Hostname,
    client_config: Configuration,
    actors: Vec<RemoteActorInfo>,
    /// Used to save actor spawn args for re-starting actors on the node (After a crash)
    actor_spawn_args: HashMap<String, SpawnArgs>,
    loaded_libs: Vec<String>,
    chaos_schedule: Vec<ChaosEvent>,
}

impl NodeHandle {
    // #[cfg(feature = "dynop")]
    // async fn register_op(&mut self, op_info: &LogicalOp) {
    //     reactor_client::apis::default_api::register_op(
    //         &self.client_config,
    //         reactor_client::models::RegistrationArgs {
    //             args: op_info.compile_info.clone(),
    //             lib: op_info.name.clone(),
    //         },
    //     )
    //     .await
    //     .unwrap();
    //     self.operators.push(op_info.clone());
    // }

    #[cfg(feature = "dynop")]
    async fn register_lib(&mut self, lib_info: &LibInfo) {
        reactor_client::apis::default_api::register_lib(
            &self.client_config,
            reactor_client::models::RegistrationArgs {
                args: lib_info.compile_info.clone(),
                lib_name: lib_info.name.clone(),
            },
        )
        .await
        .unwrap();
        self.loaded_libs.push(lib_info.name.clone());
    }

    async fn place(&mut self, logical_op: &LogicalOp, physical_op: &PhysicalOp) -> RemoteActorInfo {
        let spawn_args = SpawnArgs {
            actor_name: physical_op.actor_name.clone(),
            operator_name: logical_op.name.clone(),
            lib_name: logical_op.lib_name.clone(),
            payload: physical_op.payload.clone(),
        };
        self.actor_spawn_args
            .insert(physical_op.actor_name.clone(), spawn_args.clone());
        let mut remote_actor_info =
            reactor_client::apis::default_api::start_actor(&self.client_config, spawn_args)
                .await
                .unwrap();
        remote_actor_info.hostname = self.hostname.to_string();
        self.actors.push(remote_actor_info.clone());
        if let Some(chaos_map) = &physical_op.chaos {
            for chaos_op in chaos_map.iter() {
                let now = Instant::now();
                let start_time = now + Duration::from_millis(chaos_op.start_ms() as u64);
                self.chaos_schedule.push(ChaosEvent {
                    actor_name: physical_op.actor_name.clone(),
                    event_time: start_time,
                    action: ChaosActionKind::Apply(chaos_op.clone()),
                });
                if let Some(stop_ms) = chaos_op.stop_ms() {
                    let stop_time = now + Duration::from_millis(stop_ms as u64);
                    self.chaos_schedule.push(ChaosEvent {
                        actor_name: physical_op.actor_name.clone(),
                        event_time: stop_time,
                        action: ChaosActionKind::Revert(chaos_op.clone()),
                    });
                }
            }
        }
        remote_actor_info
    }

    async fn notify_remote_actor_added(&self, remote_actor: &RemoteActorInfo) {
        reactor_client::apis::default_api::actor_added(&self.client_config, remote_actor.clone())
            .await
            .unwrap();
    }

    async fn schedule_actor_chaos(&self) {
        // also needs some logic on, what if ctrlc pressed prematurely
        // and this function keeps running and sends requests to non-existent nodes
        for chaos_event in self.chaos_schedule.clone() {
            let client_config = self.client_config.clone();
            let spawn_args = self.actor_spawn_args[&chaos_event.actor_name].clone();

            tokio::spawn(async move {
                let now = Instant::now();
                if chaos_event.event_time > now {
                    sleep(chaos_event.event_time - now).await;
                }

                match chaos_event.action {
                    ChaosActionKind::Apply(op) => {
                        op.apply(&client_config, chaos_event.actor_name).await
                    }
                    ChaosActionKind::Revert(op) => {
                        op.revert(&client_config, chaos_event.actor_name, &spawn_args)
                            .await
                    }
                };
            });
        }
    }

    async fn stop_all_actors(self) {
        reactor_client::apis::default_api::stop_all_actors(&self.client_config)
            .await
            .unwrap();
    }
}

pub struct JobController<PM> {
    pm: PM,
    nodes: BTreeMap<String, NodeHandle>,
}

impl<PM: PlacementManager> JobController<PM> {
    pub fn new(pm: PM) -> JobController<PM> {
        JobController {
            pm,
            nodes: BTreeMap::new(),
        }
    }
    pub fn register_node(&mut self, name: &str, hostname: Hostname, port: u16) {
        self.nodes.insert(
            name.to_string(),
            NodeHandle {
                hostname,
                client_config: self.client_config(hostname, port),
                actors: Vec::new(),
                actor_spawn_args: HashMap::new(),
                loaded_libs: Vec::new(),
                chaos_schedule: Vec::new(),
            },
        );
    }

    #[cfg(feature = "dynop")]
    pub async fn register_lib(&mut self, lib: &LibInfo, node_name: &str) {
        let node_handle = self
            .nodes
            .get_mut(node_name)
            .expect("Node must be register before placement");
        node_handle.register_lib(lib).await;
    }

    pub async fn start_job(&mut self, ops: Vec<LogicalOp>) {
        for op in ops {
            for physical_op in self.pm.place(&op) {
                log::info!("Starting Physical op: {physical_op:?}");
                let remote_actor_info = self
                    .nodes
                    .get_mut(&physical_op.nodename)
                    .expect("Node must be register before placement")
                    .place(&op, &physical_op)
                    .await;

                let handles: Vec<_> = self
                    .nodes
                    .iter()
                    .filter_map(|(k, v)| {
                        if *k != physical_op.nodename {
                            Some(v)
                        } else {
                            None
                        }
                    })
                    .map(|node| async {
                        node.notify_remote_actor_added(&remote_actor_info).await;
                    })
                    .collect();
                join_all(handles).await;
            }
        }
    }

    pub async fn chaos_scheduler(&self) {
        for (_, node_handle) in self.nodes.iter() {
            node_handle.schedule_actor_chaos().await;
        }
    }

    pub async fn stop_job(mut self) {
        while let Some((_, node_handle)) = self.nodes.pop_first() {
            node_handle.stop_all_actors().await;
        }
    }

    fn client_config(
        &self,
        hostname: Hostname,
        port: u16,
    ) -> reactor_client::apis::configuration::Configuration {
        reactor_client::apis::configuration::Configuration {
            base_path: format!("http://{hostname}:{port}"),
            ..Default::default()
        }
    }
}
