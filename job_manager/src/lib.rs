use std::collections::BTreeMap;

use futures::future::join_all;
use std::time::{Duration, Instant};
use tokio::time::sleep;

use placement::{ChaosOp, Hostname, LibInfo, LogicalOp, PhysicalOp, PlacementManager};
use reactor_client::{
    self,
    models::{ChaosConfig, ChaosType, RemoteActorInfo, SpawnArgs},
};

pub mod placement;

struct NodeHandle {
    hostname: Hostname,
    client_config: reactor_client::apis::configuration::Configuration,
    actors: Vec<RemoteActorInfo>,
    loaded_libs: Vec<String>,
    chaos_schedule: Vec<(RemoteActorInfo, ChaosOp, Instant, Option<Instant>)>,
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
        let mut remote_actor_info = reactor_client::apis::default_api::start_actor(
            &self.client_config,
            SpawnArgs {
                actor_name: physical_op.actor_name.clone(),
                operator_name: logical_op.name.clone(),
                lib_name: logical_op.lib_name.clone(),
                payload: physical_op.payload.clone(),
            },
        )
        .await
        .unwrap();
        remote_actor_info.hostname = self.hostname.to_string();
        self.actors.push(remote_actor_info.clone());
        for chaos_op in &physical_op.chaos.clone().unwrap_or_default() {
            let now = Instant::now();
            let start_time = now + Duration::from_millis(chaos_op.start_ms() as u64);
            let stop_time = chaos_op
                .stop_ms()
                .map(|stop_ms| now + Duration::from_millis(stop_ms as u64));
            self.chaos_schedule.push((
                remote_actor_info.clone(),
                chaos_op.clone(),
                start_time,
                stop_time,
            ));
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
        for (actor, op, start, _stop_opt) in self.chaos_schedule.clone() {
            let client_config = self.client_config.clone();

            let chaos_config = match op {
                ChaosOp::Crash { .. } => ChaosConfig {
                    kind: ChaosType::Crash,
                    actor_name: actor.name.clone(),
                    factor: None,
                    probability: None,
                },

                ChaosOp::MsgLoss { probability, .. } => ChaosConfig {
                    kind: ChaosType::MsgLoss,
                    actor_name: actor.name.clone(),
                    factor: None,
                    probability: Some(probability.into_inner()),
                },

                ChaosOp::MsgDuplication {
                    factor,
                    probability,
                    ..
                } => ChaosConfig {
                    kind: ChaosType::MsgDuplication,
                    actor_name: actor.name.clone(),
                    factor: Some(factor),
                    probability: Some(probability.into_inner()),
                },
            };
            tokio::spawn(async move {
                let now = Instant::now();
                if start > now {
                    sleep(start - now).await;
                }
                reactor_client::apis::default_api::add_chaos(&client_config, chaos_config)
                    .await
                    .unwrap();
            });
            //? This is for later
            // if let Some(stop) = stop_opt {
            //     tokio::spawn(async move {
            //         let now = Instant::now();
            //         if stop > now {
            //             sleep(stop - now).await;
            //         }
            //         // reactor_client::apis::default_api::stop_actor(&client_config, actor.clone())
            //         //     .await
            //         //     .unwrap();
            //     });
            // }
        }
    }

    async fn stop_all_actors(&self) {
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
