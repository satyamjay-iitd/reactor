use std::collections::BTreeMap;

use futures::future::join_all;
use std::time::{Duration, Instant};
use tokio::time::sleep;

use placement::{ChaosOp, Hostname, LibInfo, LogicalOp, PhysicalOp, PlacementManager};
use reactor_client::{
    self,
    models::{RemoteActorInfo, SpawnArgs},
};

pub mod placement;

struct NodeHandle {
    hostname: Hostname,
    client_config: reactor_client::apis::configuration::Configuration,
    actors: Vec<RemoteActorInfo>,
    loaded_libs: Vec<String>,
    crash_schedule: Vec<(RemoteActorInfo, Instant)>,
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
        if let Some(ChaosOp::Crash { start_ms }) = &physical_op.chaos {
            let now = Instant::now();
            let delay = Duration::from_millis(*start_ms as u64);
            self.crash_schedule
                .push((remote_actor_info.clone(), now + delay));
        }
        remote_actor_info
    }

    async fn notify_remote_actor_added(&self, remote_actor: &RemoteActorInfo) {
        reactor_client::apis::default_api::actor_added(&self.client_config, remote_actor.clone())
            .await
            .unwrap();
    }

    async fn schedule_actor_crash(&self) {
        // also needs some logic on, what if ctrlc pressed prematurely
        // and this function keeps running and sends requests to non-existent nodes
        for (actor, when) in self.crash_schedule.clone() {
            // let this = self.clone();
            let client_config = self.client_config.clone();
            tokio::spawn(async move {
                let now = Instant::now();
                if when > now {
                    sleep(when - now).await;
                }
                // this.stop_actor(&actor).await;
                reactor_client::apis::default_api::stop_actor(&client_config, actor.clone())
                    .await
                    .unwrap();
            });
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
                crash_schedule: Vec::new(),
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
            node_handle.schedule_actor_crash().await;
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
