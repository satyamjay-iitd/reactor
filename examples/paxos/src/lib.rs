mod acceptor;
mod common;
mod leader;

pub use reactor_actor::setup_shared_logger_ref;
use reactor_actor::{ActorAddr, RuntimeCtx, actor};

use lazy_static::lazy_static;
use serde_json::Value;
use std::collections::HashMap;

const SLEEP_MS: u64 = 100;

// Boiler plate
lazy_static! {
    static ref RUNTIME: tokio::runtime::Runtime = tokio::runtime::Runtime::new().unwrap();
}
#[actor]
pub fn acceptor(ctx: RuntimeCtx, _payload: HashMap<String, Value>) {
    RUNTIME.spawn(acceptor::acceptor(ctx));
}

#[actor]
pub fn leader(ctx: RuntimeCtx, mut payload: HashMap<String, Value>) {
    let acceptors = payload
        .remove("acceptors")
        .unwrap_or_else(|| panic!("{} need to know who acceptors are", ctx.addr));
    let accs: Vec<ActorAddr> = acceptors
        .as_array()
        .unwrap()
        .clone()
        .into_iter()
        .map(|other| other.as_str().unwrap().to_string())
        .collect();
    RUNTIME.spawn(leader::actor(ctx, accs));
}
