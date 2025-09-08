mod leader;
mod acceptor;
mod common;

pub use reactor_actor::setup_shared_logger_ref;
use reactor_actor::{ActorAddr, RuntimeCtx};

use lazy_static::lazy_static;
use serde_json::Value;
use std::collections::HashMap;




// Boiler plate
lazy_static! {
    static ref RUNTIME: tokio::runtime::Runtime = tokio::runtime::Runtime::new().unwrap();
}
#[unsafe(no_mangle)]
pub fn acceptor(ctx: RuntimeCtx, mut payload: HashMap<String, Value>) {
    RUNTIME.spawn(acceptor::acceptor(ctx));
}

#[unsafe(no_mangle)]
pub fn leader(ctx: RuntimeCtx, mut payload: HashMap<String, Value>) {
    let acceptors = payload.remove("acceptors")
        .expect(&format!("{} need to know who acceptors are", ctx.addr));
    let accs: Vec<ActorAddr> = acceptors.as_array().unwrap().clone().into_iter()
        .map(|other| other.as_str().unwrap().to_string()).collect();
    RUNTIME.spawn(leader::actor(ctx, accs));
}
