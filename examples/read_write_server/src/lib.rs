mod client_utils;
mod reader;
mod server;
mod writer;

pub use reactor_actor::setup_shared_logger_ref;
use reader::ReaderIn;
use server::{ServerIn, ServerOut};
use writer::WriterIn;

use crate::reader::{ReadAck, ReadOut, reader as reader_behaviour};
use crate::server::server as server_behaviour;
use crate::writer::{WriteAck, WriteOut, writer as writer_behaviour};
use reactor_actor::{RuntimeCtx, actor};
use reactor_macros::msg_converter;
use std::collections::HashMap;
// //////////////////////////////////////////////////////////////////////////////
//                                    MSG
// //////////////////////////////////////////////////////////////////////////////

msg_converter! {
   Decoders: [
       server_decoder can decode ReadOut, WriteOut to ServerIn;
       reader_decoder can decode ReadAck, ServerOut to ReaderIn;
       writer_decoder can decode WriteAck, ServerOut to WriterIn;
   ];
}

// //////////////////////////////////////////////////////////////////////////////
//                                ACTORS
// //////////////////////////////////////////////////////////////////////////////

lazy_static::lazy_static! {
    static ref RUNTIME: tokio::runtime::Runtime = tokio::runtime::Runtime::new().unwrap();
}

#[actor]
fn server(ctx: RuntimeCtx, _payload: HashMap<String, serde_json::Value>) {
    RUNTIME.spawn(server_behaviour(ctx, server_decoder));
}

#[actor]
fn writer(ctx: RuntimeCtx, mut payload: HashMap<String, serde_json::Value>) {
    let server_addr = payload
        .remove("server_addr")
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();
    RUNTIME.spawn(writer_behaviour(ctx, server_addr, writer_decoder));
}

#[actor]
fn reader(ctx: RuntimeCtx, mut payload: HashMap<String, serde_json::Value>) {
    let server_addr = payload
        .remove("server_addr")
        .unwrap()
        .as_str()
        .unwrap()
        .to_string();
    RUNTIME.spawn(reader_behaviour(ctx, server_addr, reader_decoder));
}
