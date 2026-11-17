use clap::Parser;
use askama::{Template};
use serde_json::Value;
use reactor_node::node_controller;
use reactor_node::code_gen::CodeGenerator;


const CARGO_TOML: &str = r#"
[package]
name = "ping_pong_op"
version = "0.1.0"
rust-version = "1.86"
edition = "2024"

[lib]
name = "ping_pong"
crate-type = ["cdylib"]


[dependencies]
tokio = { version = "1", features = ["full"] }
ping_pong_actor = { git = "ssh://git@github.com/satyamjay-iitd/reactor.git", branch = "reactor_improvements" }
reactor-actor = { git = "ssh://git@github.com/satyamjay-iitd/reactor.git", branch = "reactor_improvements" }
lazy_static = "1.5.0"
env_logger = "0.11"
"#;


#[derive(Template)]
#[template(
    source = "
use lazy_static;
use ping_pong_actor::actor;
pub use reactor_actor::setup_shared_logger_ref;
use reactor_actor::ControlInst;
use reactor_actor::ControlReq;
use tokio::sync::{mpsc, Mutex};

lazy_static::lazy_static! {
    static ref RUNTIME: tokio::runtime::Runtime = tokio::runtime::Runtime::new().unwrap();
}

#[unsafe(no_mangle)]
pub extern \"C\" fn pingpong(
    actor_name: &'static str,
    node_comm: reactor_actor::NodeComm,
    mut payload: HashMap<String, String>,
) {
    let other = payload.remove(\"other\").unwrap();
    RUNTIME.spawn(actor(node_comm, actor_name, other.leak()));
}
", ext = "txt"
)]
struct LibTemplate {}



#[derive(Parser)]
#[command(name = "Reactor", about = "Run a reactor node or a job manager")]
pub struct Cli {
    #[arg(short, long)]
    port: u16,
}

struct PingPongCodeGen;

impl CodeGenerator for PingPongCodeGen {
    type Error = std::convert::Infallible;

    fn generate(&self, _args: std::collections::HashMap<String, Value>) -> Result<(String, String), Self::Error> {
        let template = LibTemplate {};
        Ok((template.render().expect(""), CARGO_TOML.to_string()))
    }
}


#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let cg = PingPongCodeGen {};
    node_controller(cg, cli.port).await;
}


#[cfg(test)]
mod tests {
    use crate::PingPongCodeGen;
    use reactor_node::lib_builder::LibBuilder;
    use reactor_node::code_gen::CodeGenerator;

    #[test]
    fn test_codegen() {
        let cg = PingPongCodeGen {};
        let (code, deps) = cg.generate(std::collections::HashMap::new()).unwrap();
        let lib = LibBuilder::build(code, deps).unwrap();

    }
}
