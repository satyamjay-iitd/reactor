//! a generic node controller. use this if you dont intend to modify any
//! node_controller logic

use clap::{Parser, arg};
use reactor_node::node_controller;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "Node Controller", about = "Run reactor Node controller")]
pub struct Cli {
    /// Port to run the reactor node on
    #[arg(short, long)]
    pub port: u16,

    /// Directory path
    pub dir: PathBuf,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    #[cfg(feature = "jaeger")]
    let _gurad = reactor_inst::init_tracing();

    #[cfg(not(feature = "jaeger"))]
    {
        use env_logger::Builder;
        use log::LevelFilter;

        Builder::new().filter_level(LevelFilter::Info).init();
    }

    node_controller(cli.port, cli.dir).await;
}
