//! a generic node controller. use this if you dont intend to modify any
//! node_controller logic

use clap::Parser;
use reactor_node::{NodeArgs, NodeExtension, node_controller};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "Node Controller", about = "Run reactor Node controller")]
pub struct Cli {
    #[command(flatten)]
    pub node: NodeArgs,

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

    if let Err(e) = node_controller(cli.node.into(), cli.dir, NodeExtension::empty()).await {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}
