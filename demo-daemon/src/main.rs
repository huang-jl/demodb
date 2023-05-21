use clap::Parser;
use demo_core::{Error, Result};
use demo_daemon::start_daemon;
use std::net::SocketAddr;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "DemoDB daemon")]
struct Opt {
    path: PathBuf,
    #[arg(short, long)]
    config: PathBuf,
    #[arg(short, long, default_value = "127.0.0.1:8080")]
    addr: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let opt: Opt = Opt::parse();
    match opt.addr.parse::<SocketAddr>() {
        Ok(addr) => {
            start_daemon(opt.path, opt.config, addr).await?;
        }
        Err(e) => {
            return Err(Error::Config(format!(
                "parsing listen addr {} failed: {e}",
                opt.addr
            )));
        }
    }
    Ok(())
}
