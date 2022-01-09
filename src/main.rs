use std::net::SocketAddr;

use anyhow::Result;
use dotenv::dotenv;
use tonic::transport::Server;
use tracing::info;

use crate::commit_log::Log;

mod api;
mod commit_log;
mod index;
mod segment;
mod server;
mod store;

#[tokio::main]
async fn main() -> Result<()> {
  std::env::set_var(
    "RUST_LOG",
    std::env::var("RUST_LOG").unwrap_or(String::from("proglog=trace")),
  );

  dotenv().ok();

  tracing_subscriber::fmt::init();

  let host = std::env::var("HOST")?;
  let port = std::env::var("PORT")?.parse::<u16>()?;
  let address: SocketAddr = format!("{}:{}", host, port).parse()?;

  let log_server = api::v1::log_server::LogServer::new(server::LogServer::new(Log::new(
    String::from("./log_dir"),
    commit_log::Config::default(),
  )?));

  info!("starting server at {}", &address);

  Server::builder()
    .add_service(log_server)
    .serve(address)
    .await?;

  Ok(())
}
