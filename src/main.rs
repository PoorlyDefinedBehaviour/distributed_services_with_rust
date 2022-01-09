use dotenv::dotenv;
use tracing::info;

mod api;
mod commit_log;
mod commit_log_v2;
mod index;
mod segment;
mod server;
mod store;

#[tokio::main]
async fn main() -> std::io::Result<()> {
  std::env::set_var(
    "RUST_LOG",
    std::env::var("RUST_LOG").unwrap_or(String::from("proglog=trace")),
  );

  dotenv().ok();

  tracing_subscriber::fmt::init();

  let host = std::env::var("HOST").unwrap();
  let port = std::env::var("PORT").unwrap().parse::<u16>().unwrap();

  info!("starting server at {}:{}", &host, port);

  Ok(())
}
