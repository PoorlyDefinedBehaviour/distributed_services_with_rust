use actix_web::HttpServer;
use dotenv::dotenv;
use tracing::info;

mod api;
mod app;
mod commit_log;
mod commit_log_v2;
mod index;
mod routes;
mod segment;
mod store;

#[actix_web::main]
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

  HttpServer::new(move || create_app!())
    .bind((host, port))
    .unwrap()
    .run()
    .await
}
