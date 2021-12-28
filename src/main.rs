#[macro_use]
extern crate log;

use actix_web::HttpServer;
use dotenv::dotenv;

mod app;
mod commit_log;
mod routes;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
  std::env::set_var("RUST_LOG", "proglog=trace");

  dotenv().ok();

  env_logger::init();

  let host = std::env::var("HOST").unwrap();
  let port = std::env::var("PORT").unwrap().parse::<u16>().unwrap();

  info!("starting server at {}:{}", &host, port);

  HttpServer::new(move || app::new())
    .bind((host, port))
    .unwrap()
    .run()
    .await
}
