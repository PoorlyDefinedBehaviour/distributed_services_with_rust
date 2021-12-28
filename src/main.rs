#[macro_use]
extern crate log;

mod commit_log;

use dotenv::dotenv;
use std::env;

use actix_web::{middleware, App, HttpServer};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
  std::env::set_var("RUST_LOG", "proglog=trace");

  dotenv().ok();

  env_logger::init();

  let host = env::var("HOST").unwrap();
  let port = env::var("PORT").unwrap().parse::<u16>().unwrap();

  info!("starting server at {}:{}", &host, port);

  HttpServer::new(move || App::new().wrap(middleware::Logger::default()))
    .bind((host, port))?
    .run()
    .await
}
