use actix_web::web;

mod commit_log;

pub fn init(config: &mut web::ServiceConfig) {
  commit_log::init(config);
}
