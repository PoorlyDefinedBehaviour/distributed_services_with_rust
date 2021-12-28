use std::sync::RwLock;

use actix_web::{
  body::MessageBody,
  dev::{ServiceFactory, ServiceRequest, ServiceResponse},
  middleware,
  web::Data,
  App, Error,
};

use crate::{commit_log::CommitLog, routes};

pub fn new() -> App<
  impl ServiceFactory<
    ServiceRequest,
    Response = ServiceResponse<impl MessageBody>,
    Config = (),
    InitError = (),
    Error = Error,
  >,
> {
  // TODO: can something be done to remove the lock?
  let commit_log = Data::new(RwLock::new(CommitLog::new()));

  App::new()
    .app_data(commit_log.clone())
    .wrap(middleware::Logger::default())
    .configure(routes::init)
}
