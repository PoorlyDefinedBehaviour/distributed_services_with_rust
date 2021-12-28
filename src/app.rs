#[macro_export]
macro_rules! create_app {
  () => {{
    let commit_log =
      actix_web::web::Data::new(std::sync::RwLock::new($crate::commit_log::CommitLog::new()));

    actix_web::App::new()
      .app_data(commit_log.clone())
      .wrap(actix_web::middleware::Logger::default())
      .configure($crate::routes::init)
  }};
}
