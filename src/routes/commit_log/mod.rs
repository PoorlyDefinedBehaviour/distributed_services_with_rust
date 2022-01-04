use std::sync::RwLock;

use actix_web::{get, post, web, HttpResponse, Responder};

use crate::commit_log::CommitLog;

mod viewmodel;

pub fn init(config: &mut web::ServiceConfig) {
  config.service(produce_log).service(consume_log);
}

#[post("/log")]
async fn produce_log(
  log: web::Data<RwLock<CommitLog>>,
  data: web::Json<viewmodel::ProduceRequest>,
) -> impl Responder {
  let offset = log.write().unwrap().append(data.into_inner().value);
  HttpResponse::Ok().json(viewmodel::ProduceResponse { offset })
}

#[get("/log/{offset}")]
async fn consume_log(log: web::Data<RwLock<CommitLog>>, path: web::Path<usize>) -> impl Responder {
  match log.read().unwrap().read(path.into_inner()) {
    None => HttpResponse::NotFound().finish(),
    Some(record) => HttpResponse::Ok().json(viewmodel::ConsumeResponse::from(record)),
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::create_app;

  use actix_web::{
    http::StatusCode,
    test::{self, TestRequest},
  };
  use serde_json::json;

  #[actix_rt::test]
  async fn post_log() {
    let mut app = test::init_service(create_app!()).await;

    let tests = vec![
      (
        json!({
          "value": "a",
        }),
        viewmodel::ProduceResponse { offset: 0 },
      ),
      (
        json!({
          "value": "b",
        }),
        viewmodel::ProduceResponse { offset: 1 },
      ),
    ];

    for (request_body, expected_response_body) in tests {
      let response = TestRequest::post()
        .uri("/log")
        .set_json(&request_body)
        .send_request(&mut app)
        .await;

      assert!(response.status().is_success());

      let response_body: viewmodel::ProduceResponse = test::read_body_json(response).await;

      assert_eq!(expected_response_body, response_body);
    }
  }

  #[actix_rt::test]
  async fn get_log_returns_not_found_if_offset_is_invalid() {
    let mut app = test::init_service(create_app!()).await;

    let response = TestRequest::get()
      .uri("/log/100000")
      .send_request(&mut app)
      .await;

    assert_eq!(StatusCode::NOT_FOUND, response.status());
  }

  #[actix_rt::test]
  async fn get_log_returns_log() {
    let mut app = test::init_service(create_app!()).await;

    let post_log_response_body: viewmodel::ProduceResponse = test::read_body_json(
      TestRequest::post()
        .uri("/log")
        .set_json(&json!({
          "value": "a",
        }))
        .send_request(&mut app)
        .await,
    )
    .await;

    let get_log_response = TestRequest::get()
      .uri(&format!("/log/{}", post_log_response_body.offset))
      .send_request(&mut app)
      .await;

    assert_eq!(StatusCode::OK, get_log_response.status());

    let body: viewmodel::ConsumeResponse = test::read_body_json(get_log_response).await;

    assert_eq!(
      viewmodel::ConsumeResponse {
        record: viewmodel::Record {
          value: String::from("a"),
          offset: post_log_response_body.offset,
        }
      },
      body
    );
  }
}
