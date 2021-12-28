use std::sync::RwLock;

use actix_web::{post, web, HttpResponse, Responder};

use crate::commit_log::CommitLog;

mod viewmodel;

pub fn init(config: &mut web::ServiceConfig) {
  config.service(produce_log);
}

#[post("/log")]
async fn produce_log(
  log: web::Data<RwLock<CommitLog>>,
  data: web::Json<viewmodel::ProduceRequest>,
) -> impl Responder {
  let offset = log.write().unwrap().append(data.into_inner().value);
  HttpResponse::Ok().json(viewmodel::ProduceResponse { offset })
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::create_app;

  use actix_web::test::{self, TestRequest};
  use serde_json::json;

  #[actix_rt::test]
  async fn product_log() {
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
}
