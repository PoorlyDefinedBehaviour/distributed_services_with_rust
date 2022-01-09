use std::sync::Arc;

use tokio::sync::{mpsc, RwLock};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::{api, commit_log::Log};
use tracing::error;

#[derive(Debug, Clone)]
pub struct LogServer {
  log: Arc<RwLock<Log>>,
}

impl LogServer {
  pub fn new(log: Log) -> Self {
    Self {
      log: Arc::new(RwLock::new(log)),
    }
  }
}

#[tonic::async_trait]
impl api::v1::log_server::Log for LogServer {
  async fn produce(
    &self,
    request: Request<api::v1::ProduceRequest>,
  ) -> Result<Response<api::v1::ProduceResponse>, Status> {
    match self.log.write().await.append(request.into_inner().value) {
      Ok(offset) => Ok(Response::new(api::v1::ProduceResponse { offset })),
      Err(e) => {
        error!("{}", e);
        Err(Status::unavailable("service unavailable"))
      }
    }
  }

  async fn consume(
    &self,
    request: Request<api::v1::ConsumeRequest>,
  ) -> Result<Response<api::v1::ConsumeResponse>, Status> {
    match self.log.read().await.read(request.into_inner().offset) {
      Ok(record) => Ok(Response::new(api::v1::ConsumeResponse {
        record: Some(record),
      })),
      Err(e) => {
        error!("{}", e);
        Err(Status::unavailable("service unavailable"))
      }
    }
  }

  type consume_streamStream = ReceiverStream<Result<api::v1::ConsumeResponse, Status>>;

  async fn consume_stream(
    &self,
    request: Request<api::v1::ConsumeRequest>,
  ) -> Result<Response<Self::consume_streamStream>, Status> {
    let mut offset = request.into_inner().offset;

    let (tx, rx) = mpsc::channel(4);

    let log = Arc::clone(&self.log);

    tokio::spawn(async move {
      loop {
        match log.read().await.read(offset) {
          Ok(record) => {
            tx.send(Ok(api::v1::ConsumeResponse {
              record: Some(record),
            }))
            .await;
          }
          Err(e) => {
            error!("{}", e);
            tx.send(Err(Status::ok("DONE"))).await;
          }
        }
      }
    });

    Ok(Response::new(ReceiverStream::new(rx)))
  }

  type produce_streamStream = ReceiverStream<Result<api::v1::ProduceResponse, Status>>;

  async fn produce_stream(
    &self,
    request: Request<Streaming<api::v1::ProduceRequest>>,
  ) -> Result<Response<Self::produce_streamStream>, Status> {
    let mut request_streamer = request.into_inner();

    let (tx, rx) = mpsc::channel(4);

    let log = Arc::clone(&self.log);

    tokio::spawn(async move {
      while let Some(request) = request_streamer.message().await.unwrap() {
        match log.write().await.append(request.value) {
          Ok(offset) => {
            tx.send(Ok(api::v1::ProduceResponse { offset })).await;
          }
          Err(e) => {
            error!("{}", e);
            tx.send(Err(Status::unavailable("service unavailable")))
              .await;
          }
        }
      }
    });

    Ok(Response::new(ReceiverStream::new(rx)))
  }
}
