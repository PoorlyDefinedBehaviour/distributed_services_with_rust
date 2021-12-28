use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ProduceRequest {
  pub value: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ProduceResponse {
  pub offset: usize,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ConsumeRequest {
  pub offset: usize,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ConsumeResponse {
  pub record: Record,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Record {
  pub value: String,
  pub offset: usize,
}
