use serde::{Deserialize, Serialize};

use crate::commit_log;

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

impl From<commit_log::Record> for ConsumeResponse {
  fn from(item: commit_log::Record) -> Self {
    Self {
      record: Record {
        value: item.value,
        offset: item.offset,
      },
    }
  }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Record {
  pub value: String,
  pub offset: usize,
}
