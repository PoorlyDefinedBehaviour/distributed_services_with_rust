use std::{
  fs::OpenOptions,
  io::Cursor,
  path::{Path, PathBuf},
};

use anyhow::Result;
use prost::Message;

use crate::{
  api,
  index::{self, Index},
  store::Store,
};

/// The segment wraps the index and store types to coordinate operations
/// across the two.
///
/// When the log appends a record to the active segment,
/// the segment needs to write the data to its store and add
/// a new entry in the index.
///
/// For reads, the segment needs to lookup the entry from the index
/// and then fetch the data from the store.
#[derive(Debug, Clone)]
pub struct Config {
  pub max_index_bytes: u64,
  pub max_store_bytes: u64,
  pub initial_offset: u64,
}

#[derive(Debug)]
pub struct Segment {
  store_file_path: PathBuf,
  index_file_path: PathBuf,
  store: Store,
  index: Index,
  /// Contains the offset used to calculate offsets relative to the
  /// offset where this segment starts.
  base_offset: u64,
  /// Contains the offset that will be used to append new records.
  next_offset: u64,
  config: Config,
}

impl Segment {
  pub fn new(directory: &str, base_offset: u64, config: Config) -> Result<Self> {
    let store_file_path = Path::new(directory).join(format!("{}.store", base_offset));

    let store_file = OpenOptions::new()
      .read(true)
      .create(true)
      .append(true)
      .open(store_file_path.clone())?;

    let store = Store::new(store_file)?;

    let index_file_path = Path::new(directory).join(format!("{}.index", base_offset));

    let index_file = OpenOptions::new()
      .read(true)
      .write(true)
      .create(true)
      .open(index_file_path.clone())?;

    let index = Index::new(
      index_file,
      index::Config {
        segment: config.clone(),
      },
    )?;

    // If the index is empty, the next offset is the the first
    // offset(the base offset).
    // if the index has entries, the next offset is the offset
    // after the last index entry.
    let next_offset = match index.last_offset() {
      Some(offset) => base_offset + (offset as u64) + 1,
      None => base_offset,
    };

    Ok(Segment {
      base_offset,
      next_offset,
      config,
      index_file_path,
      index: index,
      store_file_path,
      store: store,
    })
  }

  /// Creates a new record and writes it to the store and
  /// to the index.
  /// The offset of the new record is returned.
  pub fn append(&mut self, value: Vec<u8>) -> Result<u64> {
    let offset = self.next_offset;

    let record = api::v1::Record { value, offset };

    let mut buffer = Vec::with_capacity(record.encoded_len());
    // SAFETY: unwrap() is safe because we reserved the buffer capacity.
    record.encode(&mut buffer).unwrap();

    let append_output = self.store.append(&buffer)?;

    self.index.write(
      (self.next_offset - self.base_offset) as u32,
      append_output.appended_at,
    )?;

    self.next_offset += 1;

    Ok(offset)
  }

  /// Returns the record for given offset.
  pub fn read(&self, offset: u64) -> Result<api::v1::Record> {
    let position = self.index.read(offset - self.base_offset)?;

    let bytes = self.store.read(position)?;

    let record = api::v1::Record::decode(&mut Cursor::new(bytes))?;

    Ok(record)
  }

  /// Returns true when the segment has reached its max size.
  ///
  /// The segment has reached its max size if
  /// the store is or the index are full.
  pub fn is_maxed(&self) -> bool {
    self.store.size() >= self.config.max_store_bytes
      || self.index.size() >= self.config.max_index_bytes
  }

  /// Closes store and segment files
  /// and then deletes them from disk.
  pub fn remove(self) -> Result<()> {
    std::fs::remove_file(self.index_file_path.clone())?;
    std::fs::remove_file(self.store_file_path.clone())?;

    self.close()?;

    Ok(())
  }

  /// Closes index and store files.
  pub fn close(self) -> Result<()> {
    self.index.close()?;

    self.store.close()?;

    Ok(())
  }

  /// Returns the segment base offset.
  pub fn base_offset(&self) -> u64 {
    self.base_offset
  }

  /// Returns the segment next offset.
  pub fn next_offset(&self) -> u64 {
    self.next_offset
  }
}

/// Returns the nearest and lesser multiple of k in j.
///
///
/// # Examples
///
/// ```
/// assert_eq!(9, nearest_multiple(9, 4));
/// ```
pub fn nearest_multiple(j: u64, k: u64) -> u64 {
  (j / k) * k
}

#[cfg(test)]
mod tests {
  use super::*;
  use tempfile;

  #[test]
  fn append_then_read() {
    let mut segment = Segment::new(
      tempfile::tempdir().unwrap().into_path().to_str().unwrap(),
      0,
      Config {
        initial_offset: 0,
        max_index_bytes: 1024,
        max_store_bytes: 128,
      },
    )
    .unwrap();

    let bytes = "hello_world".as_bytes().to_vec();

    let offset = segment.append(bytes.clone()).unwrap();

    assert_eq!(
      api::v1::Record {
        value: bytes.clone(),
        offset: 0,
      },
      segment.read(offset).unwrap()
    );

    let offset = segment.append(bytes.clone()).unwrap();

    assert_eq!(
      api::v1::Record {
        value: bytes,
        // TODO: is this correct?
        offset: 1,
      },
      segment.read(offset).unwrap()
    );
  }

  #[test]
  fn test_is_maxed_returns_true_when_store_file_is_full() {
    let mut segment = Segment::new(
      tempfile::tempdir().unwrap().into_path().to_str().unwrap(),
      0,
      Config {
        initial_offset: 0,
        max_index_bytes: 128,
        max_store_bytes: 128,
      },
    )
    .unwrap();

    assert_eq!(false, segment.is_maxed());

    // Append long big enough to make store file full.
    segment.append(vec![0u8; 128]).unwrap();

    // true because store file is full.
    assert_eq!(true, segment.is_maxed());
  }

  #[test]
  fn test_is_maxed_returns_true_when_index_file_is_full() {
    let mut segment = Segment::new(
      tempfile::tempdir().unwrap().into_path().to_str().unwrap(),
      0,
      Config {
        initial_offset: 0,
        max_index_bytes: 24,
        max_store_bytes: 128,
      },
    )
    .unwrap();

    assert_eq!(false, segment.is_maxed());

    // Append two entries to the index, each occupying 12 bytes:
    // 4 for the offset and 8 for the position.
    segment.append(vec![0u8; 128]).unwrap();
    segment.append(vec![0u8; 128]).unwrap();

    // true because index file is full.
    assert_eq!(true, segment.is_maxed());
  }
}
