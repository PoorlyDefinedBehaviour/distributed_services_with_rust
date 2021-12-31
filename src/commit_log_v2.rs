use std::sync::RwLock;
use thiserror::Error;

use anyhow::Result;

use crate::{
  api,
  segment::{self, Segment},
};

#[derive(Debug)]
pub struct Log {
  directory: String,
  config: Config,
  /// Index of the segment in the `segments` Vec.
  ///
  /// The newest segment is the active one and because
  /// of that active_segment will always be the index
  /// oof the last segment in segments;
  active_segment: usize,
  /// Segments are ordered from oldest to newest.
  segments: Vec<Segment>,
  // TODO: remove me
  lock: RwLock<bool>,
}

#[derive(Debug)]
pub struct Config {
  initial_offset: u64,
  max_store_bytes_per_segment: u64,
  max_index_bytes_per_segment: u64,
}

#[derive(Debug, PartialEq, Error)]
pub enum CommitLogError {
  #[error("offset is out of bounds, no segment contains the offset {0}")]
  OffsetOutOfBounds(u64),
}

impl Default for Config {
  fn default() -> Self {
    Self {
      initial_offset: 0,
      max_store_bytes_per_segment: 1024,
      max_index_bytes_per_segment: 1024,
    }
  }
}

impl Log {
  fn read_segments_from_disk(directory: &str) -> Result<Vec<Segment>> {
    let file_names: Vec<String> = std::fs::read_dir(directory)?
      .filter(|entry| entry.is_ok())
      .map(|entry| entry.unwrap().file_name())
      .map(|file_name| file_name.into_string().unwrap())
      // We only care about .store files because store and index files
      // have the same offsets and we only want each offset once.
      .filter(|file_name| file_name.ends_with(".store"))
      .collect();

    // Given a directory that has a bunch of store and index files
    // we want the offset that's in the name of the store files.
    //
    // 0.store
    // 1.store
    // 2.store
    // ==>
    // [0, 1, 2]
    let mut offsets: Vec<u64> = file_names
      .iter()
      .map(|file_name| file_name.split(".").collect::<Vec<_>>())
      .map(|file_name_pieces| file_name_pieces.iter().rev().nth(1).unwrap().to_string())
      .map(|offset| offset.parse::<u64>().unwrap())
      .collect();

    // Sort offsets in ascending order.
    // Offsets should look like this: 0, 1, 2
    offsets.sort_unstable();

    let segments = offsets
      .into_iter()
      .map(|offset| {
        Segment::new(
          directory,
          offset,
          segment::Config {
            max_index_bytes: 0,
            max_store_bytes: 0,
            initial_offset: 0,
          },
        )
      })
      .collect::<Result<Vec<Segment>, anyhow::Error>>()?;

    Ok(segments)
  }

  pub fn new(directory: String, config: Config) -> Result<Self> {
    let mut segments = Self::read_segments_from_disk(&directory)?;

    // If the log is new and there are no segments on disk,
    // we create the first one.
    if segments.is_empty() {
      segments.push(Segment::new(
        &directory,
        config.initial_offset,
        segment::Config {
          max_index_bytes: 0,
          max_store_bytes: 0,
          initial_offset: 0,
        },
      )?)
    }

    // Segments are ordered from oldest to newest and the newest segment is the active one.
    let active_segment = segments.len() - 1;

    Ok(Self {
      active_segment,
      config,
      directory,
      segments,
      lock: RwLock::new(false),
    })
  }

  /// Appends a new record to the log to the active segment.
  ///
  /// If the segment reaches its max size after the new
  /// record is appended, a new active segment is created.
  pub fn append(&mut self, value: Vec<u8>) -> Result<u64> {
    let _lock = self.lock.write().unwrap();

    let segment = &mut self.segments[self.active_segment];

    let new_record_offset = segment.append(value)?;

    if segment.is_maxed() {
      self.segments.push(Segment::new(
        &self.directory,
        new_record_offset + 1,
        segment::Config {
          max_index_bytes: 0,
          max_store_bytes: 0,
          initial_offset: 0,
        },
      )?);

      self.active_segment += 1;
    }

    Ok(new_record_offset)
  }

  /// Reads the record stored at a given offset.
  pub fn read(&self, offset: u64) -> Result<api::v1::Record> {
    let _lock = self.lock.read().unwrap();

    // Try to find a segment that contains offset in its range.
    let segment = self
      .segments
      .iter()
      .find(|segment| segment.base_offset() <= offset && offset < segment.next_offset());

    match segment {
      None => Err(CommitLogError::OffsetOutOfBounds(offset).into()),
      Some(segment) => segment.read(offset),
    }
  }

  /// Closes every segment in the log.
  pub fn close(self) -> Result<()> {
    // Take ownership of the mutex data since we are cleaning it up.
    let _lock = self.lock.write().unwrap();

    for segment in self.segments.into_iter() {
      segment.close()?;
    }

    Ok(())
  }

  /// Deletes the log directory and then closes every segment in the log.
  pub fn remove(self) -> Result<()> {
    std::fs::remove_dir_all(&self.directory)?;

    self.close()?;

    Ok(())
  }

  /// Returns the base offset of the first segment.
  ///
  /// The lowest offset will be used for consensus
  /// in the replicated cluster.
  pub fn lowest_offset(&self) -> Result<u64> {
    let _lock = self.lock.read().unwrap();

    Ok(self.segments.first().unwrap().base_offset())
  }

  /// Returns the next offset of the last segment.
  ///
  /// The highest offset will be used for consensus
  /// in the replicated cluster.
  pub fn highest_offset(&self) -> Result<u64> {
    let _lock = self.lock.read().unwrap();

    Ok(self.segments.last().unwrap().next_offset() - 1)
  }

  /// Removes segments whose highest offset is lower than lowest.
  /// TODO: add diagram [removed, removed, removed, kept, kept]
  pub fn truncate(&mut self, lowest: u64) -> Result<()> {
    let _lock = self.lock.write().unwrap();

    let mut end_index = 0;

    // Find index of the last segment that does not pass the threshold.
    for (i, segment) in self.segments.iter().enumerate() {
      if segment.next_offset() <= lowest + 1 {
        end_index = i;
      }
    }

    // TODO: does drain change element order?
    for segment in self.segments.drain(0..end_index) {
      segment.remove()?;
    }

    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::segment;
  use tempfile;

  #[test]
  fn foo() {
    let path = tempfile::tempdir().unwrap().into_path();
    let dir = path.to_str().unwrap();
    let mut segment = Segment::new(
      dir,
      0,
      segment::Config {
        initial_offset: 0,
        max_index_bytes: 24,
        max_store_bytes: 128,
      },
    )
    .unwrap();

    assert_eq!(false, segment.is_maxed());

    segment.append(vec![0u8; 8]).unwrap();

    Log::read_segments_from_disk(dir);
  }
}
