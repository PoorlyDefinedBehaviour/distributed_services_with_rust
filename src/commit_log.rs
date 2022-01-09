use std::sync::RwLock;
use thiserror::Error;

use anyhow::Result;
use tracing::info;

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

#[derive(Debug, Clone)]
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
  fn read_segments_from_disk(directory: &str, config: &Config) -> Result<Vec<Segment>> {
    info!(directory, "reading segments from disk");

    // Ensure `directory` exists.
    std::fs::create_dir_all(directory)?;

    let file_names: Vec<String> = std::fs::read_dir(directory)?
      .filter(|entry| entry.is_ok())
      .map(|entry| entry.unwrap().file_name())
      .map(|file_name| file_name.into_string().unwrap())
      // We only care about .store files because store and index files
      // have the same offsets and we only want each offset once.
      .filter(|file_name| file_name.ends_with(".store"))
      .collect();

    info!("store files found on disk: {:?}", &file_names);

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

    info!("store files offsets found on disk: {:?}", &offsets);

    let segments = offsets
      .into_iter()
      .map(|offset| {
        Segment::new(
          directory,
          offset,
          segment::Config {
            max_index_bytes: config.max_index_bytes_per_segment,
            max_store_bytes: config.max_store_bytes_per_segment,
            initial_offset: 0,
          },
        )
      })
      .collect::<Result<Vec<Segment>, anyhow::Error>>()?;

    info!("{} segments found on disk", segments.len());

    Ok(segments)
  }

  pub fn new(directory: String, config: Config) -> Result<Self> {
    info!("creating log in {}", &directory);

    let mut segments = Self::read_segments_from_disk(&directory, &config)?;

    // If the log is new and there are no segments on disk,
    // we create the first one.
    if segments.is_empty() {
      info!("creating first segment in the log");

      segments.push(Segment::new(
        &directory,
        config.initial_offset,
        segment::Config {
          max_index_bytes: config.max_index_bytes_per_segment,
          max_store_bytes: config.max_store_bytes_per_segment,
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
    let directory = self.directory.clone();

    self.close()?;

    // TODO: is this a waste?
    // We are flushing the store and index to disk
    // because of Store::close and Index::close
    // and then deleting the folder containing the flushed data.
    std::fs::remove_dir_all(directory)?;

    Ok(())
  }

  /// Returns the base offset of the first segment.
  ///
  /// The lowest offset will be used for consensus
  /// in the replicated cluster.
  pub fn lowest_offset(&self) -> u64 {
    let _lock = self.lock.read().unwrap();

    self.segments.first().unwrap().base_offset()
  }

  /// Returns the next offset of the last segment.
  ///
  /// The highest offset will be used for consensus
  /// in the replicated cluster.
  pub fn highest_offset(&self) -> u64 {
    let _lock = self.lock.read().unwrap();

    self.segments.last().unwrap().next_offset()
  }

  /// Removes segments whose highest offset is lower than lowest.
  ///
  /// It is called periodically to remove old segments whose
  /// data has already been processed.
  ///
  /// TODO: add diagram [removed, removed, removed, kept, kept]
  pub fn truncate(&mut self, lowest: u64) -> Result<()> {
    info!(lowest, "truncating segments");

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

  /// Creates a new segment, appends it to the list of segments
  /// and makes it the active segment.
  pub fn new_segment(&mut self, offset: u64) -> Result<()> {
    info!("creating new segment at offset {}", offset);

    let segment = Segment::new(
      &self.directory,
      self.config.initial_offset + offset,
      // TODO: use actual config
      segment::Config {
        max_index_bytes: self.config.max_index_bytes_per_segment,
        max_store_bytes: self.config.max_store_bytes_per_segment,
        initial_offset: offset,
      },
    )?;

    self.segments.push(segment);
    self.active_segment = self.segments.len() - 1;

    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use tempfile;

  fn new_log() -> Log {
    Log::new(
      tempfile::tempdir()
        .unwrap()
        .into_path()
        .to_str()
        .unwrap()
        .to_owned(),
      Config::default(),
    )
    .unwrap()
  }

  #[test_log::test]
  fn append_then_read() {
    let mut log = new_log();

    let tests = vec![("a", 0), ("b", 1), ("c", 2)];

    for (input, expected_offset) in tests {
      let input = input.as_bytes().to_vec();

      let offset = log.append(input.clone()).unwrap();

      assert_eq!(expected_offset, offset);

      assert_eq!(
        api::v1::Record {
          offset: expected_offset,
          value: input,
        },
        log.read(offset).unwrap(),
      );
    }
  }

  #[test_log::test]
  fn log_reuses_data_stored_on_disk_by_prior_log_instances() {
    let mut log = new_log();

    let data = vec![(0, "a"), (1, "b"), (2, "c")];

    for (_, input) in &data {
      log.append(input.as_bytes().to_vec()).unwrap();
    }

    let directory = log.directory.clone();
    let config = log.config.clone();

    // Ensure contents are flushed to storage.
    log.close().unwrap();

    // Create a new log that should reuse the files that contain
    // the data created by the first log.
    let log = Log::new(directory, config).unwrap();

    for (expected_offset, input) in data {
      assert_eq!(
        api::v1::Record {
          offset: expected_offset,
          value: input.as_bytes().to_vec(),
        },
        log.read(expected_offset).unwrap()
      );
    }
  }

  #[test_log::test]
  fn lowest_offset_returns_base_offset_of_the_first_segment() {
    let mut log = new_log();

    assert_eq!(log.config.initial_offset, log.lowest_offset());

    log.new_segment(log.config.initial_offset + 1).unwrap();

    assert_eq!(log.config.initial_offset, log.lowest_offset());
  }

  #[test_log::test]
  fn highest_offset_returns_the_next_offset_that_will_be_used_by_the_newest_segment() {
    let mut log = new_log();

    // The last used offset of the last segment will be the initial offset
    // because the log is empty.
    assert_eq!(log.config.initial_offset, log.highest_offset());

    log.append("hello world".as_bytes().to_vec()).unwrap();

    assert_eq!(log.config.initial_offset + 1, log.highest_offset());
  }

  #[test_log::test]
  fn test_truncate() {
    let mut log = new_log();

    log.new_segment(1).unwrap();
    log.new_segment(2).unwrap();

    // Initial segment + 2 segments added.
    assert_eq!(3, log.segments.len());

    log.truncate(1).unwrap();

    assert_eq!(1, log.segments.len());
    assert_eq!(2, log.segments[0].base_offset())
  }
}
