/// Index represents a file where we index each record in the store file.
///
/// It contains an in memory file and a persisted memory mapped file.
///
/// Memory-mapped file - https://en.wikipedia.org/wiki/Memory-mapped_file
///
/// A memory-mapped file is a segment of virtual memory that has
/// been assigned a direct byte-for-byte correlation with some
/// portion of a file or file-like resource.
/// This resource is typically a file that is physically present on disk,
/// but can also be a device, shared memory object,
/// or other resource that the operating system can reference through
/// a file descriptor.
///
/// The benefit of memory mapping a file is increasing I/O performance,
/// especially when used on large files.
///
/// Accessing memory mapped files is faster than using direct read
/// and write operations for two reasons:
///
/// Firstly, a system call is orders of magnitude slower than a simple
/// change to a program's local memory.
///
/// Secondly, in most operating systems the memory region mapped
/// actually is the kernel's page cache, meaning that no copies need to be
/// created in user space.
use std::{fs::File, io::Write};

use anyhow::Result;
use memmap::MmapMut;
use thiserror::Error;
/// WIDTH constants define the number of bytes that
/// make up each index entry.
///
/// Index entries contain two fields:
///
/// The record's offset and its position in the store file.
/// The offset is stored as 4 bytes and the position as 8.
static OFFSET_WIDTH: u64 = 4;
static POSITION_WIDTH: u64 = 8;
static ENTRY_WIDTH: u64 = OFFSET_WIDTH + POSITION_WIDTH;

#[derive(Debug)]
pub struct Index {
  file: File,
  /// Contains the size of the index and
  /// where to write the next entry appended to the index.
  size: u64,
  mmap: MmapMut,
}

#[derive(Debug)]
pub struct SegmentConfig {
  max_index_bytes: u64,
}

#[derive(Debug)]
pub struct Config {
  segment: SegmentConfig,
}

#[derive(Debug, PartialEq, Error)]
pub enum IndexError {
  #[error("index has reached it's maximum amount of entries")]
  IndexIsFull,
  #[error("index with len {index_len:?} does not contain offset {offset:?}")]
  OffsetOutOfBounds { offset: u64, index_len: u64 },
}

impl Index {
  pub fn new(file: File, config: Config) -> Result<Self> {
    let metadata = file.metadata()?;

    // Grow file to the max index size before memory mapping it
    // because we cannot resize the file after it is memory mapped.
    file.set_len(config.segment.max_index_bytes)?;

    let mmap = unsafe { MmapMut::map_mut(&file)? };

    Ok(Self {
      file,
      mmap,
      size: metadata.len(),
    })
  }

  /// Returns how many entries the index contains.
  fn len(&self) -> u64 {
    self.size / ENTRY_WIDTH
  }

  /// Returns true when the index contains no entries.
  fn is_empty(&self) -> bool {
    self.size == 0
  }

  /// Returns true when the index has the maximum
  /// amount of entries.
  fn is_full(&self) -> bool {
    (self.mmap.len() as u64) < self.size + ENTRY_WIDTH
  }

  /// Appends the given offset and position to the index.
  ///
  /// Returns `IndexError::IndexIsFull` if the index file
  /// does not contain enough space for the new entry.
  pub fn write(&mut self, offset: u32, position: u64) -> Result<()> {
    if self.is_full() {
      return Err(IndexError::IndexIsFull.into());
    }

    let size = self.size as usize;

    let offset_ends_at = (self.size + OFFSET_WIDTH) as usize;

    let position_ends_at = offset_ends_at + POSITION_WIDTH as usize;

    (&mut self.mmap[size..offset_ends_at]).write_all(&(offset).to_be_bytes())?;
    (&mut self.mmap[offset_ends_at..position_ends_at]).write_all(&(position).to_be_bytes())?;

    self.size += ENTRY_WIDTH;

    Ok(())
  }

  /// Takes an offset as argument and returns the associated
  /// record's position in the store.
  ///
  /// The given offset is relative to the segment's base offset:
  /// 0 is always the offset of the index's first entry,
  /// 1 is the second entry, and so on.
  pub fn read(&self, offset: u64) -> Result<u64, IndexError> {
    if self.is_empty() || offset >= self.len() {
      return Err(IndexError::OffsetOutOfBounds {
        offset,
        index_len: self.len(),
      });
    }

    let position_starts_at = ((offset * ENTRY_WIDTH) as usize) + OFFSET_WIDTH as usize;

    let position_range =
      position_starts_at..(position_starts_at as usize + POSITION_WIDTH as usize);

    let mut buffer = [0u8; 8];

    // Copy position bytes(8 bytes) to buffer.
    buffer[..].copy_from_slice(&self.mmap[position_range]);

    let position = u64::from_be_bytes(buffer);

    Ok(position)
  }

  /// Syncs memory-mapped file to the persisted file,
  /// flushes persisted file contents to stable storage
  /// and truncates the persisted file to the amount of data
  /// that's actually in it and then closes the file.
  pub fn close(mut self) -> Result<(), std::io::Error> {
    self.mmap.flush()?;

    self.file.set_len(self.size)?;

    self.file.flush()?;

    drop(self.file);

    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::io::Read;
  use tempfile::NamedTempFile;

  #[test]
  fn index_rebuilds_state_from_file_if_file_is_not_empty() {
    let file = NamedTempFile::new().unwrap();
    let file_copy = file.reopen().unwrap();

    let mut index1 = Index::new(
      file.into_file(),
      Config {
        segment: SegmentConfig {
          max_index_bytes: 1024,
        },
      },
    )
    .unwrap();

    index1.write(1, 10).unwrap();

    // Ensure file contents are flushed to storage.
    index1.close().unwrap();

    // File has one entry, so if we create an index
    // from it, the index should contain the entry.
    let index2 = Index::new(
      file_copy,
      Config {
        segment: SegmentConfig {
          max_index_bytes: 1024,
        },
      },
    )
    .unwrap();

    assert_eq!(Ok(10), index2.read(0));
  }

  #[test]
  fn write() {
    let file_write = NamedTempFile::new().unwrap();
    let mut file_read = file_write.reopen().unwrap();

    let mut index = Index::new(
      file_write.into_file(),
      Config {
        segment: SegmentConfig {
          max_index_bytes: 1024,
        },
      },
    )
    .unwrap();

    index.write(0, 0).unwrap();
    index.write(1, 10).unwrap();
    index.write(2, 1000).unwrap();

    // Ensure file contents are flushed to storage.
    index.close().unwrap();

    let mut buffer: Vec<u8> = Vec::new();

    file_read.read_to_end(&mut buffer).unwrap();

    // Expected file bytes, bytes are represented as decimal.
    let expected = vec![
      // 00000000 00000000 00000000 00000000 (4 bytes)
      0, 0, 0, 0, // offset(4 bytes) = 0
      // 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000 (8 bytes)
      0, 0, 0, 0, 0, 0, 0, 0, // position (8 bytes) = 0
      // ---
      // 00000000 00000000 00000000 00000001 (4 bytes)
      0, 0, 0, 1, // offset(4 bytes) = 1
      // 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00001010 (8 bytes)
      0, 0, 0, 0, 0, 0, 0, 10, // position (8 bytes) = 10
      // ---
      // 00000000 00000000 00000000 00000010 (4 bytes)
      0, 0, 0, 2, // offset(4 bytes) = 2
      // 00000000 00000000 00000000 00000000 00000000 00000000 00000011 11101000 (8 bytes)
      0, 0, 0, 0, 0, 0, 3, 232, // position (8 bytes) = 1000
    ];

    assert_eq!(expected, buffer);
  }

  #[test]
  fn read_returns_error_if_offset_is_greater_than_the_index_size() {
    let file_write = NamedTempFile::new().unwrap();

    let mut index = Index::new(
      file_write.into_file(),
      Config {
        segment: SegmentConfig {
          max_index_bytes: 1024,
        },
      },
    )
    .unwrap();

    // Index size is 0.
    assert_eq!(
      Err(IndexError::OffsetOutOfBounds {
        index_len: 0,
        offset: 0,
      }),
      index.read(0)
    );

    index.write(0, 11).unwrap();

    // Index size is 1 but to read the first index we should call read(0).
    assert_eq!(
      Err(IndexError::OffsetOutOfBounds {
        index_len: 1,
        offset: 1,
      }),
      index.read(1)
    );
  }

  #[test]
  fn read_returns_position_thats_mapped_to_the_offset() {
    let file_write = NamedTempFile::new().unwrap();

    let mut index = Index::new(
      file_write.into_file(),
      Config {
        segment: SegmentConfig {
          max_index_bytes: 1024,
        },
      },
    )
    .unwrap();

    index.write(0, 10).unwrap();
    index.write(1, 0).unwrap();
    index.write(2, 1).unwrap();
    index.write(3, 333).unwrap();
    index.write(999, 42).unwrap();

    assert_eq!(Ok(10), index.read(0));
    assert_eq!(Ok(0), index.read(1));
    assert_eq!(Ok(1), index.read(2));
    assert_eq!(Ok(333), index.read(3));
    assert_eq!(Ok(42), index.read(4));
  }
}
