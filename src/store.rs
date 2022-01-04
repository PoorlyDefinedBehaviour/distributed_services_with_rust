/// Store represents a file where records are stored.
use std::{
  fs::{File, Metadata},
  io::{BufWriter, Write},
  os::unix::prelude::FileExt,
  sync::Mutex,
};

use anyhow::Result;
use tracing::info;

const LEN_WIDTH: usize = 8;

#[derive(Debug)]
pub struct Store {
  /// File is wrapped in a BufWriter because it can be inefficient
  /// to work directly with something that implements Write
  /// because it may issue too many systems calls.
  ///
  /// BufWriter will keep an in-memory buffer of data
  /// and write it to the underlying writer in batches.
  writer: Mutex<BufWriter<File>>,
  file_metadata: Metadata,
  file_size: u64,
}

#[derive(Debug, PartialEq)]
pub struct AppendOutput {
  pub appended_at: u64,
  pub bytes_written: u64,
}

impl Store {
  pub fn new(file: File) -> Result<Self> {
    let file_metadata = file.metadata()?;

    Ok(Self {
      writer: Mutex::new(BufWriter::new(file)),
      file_size: file_metadata.len(),
      file_metadata,
    })
  }

  /// Appends a new entry to the store file.
  ///
  /// Each entry contains the buffer length followed by the buffer
  /// contents.
  ///
  /// An entry looks like this:
  ///
  ///                              Entry
  /// ┌────────────────────────────────────────────────────────────────┐
  /// │                                                                │
  /// │   LEN                   hello world                            │
  /// │ ┌────┬┬──────────────────────────────────────────────────────┐ │
  /// │ │ 11 ││ 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100 │ │
  /// │ └────┴┴──────────────────────────────────────────────────────┘ │
  /// │                                                                │
  /// └────────────────────────────────────────────────────────────────┘
  ///
  /// Returns how many bytes were written to the store file and
  /// the position in the store file where the entry begins.
  pub fn append(&mut self, buffer: &[u8]) -> Result<AppendOutput> {
    let mut writer = self.writer.lock().unwrap();

    let appended_at = self.file_size;

    writer.write_all(&buffer.len().to_be_bytes())?;
    writer.write_all(buffer)?;

    let bytes_written = (LEN_WIDTH + buffer.len()) as u64;

    self.file_size += bytes_written;

    Ok(AppendOutput {
      appended_at,
      bytes_written,
    })
  }

  /// Returns the entry contents at position.
  ///
  /// First, the entry length is read from the file,
  /// then, the entry contents is read using the entry length
  /// that we jusst read.
  pub fn read(&self, position: u64) -> Result<Vec<u8>> {
    // Flush BufWriter to ensure that content has been written to the underlying
    // file before we read it.
    let mut writer = self.writer.lock().unwrap();

    let _ = writer.flush()?;

    // Buffer that will contain the entry length
    let mut buffer = [0u8; LEN_WIDTH];

    let file = writer.get_ref();

    // Read the entry length(first 8 bytes) into the buffer.
    file.read_exact_at(&mut buffer, position)?;

    let entry_length = u64::from_be_bytes(buffer);

    // Buffer that will contain the entry contents
    let mut buffer = vec![0u8; entry_length as usize];

    // Read entry contents (entry_length bytes after position + bytes that contain the entry length)
    file.read_exact_at(&mut buffer, position + LEN_WIDTH as u64)?;

    Ok(buffer)
  }

  /// Same as Store::read but the buffer is provided by the caller.
  ///
  /// An error will be returned if the buffer length is not the same as the
  /// entry contents at position.
  pub fn read_at(&self, buffer: &mut [u8], position: u64) -> std::io::Result<()> {
    // Flush BufWriter to ensure that content has been written to the underlying
    // file before we read it.
    let mut writer = self.writer.lock().unwrap();

    let _ = writer.flush()?;

    let file = writer.get_ref();

    file.read_exact_at(buffer, position + LEN_WIDTH as u64)
  }

  /// Flushes BufWriter contents to storage.
  ///
  /// The BufWriter is dropped as well.
  pub fn close(self) -> Result<(), std::io::Error> {
    info!(self.file_size, "closing store");

    let mut writer = self.writer.lock().unwrap();

    writer.flush()?;

    Ok(())
  }

  /// Returns the store file size.
  ///
  /// The file size is the sum of all entries in the file.
  pub fn size(&self) -> u64 {
    self.file_size
  }
}

#[cfg(test)]
mod tests {
  use tempfile::NamedTempFile;

  use super::*;

  #[test_log::test]
  fn test_append() {
    let file_write = NamedTempFile::new().unwrap();

    let mut store = Store::new(file_write.into_file()).unwrap();

    let bytes = "hello world".as_bytes();

    // appended_at should be 0 because file is empty.
    assert_eq!(
      AppendOutput {
        appended_at: 0,
        bytes_written: (LEN_WIDTH + bytes.len()) as u64,
      },
      store.append(bytes).unwrap(),
    );

    // appended_at should be 19 because the store file
    // contains one entry.
    assert_eq!(
      AppendOutput {
        appended_at: 19,
        bytes_written: (LEN_WIDTH + bytes.len()) as u64,
      },
      store.append(bytes).unwrap(),
    );
  }

  #[test_log::test]
  fn test_read() {
    let file_write = NamedTempFile::new().unwrap();

    let mut store = Store::new(file_write.into_file()).unwrap();

    let tests = vec!["hello world", r#"{"key": "value"}"#];

    for input in tests {
      let bytes = input.as_bytes();

      let output = store.append(bytes).unwrap();

      assert_eq!(bytes.to_vec(), store.read(output.appended_at).unwrap());
    }
  }

  #[test_log::test]
  fn test_read_at() {
    let file_write = NamedTempFile::new().unwrap();

    let mut store = Store::new(file_write.into_file()).unwrap();

    let tests = vec!["hello world", r#"{"key": "value"}"#];

    for input in tests {
      let bytes = input.as_bytes();

      let mut buffer = vec![0u8; bytes.len()];

      let output = store.append(bytes).unwrap();

      store.read_at(&mut buffer, output.appended_at).unwrap();

      assert_eq!(bytes.to_vec(), buffer);
    }
  }

  #[test_log::test]
  fn test_size() {
    let file_write = NamedTempFile::new().unwrap();

    let mut store = Store::new(file_write.into_file()).unwrap();

    assert_eq!(store.size(), 0);

    let bytes = "abc 123".as_bytes();

    store.append(bytes).unwrap();

    assert_eq!(store.size(), (bytes.len() + LEN_WIDTH) as u64);
  }
}
