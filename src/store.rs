/// Store represents a file where records are stored.
use std::{
  fs::{File, Metadata},
  io::{BufWriter, Write},
  os::unix::prelude::FileExt,
  sync::Mutex,
};

use anyhow::Result;

static LEN_WIDTH: usize = 8;

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

#[derive(Debug)]
pub struct AppendOutput {
  appended_at: u64,
  bytes_written: u64,
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

  pub fn append(&mut self, buffer: &[u8]) -> Result<AppendOutput> {
    let mut writer = self.writer.lock().unwrap();

    let appended_at = self.file_size;

    // TODO: why is LEN_WIDTH used here?
    let bytes_written = (LEN_WIDTH + writer.write(buffer)?) as u64;

    self.file_size += bytes_written;

    Ok(AppendOutput {
      appended_at,
      bytes_written,
    })
  }

  pub fn read(&self, position: u64) -> Result<Vec<u8>> {
    // Flush BufWriter to ensure that content has been written to the underlying
    // file before we read it.
    let mut writer = self.writer.lock().unwrap();

    let _ = writer.flush()?;

    // buffer len is LEN_WIDTH because we will read LEN_WIDTH bytes
    // stating from position.
    let mut buffer = vec![0u8; LEN_WIDTH];

    let file = writer.get_ref();
    // TODO:
    // First file read should read only a header
    // that says how many bytes the record contains.
    // Then we should do another file read to get the whole record.

    // TODO: should an error be returned if we're unable to read 8 bytes?
    let _ = file.read_at(&mut buffer, position)?;

    Ok(buffer)
  }

  pub fn read_at(&self, buffer: &mut [u8], position: u64) -> std::io::Result<usize> {
    // Flush BufWriter to ensure that content has been written to the underlying
    // file before we read it.
    let mut writer = self.writer.lock().unwrap();

    let _ = writer.flush()?;

    let file = writer.get_ref();

    file.read_at(buffer, position)
  }

  pub fn flush(&self) -> Result<(), std::io::Error> {
    let mut writer = self.writer.lock().unwrap();

    writer.flush()
  }
}
