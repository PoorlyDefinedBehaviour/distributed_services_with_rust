use crate::{index::Index, store::Store};

/// The segment wraps the index and store types to coordinate operations
/// across the two.
///
/// When the log appends a record to the active segment,
/// the segment needs to write the data to its store and add
/// a new entry in the index.
///
/// For reads, the segment needs to lookup the entry from the index
/// and then fetch the data from the store.

#[derive(Debug)]
pub struct Config {
  pub max_index_bytes: u64,
  pub max_store_bytes: u64,
  pub initial_offset: u64,
}

#[derive(Debug)]
pub struct Segment {
  store: Store,
  index: Index,
  base_offset: u64,
  next_offset: u64,
  config: Config,
}
