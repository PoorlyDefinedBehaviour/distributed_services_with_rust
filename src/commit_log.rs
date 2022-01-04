/// A commit log is a data structure for an append-only sequence
/// of records ordered by time.
use std::sync::RwLock;

#[derive(Debug)]
pub struct CommitLog {
  records: RwLock<Vec<Record>>,
}

impl CommitLog {
  pub fn new() -> Self {
    Self {
      records: RwLock::new(vec![]),
    }
  }

  pub fn append(&mut self, value: String) -> usize {
    let mut records = self.records.write().unwrap();

    let offset = records.len();

    records.push(Record { value, offset });

    offset
  }

  pub fn read(&self, offset: usize) -> Option<Record> {
    // TODO: cloning a String thats inside of Record may be too expensive.
    self.records.read().unwrap().get(offset).cloned()
  }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Record {
  pub value: String,
  pub offset: usize,
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test_log::test]
  fn append_appends_value_with_correct_offset() {
    let mut log = CommitLog::new();

    log.append(String::from("a"));
    log.append(String::from("b"));
    log.append(String::from("c"));

    assert_eq!(
      vec![
        Record {
          value: String::from("a"),
          offset: 0
        },
        Record {
          value: String::from("b"),
          offset: 1
        },
        Record {
          value: String::from("c"),
          offset: 2,
        }
      ],
      *log.records.read().unwrap(),
    );
  }

  #[test_log::test]
  fn append_returns_record_offset() {
    let mut log = CommitLog::new();

    assert_eq!(0, log.append(String::from("a")));
    assert_eq!(1, log.append(String::from("b")));
    assert_eq!(2, log.append(String::from("c")));
  }

  #[test_log::test]
  fn read_returns_record_at_given_offset() {
    let mut log = CommitLog::new();

    let a_offset = log.append(String::from("a"));
    let b_offset = log.append(String::from("b"));
    let c_offset = log.append(String::from("c"));
    let invalid_offset = log.records.read().unwrap().len() + 1;

    assert_eq!(
      Some(Record {
        value: String::from("a"),
        offset: 0
      }),
      log.read(a_offset)
    );

    assert_eq!(
      Some(Record {
        value: String::from("b"),
        offset: 1
      }),
      log.read(b_offset)
    );

    assert_eq!(
      Some(Record {
        value: String::from("c"),
        offset: 2
      }),
      log.read(c_offset)
    );

    assert_eq!(None, log.read(invalid_offset));
  }
}
