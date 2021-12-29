extern crate prost_build;

fn main() {
  prost_build::compile_protos(&["src/api/v1/log.proto"], &["src/"]).unwrap();
}
