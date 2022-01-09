extern crate tonic_build;

fn main() -> Result<(), Box<dyn std::error::Error>> {
  tonic_build::compile_protos("src/api/v1/log.proto")?;

  Ok(())
}
