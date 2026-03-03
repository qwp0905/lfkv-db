use std::{
  io::{stderr, stdout},
  process::{Command, Stdio},
};

const COV_DIR: &str = "./coverage/";

fn remove_profraw() -> std::io::Result<()> {
  for path in std::fs::read_dir(".")? {
    let path = path?.path();
    if path.extension().map_or(true, |ext| ext != "profraw") {
      continue;
    }
    std::fs::remove_file(path)?;
  }
  Ok(())
}

#[test]
#[ignore]
fn create_coverage() -> std::io::Result<()> {
  std::env::set_var("RUSTFLAGS", "-C instrument-coverage");
  std::env::set_var("LLVM_PROFILE_FILE", "%p-%m.profraw");

  if !Command::new("cargo")
    .arg("build")
    .arg("--lib")
    .arg("--tests")
    .stdout(Stdio::null())
    .stderr(Stdio::null())
    .status()?
    .success()
  {
    panic!("build failed.")
  }

  if !Command::new("cargo")
    .arg("test")
    .arg("--tests")
    .arg("--")
    .arg("--test-threads=5")
    .stdout(stdout())
    .stderr(stderr())
    .status()?
    .success()
  {
    remove_profraw()?;
    panic!("failed to run tests.");
  }

  std::fs::remove_dir_all(COV_DIR)?;

  if !Command::new("grcov")
    .arg(".")
    .arg("-s")
    .arg(".")
    .arg("--keep-only")
    .arg("src/**/*")
    .arg("--binary-path")
    .arg("./target/debug/")
    .arg("-t")
    .arg("html")
    .arg("--branch")
    .arg("--ignore-not-existing")
    .arg("-o")
    .arg(COV_DIR)
    .status()?
    .success()
  {
    panic!("failed to create coverage report.");
  }

  remove_profraw()
}
