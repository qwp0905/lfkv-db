use std::process::Command;

const COV_DIR: &str = "./coverage/";

fn remove_profraw() {
  // Clean up profraw files
  let paths = std::fs::read_dir(".").expect("failed to read directory.");
  for path in paths
    .map(|p| p.unwrap().path())
    .filter(|p| p.extension().map_or(false, |ext| ext == "profraw"))
  {
    std::fs::remove_file(path).expect("failed to remove profraw file");
  }
}

#[test]
#[ignore]
fn create_coverage() {
  std::env::set_var("RUSTFLAGS", "-Cinstrument-coverage");
  std::env::set_var("LLVM_PROFILE_FILE", "%p-%m.profraw");

  let status = Command::new("cargo")
    .arg("build")
    .status()
    .expect("failed to build.");
  if !status.success() {
    panic!("failed to build.");
  }

  let status = Command::new("cargo")
    .args(["test", "--", "--test-threads=5"])
    .status()
    .expect("failed to run tests.");
  if !status.success() {
    remove_profraw();
    panic!("failed to run tests.");
  }

  let _ = std::fs::remove_dir_all(COV_DIR);
  let status = Command::new("grcov")
    .args([
      ".",
      "-s",
      ".",
      "--keep-only",
      "src/**/*",
      "--binary-path",
      "./target/debug/",
      "-t",
      "html",
      "--branch",
      "--ignore-not-existing",
      "-o",
      COV_DIR,
    ])
    .status()
    .expect("failed to create coverage report.");

  if !status.success() {
    panic!("failed to create coverage report.");
  }

  println!("Coverage report generated.");
  remove_profraw();
}
