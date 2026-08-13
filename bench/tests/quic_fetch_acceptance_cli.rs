#![cfg(unix)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Command, Output};

fn driver(arguments: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_quic-fetch-acceptance"))
        .args(arguments)
        .output()
        .expect("run acceptance driver")
}

fn git(repository: &Path, arguments: &[&str]) {
    let output = Command::new("git")
        .current_dir(repository)
        .args(arguments)
        .output()
        .expect("run fixture git");
    assert!(
        output.status.success(),
        "git {arguments:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn capture_source_process_prints_prepared_and_exits_zero_without_product_execution() {
    let temporary = tempfile::tempdir().expect("create CLI fixture");
    let root = fs::canonicalize(temporary.path()).expect("canonical fixture root");
    let repository = root.join("repository");
    fs::create_dir_all(repository.join("server/src")).expect("create fixture repository");
    fs::write(repository.join("Cargo.lock"), b"# fixture lock\n").expect("write Cargo.lock");
    fs::write(repository.join("server/src/quic.rs"), b"// fixture quic\n")
        .expect("write quic source");
    let task_spec = repository.join("task.md");
    fs::write(&task_spec, b"# accepted fixture task\n").expect("write task spec");
    git(&repository, &["init", "--quiet"]);
    git(&repository, &["config", "user.name", "Acceptance Test"]);
    git(
        &repository,
        &["config", "user.email", "acceptance@example.invalid"],
    );
    git(&repository, &["add", "."]);
    git(&repository, &["commit", "--quiet", "-m", "fixture"]);

    let capture = root.join("capture");
    let output = driver(&[
        "capture-source",
        "--repository",
        repository.to_str().expect("UTF-8 repository path"),
        "--output",
        capture.to_str().expect("UTF-8 capture path"),
        "--task-spec",
        task_spec.to_str().expect("UTF-8 task path"),
    ]);

    assert_eq!(output.status.code(), Some(0));
    let stdout = String::from_utf8(output.stdout).expect("UTF-8 stdout");
    assert!(stdout.starts_with("PREPARED evidence="), "{stdout}");
    assert!(stdout.contains("stage=capture-source"), "{stdout}");
    assert!(stdout.contains("product_invocations=0"), "{stdout}");
    assert!(output.stderr.is_empty());

    fs::set_permissions(&capture, fs::Permissions::from_mode(0o755))
        .expect("make capture removable");
}

#[test]
fn malformed_compose_input_process_prints_invalid_and_exits_one() {
    let temporary = tempfile::tempdir().expect("create CLI fixture");
    let malformed = temporary.path().join("compose-input.json");
    fs::write(
        &malformed,
        b"{\"schema\":\"distd.quic.fetch.compose-input.v3\",\"unknown\":true}\n",
    )
    .expect("write malformed compose input");
    let request = temporary.path().join("request.json");

    let output = driver(&[
        "compose-request",
        "--input",
        malformed.to_str().expect("UTF-8 input path"),
        "--output",
        request.to_str().expect("UTF-8 output path"),
    ]);

    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).expect("UTF-8 stderr");
    assert!(
        stderr.starts_with("INVALID evidence=none detail="),
        "{stderr}"
    );
    assert!(!request.exists());
}
