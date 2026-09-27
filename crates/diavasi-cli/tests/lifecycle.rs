//! End-to-end: spawn `diavasi serve` and drive lifecycle via the CLI binary.

mod common;

use common::{bin, spawn_serve};

use std::process::Command;
use std::time::Duration;

use tempfile::tempdir;

fn wait_healthy(url: &str, timeout: Duration) {
    let start = std::time::Instant::now();
    loop {
        if let Ok(resp) = reqwest::blocking::get(format!("{url}/health")) {
            if resp.status().is_success() {
                return;
            }
        }
        if start.elapsed() > timeout {
            panic!("server did not become healthy at {url}");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn cli(url: &str, token: &str, args: &[&str]) -> std::process::Output {
    let mut cmd = Command::new(bin());
    cmd.arg("--url")
        .arg(url)
        .arg("--token")
        .arg(token)
        .args(args);
    cmd.output().expect("run diavasi cli")
}

fn assert_ok(out: std::process::Output, label: &str) {
    if !out.status.success() {
        panic!(
            "{label} failed (status {:?})\nstdout:\n{}\nstderr:\n{}",
            out.status.code(),
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    }
}

#[test]
fn cli_lifecycle_against_serve() {
    let dir = tempdir().unwrap();
    let store = dir.path().join("meta.redb");
    let token = "test-token-stage4";
    let key = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    let server = spawn_serve(&store, token, key);
    let url = server.url.clone();
    wait_healthy(&url, Duration::from_secs(10));

    assert_ok(
        cli(
            &url,
            token,
            &[
                "connection",
                "add",
                "--id",
                "c1",
                "--kind",
                "synthetic",
                "--config-json",
                r#"{"host":"localhost"}"#,
                "--secret",
                "s3cret",
            ],
        ),
        "connection add",
    );
    assert_ok(cli(&url, token, &["connection", "list"]), "connection list");
    assert_ok(
        cli(&url, token, &["connection", "show", "c1"]),
        "connection show",
    );

    assert_ok(
        cli(
            &url,
            token,
            &[
                "group",
                "create",
                "--group-id",
                "g1",
                "--total-records",
                "50",
            ],
        ),
        "group create",
    );
    assert_ok(cli(&url, token, &["group", "start", "g1"]), "group start");
    assert_ok(cli(&url, token, &["status"]), "status");
    assert_ok(
        cli(&url, token, &["checkpoint", "show", "g1"]),
        "checkpoint show",
    );
    assert_ok(
        cli(&url, token, &["consumer", "list", "g1"]),
        "consumer list",
    );
    assert_ok(cli(&url, token, &["group", "drain", "g1"]), "group drain");
    assert_ok(cli(&url, token, &["group", "pause", "g1"]), "group pause");
    assert_ok(cli(&url, token, &["group", "resume", "g1"]), "group resume");
    assert_ok(cli(&url, token, &["group", "pause", "g1"]), "group pause 2");
    assert_ok(cli(&url, token, &["group", "delete", "g1"]), "group delete");
    assert_ok(
        cli(&url, token, &["connection", "delete", "c1"]),
        "connection delete",
    );

    drop(server);
}

/// G2 and G8: the secret comes from a flag, a file, stdin, or the
/// environment (only one flag at a time), and `store backup` writes a copy.
#[test]
fn cli_secret_sources_and_store_backup() {
    use std::io::Write;
    use std::process::Stdio;

    let dir = tempdir().unwrap();
    let store = dir.path().join("meta.redb");
    let token = "test-token-g2";
    let key = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    let server = spawn_serve(&store, token, key);
    let url = server.url.clone();
    wait_healthy(&url, Duration::from_secs(10));
    let add = |id: &str| {
        vec![
            "connection".to_string(),
            "add".into(),
            "--id".into(),
            id.into(),
            "--kind".into(),
            "synthetic".into(),
        ]
    };

    let secret_file = dir.path().join("secret");
    std::fs::write(&secret_file, "from-file\n").unwrap();
    let mut args = add("file");
    args.extend(["--secret-file".into(), secret_file.display().to_string()]);
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    assert_ok(cli(&url, token, &args), "secret file");

    let mut child = Command::new(bin())
        .args(["--url", &url, "--token", token])
        .args(add("stdin"))
        .arg("--secret-stdin")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(b"from-stdin\n")
        .unwrap();
    assert_ok(child.wait_with_output().unwrap(), "secret stdin");

    let out = Command::new(bin())
        .args(["--url", &url, "--token", token])
        .args(add("env"))
        .env("DIAVASI_CONNECTION_SECRET", "from-env")
        .output()
        .unwrap();
    assert_ok(out, "secret env");

    let out = Command::new(bin())
        .args(["--url", &url, "--token", token])
        .args(add("none"))
        .env_remove("DIAVASI_CONNECTION_SECRET")
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2), "no secret source");

    let mut args = add("both");
    args.extend([
        "--secret".into(),
        "x".into(),
        "--secret-file".into(),
        secret_file.display().to_string(),
    ]);
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    let out = cli(&url, token, &args);
    assert!(!out.status.success(), "two secret sources must be refused");

    let backup = dir.path().join("backup.redb");
    let backup_arg = backup.display().to_string();
    assert_ok(
        cli(&url, token, &["store", "backup", &backup_arg]),
        "store backup",
    );
    assert!(backup.exists());
    let again = cli(&url, token, &["store", "backup", &backup_arg]);
    assert!(
        !again.status.success(),
        "a second backup must not overwrite"
    );

    drop(server);
}
