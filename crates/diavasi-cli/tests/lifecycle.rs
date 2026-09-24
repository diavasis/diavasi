//! End-to-end: spawn `diavasi serve` and drive lifecycle via the CLI binary.

use std::net::TcpListener;
use std::process::{Command, Stdio};
use std::time::Duration;

use tempfile::tempdir;

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn bin() -> String {
    env!("CARGO_BIN_EXE_diavasi").to_string()
}

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
    let port = free_port();
    let bind = format!("127.0.0.1:{port}");
    let url = format!("http://{bind}");
    let token = "test-token-stage4";
    let key = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    let mut child = Command::new(bin())
        .args([
            "serve",
            "--bind",
            &bind,
            "--store",
            store.to_str().unwrap(),
            "--token",
            token,
            "--store-key",
            key,
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn serve");

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

    let _ = child.kill();
    let _ = child.wait();
}
