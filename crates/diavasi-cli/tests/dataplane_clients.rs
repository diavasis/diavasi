//! Python and Elixir compatibility clients against a local `diavasi serve`.

use std::net::TcpListener;
use std::path::PathBuf;
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

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap()
}

fn wait_healthy(url: &str) {
    let start = std::time::Instant::now();
    loop {
        if let Ok(resp) = reqwest::blocking::get(format!("{url}/health")) {
            if resp.status().is_success() {
                return;
            }
        }
        if start.elapsed() > Duration::from_secs(10) {
            panic!("server did not become healthy at {url}");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn cli(url: &str, token: &str, args: &[&str]) {
    let out = Command::new(bin())
        .arg("--url")
        .arg(url)
        .arg("--token")
        .arg(token)
        .args(args)
        .output()
        .unwrap();
    if !out.status.success() {
        panic!(
            "cli {args:?} failed\nstdout {}\nstderr {}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    }
}

#[test]
fn python_and_elixir_consume_synthetic_group() {
    let root = workspace_root();
    let python = root.join("clients/python/.venv/bin/python");
    let elixir_dir = root.join("clients/elixir");
    if !python.exists() {
        eprintln!("skip python client: venv missing");
    }
    if Command::new("mise").arg("--version").output().is_err() {
        eprintln!("skip elixir client: mise missing");
    }

    let dir = tempdir().unwrap();
    let store = dir.path().join("meta.redb");
    let control_port = free_port();
    let data_port = free_port();
    let bind = format!("127.0.0.1:{control_port}");
    let data_bind = format!("127.0.0.1:{data_port}");
    let url = format!("http://{bind}");
    let token = "stage5-token";
    let key = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    let mut child = Command::new(bin())
        .args([
            "serve",
            "--bind",
            &bind,
            "--data-bind",
            &data_bind,
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
        .unwrap();

    wait_healthy(&url);
    let ca = dir.path().join("dataplane-ca.crt");
    assert!(ca.exists(), "expected generated cert at {}", ca.display());

    if python.exists() {
        cli(
            &url,
            token,
            &[
                "group",
                "create",
                "--group-id",
                "py",
                "--total-records",
                "16",
                "--batch-max-records",
                "4",
            ],
        );
        cli(&url, token, &["group", "start", "py"]);
        let out = Command::new(&python)
            .arg("-m")
            .arg("diavasi_data")
            .args([
                "--addr",
                &data_bind,
                "--ca",
                ca.to_str().unwrap(),
                "--token",
                token,
                "--group",
                "py",
                "--consumer",
                "python",
                "--total",
                "16",
            ])
            .current_dir(root.join("clients/python"))
            .env("PYTHONPATH", root.join("clients/python"))
            .output()
            .unwrap();
        if !out.status.success() {
            let _ = child.kill();
            panic!(
                "python client failed\nstdout {}\nstderr {}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            );
        }
    }

    if Command::new("mise").arg("--version").output().is_ok() {
        cli(
            &url,
            token,
            &[
                "group",
                "create",
                "--group-id",
                "ex",
                "--total-records",
                "16",
                "--batch-max-records",
                "4",
            ],
        );
        cli(&url, token, &["group", "start", "ex"]);
        let out = Command::new("mise")
            .arg("exec")
            .arg("--")
            .arg("mix")
            .arg("diavasi.consume")
            .args([
                "--addr",
                &data_bind,
                "--ca",
                ca.to_str().unwrap(),
                "--token",
                token,
                "--group",
                "ex",
                "--consumer",
                "elixir",
                "--total",
                "16",
            ])
            .current_dir(&elixir_dir)
            .output()
            .unwrap();
        if !out.status.success() {
            let _ = child.kill();
            panic!(
                "elixir client failed\nstdout {}\nstderr {}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            );
        }
    }

    let _ = child.kill();
    let _ = child.wait();
}
