//! Python and Elixir compatibility clients against a local `diavasi serve`.

mod common;

use common::{bin, spawn_serve};

use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use tempfile::tempdir;

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
    let sdk = std::env::var_os("DIAVASI_SDK_ROOT").map(PathBuf::from);
    let python_dir = sdk
        .as_ref()
        .map(|path| path.join("diavasi-python"))
        .filter(|path| path.is_dir())
        .unwrap_or_else(|| root.join("clients/python"));
    let python = python_dir.join(".venv/bin/python");
    let elixir_dir = sdk
        .as_ref()
        .map(|path| path.join("diavasi-elixir"))
        .filter(|path| path.is_dir())
        .unwrap_or_else(|| root.join("clients/elixir"));
    if !python.exists() {
        eprintln!("skip python client: venv missing");
    }
    if Command::new("mise").arg("--version").output().is_err() {
        eprintln!("skip elixir client: mise missing");
    }

    let dir = tempdir().unwrap();
    let store = dir.path().join("meta.redb");
    let token = "stage5-token";
    let key = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    let server = spawn_serve(&store, token, key);
    let url = server.url.clone();
    let data_bind = server.data_addr.clone();

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
            .current_dir(&python_dir)
            .env("PYTHONPATH", &python_dir)
            .output()
            .unwrap();
        if !out.status.success() {
            panic!(
                "python client failed\nstdout {}\nstderr {}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            );
        }
    }

    if elixir_dir.is_dir() && Command::new("mise").arg("--version").output().is_ok() {
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
            panic!(
                "elixir client failed\nstdout {}\nstderr {}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            );
        }
    }

    drop(server);
}
