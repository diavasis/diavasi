//! Start `diavasi serve` for an integration test.

use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

/// A running `diavasi serve`. The process is killed when this is dropped.
pub struct Server {
    pub child: Child,
    /// Control-plane base URL, `http://127.0.0.1:<port>`.
    pub url: String,
    /// Data-plane address, `127.0.0.1:<port>`.
    #[allow(dead_code)] // used by some test binaries, not all
    pub data_addr: String,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// The `diavasi` executable under test.
pub fn bin() -> String {
    env!("CARGO_BIN_EXE_diavasi").to_string()
}

/// Start `diavasi serve` on ports the system picks and read them from its
/// `diavasi listening` log line, so no other process can take them first.
pub fn spawn_serve(store: &Path, token: &str, key: &str) -> Server {
    let mut child = Command::new(bin())
        .args([
            "serve",
            "--bind",
            "127.0.0.1:0",
            "--data-bind",
            "127.0.0.1:0",
            "--store",
            store.to_str().unwrap(),
            "--token",
            token,
            "--store-key",
            key,
        ])
        .env("RUST_LOG", "info")
        .env("DIAVASI_LOG_FORMAT", "text")
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn diavasi serve");
    let stdout = child.stdout.take().unwrap();
    let (found_tx, found_rx) = std::sync::mpsc::channel();
    // Read every line so the server never blocks on a full pipe, and report
    // the listening addresses once.
    std::thread::spawn(move || {
        let mut found_tx = Some(found_tx);
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if let Some(found) = parse_listening(&line) {
                if let Some(tx) = found_tx.take() {
                    let _ = tx.send(found);
                }
            }
        }
    });
    let (url, data_addr) = match found_rx.recv_timeout(Duration::from_secs(30)) {
        Ok(found) => found,
        Err(_) => {
            let _ = child.kill();
            panic!("diavasi serve did not report its addresses within 30 s");
        }
    };
    Server {
        child,
        url,
        data_addr,
    }
}

/// `control=http://127.0.0.1:1 data=127.0.0.1:2` from the listening line,
/// with any ANSI color codes removed first.
fn parse_listening(line: &str) -> Option<(String, String)> {
    let line = strip_ansi(line);
    if !line.contains("diavasi listening") {
        return None;
    }
    let field = |name: &str| {
        let start = line.find(&format!("{name}="))? + name.len() + 1;
        let rest = &line[start..];
        Some(
            rest.split_whitespace()
                .next()?
                .trim_matches('"')
                .to_string(),
        )
    };
    Some((field("control")?, field("data")?))
}

/// `text` without ANSI escape sequences (`ESC [ ... letter`).
fn strip_ansi(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            for next in chars.by_ref() {
                if next.is_ascii_alphabetic() {
                    break;
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}
