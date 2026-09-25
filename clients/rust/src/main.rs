use std::env;
use std::process::ExitCode;

use diavasi_client::{ClientError, Options, run};

fn main() -> ExitCode {
    let mut args = env::args().skip(1);
    let mut addr = None;
    let mut ca = None;
    let mut token = None;
    let mut group = None;
    let mut consumer = "rust".to_string();
    let mut total = None;
    let mut max_in_flight = 1u32;
    let mut halt_after = 0u32;
    while let Some(flag) = args.next() {
        let value = args.next();
        match flag.as_str() {
            "--addr" => addr = value,
            "--ca" => ca = value,
            "--token" => token = value,
            "--group" => group = value,
            "--consumer" => consumer = value.unwrap_or(consumer),
            "--total" => {
                total = value.and_then(|raw| raw.parse().ok());
            }
            "--max-in-flight" => {
                max_in_flight = value.and_then(|raw| raw.parse().ok()).unwrap_or(1);
            }
            "--halt-after" => {
                halt_after = value.and_then(|raw| raw.parse().ok()).unwrap_or(0);
            }
            other => {
                eprintln!("unknown argument {other}");
                return ExitCode::from(2);
            }
        }
    }
    let (Some(addr), Some(ca), Some(token), Some(group), Some(total)) =
        (addr, ca, token, group, total)
    else {
        eprintln!("usage: diavasi-consume --addr --ca --token --group --total");
        return ExitCode::from(2);
    };
    let pem = match std::fs::read(&ca) {
        Ok(pem) => pem,
        Err(err) => {
            eprintln!("read ca: {err}");
            return ExitCode::from(1);
        }
    };
    let mut opts = Options::new(addr, pem, token, group, consumer);
    opts.max_in_flight = max_in_flight;
    opts.expect_records = Some(total);
    if halt_after > 0 {
        opts.halt_after_acks = Some(halt_after);
    }
    match run(opts) {
        Ok(report) => {
            println!(
                "record_ids {}",
                report
                    .record_ids
                    .iter()
                    .map(|id| id.to_string())
                    .collect::<Vec<_>>()
                    .join(" ")
            );
            println!(
                "batch_ids {}",
                report
                    .batch_ids
                    .iter()
                    .map(|id| id.to_string())
                    .collect::<Vec<_>>()
                    .join(" ")
            );
            println!(
                "rust consumed {} records in {} batches",
                report.record_ids.len(),
                report.batch_ids.len()
            );
            ExitCode::SUCCESS
        }
        Err(ClientError::Protocol { code, message }) => {
            eprintln!("protocol error {code}: {message}");
            ExitCode::from(code.min(255) as u8)
        }
        Err(err) => {
            eprintln!("{err}");
            ExitCode::from(1)
        }
    }
}
