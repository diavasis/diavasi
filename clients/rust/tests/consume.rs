use diavasi_client::{ClientError, Options, run};

#[test]
fn consume_skips_without_server() {
    let Ok(addr) = std::env::var("DIAVASI_DATA_ADDR") else {
        return;
    };
    let Ok(ca) = std::env::var("DIAVASI_CA") else {
        return;
    };
    let Ok(token) = std::env::var("DIAVASI_API_TOKEN") else {
        return;
    };
    let group = std::env::var("DIAVASI_GROUP").unwrap_or_else(|_| "sdk".to_string());
    let total: u64 = std::env::var("DIAVASI_TOTAL")
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(8);
    let pem = std::fs::read(ca).expect("ca");
    let mut opts = Options::new(addr, pem, token, group, "rust-test");
    opts.expect_records = Some(total);
    let report = run(opts).expect("consume");
    assert_eq!(report.record_ids.len() as u64, total);
}

#[test]
fn missing_group_is_not_running() {
    let Ok(addr) = std::env::var("DIAVASI_DATA_ADDR") else {
        return;
    };
    let Ok(ca) = std::env::var("DIAVASI_CA") else {
        return;
    };
    let Ok(token) = std::env::var("DIAVASI_API_TOKEN") else {
        return;
    };
    let pem = std::fs::read(ca).expect("ca");
    let opts = Options::new(addr, pem, token, "sdk-missing", "rust-missing");
    match run(opts) {
        Err(ClientError::Protocol { code, .. }) => assert_eq!(code, 5),
        other => panic!("expected protocol error 5, got {other:?}"),
    }
}
