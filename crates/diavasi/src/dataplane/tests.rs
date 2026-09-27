use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use prost::Message;
use tempfile::tempdir;

use super::error_codes::{BAD_STATE, DUPLICATE_ACK, UNKNOWN_ACK};
use super::pb::envelope::Body;
use super::session::{Phase, Session};
use super::{
    ConsumerClient, ConsumerOptions, DataPlaneConfig, Envelope, PROTOCOL_VERSION, ack, hello,
    join_group, leave, nack, serve_dataplane,
};
use crate::control::{ControlService, GroupCreateRequest};
use crate::store::{RedbStore, StoreKey};

const FIXTURE_HELLO: &[u8] = &[0x08, 0x01, 0x12, 0x02, 0x08, 0x01];
const FIXTURE_JOIN: &[u8] = &[0x08, 0x01, 0x22, 0x06, 0x0a, 0x01, 0x67, 0x12, 0x01, 0x63];
const FIXTURE_ACK: &[u8] = &[0x08, 0x01, 0x3a, 0x02, 0x08, 0x07];
const FIXTURE_LEAVE: &[u8] = &[0x08, 0x01, 0x62, 0x00];
const FIXTURE_ERROR: &[u8] = &[
    0x08, 0x01, 0x5a, 0x07, 0x08, 0x02, 0x12, 0x03, 0x62, 0x61, 0x64,
];
const FIXTURE_BATCH: &[u8] = &[
    0x08, 0x01, 0x32, 0x09, 0x08, 0x01, 0x12, 0x05, 0x08, 0x09, 0x12, 0x01, 0x70,
];

#[test]
fn golden_fixtures_round_trip() {
    assert_eq!(hello(PROTOCOL_VERSION).encode_to_vec(), FIXTURE_HELLO);
    assert_eq!(join_group("g", "c").encode_to_vec(), FIXTURE_JOIN);
    assert_eq!(ack(7).encode_to_vec(), FIXTURE_ACK);
    assert_eq!(leave().encode_to_vec(), FIXTURE_LEAVE);
    assert_eq!(
        super::error_envelope(BAD_STATE, "bad").encode_to_vec(),
        FIXTURE_ERROR
    );
    assert_eq!(
        super::record_batch(super::pb::RecordBatch {
            batch_id: 1,
            records: vec![super::pb::Record {
                record_id: 9,
                payload: bytes::Bytes::from_static(b"p"),
            }],
        })
        .encode_to_vec(),
        FIXTURE_BATCH
    );
    for bytes in [
        FIXTURE_HELLO,
        FIXTURE_JOIN,
        FIXTURE_ACK,
        FIXTURE_LEAVE,
        FIXTURE_ERROR,
        FIXTURE_BATCH,
    ] {
        let decoded = Envelope::decode(bytes).unwrap();
        assert_eq!(decoded.version, PROTOCOL_VERSION);
        assert!(decoded.body.is_some());
    }
}

#[test]
fn session_requires_hello_then_join() {
    let mut session = Session::new();
    let step = session.on_frame(&ack(1));
    assert!(step.close);
    assert_eq!(error_code(&step.frames[0]), BAD_STATE);

    let mut session = Session::new();
    let step = session.on_frame(&hello(PROTOCOL_VERSION));
    assert!(!step.close);
    assert_eq!(session.phase(), Phase::ExpectJoin);
    let step = session.on_frame(&join_group("g1", "c1"));
    assert!(matches!(
        step.effects.first(),
        Some(super::Effect::Join { .. })
    ));
    assert_eq!(session.phase(), Phase::Active);
}

#[test]
fn session_rejects_duplicate_and_unknown_ack() {
    let mut session = active_session();
    session.note_assigned(3);
    let step = session.on_frame(&ack(9));
    assert!(step.close);
    assert_eq!(error_code(&step.frames[0]), UNKNOWN_ACK);

    let mut session = active_session();
    session.note_assigned(3);
    let step = session.on_frame(&ack(3));
    assert!(!step.close);
    let step = session.on_frame(&ack(3));
    assert!(step.close);
    assert_eq!(error_code(&step.frames[0]), DUPLICATE_ACK);
}

#[test]
fn session_flow_control_gates_assign_and_nack_is_rejected() {
    let mut session = active_session();
    assert!(session.can_assign());
    session.note_assigned(1);
    assert!(!session.can_assign());
    let step = session.on_frame(&super::flow_control(2));
    assert!(!step.close);
    assert!(session.can_assign());
    let step = session.on_frame(&nack(1));
    assert!(step.close);
}

#[test]
fn malformed_protobuf_does_not_panic() {
    let samples: &[&[u8]] = &[&[], &[0xff, 0xff, 0xff], &[0x08, 0x01, 0x12]];
    for bytes in samples {
        let _ = Envelope::decode(*bytes);
    }
}

proptest::proptest! {
    #[test]
    fn fuzz_envelope_decode(bytes in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..48)) {
        let _ = Envelope::decode(bytes.as_slice());
    }
}

fn active_session() -> Session {
    let mut session = Session::new();
    let _ = session.on_frame(&hello(PROTOCOL_VERSION));
    let _ = session.on_frame(&join_group("g", "c"));
    session
}

fn error_code(env: &Envelope) -> u32 {
    match &env.body {
        Some(Body::Error(err)) => err.code,
        other => panic!("expected error, got {other:?}"),
    }
}

struct Plane {
    addr: SocketAddr,
    ca_pem: Vec<u8>,
    svc: Arc<ControlService>,
    _dir: tempfile::TempDir,
}

async fn start_plane(total: u64, max_buffer: usize, batch: usize) -> Plane {
    start_plane_inner(
        total,
        max_buffer,
        batch,
        true,
        Duration::from_secs(30),
        Duration::from_secs(30),
    )
    .await
}

async fn start_plane_inner(
    total: u64,
    max_buffer: usize,
    batch: usize,
    start: bool,
    heartbeat_interval: Duration,
    heartbeat_timeout: Duration,
) -> Plane {
    let dir = tempdir().unwrap();
    let store = Arc::new(RedbStore::create(dir.path().join("meta.redb")).unwrap());
    let svc = Arc::new(ControlService::new(
        store,
        StoreKey::generate(),
        "127.0.0.1:0".to_string(),
    ));
    svc.create_group(GroupCreateRequest {
        group_id: "g1".into(),
        total_records: total,
        payload_size: 4,
        max_buffer_records: max_buffer,
        max_buffer_bytes: 1024 * 1024,
        batch_max_records: batch,
        batch_timeout_ms: 5_000,
        ordering_contract: "synthetic-u64".into(),
        connection_id: None,
        source_spec: None,
    })
    .await
    .unwrap();
    if start {
        svc.start_group("g1").await.unwrap();
    }

    let (ca, cert, key) = super::tls::generate_self_signed().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let supervisor = svc.supervisor();
    let cert_bytes = cert.clone().into_bytes();
    let key_bytes = key.into_bytes();
    tokio::spawn(async move {
        let _ = serve_dataplane(DataPlaneConfig {
            bind: addr,
            tls_cert_pem: cert_bytes,
            tls_key_pem: key_bytes,
            api_token: "tok".into(),
            supervisor,
            heartbeat_interval,
            heartbeat_timeout,
        })
        .await;
    });
    tokio::time::sleep(Duration::from_millis(80)).await;
    Plane {
        addr,
        ca_pem: ca.into_bytes(),
        svc,
        _dir: dir,
    }
}

fn client_opts(plane: &Plane, consumer: &str, inflight: u32) -> ConsumerOptions {
    ConsumerOptions {
        addr: plane.addr.to_string(),
        ca_pem: plane.ca_pem.clone(),
        token: "tok".into(),
        group_id: "g1".into(),
        consumer_id: consumer.into(),
        max_in_flight: inflight,
        stop_after_batches: None,
        expect_records: None,
        idle_after_join: None,
        leave_after_join: false,
        duplicate_first_ack: false,
        ack_delay: Duration::ZERO,
        shared_progress: None,
        timeout: Duration::from_secs(10),
    }
}

#[tokio::test]
async fn tls_client_consumes_and_acks() {
    let plane = start_plane(12, 32, 4).await;
    let mut opts = client_opts(&plane, "c1", 2);
    opts.expect_records = Some(12);
    let report = ConsumerClient::run(opts).await.unwrap();
    let unique: HashSet<_> = report.record_ids.iter().copied().collect();
    assert_eq!(unique.len(), 12);
    assert!(report.acked > 0);
}

#[tokio::test]
async fn missing_token_is_rejected() {
    let plane = start_plane(4, 16, 2).await;
    let mut opts = client_opts(&plane, "c1", 1);
    opts.token = "nope".into();
    opts.timeout = Duration::from_secs(3);
    let err = ConsumerClient::run(opts).await.unwrap_err();
    let text = err.to_string().to_ascii_lowercase();
    assert!(
        text.contains("unauthenticated") || text.contains("unauthorized"),
        "{text}"
    );
}

#[tokio::test]
async fn slow_client_keeps_buffer_inside_cap() {
    let plane = start_plane(40, 6, 2).await;
    let mut opts = client_opts(&plane, "slow", 1);
    opts.stop_after_batches = Some(1);
    opts.timeout = Duration::from_secs(5);
    let report = ConsumerClient::run(opts).await.unwrap();
    assert_eq!(report.batches, 1);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let gid = crate::core::GroupId::new("g1").unwrap();
    let handle = plane
        .svc
        .supervisor()
        .lock()
        .await
        .get_handle(&gid)
        .unwrap();
    let stats = handle.buffer_stats().await.unwrap();
    assert!(stats.buffer_len <= stats.max_buffer_records);
    assert!(stats.inflight_len <= 1);
}

#[tokio::test]
async fn abrupt_disconnect_requeues_for_second_consumer() {
    let plane = start_plane(20, 32, 5).await;
    let mut first = client_opts(&plane, "a", 1);
    first.stop_after_batches = Some(1);
    first.timeout = Duration::from_secs(5);
    let dropped = ConsumerClient::run(first).await.unwrap();
    assert_eq!(dropped.batches, 1);
    assert_eq!(dropped.acked, 0);
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut second = client_opts(&plane, "b", 4);
    second.expect_records = Some(20);
    let finished = ConsumerClient::run(second).await.unwrap();
    let unique: HashSet<_> = finished.record_ids.iter().copied().collect();
    assert_eq!(unique, (1..=20).collect());
}

#[test]
fn session_covers_rejected_sequences() {
    let mut session = Session::default();
    assert_eq!(session.max_in_flight(), super::DEFAULT_MAX_IN_FLIGHT);
    assert_eq!(session.outstanding_len(), 0);

    let bad_version = Envelope {
        version: 99,
        body: Some(super::pb::envelope::Body::Hello(super::pb::Hello {
            protocol_version: 1,
        })),
    };
    let step = session.on_frame(&bad_version);
    assert!(step.close);
    assert_eq!(session.phase(), Phase::Closed);
    let step = session.on_frame(&hello(PROTOCOL_VERSION));
    assert!(step.close);

    let mut session = Session::new();
    let empty = Envelope {
        version: PROTOCOL_VERSION,
        body: None,
    };
    assert!(session.on_frame(&empty).close);

    let mut session = Session::new();
    let bad_hello = super::pb::Envelope {
        version: PROTOCOL_VERSION,
        body: Some(super::pb::envelope::Body::Hello(super::pb::Hello {
            protocol_version: 9,
        })),
    };
    assert!(session.on_frame(&bad_hello).close);

    let mut session = Session::new();
    let _ = session.on_frame(&hello(PROTOCOL_VERSION));
    let empty_join = join_group("", "c");
    assert!(session.on_frame(&empty_join).close);

    let mut session = active_session();
    let step = session.on_frame(&super::heartbeat());
    assert!(!step.close);
    let step = session.on_frame(&leave());
    assert!(step.close);
    assert!(matches!(step.effects.first(), Some(super::Effect::Leave)));
    assert_eq!(session.on_frame(&super::flow_control(0)).frames.len(), 1);
    let mut session = active_session();
    assert!(session.on_frame(&super::flow_control(10_000)).close);
    let mut session = active_session();
    assert!(session.on_frame(&hello(PROTOCOL_VERSION)).close);
}

#[test]
fn tls_material_is_reused_when_present() {
    let dir = tempdir().unwrap();
    let cert = dir.path().join("dataplane.crt");
    let key = dir.path().join("dataplane.key");
    let (first_cert, first_key) = super::load_or_generate_pem(&cert, &key).unwrap();
    assert!(dir.path().join("dataplane-ca.crt").exists());
    let (second_cert, second_key) = super::load_or_generate_pem(&cert, &key).unwrap();
    assert_eq!(first_cert, second_cert);
    assert_eq!(first_key, second_key);
}

#[tokio::test]
async fn join_missing_group_and_duplicate_ack_are_errors() {
    let plane = start_plane_inner(
        8,
        16,
        2,
        false,
        Duration::from_secs(30),
        Duration::from_secs(30),
    )
    .await;
    let err = ConsumerClient::run(client_opts(&plane, "c1", 1))
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("not running") || err.contains("protocol error"),
        "{err}"
    );

    let plane = start_plane(8, 16, 2).await;
    let mut opts = client_opts(&plane, "dup", 1);
    opts.duplicate_first_ack = true;
    let err = ConsumerClient::run(opts).await.unwrap_err().to_string();
    assert!(err.contains("duplicate"), "{err}");
}

#[tokio::test]
async fn heartbeat_timeout_closes_idle_consumer() {
    let plane = start_plane_inner(
        8,
        16,
        2,
        true,
        Duration::from_millis(50),
        Duration::from_millis(200),
    )
    .await;
    let mut opts = client_opts(&plane, "idle", 1);
    opts.idle_after_join = Some(Duration::from_millis(800));
    opts.timeout = Duration::from_secs(3);
    let _ = ConsumerClient::run(opts).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let gid = crate::core::GroupId::new("g1").unwrap();
    let handle = plane
        .svc
        .supervisor()
        .lock()
        .await
        .get_handle(&gid)
        .unwrap();
    let consumers = handle.list_consumers().await.unwrap();
    assert!(consumers.is_empty(), "{consumers:?}");
}

#[tokio::test]
async fn leave_frame_removes_the_consumer() {
    let plane = start_plane(8, 16, 2).await;
    let mut opts = client_opts(&plane, "leaver", 1);
    opts.leave_after_join = true;
    ConsumerClient::run(opts).await.unwrap();
    let gid = crate::core::GroupId::new("g1").unwrap();
    let handle = plane
        .svc
        .supervisor()
        .lock()
        .await
        .get_handle(&gid)
        .unwrap();
    let consumers = handle.list_consumers().await.unwrap();
    assert!(consumers.is_empty(), "{consumers:?}");
}

#[tokio::test]
async fn authorization_must_use_a_bearer_token() {
    use super::pb::data_plane_client::DataPlaneClient;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;
    use tonic::Request;
    use tonic::metadata::MetadataValue;
    use tonic::transport::{Certificate, Channel, ClientTlsConfig};

    let plane = start_plane(4, 8, 2).await;
    let connect = |ca: Vec<u8>, addr: String| async move {
        let tls = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(ca))
            .domain_name("localhost");
        Channel::from_shared(format!("https://{addr}"))
            .unwrap()
            .tls_config(tls)
            .unwrap()
            .connect()
            .await
            .unwrap()
    };

    let mut client =
        DataPlaneClient::new(connect(plane.ca_pem.clone(), plane.addr.to_string()).await);
    let (tx, rx) = mpsc::channel(4);
    tx.send(hello(PROTOCOL_VERSION)).await.unwrap();
    let mut request = Request::new(ReceiverStream::new(rx));
    request.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from("Basic tok").unwrap(),
    );
    let err = client.consume(request).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
    drop(tx);

    let mut client =
        DataPlaneClient::new(connect(plane.ca_pem.clone(), plane.addr.to_string()).await);
    let (tx, rx) = mpsc::channel(4);
    tx.send(hello(PROTOCOL_VERSION)).await.unwrap();
    let request = Request::new(ReceiverStream::new(rx));
    let err = client.consume(request).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
    drop(tx);
}

// Regression tests for the v0.12.0 review. Each name carries its finding id.

/// B12: a client that reconnects with the same consumer id while its old
/// stream is still open takes over, and gets the old stream's unacked batch.
#[tokio::test]
async fn regress_b12_reconnect_with_the_same_consumer_id_takes_over() {
    let plane = start_plane(20, 32, 5).await;
    let mut stale = client_opts(&plane, "a", 1);
    stale.idle_after_join = Some(Duration::from_secs(5));
    stale.timeout = Duration::from_secs(8);
    let stale = tokio::spawn(ConsumerClient::run(stale));
    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut fresh = client_opts(&plane, "a", 4);
    fresh.expect_records = Some(20);
    fresh.timeout = Duration::from_secs(5);
    let report = ConsumerClient::run(fresh)
        .await
        .expect("reconnect with the same consumer id was rejected");
    let unique: HashSet<_> = report.record_ids.iter().copied().collect();
    assert_eq!(unique, (1..=20).collect());
    stale.abort();
}

/// B6: certificate and key files supplied by the operator are read as they
/// are, even when no `dataplane-ca.crt` sits next to them.
#[test]
fn regress_b06_supplied_tls_files_are_never_overwritten() {
    let dir = tempdir().unwrap();
    let (_ca, cert, key) = super::tls::generate_self_signed().unwrap();
    let cert_path = dir.path().join("operator.crt");
    let key_path = dir.path().join("operator.key");
    std::fs::write(&cert_path, &cert).unwrap();
    std::fs::write(&key_path, &key).unwrap();

    let (loaded_cert, loaded_key) = super::load_or_generate_pem(&cert_path, &key_path).unwrap();
    assert_eq!(std::fs::read_to_string(&cert_path).unwrap(), cert);
    assert_eq!(std::fs::read_to_string(&key_path).unwrap(), key);
    assert_eq!(loaded_cert, cert.into_bytes());
    assert_eq!(loaded_key, key.into_bytes());
}
