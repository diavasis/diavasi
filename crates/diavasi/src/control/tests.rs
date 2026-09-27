use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use tempfile::tempdir;
use tower::ServiceExt;

use crate::control::auth::BearerTokenAuth;
use crate::control::dto::{ConnectionCreateRequest, GroupCreateRequest};
use crate::control::routes::{AppState, router};
use crate::control::service::ControlService;
use crate::store::{RedbStore, StateStore, StoreKey};

fn test_state(token: &str) -> (tempfile::TempDir, AppState) {
    let dir = tempdir().unwrap();
    let store = Arc::new(RedbStore::create(dir.path().join("meta.redb")).unwrap());
    let key = StoreKey::generate();
    let service = Arc::new(ControlService::new(store, key, "127.0.0.1:0".to_string()));
    let state = AppState {
        service,
        auth: Arc::new(BearerTokenAuth::new(token.to_string())),
    };
    (dir, state)
}

async fn oneshot(state: AppState, req: Request<Body>) -> (StatusCode, bytes::Bytes) {
    let app = router(state);
    let response = app.oneshot(req).await.unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, body)
}

fn auth_json(method: &str, uri: &str, token: &str, body: Option<&str>) -> Request<Body> {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("authorization", format!("Bearer {token}"));
    if body.is_some() {
        builder = builder.header("content-type", "application/json");
    }
    builder
        .body(Body::from(body.unwrap_or("").to_string()))
        .unwrap()
}

#[tokio::test]
async fn health_no_auth() {
    let (_dir, state) = test_state("secret-token");
    let (status, body) = oneshot(
        state,
        Request::builder()
            .uri("/health")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(&body[..], b"ok");
}

#[tokio::test]
async fn ready_reads_the_store() {
    let (_dir, state) = test_state("secret-token");
    let (status, body) = oneshot(
        state,
        Request::builder()
            .uri("/ready")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(&body[..], b"ok");
}

#[tokio::test]
async fn auth_rejects_missing_token() {
    let (_dir, state) = test_state("secret-token");
    let (status, _) = oneshot(
        state,
        Request::builder()
            .uri("/v1/status")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn auth_rejects_wrong_token() {
    let (_dir, state) = test_state("secret-token");
    let (status, _) = oneshot(state, auth_json("GET", "/v1/status", "wrong", None)).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn connection_create_hides_secret() {
    let (_dir, state) = test_state("tok");
    let store = Arc::clone(state.service.store());
    let create = ConnectionCreateRequest {
        id: "pg1".into(),
        kind: "postgres".into(),
        config_json: serde_json::json!({"host": "localhost"}),
        secret: "hunter2-password".into(),
    };
    let body = serde_json::to_string(&create).unwrap();
    let (status, resp) = oneshot(
        state.clone(),
        auth_json("POST", "/v1/connections", "tok", Some(&body)),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let view: serde_json::Value = serde_json::from_slice(&resp).unwrap();
    let text = String::from_utf8(resp.to_vec()).unwrap();
    assert!(!text.contains("hunter2-password"));
    assert_eq!(view["secret_sealed"], true);
    assert!(view.get("secret").is_none());

    let (status, resp) = oneshot(
        state.clone(),
        auth_json("GET", "/v1/connections/pg1", "tok", None),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let show = String::from_utf8(resp.to_vec()).unwrap();
    assert!(!show.contains("hunter2-password"));

    let (status, resp) = oneshot(
        state.clone(),
        auth_json("GET", "/v1/connections", "tok", None),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let list = String::from_utf8(resp.to_vec()).unwrap();
    assert!(!list.contains("hunter2-password"));

    let rec = store.get_connection("pg1").unwrap().unwrap();
    assert_ne!(rec.sealed_secret.ciphertext, b"hunter2-password");
}

#[tokio::test]
async fn group_lifecycle_http() {
    let (_dir, state) = test_state("tok");
    let svc = Arc::clone(&state.service);

    let create = GroupCreateRequest {
        group_id: "g1".into(),
        total_records: 20,
        payload_size: 8,
        max_buffer_records: 64,
        max_buffer_bytes: 64 * 1024,
        batch_max_records: 5,
        batch_timeout_ms: 100,
        ordering_contract: "synthetic-u64".into(),
        connection_id: None,
        source_spec: None,
    };
    let body = serde_json::to_string(&create).unwrap();
    let (status, _) = oneshot(
        state.clone(),
        auth_json("POST", "/v1/groups", "tok", Some(&body)),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, resp) = oneshot(
        state.clone(),
        auth_json("POST", "/v1/groups/g1/start", "tok", None),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let view: serde_json::Value = serde_json::from_slice(&resp).unwrap();
    assert_eq!(view["running"], true);

    svc.advance_for_test("g1", "c1", 2).await.unwrap();

    let (status, resp) = oneshot(
        state.clone(),
        auth_json("GET", "/v1/groups/g1/checkpoint", "tok", None),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let cp: serde_json::Value = serde_json::from_slice(&resp).unwrap();
    assert!(
        !cp["durable_cursor"].is_null(),
        "durable cursor should advance after acks: {cp}"
    );

    let (status, resp) = oneshot(
        state.clone(),
        auth_json("GET", "/v1/groups/g1/consumers", "tok", None),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let consumers: serde_json::Value = serde_json::from_slice(&resp).unwrap();
    assert!(
        consumers["consumers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|c| c == "c1")
    );

    let (status, resp) = oneshot(
        state.clone(),
        auth_json("POST", "/v1/groups/g1/drain", "tok", None),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let view: serde_json::Value = serde_json::from_slice(&resp).unwrap();
    assert_eq!(view["running"], true);

    let (status, resp) = oneshot(
        state.clone(),
        auth_json("POST", "/v1/groups/g1/pause", "tok", None),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let view: serde_json::Value = serde_json::from_slice(&resp).unwrap();
    assert_eq!(view["running"], false);

    let (status, resp) = oneshot(
        state.clone(),
        auth_json("GET", "/v1/groups/g1/diagnostics", "tok", None),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let diag: serde_json::Value = serde_json::from_slice(&resp).unwrap();
    assert_eq!(diag["running"], false);
    assert_eq!(diag["last_stop_reason"], "paused");
    assert_eq!(diag["recovered"], false);
    assert!(diag["records_acked"].as_u64().unwrap() >= 1);

    let (status, resp) = oneshot(state.clone(), auth_json("GET", "/metrics", "tok", None)).await;
    assert_eq!(status, StatusCode::OK);
    let metrics = String::from_utf8(resp.to_vec()).unwrap();
    assert!(metrics.contains("diavasi_groups_running 0"));
    assert!(metrics.contains("diavasi_group_records_acked_total"));

    let (status, resp) = oneshot(
        state.clone(),
        auth_json("POST", "/v1/groups/g1/resume", "tok", None),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let view: serde_json::Value = serde_json::from_slice(&resp).unwrap();
    assert_eq!(view["running"], true);

    let (status, _) = oneshot(
        state.clone(),
        auth_json("POST", "/v1/groups/g1/pause", "tok", None),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = oneshot(
        state.clone(),
        auth_json("DELETE", "/v1/groups/g1", "tok", None),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn missing_routes_use_error_responses() {
    let (_dir, state) = test_state("tok");
    let (status, body) = oneshot(
        state.clone(),
        auth_json("GET", "/v1/connections/missing", "tok", None),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert!(text.contains("not found"));

    let (status, _) = oneshot(state.clone(), auth_json("GET", "/v1/status", "tok", None)).await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = oneshot(state.clone(), auth_json("GET", "/metrics", "tok", None)).await;
    assert_eq!(status, StatusCode::OK);
    let metrics = String::from_utf8(body.to_vec()).unwrap();
    assert!(metrics.contains("diavasi_groups_running"));
    let (status, _) = oneshot(state.clone(), auth_json("GET", "/v1/groups", "tok", None)).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = oneshot(
        state.clone(),
        auth_json("GET", "/v1/groups/missing", "tok", None),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = oneshot(
        state.clone(),
        auth_json("DELETE", "/v1/connections/missing", "tok", None),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = oneshot(
        state,
        auth_json("GET", "/v1/groups/missing/consumers", "tok", None),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn serve_writes_certs_and_rejects_a_taken_data_port() {
    use std::net::SocketAddr;

    use crate::control::{ServeConfig, serve};

    let dir = tempdir().unwrap();
    let err = serve(ServeConfig {
        bind: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        data_bind: "127.0.0.1:0".parse().unwrap(),
        store_path: dir.path().join("meta.redb"),
        api_token: "tok".into(),
        store_key: None,
        tls_cert: Some(dir.path().join("only-cert.pem")),
        tls_key: None,
        source_factory: None,
        checkpoint_interval: std::time::Duration::ZERO,
    })
    .await
    .unwrap_err();
    assert!(err.to_string().contains("together"));

    let hold = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let data_bind = hold.local_addr().unwrap();
    let control = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bind = control.local_addr().unwrap();
    drop(control);

    let store = dir.path().join("nested").join("meta.redb");
    let config = ServeConfig {
        bind,
        data_bind,
        store_path: store.clone(),
        api_token: "tok".into(),
        store_key: Some(StoreKey::generate()),
        tls_cert: None,
        tls_key: None,
        source_factory: None,
        checkpoint_interval: std::time::Duration::ZERO,
    };
    let err = serve(config.clone()).await.unwrap_err();
    assert!(store.exists(), "{err}");
    assert!(store.parent().unwrap().join("dataplane-ca.crt").exists());
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    let mut with_tls = config.clone();
    with_tls.tls_cert = Some(store.parent().unwrap().join("dataplane.crt"));
    with_tls.tls_key = Some(store.parent().unwrap().join("dataplane.key"));
    let err = serve(with_tls).await.unwrap_err();
    let _ = err;
    drop(hold);
}

// Regression tests for the v0.12.0 review. Each name carries its finding id.

use std::time::{Duration, Instant};

use futures::future::BoxFuture;

use crate::core::{GroupId, RecordSource, SyntheticSource};
use crate::runtime::{SourceFactory, SourceOpen};

/// Opens a small synthetic source after an optional delay.
struct TestFactory {
    open_delay: Duration,
}

impl SourceFactory for TestFactory {
    fn kind(&self) -> &str {
        "test"
    }

    fn open(
        &self,
        _request: SourceOpen,
    ) -> BoxFuture<'static, Result<Box<dyn RecordSource>, String>> {
        let delay = self.open_delay;
        Box::pin(async move {
            tokio::time::sleep(delay).await;
            Ok(Box::new(SyntheticSource::new(8, 8)) as Box<dyn RecordSource>)
        })
    }

    fn validate(&self, _request: SourceOpen) -> BoxFuture<'static, Result<(), String>> {
        Box::pin(async { Ok(()) })
    }
}

fn synthetic_group(id: &str, total: u64, buffer: usize, batch: usize) -> GroupCreateRequest {
    GroupCreateRequest {
        group_id: id.into(),
        total_records: total,
        payload_size: 8,
        max_buffer_records: buffer,
        max_buffer_bytes: 64 * 1024,
        batch_max_records: batch,
        batch_timeout_ms: 5_000,
        ordering_contract: "synthetic-u64".into(),
        connection_id: None,
        source_spec: None,
    }
}

async fn with_test_connection(state: &AppState, open_delay: Duration) {
    state
        .service
        .install_source_factory(Arc::new(TestFactory { open_delay }))
        .await;
    state
        .service
        .create_connection(ConnectionCreateRequest {
            id: "conn".into(),
            kind: "test".into(),
            config_json: serde_json::json!({}),
            secret: "unused".into(),
        })
        .unwrap();
}

fn adapter_group(id: &str) -> GroupCreateRequest {
    GroupCreateRequest {
        connection_id: Some("conn".into()),
        source_spec: Some(serde_json::json!({})),
        ..synthetic_group(id, 0, 8, 2)
    }
}

async fn view(state: &AppState, method: &str, uri: &str) -> (StatusCode, serde_json::Value) {
    let (status, body) = oneshot(state.clone(), auth_json(method, uri, "tok", None)).await;
    let value = serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null);
    (status, value)
}

/// B3: `lifecycle` and `running` agree after create, start, and pause.
#[tokio::test]
async fn regress_b03_lifecycle_matches_running() {
    let (_dir, state) = test_state("tok");
    state
        .service
        .create_group(synthetic_group("g1", 20, 8, 2))
        .await
        .unwrap();

    let (_, created) = view(&state, "GET", "/v1/groups/g1").await;
    assert_eq!(created["running"], false);
    assert_eq!(created["lifecycle"], "Stopped");

    let (status, started) = view(&state, "POST", "/v1/groups/g1/start").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(started["running"], true);
    assert_eq!(started["lifecycle"], "Running", "after start: {started}");

    let (status, paused) = view(&state, "POST", "/v1/groups/g1/pause").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(paused["running"], false);
    assert_eq!(paused["lifecycle"], "Stopped", "after pause: {paused}");

    let (_, listed) = view(&state, "GET", "/v1/groups").await;
    assert_eq!(
        listed[0]["lifecycle"], "Stopped",
        "list after pause: {listed}"
    );
    let (_, diag) = view(&state, "GET", "/v1/groups/g1/diagnostics").await;
    assert_eq!(
        diag["lifecycle"], "Stopped",
        "diagnostics after pause: {diag}"
    );
}

/// B2: drain delivers what the group already fetched, reads nothing new,
/// and stops the group when that work is acked.
#[tokio::test]
async fn regress_b02_drain_finishes_fetched_work_then_stops() {
    use crate::core::{ConsumerId, CoreError};
    use crate::runtime::RuntimeError;

    let (_dir, state) = test_state("tok");
    let svc = Arc::clone(&state.service);
    svc.create_group(synthetic_group("g1", 50, 4, 2))
        .await
        .unwrap();
    svc.start_group("g1").await.unwrap();
    let gid = GroupId::new("g1").unwrap();
    let handle = svc.supervisor().lock().await.get_handle(&gid).unwrap();
    let consumer = ConsumerId::new("c1").unwrap();
    handle.join(consumer.clone()).await.unwrap();

    let deadline = Instant::now() + Duration::from_secs(2);
    let fetched = loop {
        let snap = handle.live_snapshot().await.unwrap();
        if snap.buffer_records == 4 {
            break snap.fetched;
        }
        assert!(Instant::now() < deadline, "buffer never filled");
        tokio::time::sleep(Duration::from_millis(5)).await;
    };

    let (status, _) = view(&state, "POST", "/v1/groups/g1/drain").await;
    assert_eq!(status, StatusCode::OK);

    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        match handle.assign(&consumer).await {
            Ok(batch) => {
                let _ = handle.ack(batch.id).await;
            }
            Err(RuntimeError::Core(CoreError::NoWork)) => {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(_) => {}
        }
        let (_, group) = view(&state, "GET", "/v1/groups/g1").await;
        if group["running"] == false {
            break;
        }
        assert!(Instant::now() < deadline, "drained group is still running");
    }

    let (_, cp) = view(&state, "GET", "/v1/groups/g1/checkpoint").await;
    assert_eq!(
        cp["durable_cursor"],
        serde_json::to_value(&fetched).unwrap(),
        "drain read past the fetched position"
    );
    let (_, diag) = view(&state, "GET", "/v1/groups/g1/diagnostics").await;
    assert_eq!(diag["last_stop_reason"], "drained");
}

/// B10: a connection that a group still uses cannot be deleted.
#[tokio::test]
async fn regress_b10_connection_in_use_cannot_be_deleted() {
    let (_dir, state) = test_state("tok");
    with_test_connection(&state, Duration::ZERO).await;
    state
        .service
        .create_group(adapter_group("g1"))
        .await
        .unwrap();
    let (status, _) = view(&state, "DELETE", "/v1/connections/conn").await;
    assert_eq!(status, StatusCode::CONFLICT);
    let (status, _) = view(&state, "GET", "/v1/connections/conn").await;
    assert_eq!(status, StatusCode::OK);
}

/// B13: bad input is a 4xx, not a 500 or a success.
#[tokio::test]
async fn regress_b13_invalid_requests_are_client_errors() {
    let (_dir, state) = test_state("tok");
    let post = |body: GroupCreateRequest| {
        let state = state.clone();
        async move {
            let json = serde_json::to_string(&body).unwrap();
            oneshot(state, auth_json("POST", "/v1/groups", "tok", Some(&json)))
                .await
                .0
        }
    };

    let mut zero_bytes = synthetic_group("a", 8, 8, 2);
    zero_bytes.max_buffer_bytes = 0;
    assert_eq!(
        post(zero_bytes).await,
        StatusCode::BAD_REQUEST,
        "max_buffer_bytes 0"
    );

    let mut zero_timeout = synthetic_group("b", 8, 8, 2);
    zero_timeout.batch_timeout_ms = 0;
    assert_eq!(
        post(zero_timeout).await,
        StatusCode::BAD_REQUEST,
        "batch_timeout_ms 0"
    );

    let big_batch = synthetic_group("c", 8, 8, 64);
    assert_eq!(
        post(big_batch).await,
        StatusCode::BAD_REQUEST,
        "batch larger than the buffer"
    );

    assert_eq!(
        post(synthetic_group("bad/id", 8, 8, 2)).await,
        StatusCode::BAD_REQUEST,
        "slash in group id"
    );
    assert_eq!(
        post(synthetic_group(&"x".repeat(200), 8, 8, 2)).await,
        StatusCode::BAD_REQUEST,
        "200-byte group id"
    );

    let conn = serde_json::json!({
        "id": "bad id", "kind": "test", "config_json": {}, "secret": "s"
    })
    .to_string();
    let (status, _) = oneshot(
        state.clone(),
        auth_json("POST", "/v1/connections", "tok", Some(&conn)),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "space in connection id");

    state
        .service
        .create_group(synthetic_group("ok", 8, 8, 2))
        .await
        .unwrap();
    state.service.start_group("ok").await.unwrap();
    let (status, _) = view(&state, "POST", "/v1/groups/ok/drain").await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = view(&state, "POST", "/v1/groups/ok/drain").await;
    assert_eq!(status, StatusCode::CONFLICT, "second drain");
}

/// P3: a slow source open does not block unrelated control requests.
#[tokio::test]
async fn regress_p03_status_answers_while_a_group_is_starting() {
    let (_dir, state) = test_state("tok");
    with_test_connection(&state, Duration::from_secs(2)).await;
    state
        .service
        .create_group(adapter_group("slow"))
        .await
        .unwrap();
    let svc = Arc::clone(&state.service);
    let starting = tokio::spawn(async move { svc.start_group("slow").await });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let started = Instant::now();
    let _ = state.service.status().await;
    let _ = state.service.list_groups().await.unwrap();
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_millis(500),
        "status and list waited {elapsed:?} behind a starting group"
    );
    starting.await.unwrap().unwrap();
}

/// B6: TLS flags that point at missing files are an error. The server must
/// not generate certificates in their place.
#[tokio::test]
async fn regress_b06_missing_tls_files_are_an_error() {
    use crate::control::{ServeConfig, serve};

    let dir = tempdir().unwrap();
    let cert = dir.path().join("server.crt");
    let key = dir.path().join("server.key");
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        serve(ServeConfig {
            bind: "127.0.0.1:0".parse().unwrap(),
            data_bind: "127.0.0.1:0".parse().unwrap(),
            store_path: dir.path().join("meta.redb"),
            api_token: "tok".into(),
            store_key: Some(StoreKey::generate()),
            tls_cert: Some(cert.clone()),
            tls_key: Some(key.clone()),
            source_factory: None,
            checkpoint_interval: std::time::Duration::ZERO,
        }),
    )
    .await;
    assert!(
        matches!(result, Ok(Err(_))),
        "serve did not fail on missing TLS files"
    );
    assert!(
        !cert.exists(),
        "serve wrote a certificate at the supplied path"
    );
    assert!(!key.exists(), "serve wrote a key at the supplied path");
}

/// B11: a store that holds sealed secrets does not start with a key that
/// cannot open them.
#[tokio::test]
async fn regress_b11_store_with_secrets_rejects_a_wrong_key() {
    use crate::control::{ServeConfig, serve};
    use crate::store::{ConnectionRecord, seal_secret};

    let dir = tempdir().unwrap();
    let path = dir.path().join("meta.redb");
    let original = StoreKey::generate();
    {
        let store = RedbStore::create(&path).unwrap();
        store
            .put_connection(&ConnectionRecord {
                id: "pg".into(),
                kind: "postgres".into(),
                config_json: serde_json::json!({}),
                sealed_secret: seal_secret(&original, b"s3cret").unwrap(),
            })
            .unwrap();
    }

    for store_key in [Some(StoreKey::generate()), None] {
        let explicit = store_key.is_some();
        let result = tokio::time::timeout(
            Duration::from_secs(3),
            serve(ServeConfig {
                bind: "127.0.0.1:0".parse().unwrap(),
                data_bind: "127.0.0.1:0".parse().unwrap(),
                store_path: path.clone(),
                api_token: "tok".into(),
                store_key,
                tls_cert: None,
                tls_key: None,
                source_factory: None,
                checkpoint_interval: std::time::Duration::ZERO,
            }),
        )
        .await;
        assert!(
            matches!(result, Ok(Err(_))),
            "serve started with a key that cannot open the store's secrets (explicit key: {explicit})"
        );
    }
}

/// G1 and B3: shutdown saves progress and keeps the running lifecycle, and
/// the next start resumes the group.
#[tokio::test]
async fn regress_g01_shutdown_keeps_groups_and_boot_resumes_them() {
    use crate::control::{ServeConfig, serve_until};
    use crate::core::GroupLifecycle;

    let dir = tempdir().unwrap();
    let path = dir.path().join("meta.redb");
    let key = StoreKey::generate();
    let config = |bind: std::net::SocketAddr| ServeConfig {
        bind,
        data_bind: "127.0.0.1:0".parse().unwrap(),
        store_path: path.clone(),
        api_token: "tok".into(),
        store_key: Some(key.clone()),
        tls_cert: None,
        tls_key: None,
        source_factory: None,
        checkpoint_interval: std::time::Duration::ZERO,
    };
    let free_addr = || {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap()
    };
    let http = reqwest::Client::new();
    let call = |method: reqwest::Method, url: String, body: Option<serde_json::Value>| {
        let mut request = http.request(method, url).bearer_auth("tok");
        if let Some(body) = body {
            request = request.json(&body);
        }
        async move {
            let response = request.send().await.unwrap();
            let status = response.status();
            let value: serde_json::Value = response.json().await.unwrap_or_default();
            (status, value)
        }
    };
    let wait_up = |base: String| async move {
        let deadline = Instant::now() + Duration::from_secs(5);
        while reqwest::get(format!("{base}/health")).await.is_err() {
            assert!(Instant::now() < deadline, "server did not start");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };

    let addr = free_addr();
    let base = format!("http://{addr}");
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(serve_until(config(addr), async {
        let _ = stopped.await;
    }));
    wait_up(base.clone()).await;
    let create = serde_json::to_value(synthetic_group("g1", 20, 8, 2)).unwrap();
    let (status, _) = call(
        reqwest::Method::POST,
        format!("{base}/v1/groups"),
        Some(create),
    )
    .await;
    assert_eq!(status, 200);
    let (status, _) = call(
        reqwest::Method::POST,
        format!("{base}/v1/groups/g1/start"),
        None,
    )
    .await;
    assert_eq!(status, 200);
    stop.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("serve did not stop")
        .unwrap()
        .unwrap();

    {
        let store = RedbStore::open(&path).unwrap();
        let group = store
            .get_group(&GroupId::new("g1").unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(group.lifecycle, GroupLifecycle::Running);
    }

    let addr = free_addr();
    let base = format!("http://{addr}");
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(serve_until(config(addr), async {
        let _ = stopped.await;
    }));
    wait_up(base.clone()).await;
    let (_, group) = call(reqwest::Method::GET, format!("{base}/v1/groups/g1"), None).await;
    assert_eq!(group["running"], true, "group was not resumed: {group}");
    assert_eq!(group["lifecycle"], "Running");
    stop.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("serve did not stop")
        .unwrap()
        .unwrap();
}
