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
        auth: BearerTokenAuth::new(token.to_string()),
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
        batch_timeout_ms: 50,
        ordering_contract: "synthetic-u64".into(),
        connection_id: None,
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
    assert!(String::from_utf8(body.to_vec()).unwrap().contains("ok"));
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
