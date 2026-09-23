use std::sync::Arc;

use axum::extract::{Path, State};
use axum::middleware;
use axum::routing::{get, post};
use axum::{Json, Router};
use tower_http::limit::RequestBodyLimitLayer;

use super::auth::{BearerTokenAuth, require_bearer};
use super::dto::{ConnectionCreateRequest, GroupCreateRequest};
use super::error::ControlResult;
use super::service::{ControlService, MAX_CONFIG_JSON_BYTES, MAX_SECRET_BYTES};

const BODY_LIMIT: usize = MAX_CONFIG_JSON_BYTES + MAX_SECRET_BYTES + 8 * 1024;

#[derive(Clone)]
pub struct AppState {
    pub service: Arc<ControlService>,
    pub auth: BearerTokenAuth,
}

pub fn router(state: AppState) -> Router {
    let authed = Router::new()
        .route("/v1/status", get(status))
        .route("/metrics", get(metrics))
        .route(
            "/v1/connections",
            get(list_connections).post(create_connection),
        )
        .route(
            "/v1/connections/{id}",
            get(get_connection).delete(delete_connection),
        )
        .route("/v1/groups", get(list_groups).post(create_group))
        .route("/v1/groups/{id}", get(get_group).delete(delete_group))
        .route("/v1/groups/{id}/start", post(start_group))
        .route("/v1/groups/{id}/pause", post(pause_group))
        .route("/v1/groups/{id}/resume", post(resume_group))
        .route("/v1/groups/{id}/drain", post(drain_group))
        .route("/v1/groups/{id}/consumers", get(list_consumers))
        .route("/v1/groups/{id}/checkpoint", get(checkpoint))
        .layer(middleware::from_fn_with_state(
            state.auth.clone(),
            require_bearer::<BearerTokenAuth>,
        ));

    Router::new()
        .route("/health", get(health))
        .merge(authed)
        .layer(RequestBodyLimitLayer::new(BODY_LIMIT))
        .with_state(state)
}

async fn health() -> &'static str {
    "ok"
}

async fn metrics() -> &'static str {
    "# diavasi metrics stub\nok 1\n"
}

async fn status(State(state): State<AppState>) -> Json<super::dto::StatusView> {
    Json(state.service.status().await)
}

async fn create_connection(
    State(state): State<AppState>,
    Json(req): Json<ConnectionCreateRequest>,
) -> ControlResult<Json<super::dto::ConnectionView>> {
    Ok(Json(state.service.create_connection(req)?))
}

async fn list_connections(
    State(state): State<AppState>,
) -> ControlResult<Json<Vec<super::dto::ConnectionView>>> {
    Ok(Json(state.service.list_connections()?))
}

async fn get_connection(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ControlResult<Json<super::dto::ConnectionView>> {
    Ok(Json(state.service.get_connection(&id)?))
}

async fn delete_connection(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ControlResult<()> {
    state.service.delete_connection(&id)
}

async fn create_group(
    State(state): State<AppState>,
    Json(req): Json<GroupCreateRequest>,
) -> ControlResult<Json<super::dto::GroupView>> {
    Ok(Json(state.service.create_group(req).await?))
}

async fn list_groups(
    State(state): State<AppState>,
) -> ControlResult<Json<Vec<super::dto::GroupView>>> {
    Ok(Json(state.service.list_groups().await?))
}

async fn get_group(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ControlResult<Json<super::dto::GroupView>> {
    Ok(Json(state.service.get_group(&id).await?))
}

async fn delete_group(State(state): State<AppState>, Path(id): Path<String>) -> ControlResult<()> {
    state.service.delete_group(&id).await
}

async fn start_group(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ControlResult<Json<super::dto::GroupView>> {
    Ok(Json(state.service.start_group(&id).await?))
}

async fn pause_group(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ControlResult<Json<super::dto::GroupView>> {
    Ok(Json(state.service.pause_group(&id).await?))
}

async fn resume_group(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ControlResult<Json<super::dto::GroupView>> {
    Ok(Json(state.service.resume_group(&id).await?))
}

async fn drain_group(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ControlResult<Json<super::dto::GroupView>> {
    Ok(Json(state.service.drain_group(&id).await?))
}

async fn list_consumers(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ControlResult<Json<super::dto::ConsumersView>> {
    Ok(Json(state.service.list_consumers(&id).await?))
}

async fn checkpoint(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> ControlResult<Json<super::dto::CheckpointView>> {
    Ok(Json(state.service.checkpoint(&id).await?))
}
