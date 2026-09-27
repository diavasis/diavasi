use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use thiserror::Error;

use crate::core::CoreError;
use crate::runtime::RuntimeError;
use crate::store::StoreError;

use super::dto::ErrorBody;

pub type ControlResult<T> = Result<T, ControlError>;

#[derive(Debug, Error)]
pub enum ControlError {
    #[error(transparent)]
    Store(#[from] StoreError),

    #[error(transparent)]
    Runtime(#[from] RuntimeError),

    #[error("{0}")]
    BadRequest(String),

    #[error("{0}")]
    Conflict(String),

    #[error("{0}")]
    NotFound(String),

    #[error("internal: {0}")]
    Internal(String),
}

/// HTTP status for an engine error: bad input is 400, a verb that does not
/// fit the group's state is 409.
fn core_status(err: &CoreError) -> StatusCode {
    match err {
        CoreError::InvalidArgument(_) => StatusCode::BAD_REQUEST,
        CoreError::InvalidTransition { .. }
        | CoreError::NotRunning(_)
        | CoreError::DuplicateConsumer(_) => StatusCode::CONFLICT,
        CoreError::UnknownConsumer(_) | CoreError::UnknownBatch(_) => StatusCode::NOT_FOUND,
        CoreError::BufferFull | CoreError::NoWork => StatusCode::CONFLICT,
    }
}

impl IntoResponse for ControlError {
    fn into_response(self) -> Response {
        let status = match &self {
            ControlError::BadRequest(_) => StatusCode::BAD_REQUEST,
            ControlError::Conflict(_) => StatusCode::CONFLICT,
            ControlError::NotFound(_) => StatusCode::NOT_FOUND,
            ControlError::Store(StoreError::GroupNotFound(_))
            | ControlError::Store(StoreError::ConnectionNotFound(_)) => StatusCode::NOT_FOUND,
            ControlError::Store(StoreError::GroupExists(_)) => StatusCode::CONFLICT,
            ControlError::Runtime(RuntimeError::GroupNotRunning(_)) => StatusCode::CONFLICT,
            ControlError::Runtime(RuntimeError::GroupAlreadyRunning(_)) => StatusCode::CONFLICT,
            ControlError::Runtime(RuntimeError::ChannelFull) => StatusCode::SERVICE_UNAVAILABLE,
            ControlError::Runtime(RuntimeError::Core(core))
            | ControlError::Store(StoreError::Core(core)) => core_status(core),
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (
            status,
            Json(ErrorBody {
                error: self.to_string(),
            }),
        )
            .into_response()
    }
}
