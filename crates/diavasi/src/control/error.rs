use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use thiserror::Error;

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

impl IntoResponse for ControlError {
    fn into_response(self) -> Response {
        let status = match &self {
            ControlError::BadRequest(_) => StatusCode::BAD_REQUEST,
            ControlError::Conflict(_) => StatusCode::CONFLICT,
            ControlError::NotFound(_) => StatusCode::NOT_FOUND,
            ControlError::Store(StoreError::GroupNotFound(_))
            | ControlError::Store(StoreError::ConnectionNotFound(_)) => StatusCode::NOT_FOUND,
            ControlError::Runtime(RuntimeError::GroupNotRunning(_)) => StatusCode::CONFLICT,
            ControlError::Runtime(RuntimeError::GroupAlreadyRunning(_)) => StatusCode::CONFLICT,
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
