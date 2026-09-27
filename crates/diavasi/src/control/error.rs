use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use thiserror::Error;

use crate::core::CoreError;
use crate::runtime::RuntimeError;
use crate::store::StoreError;

use super::dto::ErrorBody;

/// Result of a control-plane operation.
pub type ControlResult<T> = Result<T, ControlError>;

/// A control-plane failure. It becomes an HTTP status and a body
/// `{"error": "<message>"}`: 400 for bad input, 404 for a missing group or
/// connection, 409 for a state conflict or duplicate id, 503 for a busy
/// group, 500 otherwise.
#[derive(Debug, Error)]
pub enum ControlError {
    /// The store failed or refused.
    #[error(transparent)]
    Store(#[from] StoreError),

    /// The runtime or engine refused.
    #[error(transparent)]
    Runtime(#[from] RuntimeError),

    /// Invalid input. HTTP 400.
    #[error("{0}")]
    BadRequest(String),

    /// The request conflicts with current state. HTTP 409.
    #[error("{0}")]
    Conflict(String),

    /// The group or connection does not exist. HTTP 404.
    #[error("{0}")]
    NotFound(String),

    /// An unexpected failure. HTTP 500.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::GroupLifecycle;

    fn status(err: ControlError) -> StatusCode {
        err.into_response().status()
    }

    /// Every engine error maps to a client status, never 500.
    #[test]
    fn core_errors_map_to_client_statuses() {
        let cases = [
            (
                CoreError::InvalidArgument("x".into()),
                StatusCode::BAD_REQUEST,
            ),
            (
                CoreError::InvalidTransition {
                    from: GroupLifecycle::Stopped,
                    to: GroupLifecycle::Draining,
                },
                StatusCode::CONFLICT,
            ),
            (
                CoreError::NotRunning(GroupLifecycle::Stopped),
                StatusCode::CONFLICT,
            ),
            (
                CoreError::DuplicateConsumer("c".into()),
                StatusCode::CONFLICT,
            ),
            (
                CoreError::UnknownConsumer("c".into()),
                StatusCode::NOT_FOUND,
            ),
            (CoreError::UnknownBatch(1), StatusCode::NOT_FOUND),
            (CoreError::BufferFull, StatusCode::CONFLICT),
            (CoreError::NoWork, StatusCode::CONFLICT),
        ];
        for (err, expected) in cases {
            assert_eq!(
                status(RuntimeError::Core(err.clone()).into()),
                expected,
                "{err}"
            );
            assert_eq!(
                status(StoreError::Core(err.clone()).into()),
                expected,
                "{err}"
            );
        }
    }

    #[test]
    fn service_errors_map_to_their_statuses() {
        assert_eq!(
            status(ControlError::BadRequest("x".into())),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            status(ControlError::Conflict("x".into())),
            StatusCode::CONFLICT
        );
        assert_eq!(
            status(ControlError::NotFound("x".into())),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            status(ControlError::Internal("x".into())),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            status(StoreError::GroupExists("g".into()).into()),
            StatusCode::CONFLICT
        );
        assert_eq!(
            status(StoreError::GroupNotFound("g".into()).into()),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            status(RuntimeError::ChannelFull.into()),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            status(RuntimeError::GroupNotRunning("g".into()).into()),
            StatusCode::CONFLICT
        );
    }
}
