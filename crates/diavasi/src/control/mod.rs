//! HTTP control plane (Stage 4).

mod auth;
mod dto;
mod error;
mod routes;
mod server;
mod service;

#[cfg(test)]
mod tests;

pub use auth::{AuthValidator, BearerTokenAuth};
pub use dto::*;
pub use error::{ControlError, ControlResult};
pub use routes::router;
pub use server::{API_TOKEN_ENV, ServeConfig, serve, serve_until};
pub use service::ControlService;
