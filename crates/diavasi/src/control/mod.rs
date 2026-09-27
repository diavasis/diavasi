//! The HTTP control plane and the `serve` entry point.
//!
//! [`serve`] runs the control plane and the data plane together. [`router`]
//! builds the axum routes: `/health` and `/ready` without auth, `/metrics`
//! and `/v1/...` behind an [`AuthValidator`]. [`ControlService`] is the logic
//! behind the routes; the request and response bodies are the `*Request` and
//! `*View` types. `docs/api.md` lists every route.

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
