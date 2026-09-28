use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

/// Decides whether a request's bearer token is accepted. The router calls it
/// for every route except `/health` and `/ready`.
///
/// Accept any of several tokens, for rotation:
///
/// ```
/// use std::sync::Arc;
/// use diavasi::control::AuthValidator;
///
/// struct TokenSet(Vec<String>);
///
/// impl AuthValidator for TokenSet {
///     fn validate_bearer(&self, token: Option<&str>) -> bool {
///         // A production check compares in constant time, as BearerTokenAuth does.
///         token.is_some_and(|t| self.0.iter().any(|k| k == t))
///     }
/// }
///
/// let auth: Arc<dyn AuthValidator> = Arc::new(TokenSet(vec!["old".into(), "new".into()]));
/// assert!(auth.validate_bearer(Some("new")));
/// assert!(!auth.validate_bearer(None));
/// ```
pub trait AuthValidator: Send + Sync + 'static {
    /// True when `token`, the text after `Bearer `, is accepted. `None` when the header is missing.
    fn validate_bearer(&self, token: Option<&str>) -> bool;
}

/// One shared token, compared in constant time.
#[derive(Clone)]
pub struct BearerTokenAuth {
    token: String,
}

impl BearerTokenAuth {
    /// Accept exactly `token`.
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into(),
        }
    }

    /// The accepted token.
    pub fn token(&self) -> &str {
        &self.token
    }
}

impl AuthValidator for BearerTokenAuth {
    fn validate_bearer(&self, token: Option<&str>) -> bool {
        match token {
            Some(t) => subtle_eq(t.as_bytes(), self.token.as_bytes()),
            None => false,
        }
    }
}

fn subtle_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Axum middleware: pass the request on when `Authorization: Bearer <token>`
/// satisfies `auth`, otherwise answer 401.
pub async fn require_bearer(
    axum::extract::State(auth): axum::extract::State<std::sync::Arc<dyn AuthValidator>>,
    req: Request<Body>,
    next: Next,
) -> Response {
    let header = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());
    let token = header.and_then(|h| h.strip_prefix("Bearer ").map(str::trim));
    if auth.validate_bearer(token) {
        next.run(req).await
    } else {
        (StatusCode::UNAUTHORIZED, "unauthorized").into_response()
    }
}
