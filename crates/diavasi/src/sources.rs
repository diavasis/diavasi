use std::sync::Arc;

use diavasi::core::RecordSource;
use diavasi::runtime::{SourceFactory, SourceOpen};
use futures::future::BoxFuture;

/// Dispatches `open` and `validate` to the adapter whose kind matches the connection.
pub struct RoutingFactory {
    factories: Vec<Arc<dyn SourceFactory>>,
}

impl RoutingFactory {
    pub fn installed() -> Self {
        Self {
            factories: vec![
                Arc::new(diavasi_adapter_postgres::PostgresFactory),
                Arc::new(diavasi_adapter_mongodb::MongoFactory),
                Arc::new(diavasi_adapter_redis::RedisFactory),
                Arc::new(diavasi_adapter_scylla::ScyllaFactory),
            ],
        }
    }
}

impl SourceFactory for RoutingFactory {
    fn kind(&self) -> &str {
        "router"
    }

    fn supports(&self, kind: &str) -> bool {
        self.factories.iter().any(|factory| factory.supports(kind))
    }

    fn open(
        &self,
        request: SourceOpen,
    ) -> BoxFuture<'static, Result<Box<dyn RecordSource>, String>> {
        let kind = request.connection.kind.clone();
        match self
            .factories
            .iter()
            .find(|factory| factory.supports(&kind))
        {
            Some(factory) => factory.open(request),
            None => Box::pin(async move { Err(format!("unsupported connection kind {kind}")) }),
        }
    }

    fn validate(&self, request: SourceOpen) -> BoxFuture<'static, Result<(), String>> {
        let kind = request.connection.kind.clone();
        match self
            .factories
            .iter()
            .find(|factory| factory.supports(&kind))
        {
            Some(factory) => factory.validate(request),
            None => Box::pin(async move { Err(format!("unsupported connection kind {kind}")) }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(kind: &str) -> SourceOpen {
        SourceOpen {
            connection: diavasi::store::ConnectionRecord {
                id: "c".into(),
                kind: kind.into(),
                config_json: serde_json::json!({}),
                sealed_secret: diavasi::store::SealedSecret {
                    nonce: Vec::new(),
                    ciphertext: Vec::new(),
                },
            },
            source_spec: serde_json::json!({}),
            secret: Vec::new(),
        }
    }

    #[test]
    fn routes_postgres_mongodb_redis_and_scylla() {
        let factory = RoutingFactory::installed();
        for kind in ["postgres", "mongodb", "redis", "scylla"] {
            assert!(factory.supports(kind), "{kind}");
        }
        assert!(!factory.supports("kafka"));
    }

    /// Each kind reaches its own adapter: the adapter's spec check answers,
    /// not the router's "unsupported" error.
    #[tokio::test]
    async fn validate_reaches_the_adapter_for_each_kind() {
        let factory = RoutingFactory::installed();
        for kind in ["postgres", "mongodb", "redis", "scylla"] {
            let err = factory.validate(request(kind)).await.unwrap_err();
            assert!(
                !err.contains("unsupported connection kind"),
                "{kind}: {err}"
            );
        }
        let err = factory.validate(request("kafka")).await.unwrap_err();
        assert!(err.contains("unsupported connection kind kafka"), "{err}");
    }
}
