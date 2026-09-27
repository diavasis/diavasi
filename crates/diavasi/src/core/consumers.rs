use std::collections::HashSet;

use super::error::{CoreError, CoreResult};
use super::ids::ConsumerId;

/// The consumers joined to one group.
#[derive(Debug, Default)]
pub struct ConsumerRegistry {
    members: HashSet<ConsumerId>,
}

impl ConsumerRegistry {
    /// An empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of joined consumers.
    pub fn len(&self) -> usize {
        self.members.len()
    }

    /// True when no consumer is joined.
    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    /// True when `id` is joined.
    pub fn contains(&self, id: &ConsumerId) -> bool {
        self.members.contains(id)
    }

    /// Add a consumer. Fails with [`CoreError::DuplicateConsumer`] when it is already joined.
    pub fn join(&mut self, id: ConsumerId) -> CoreResult<()> {
        if !self.members.insert(id.clone()) {
            return Err(CoreError::DuplicateConsumer(id.to_string()));
        }
        Ok(())
    }

    /// Remove a consumer. Fails with [`CoreError::UnknownConsumer`] when it is not joined.
    pub fn leave(&mut self, id: &ConsumerId) -> CoreResult<()> {
        if !self.members.remove(id) {
            return Err(CoreError::UnknownConsumer(id.to_string()));
        }
        Ok(())
    }

    /// Remove every consumer.
    pub fn clear(&mut self) {
        self.members.clear();
    }

    /// The joined consumer ids, sorted.
    pub fn ids(&self) -> Vec<ConsumerId> {
        let mut ids: Vec<_> = self.members.iter().cloned().collect();
        ids.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        ids
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_leave_contains_and_len() {
        let mut reg = ConsumerRegistry::new();
        assert!(reg.is_empty());
        assert_eq!(reg.len(), 0);

        let c1 = ConsumerId::new("c1").unwrap();
        let c2 = ConsumerId::new("c2").unwrap();
        reg.join(c1.clone()).unwrap();
        reg.join(c2.clone()).unwrap();
        assert!(!reg.is_empty());
        assert_eq!(reg.len(), 2);
        assert!(reg.contains(&c1));
        assert!(reg.contains(&c2));

        reg.leave(&c1).unwrap();
        assert!(!reg.contains(&c1));
        assert_eq!(reg.len(), 1);

        reg.clear();
        assert!(reg.is_empty());
        assert!(!reg.contains(&c2));
    }

    #[test]
    fn duplicate_join_and_unknown_leave() {
        let mut reg = ConsumerRegistry::new();
        let c = ConsumerId::new("c1").unwrap();
        reg.join(c.clone()).unwrap();
        let err = reg.join(c.clone()).unwrap_err();
        assert!(matches!(err, CoreError::DuplicateConsumer(_)));

        let missing = ConsumerId::new("nope").unwrap();
        let err = reg.leave(&missing).unwrap_err();
        assert!(matches!(err, CoreError::UnknownConsumer(_)));
    }
}
