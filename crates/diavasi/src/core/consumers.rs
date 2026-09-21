use std::collections::HashSet;

use super::error::{CoreError, CoreResult};
use super::ids::ConsumerId;

#[derive(Debug, Default)]
pub struct ConsumerRegistry {
    members: HashSet<ConsumerId>,
}

impl ConsumerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.members.len()
    }

    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    pub fn contains(&self, id: &ConsumerId) -> bool {
        self.members.contains(id)
    }

    pub fn join(&mut self, id: ConsumerId) -> CoreResult<()> {
        if !self.members.insert(id.clone()) {
            return Err(CoreError::DuplicateConsumer(id.to_string()));
        }
        Ok(())
    }

    pub fn leave(&mut self, id: &ConsumerId) -> CoreResult<()> {
        if !self.members.remove(id) {
            return Err(CoreError::UnknownConsumer(id.to_string()));
        }
        Ok(())
    }

    pub fn clear(&mut self) {
        self.members.clear();
    }
}
