use serde::{Deserialize, Serialize};

use super::error::{CoreError, CoreResult};

/// Explicit group lifecycle. Illegal transitions are rejected.
///
/// `Stopped`: not running. `Running`: fetching and delivering. `Draining`:
/// delivering what is already fetched, reading nothing new; it becomes
/// `Stopped` when that work is acked. `Failed`: stopped by an error that a
/// restart would repeat. `Starting` and `Recovering` are transient states
/// inside [`GroupEngine::start`](super::GroupEngine::start) and
/// [`GroupEngine::recover_from`](super::GroupEngine::recover_from).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum GroupLifecycle {
    Stopped,
    Starting,
    Running,
    Draining,
    Failed,
    Recovering,
}

impl GroupLifecycle {
    pub fn transition_to(self, to: Self) -> CoreResult<Self> {
        let ok = matches!(
            (self, to),
            (Self::Stopped, Self::Starting)
                | (Self::Starting, Self::Running)
                | (Self::Running, Self::Draining)
                | (Self::Running, Self::Stopped)
                | (Self::Draining, Self::Stopped)
                | (Self::Running, Self::Failed)
                | (Self::Failed, Self::Recovering)
                | (Self::Recovering, Self::Running)
                | (Self::Recovering, Self::Failed)
                | (Self::Starting, Self::Failed)
                | (Self::Draining, Self::Failed)
        );
        if ok {
            Ok(to)
        } else {
            Err(CoreError::InvalidTransition { from: self, to })
        }
    }

    pub fn allows_dispatch(self) -> bool {
        matches!(self, Self::Running | Self::Draining)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn happy_path() {
        let s = GroupLifecycle::Stopped;
        let s = s.transition_to(GroupLifecycle::Starting).unwrap();
        let s = s.transition_to(GroupLifecycle::Running).unwrap();
        let s = s.transition_to(GroupLifecycle::Draining).unwrap();
        let s = s.transition_to(GroupLifecycle::Stopped).unwrap();
        assert_eq!(s, GroupLifecycle::Stopped);
    }

    #[test]
    fn rejects_illegal() {
        let err = GroupLifecycle::Stopped
            .transition_to(GroupLifecycle::Running)
            .unwrap_err();
        assert!(matches!(err, CoreError::InvalidTransition { .. }));
    }
}
