use std::fmt;
use std::sync::Arc;

use super::error::{StoreError, StoreResult};

/// Points where crash injection may abort durable ACK processing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CrashPoint {
    AfterDeliver,
    AfterAckApplied,
    BeforeCheckpointCompute,
    BeforeTxnBegin,
    BeforeTxnCommit,
    AfterTxnCommit,
    AfterAckResponse,
}

impl fmt::Display for CrashPoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrashAction {
    Continue,
    Abort,
}

/// Test hook: return [`CrashAction::Abort`] to simulate process death.
pub type CrashHook = Arc<dyn Fn(CrashPoint) -> CrashAction + Send + Sync>;

/// Default hook that never aborts.
pub fn no_crash() -> CrashHook {
    Arc::new(|_| CrashAction::Continue)
}

/// Convenience type alias documentation helper.
pub struct NoCrash;

impl NoCrash {
    pub fn hook() -> CrashHook {
        no_crash()
    }
}

pub(crate) fn check_crash(hook: &CrashHook, point: CrashPoint) -> StoreResult<()> {
    match hook(point) {
        CrashAction::Continue => Ok(()),
        CrashAction::Abort => Err(StoreError::SimulatedCrash(point)),
    }
}
