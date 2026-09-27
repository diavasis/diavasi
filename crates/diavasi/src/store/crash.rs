use std::fmt;
use std::sync::Arc;

use super::error::{StoreError, StoreResult};

/// Points in the ack path where a [`CrashHook`] can simulate process death.
/// In order: the batch was handed out, the ack reached the engine, the new
/// cursor is computed, the write transaction starts, the transaction is about
/// to commit, it committed, the ack is answered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CrashPoint {
    /// After a batch is assigned. A restart delivers it again.
    AfterDeliver,
    /// After the engine applied the ack, before any write. The cursor is not durable.
    AfterAckApplied,
    /// Before the new group record is built.
    BeforeCheckpointCompute,
    /// Before the write transaction begins.
    BeforeTxnBegin,
    /// With the write staged, before it commits. A restart finds the old cursor.
    BeforeTxnCommit,
    /// After the commit. A restart finds the new cursor.
    AfterTxnCommit,
    /// After the ack is answered.
    AfterAckResponse,
}

impl fmt::Display for CrashPoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

/// What a [`CrashHook`] tells the ack path to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrashAction {
    /// Carry on.
    Continue,
    /// Stop here with [`StoreError::SimulatedCrash`].
    Abort,
}

/// A test hook called at each [`CrashPoint`]. Return [`CrashAction::Abort`]
/// to simulate process death there.
///
/// Abort once, just before the checkpoint transaction commits:
///
/// ```
/// use std::sync::Arc;
/// use std::sync::atomic::{AtomicBool, Ordering};
/// use diavasi::store::{CrashAction, CrashHook, CrashPoint};
///
/// let fired = Arc::new(AtomicBool::new(false));
/// let hook: CrashHook = {
///     let fired = Arc::clone(&fired);
///     Arc::new(move |point| {
///         if point == CrashPoint::BeforeTxnCommit && !fired.swap(true, Ordering::SeqCst) {
///             CrashAction::Abort
///         } else {
///             CrashAction::Continue
///         }
///     })
/// };
/// assert_eq!(hook(CrashPoint::BeforeTxnCommit), CrashAction::Abort);
/// assert_eq!(hook(CrashPoint::BeforeTxnCommit), CrashAction::Continue);
/// ```
pub type CrashHook = Arc<dyn Fn(CrashPoint) -> CrashAction + Send + Sync>;

/// The hook every group starts with: it never aborts.
pub fn no_crash() -> CrashHook {
    Arc::new(|_| CrashAction::Continue)
}

pub(crate) fn check_crash(hook: &CrashHook, point: CrashPoint) -> StoreResult<()> {
    match hook(point) {
        CrashAction::Continue => Ok(()),
        CrashAction::Abort => Err(StoreError::SimulatedCrash(point)),
    }
}
