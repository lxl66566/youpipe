use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

/// A cooperative cancellation flag that can be cloned and shared across
/// threads.
pub struct CancellationToken {
    cancelled: Arc<AtomicBool>,
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

impl CancellationToken {
    /// Creates a new, non-cancelled token.
    #[must_use]
    pub fn new() -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Signals cancellation to all token clones.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }

    /// Returns `true` if cancellation has been requested.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
    }

    /// Clears the cancellation flag, allowing reuse.
    ///
    /// RMW (`swap`) rather than a plain store: a load that happens-after this
    /// reset is guaranteed to miss a `cancel()` that was overwritten by it,
    /// which a Relaxed store would not promise on the reuse path. Cold path —
    /// at most once per run — so the RMW cost is irrelevant.
    pub fn reset(&self) {
        self.cancelled.swap(false, Ordering::AcqRel);
    }
}

impl Clone for CancellationToken {
    fn clone(&self) -> Self {
        Self {
            cancelled: self.cancelled.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cancel_basic() {
        let token = CancellationToken::new();
        assert!(!token.is_cancelled());
        token.cancel();
        assert!(token.is_cancelled());
    }

    #[test]
    fn test_cancel_clone() {
        let t1 = CancellationToken::new();
        let t2 = t1.clone();
        t1.cancel();
        assert!(t2.is_cancelled());
    }

    #[test]
    fn test_cancel_reset() {
        let token = CancellationToken::new();
        token.cancel();
        assert!(token.is_cancelled());
        token.reset();
        assert!(!token.is_cancelled());
    }
}
