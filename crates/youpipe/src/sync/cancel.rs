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
}

impl Clone for CancellationToken {
    fn clone(&self) -> Self {
        Self {
            cancelled: self.cancelled.clone(),
        }
    }
}

/// Terminal classification of a cancellable run: distinguishes a fully
/// processed run from a cancelled one, whose output is otherwise silently
/// truncated — indistinguishable from a run over a shorter input.
///
/// Produced by the checked terminals:
/// [`StreamPipe::run_checked`](crate::StreamPipe::run_checked) /
/// [`GraphBuilder::run_checked`](crate::GraphBuilder::run_checked) and their
/// `try_` fallibles. `Out` is the terminal's own output: `Vec<O>` for the
/// stream engine's collecting terminal, `()` for the graph engine (its sinks
/// are user nodes with no collector to count — `emitted` stays `0` there;
/// count in the sink if the number matters).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunOutcome<Out> {
    /// The input drained to EOF under an unfired token.
    Completed(Out),
    /// The token fired mid-run. `partial` is the terminal's output over the
    /// items that had crossed the channels before the workers stopped;
    /// `emitted` counts those items (exactly `partial.len()` for the
    /// `Vec`-collecting terminal).
    Cancelled { partial: Out, emitted: usize },
}

impl<Out> RunOutcome<Out> {
    /// Whether the run was cancelled mid-flight.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        matches!(self, Self::Cancelled { .. })
    }

    /// The output either way: the full result of a completed run, or the
    /// partial result of a cancelled one.
    #[must_use]
    pub fn into_inner(self) -> Out {
        match self {
            Self::Completed(out) | Self::Cancelled { partial: out, .. } => out,
        }
    }

    /// Shared tail of every checked terminal: classify a drained run's
    /// output by ONE token read taken after the drain (the drain loops
    /// themselves never poll the token — zero hot-path cost). A worker
    /// panic cancels the token too, but both engines re-raise the panic
    /// before the terminal returns, so this read only sees user
    /// cancellation.
    pub(crate) fn classify(out: Out, emitted: usize, cancel: Option<&CancellationToken>) -> Self {
        if cancel.is_some_and(CancellationToken::is_cancelled) {
            Self::Cancelled {
                partial: out,
                emitted,
            }
        } else {
            Self::Completed(out)
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
}
