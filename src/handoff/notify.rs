use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use crate::sync::sys::{Condvar, Mutex};

pub struct WaitGroup {
    count: AtomicUsize,
    lock: Mutex<()>,
    cvar: Condvar,
}

impl WaitGroup {
    #[must_use]
    pub fn new() -> Self {
        Self {
            count: AtomicUsize::new(0),
            lock: Mutex::new(()),
            cvar: Condvar::new(),
        }
    }

    pub fn add(&self, n: usize) {
        self.count.fetch_add(n, Ordering::AcqRel);
    }

    pub fn done(&self) {
        let prev = self.count.fetch_sub(1, Ordering::AcqRel);
        // Fail loudly on an unbalanced add/done: without this, an extra
        // `done()` wraps the counter to usize::MAX and `wait()` blocks forever
        // with no indication. A sync primitive must not silently corrupt its
        // state — surface the bug at the call site instead.
        assert!(prev > 0, "WaitGroup::done() called more times than add()");
        if prev == 1 {
            let _guard = self.lock.lock();
            self.cvar.notify_all();
        }
    }

    pub fn wait(&self) {
        let mut guard = self.lock.lock();
        while self.count.load(Ordering::Acquire) > 0 {
            self.cvar.wait(&mut guard);
        }
    }
}

impl Default for WaitGroup {
    fn default() -> Self {
        Self::new()
    }
}

/// Clonable wait-group: `add(n)` then `done()` n times, `wait()` blocks until
/// zero.
pub struct SharedWaitGroup(Arc<WaitGroup>);

impl Default for SharedWaitGroup {
    fn default() -> Self {
        Self::new()
    }
}

impl SharedWaitGroup {
    #[must_use]
    pub fn new() -> Self {
        Self(Arc::new(WaitGroup::new()))
    }

    pub fn add(&self, n: usize) {
        self.0.add(n);
    }

    pub fn done(&self) {
        self.0.done();
    }

    pub fn wait(&self) {
        self.0.wait();
    }
}

impl Clone for SharedWaitGroup {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, thread};

    use super::*;

    #[test]
    fn test_waitgroup_basic() {
        let wg = Arc::new(WaitGroup::new());
        let wg1 = wg.clone();
        let wg2 = wg.clone();
        wg.add(2);
        let h1 = thread::spawn(move || {
            thread::sleep(std::time::Duration::from_millis(10));
            wg1.done();
        });
        let h2 = thread::spawn(move || {
            thread::sleep(std::time::Duration::from_millis(10));
            wg2.done();
        });
        wg.wait();
        h1.join().unwrap();
        h2.join().unwrap();
    }
}
