use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use crate::sync::sys::{Condvar, Mutex};

#[repr(C, align(64))]
struct CachePadded<T>(T);

pub struct EventCount {
    state: CachePadded<AtomicUsize>,
    lock: Mutex<()>,
    cvar: Condvar,
}

impl EventCount {
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: CachePadded(AtomicUsize::new(0)),
            lock: Mutex::new(()),
            cvar: Condvar::new(),
        }
    }

    pub fn notify(&self) {
        self.state.0.fetch_add(1, Ordering::Release);
        let _guard = self.lock.lock();
        self.cvar.notify_all();
    }

    pub fn notify_one(&self) {
        self.state.0.fetch_add(1, Ordering::Release);
        let _guard = self.lock.lock();
        self.cvar.notify_one();
    }

    pub fn wait(&self) {
        let key = self.state.0.load(Ordering::Acquire);
        self.wait_impl(key);
    }

    pub fn wait_timeout(&self, timeout: std::time::Duration) -> bool {
        let key = self.state.0.load(Ordering::Acquire);
        self.wait_timeout_impl(key, timeout)
    }

    fn wait_impl(&self, expected_key: usize) {
        let mut guard = self.lock.lock();
        while self.state.0.load(Ordering::Acquire) == expected_key {
            self.cvar.wait(&mut guard);
        }
    }

    fn wait_timeout_impl(&self, expected_key: usize, timeout: std::time::Duration) -> bool {
        let mut guard = self.lock.lock();
        let start = std::time::Instant::now();
        loop {
            if self.state.0.load(Ordering::Acquire) != expected_key {
                return true;
            }
            let remaining = timeout.saturating_sub(start.elapsed());
            if remaining.is_zero() {
                return self.state.0.load(Ordering::Acquire) != expected_key;
            }
            self.cvar.wait_for(&mut guard, remaining);
        }
    }
}

impl Default for EventCount {
    fn default() -> Self {
        Self::new()
    }
}

/// Clonable event counter for cross-thread wakeups.
pub struct SharedEventCount(Arc<EventCount>);

impl Default for SharedEventCount {
    fn default() -> Self {
        Self::new()
    }
}

impl SharedEventCount {
    #[must_use]
    pub fn new() -> Self {
        Self(Arc::new(EventCount::new()))
    }

    pub fn notify(&self) {
        self.0.notify();
    }

    pub fn notify_one(&self) {
        self.0.notify_one();
    }

    pub fn wait(&self) {
        self.0.wait();
    }

    #[must_use]
    pub fn wait_timeout(&self, timeout: std::time::Duration) -> bool {
        self.0.wait_timeout(timeout)
    }
}

impl Clone for SharedEventCount {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

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
    fn test_eventcount_basic() {
        let ec = Arc::new(EventCount::new());
        let ec2 = ec.clone();
        let h = thread::spawn(move || {
            thread::sleep(std::time::Duration::from_millis(10));
            ec2.notify();
        });
        ec.wait();
        h.join().unwrap();
    }

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

    #[test]
    fn test_eventcount_timeout() {
        let ec = EventCount::new();
        let notified = ec.wait_timeout(std::time::Duration::from_millis(1));
        assert!(!notified);
    }
}
