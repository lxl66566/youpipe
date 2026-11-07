//! A worker of one pool driving a *different* pool — the cross-registry face
//! of the scheduler's TLS identity check (`Registry::current_worker_of`).
//!
//! A worker of A calling B's terminals must resolve as off-pool for B
//! (inject into B + block), never as B's on-pool fast path. A regression
//! mis-routes B's `Stealing` `CountLatch` owner to A's worker index inside
//! B's sleep state (wrong-worker wake at best, an out-of-bounds panic when
//! A is the larger pool) or deadlocks the nested terminal. Pools are sized
//! asymmetrically (A > B) so an index leak from A into B goes out of bounds
//! for at least the workers with index ≥ B's size. The protocol-level pin
//! (identity + injected flag) lives in `registry.rs`'s unit tests; this file
//! guards the public-API surface end to end.

use std::{sync::mpsc, time::Duration};

use youpipe::{ComputePool, pipe_ref};

#[test]
fn worker_of_pool_a_drives_pool_b() {
    let pool_a = ComputePool::new(4);
    let pool_b = ComputePool::new(2);
    let (tx, rx) = mpsc::channel();

    let size = if cfg!(miri) {
        10_000
    } else {
        100_000
    };
    let data: Vec<u64> = (0..size).collect();
    let expected: Vec<u64> = data.iter().map(|&x| x * 2).collect();
    let b = pool_b.clone();
    pool_a.submit(move || {
        // `join` — the `in_worker` surface.
        let (x, y) = b.join(|| 1u32, || 2u32);
        // A fused terminal in the chunked regime (`chunk_splits > 0` at this
        // size): its hybrid driver waits on a `Blocking` CountLatch
        // precisely because the caller is not a worker of `b`.
        let doubled: Vec<u64> = pipe_ref(&data)
            .map(|&x| x * 2)
            .with_compute_pool(b.clone())
            .collect();
        assert_eq!(doubled, expected);
        tx.send(x + y).unwrap();
    });

    let sum = rx
        .recv_timeout(Duration::from_secs(60))
        .expect("cross-registry join/terminal hung");
    assert_eq!(sum, 3);
}
