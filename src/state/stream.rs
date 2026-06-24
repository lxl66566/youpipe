use std::sync::Arc;

use crate::{
    executor::compute::ComputePool,
    handoff::{
        SharedWaitGroup,
        channel::{Receiver, Sender},
    },
    state::ReorderBuffer,
    sync::CancellationToken,
};

pub struct StreamExecutor {
    pool: Arc<ComputePool>,
    cancel: Option<CancellationToken>,
}

impl StreamExecutor {
    #[must_use]
    pub fn new(pool_size: usize) -> Self {
        Self {
            pool: Arc::new(ComputePool::new(pool_size)),
            cancel: None,
        }
    }

    pub fn with_pool(pool: Arc<ComputePool>) -> Self {
        Self { pool, cancel: None }
    }

    #[must_use]
    pub fn with_cancel_token(mut self, token: CancellationToken) -> Self {
        self.cancel = Some(token);
        self
    }

    #[must_use]
    pub fn pool(&self) -> &Arc<ComputePool> {
        &self.pool
    }
}

pub trait StageExecutor<I, O>: Send + Sync + 'static {
    fn process(&self, item: I) -> O;
}

impl<I, O, F> StageExecutor<I, O> for F
where
    F: Fn(I) -> O + Send + Sync + 'static,
{
    fn process(&self, item: I) -> O {
        self(item)
    }
}

pub fn run_sync_stage<I, O, S>(
    stage: &S,
    input_rx: &Receiver<(u64, I)>,
    output_tx: &Sender<(u64, O)>,
    pool: &ComputePool,
    parallelism: usize,
) where
    I: Send + 'static,
    O: Send + 'static,
    S: StageExecutor<I, O> + Clone + 'static,
{
    let stage = Arc::new(stage.clone());
    let wg = SharedWaitGroup::new();
    wg.add(parallelism);

    for _ in 0..parallelism {
        let stage = stage.clone();
        let rx = input_rx.clone();
        let tx = output_tx.clone();
        let wg = wg.clone();
        pool.submit(move || {
            while let Ok((seq, item)) = rx.recv() {
                let output = stage.process(item);
                if tx.send((seq, output)).is_err() {
                    break;
                }
            }
            wg.done();
        });
    }

    wg.wait();
}

pub fn run_ordered_collect<O: Send + 'static>(
    input_rx: &Receiver<(u64, O)>,
    expected_items: usize,
) -> Vec<O> {
    let capacity = expected_items.next_power_of_two().clamp(1 << 10, 1 << 20);
    let mut buffer = ReorderBuffer::new(capacity);
    let mut results = Vec::with_capacity(expected_items);
    while let Ok((seq, item)) = input_rx.recv() {
        let ready = buffer.insert(seq, item);
        results.extend(ready);
    }
    results.extend(buffer.flush_remaining());
    results
}

pub fn run_unordered_collect<O: Send + 'static>(input_rx: &Receiver<(u64, O)>) -> Vec<O> {
    let mut results = Vec::new();
    while let Ok((_, item)) = input_rx.recv() {
        results.push(item);
    }
    results
}

pub fn feed_items<T: Send + 'static>(items: Vec<T>, tx: &Sender<(u64, T)>) {
    for (seq, item) in items.into_iter().enumerate() {
        if tx.send((seq as u64, item)).is_err() {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handoff::channel::channel;
    use crate::util::miri_pool_size;

    #[test]
    fn test_sync_stage_unordered() {
        let pool = Arc::new(ComputePool::new(miri_pool_size(4)));
        let (in_tx, in_rx) = channel::<(u64, i32)>(256);
        let (out_tx, out_rx) = channel::<(u64, i32)>(256);

        let handle = std::thread::spawn(move || {
            run_sync_stage(&|x: i32| x * 2, &in_rx, &out_tx, &pool, 4);
        });

        for i in 0..100 {
            in_tx.send((i as u64, i)).unwrap();
        }
        drop(in_tx);

        let mut results = run_unordered_collect(&out_rx);
        results.sort_unstable();
        let expected: Vec<i32> = (0..100).map(|x| x * 2).collect();
        assert_eq!(results, expected);
        handle.join().unwrap();
    }

    #[test]
    fn test_sync_stage_ordered() {
        let pool = Arc::new(ComputePool::new(miri_pool_size(4)));
        let (in_tx, in_rx) = channel::<(u64, i32)>(256);
        let (out_tx, out_rx) = channel::<(u64, i32)>(256);

        let handle = std::thread::spawn(move || {
            run_sync_stage(&|x: i32| x * 2, &in_rx, &out_tx, &pool, 4);
        });

        for i in 0..100 {
            in_tx.send((i as u64, i)).unwrap();
        }
        drop(in_tx);

        let results = run_ordered_collect(&out_rx, 100);
        let expected: Vec<i32> = (0..100).map(|x| x * 2).collect();
        assert_eq!(results, expected);
        handle.join().unwrap();
    }

    #[test]
    fn test_feed_items() {
        let (tx, rx) = channel::<(u64, i32)>(64);
        let items: Vec<i32> = (0..10).collect();
        feed_items(items, &tx);
        drop(tx);
        let mut results = Vec::new();
        while let Ok((seq, val)) = rx.recv() {
            results.push((seq, val));
        }
        assert_eq!(results.len(), 10);
        assert_eq!(results[0], (0, 0));
        assert_eq!(results[9], (9, 9));
    }
}
