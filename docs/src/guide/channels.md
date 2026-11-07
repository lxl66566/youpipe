# Channels

The bounded channel that moves items between pipeline stages is also exposed as a standalone primitive: `channel`, `mpsc_channel` and `async_channel` build the same data plane without constructing a pool, a runtime or a pipeline. Use them wherever a queue library would otherwise be pulled in — worker hand-off, producer/consumer threads, async bridges — with the same throughput characteristics the engines get.

## Flavors

| Constructor | Endpoints | Shape |
| --- | --- | --- |
| `channel(cap)` | `Sender<T>` / `Receiver<T>`, both `Clone` | blocking MPMC |
| `mpsc_channel(cap)` | `MpscSender<T>` (`Clone`) / `MpscReceiver<T>` (not `Clone`) | blocking MPSC |
| `async_channel(cap)` | `AsyncSender<T>` / `AsyncReceiver<T>`, both `Clone` | async MPMC |

How to choose:

- Exactly one consumer → `mpsc_channel`. The single consumer is enforced by the type system (`MpscReceiver` is not `Clone`), and that is what pays for the cheaper recv path: an MPMC dequeue CASes the ring cursor once per item because consumers contend for it, while the MPSC ring replaces the CAS with a plain store. Under contention the MPMC CAS is ~20-40 % of per-item channel cost (mechanism in [the streaming data plane](../dev/streaming.md)).
- More than one consumer, or a consumer count that is not fixed up front → `channel`.
- The endpoint lives in an async task → `async_channel` (`send().await` / `recv().await`, `T: Unpin` on top of `Send + 'static`). Mixed-mode pairs — a blocking sender against an async receiver, and the MPSC twin of the async flavor — live one level deeper in `youpipe::handoff` (`sync_async_channel`, `mpsc_async_channel`).

## Semantics

Every flavor is bounded: capacity is allocated up front, a full `send` blocks (or awaits) until a slot frees, and an empty `recv` blocks until an item arrives — backpressure is inherent, no unbounded buffering exists.

There is no `close()` method; channels close by dropping endpoints:

- All senders dropped → the receiver drains every item already in the ring first (stragglers sent just before the last drop are still delivered), then `recv` reports `Err(ChannelError::Closed)`.
- All receivers dropped → `send` reports `Err(ChannelError::Closed)`.

The non-blocking variants return their verdict without parking: `try_send` returns `TrySendError::Full(item)` / `Closed(item)` — the item is handed back in both cases — and `try_recv` returns `TryRecvError::Empty` / `Closed`. `is_disconnected()` is the cheap open/closed poll: one load of the sender-count line, no ring walk.

## MPMC: several producers, drain to close

```rust
use youpipe::channel;

let (tx, rx) = channel::<u64>(64);
let handles: Vec<_> = (0..4)
    .map(|worker| {
        let tx = tx.clone(); // Sender is Clone: one handle per producer
        std::thread::spawn(move || {
            for i in 0..100 {
                tx.send(worker * 100 + i).unwrap(); // Err(Closed) only if the receiver is gone
            }
        })
    })
    .collect();
drop(tx); // the receiver must not count this handle as a live producer

let mut count = 0;
while rx.recv().is_ok() {
    count += 1; // the loop ends on Err(Closed): senders gone AND ring drained
}
assert_eq!(count, 400);
for h in handles {
    h.join().unwrap();
}
```

## MPSC: one collector, cloned senders

```rust
use youpipe::mpsc_channel;

let (tx, rx) = mpsc_channel::<String>(8);
let producers: Vec<_> = (0..2)
    .map(|worker| {
        let tx = tx.clone();
        std::thread::spawn(move || {
            for i in 0..50 {
                tx.send(format!("{worker}-{i}")).unwrap();
            }
        })
    })
    .collect();
drop(tx);

// rx is the only receiver handle that will ever exist — the recv path is CAS-free.
let mut lines = Vec::new();
while let Ok(s) = rx.recv() {
    lines.push(s);
}
assert_eq!(lines.len(), 100);
for h in producers {
    h.join().unwrap();
}
```

## Async: await both ends

```rust
use youpipe::async_channel;
use futures::executor::block_on;

let (tx, rx) = async_channel::<u32>(16);

let producer = async move {
    for i in 0..10 {
        tx.send(i).await.unwrap();
    }
}; // tx dropped here -> the consumer sees EOF after draining

let consumer = async {
    let mut sum = 0;
    while let Ok(i) = rx.recv().await {
        sum += i;
    }
    sum
};

// Any executor works: the endpoints are runtime-agnostic futures.
let ((), sum) = block_on(futures::future::join(producer, consumer));
assert_eq!(sum, 45);
```

## Batch operations

Every sender exposes `try_send_batch` and every receiver `try_recv_batch` plus `is_disconnected`: they amortize the ring's per-item cursor updates over a run of slots (one CAS per claimed run instead of per item). The send-side batch is `unsafe` over a `MaybeUninit` buffer — the init/move-out contract is in the rustdoc `# Safety` section — and standalone users can ignore the family unless a profile pins per-item ring cost as the bottleneck. Mechanism and measured calibers: [streaming data plane](../dev/streaming.md).

## Relation to the pipeline engines

The standalone channels construct no pool and no scheduler: `channel` and `mpsc_channel` work between plain OS threads, `async_channel` works under any executor. Internally the engines run over these same wrappers — `stream()` builds its stage-to-stage rings through the same factories (`with_buffer_size` picks their capacity) and `graph()` edges ride the same data plane — so a standalone channel measures and behaves like the engine internals. Work stealing, claim windows and every other scheduling concern are a separate layer that only exists inside pipelines; see [choosing the right engine](../advanced/choosing-engine.md) before wiring a pipeline around channels by hand.
