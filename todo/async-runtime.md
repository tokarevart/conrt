# Custom io_uring Async Runtime Design

## Goals

- Spawn `Future`s directly (no per-future `Box` allocation)
- Hardcode future types as statically-known generics, stored in fixed-capacity slab
- Zero extra allocation for in-flight IO tracking (u64 bitmap embedded in Task)
- Fast completion dispatch via `user_data` encoding

---

## Runtime Storage

Single `static mut Option<Runtime>`. Single-threaded — no TLS, no contention.

```rust
static mut RUNTIME: Option<Runtime> = None;
```

Waker reads `RUNTIME` directly (unsafe, single-threaded, no data race). Caller `take`s the `Runtime` on teardown, drops it, can install a new one.

---

## Fixed-Capacity Slab

Pre-allocated at runtime, **never reallocated**. Guarantees pinning soundness (task addresses never move). The slab *is* the index allocator — slot position = task index.

```rust
struct Slab<T> {
    slots: Box<[MaybeUninit<T>]>,
    occupied: Box<[u64]>,   // one bit per slot
    free: Vec<u32>,         // recycled indices (O(1) free-list)
}
```

- Capacity chosen at construction (e.g. 256). Insertion when full = spawn error / backpressure.
- No incrementing counter, no versioning, no hashmap lookup. Capacity is far below `2^32`, so no wrap-around concern.
- All slots pinned in memory: `Box<[T]>` never grows or shrinks.

```rust
struct Task<F: Future> {
    index: u32,            // own slab index (written on insertion)
    in_flight: InFlightIOs,
    future: F,
}
```

**Insertion**: pop free list; if empty, scan `occupied` bitmap for a zero bit. Set bit. Return index.

**Removal**: clear the bit in `occupied`, push index to `free`. If the task has in-flight IOs, 
  submit `IORING_OP_ASYNC_CANCEL` for each outstanding IO slot before freeing any buffers. Do the same on drop.

**Access**: `&slots[index]` — O(1), direct, no hashing or version checking.

Key is embedded in the value — zero extra mapping overhead per task.

---

## In-Flight IO Tracking

Each task tracks its outstanding IOs compactly:

```rust
enum InFlightIOs {
    Bitmap(u64),              // ≤64 simultaneous IOs (common case)
    Overflow(Box<[u64; N]>),  // larger bitmap if >64 needed
}
```

- **Allocation**: `(!bitmap).trailing_zeros()` gives first free bit. Set it.
- **Free**: `bitmap &= !(1 << slot)`.
- **Overflow**: when bitmap is all ones, switch to heap-allocated `[u64; N]`. No downgrade back to bitmap (stability over churn).

---

## `user_data` Encoding

```
| 31                  0 | 31                  0 |
|     task_index        |     io_slot_index     |
```

- **Upper 32 bits**: task index (slab slot position).
- **Lower 32 bits**: bit position within the task's `InFlightIOs` bitmap.

On completion: extract `task_index = user_data >> 32`, `io_slot = user_data & 0xFFFF_FFFF`, clear the bit in the task's `InFlightIOs`, push task to ready queue.

---

## Waker / Context

The task index is passed to futures via `RawWaker::data`:

```rust
fn waker(task_index: u32) -> Waker {
    let ptr = core::ptr::without_provenance(task_index as usize);
    let raw = RawWaker::new(ptr, &WAKER_VTABLE);
    unsafe { Waker::from_raw(raw) }
}
```

Vtable:

- **`wake` / `wake_by_ref`**: read `data` as `u32`, look up task in `RUNTIME`'s slab, push to ready queue.
- **`clone`**: new `RawWaker` with same `data` (no ref count needed — indices are stable).
- **`drop`**: no-op (index is just a number, no allocation).

---

## Ready Queue

```rust
let ready: Vec<u32> = Vec::new();
```

Grow-only `Vec<u32>` of slab indices. Drained each iteration of the event loop.

---

## Event Loop

```
loop {
    for task_index in ready.drain(..) {
        let task = &mut slab[task_index];
        let poll = task.future.poll(cx);
        match poll {
            Poll::Ready(()) => slab.remove(task_index),
            Poll::Pending   => {},
        }
    }

    ring.submit_and_wait(1)?;

    for cqe in ring.completion() {
        let task_index = (cqe.user_data() >> 32) as u32;
        let io_slot   = (cqe.user_data() & 0xFFFF_FFFF) as u32;

        // clear io_slot in slab[task_index].in_flight
        // store result
        ready.push(task_index);
    }
}
```
