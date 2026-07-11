# Custom io_uring Async Runtime Design

## Goals

- Spawn `Future`s directly (no per-future `Box` allocation)
- Runtime has a single task future type; different behaviors are composed into one future (e.g. an enum dispatch)
- Zero extra allocation for in-flight IO tracking (u64 bitmap + `[i32; 64]` results embedded in Task)
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
struct Runtime {
    slab: Slab<Task>,
    ring: IoUring,
}

struct Slab<T> {
    slots: Box<[MaybeUninit<T>]>,
    occupied: Box<[u64]>,   // one bit per slot
    free: Vec<u32>,         // recycled indices (O(1) free-list)
}
```

- Capacity chosen at construction (e.g. 256). Insertion when full = spawn error / backpressure.
- All slots pinned in memory — `Box<[T]>` never grows or shrinks.

```rust
struct Task<F: Future> {
    in_flight: InFlightIOs,
    results: [i32; 64],          // completion results, indexed by bit position
    results_ready: u64,          // bitmap of which result slots are fresh
    future: F,
}
```

**Insertion**: pop free list; if empty, scan `occupied` bitmap for a zero bit. Set bit. The slot position *is* the task index — no need to store it in the task.

**Removal**: clear the bit in `occupied`, push index to `free`. If the task has in-flight IOs, submit `IORING_OP_ASYNC_CANCEL` for each outstanding IO slot before freeing any buffers. Do the same on drop.

**Access**: `&slots[index]` — O(1), direct, no hashing or version checking.

---

## In-Flight IO Tracking

```rust
enum InFlightIOs {
    Bitmap(u64),              // ≤64 simultaneous IOs (common case)
    Overflow(Box<[u64; N]>),  // larger bitmap if >64 needed
}
```

- **Allocation**: `(!bitmap).trailing_zeros()` gives first free bit. Set it.
- **Free**: `bitmap &= !(1 << slot)`.
- **Overflow**: when bitmap is all ones, switch to heap-allocated `[u64; N]`. No downgrade back to bitmap (stability over churn).

On completion: write `cqe.result()` to `results[io_slot]`, set bit in `results_ready`. On poll: the future reads `results[io_slot]` for each ready bit and clears it.

---

## `user_data` Encoding

A `#[repr(C)]` struct with two `u32` fields, transmuted to/from `u64`:

```rust
#[repr(C)]
struct IoUserData {
    task_index: u32,   // slab slot position
    io_slot: u32,      // bit position in InFlightIOs
}

impl From<IoUserData> for u64 {
    fn from(ud: IoUserData) -> u64 {
        unsafe { std::mem::transmute(ud) }
    }
}

impl From<u64> for IoUserData {
    fn from(raw: u64) -> Self {
        unsafe { std::mem::transmute(raw) }
    }
}
```

On completion: decode via `IoUserData::from(cqe.user_data())`, write `cqe.result()` into `slab[ud.task_index].results[ud.io_slot as usize]`, set `results_ready` bit, poll the task.

---

## Context

Tasks are never externally woken — they are polled only when an IO completion
fires for them. The waker uses the **noop vtable** but still encodes the task
index in its `data` pointer:

```rust
fn waker(task_index: u32) -> Waker {
    let ptr = core::ptr::without_provenance(task_index as usize);
    let raw = RawWaker::new(ptr, Waker::noop().vtable());
    unsafe { Waker::from_raw(raw) }
}

let w = waker(task_index);
let mut cx = Context::from_waker(&w);
```

---

## Event Loop

```rust
loop {
    ring.submit_and_wait(1)?;

    for cqe in ring.completion() {
        let ud = IoUserData::from(cqe.user_data());
        let task = &mut slab[ud.task_index];
        task.results[ud.io_slot as usize] = cqe.result();
        task.results_ready |= 1 << ud.io_slot;

        poll_one(ud.task_index);
    }
}
```

Each completion triggers the task poll immediately. No ready queue, no deferred
vec — the waker vtable is all noop, so tasks are only polled when their IO
completes.

```rust
fn poll_one(index: u32) {
    let task = &mut slab[index];
    let poll = task.future.poll(cx);
    match poll {
        Poll::Ready(()) => slab.remove(index),
        Poll::Pending   => {},
    }
}
```
