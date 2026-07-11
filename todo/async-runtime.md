# Custom io_uring Async Runtime Design

## Goals

- Spawn `Future`s directly (no per-future `Box` allocation)
- Hardcode future types as statically-known generics, each in its own fixed-capacity slab
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

## Multiple Slabs (one per task type)

Each compile-time known future type gets its own slab. The runtime holds all slabs
as named fields. Slab ID is a small integer literal (`0`, `1`, `2`, ...) chosen at
compile time for each type.

```rust
struct Runtime {
    slab_a: Slab<TaskA>,   // slab_id = 0
    slab_b: Slab<TaskB>,   // slab_id = 1
    slab_c: Slab<TaskC>,   // slab_id = 2
    ring: IoUring,
}
```

## Fixed-Capacity Slab

Pre-allocated at runtime, **never reallocated**. Guarantees pinning soundness (task addresses never move). The slab *is* the index allocator — slot position = task index within that slab.

```rust
struct Slab<T> {
    slots: Box<[MaybeUninit<T>]>,
    occupied: Box<[u64]>,   // one bit per slot
    free: Vec<u32>,         // recycled indices (O(1) free-list)
}
```

- Capacity chosen at construction (e.g. 256 per slab). Insertion when full = spawn error / backpressure.
- All slots pinned in memory: `Box<[T]>` never grows or shrinks.

```rust
struct Task<F: Future> {
    index: u32,                  // own slab index
    slab_id: u8,                 // which slab (compile-time constant)
    in_flight: InFlightIOs,
    results: [i32; 64],          // completion results, indexed by bit position
    results_ready: u64,          // bitmap of which result slots are fresh
    future: F,
}
```

**Insertion**: pop free list; if empty, scan `occupied` bitmap for a zero bit. Set bit. Write `index` and `slab_id` into the task.

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

The 64-bit `user_data` encodes three fields:

```
| 63       56 | 55       24 | 23        0 |
|   slab_id   | task_index  |   io_slot   |
```

- **slab_id** (8 bits): which slab (up to 256 generic task types).
- **task_index** (32 bits): index within that slab's slot array.
- **io_slot** (24 bits): bit position within the task's `InFlightIOs`. The InFlightIOs overflow handling will assert `io_slot < 2^24` before heap-allocating (should never fire in practice).

```rust
const fn encode_user_data(slab_id: u8, task_index: u32, io_slot: u32) -> u64 {
    (slab_id as u64) << 56
        | (task_index as u64) << 24
        | io_slot as u64
}
```

On completion: extract all three, write `cqe.result()` into
`slabs[slab_id][task_index].results[io_slot]`, set `results_ready` bit, push
`(slab_id << 32 | task_index)` to the ready queue.

---

## Waker / Context

The waker encodes both slab_id and task_index in `RawWaker::data`:

```rust
fn waker(slab_id: u32, task_index: u32) -> Waker {
    let encoded = ((slab_id as usize) << 32) | task_index as usize;
    let ptr = core::ptr::without_provenance(encoded);
    let raw = RawWaker::new(ptr, &WAKER_VTABLE);
    unsafe { Waker::from_raw(raw) }
}
```

Vtable:

- **`wake` / `wake_by_ref`**: read `data`, extract `slab_id` and `task_index`, push `(slab_id << 32 | task_index)` to `deferred` (the local vec in the event loop that the waker has access to via a static pointer or thread-local).
- **`clone`**: new `RawWaker` with same `data`.
- **`drop`**: no-op.

## Event Loop

```rust
loop {
    if let Some(packed) = deferred.pop() {
        let slab_id = (packed >> 32) as u8;
        let task_index = packed as u32;
        dispatch_poll(slab_id, task_index);
    }

    ring.submit_and_wait(1)?;

    for cqe in ring.completion() {
        let ud = cqe.user_data();
        let slab_id    = (ud >> 56) as u8;
        let task_index = (ud >> 24) as u32;
        let io_slot    =  ud        as u32;

        let task = match slab_id {
            0 => &mut slab_a[task_index],
            1 => &mut slab_b[task_index],
            2 => &mut slab_c[task_index],
            _ => unreachable!(),
        };
        task.results[io_slot as usize] = cqe.result();
        task.results_ready |= 1 << io_slot;

        dispatch_poll(slab_id, task_index);
    }
}
```

No separate ready queue — each completion triggers the task poll immediately. The
only deferred wakeups come from waker calls during a poll; those push to a small
local `Vec<u64>` (`deferred`) that's drained before the next `submit_and_wait`.

```rust
fn dispatch_poll(slab_id: u8, task_index: u32) {
    match slab_id {
        0 => poll_one::<TaskA>(&mut slab_a, task_index),
        1 => poll_one::<TaskB>(&mut slab_b, task_index),
        2 => poll_one::<TaskC>(&mut slab_c, task_index),
        _ => unreachable!(),
    }
}

fn poll_one<T>(slab: &mut Slab<T>, index: u32) {
    let task = &mut slab[index];
    let poll = task.future.poll(cx);
    match poll {
        Poll::Ready(()) => slab.remove(index),
        Poll::Pending   => {},
    }
}
```
