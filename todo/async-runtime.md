# Custom io_uring Async Runtime Design

## Goals

- Spawn `Future`s directly (no per-future `Box` allocation)
- Runtime has a single task future type; different behaviors are composed into one future (e.g. an enum dispatch)
- Zero extra allocation for IO state tracking (inline u64 submitted bitmap + `[i32; 64]` results, heap-overflow variant for >64 IOs)
- Fast completion dispatch via `user_data` encoding

---

## Runtime Storage

Single `static mut Option<Runtime>`. Single-threaded — no TLS, no contention.

```rust
static mut RUNTIME: Option<Runtime> = None;
```

Waker reads `RUNTIME` directly (unsafe, single-threaded, no data race). Caller `take`s the `Runtime` on teardown, drops it, can install a new one.

---

## Fixed-Capacity Tasks Slab

Pre-allocated at runtime, **never reallocated**. Guarantees pinning soundness (task addresses never move). The slab *is* the index allocator — slot position = task index.

```rust
struct Runtime {
    tasks: Slab<Task>,
    ready: Queue<u32>, // indices of tasks ready to run
    ring: IoUring,
}

struct Slab<T> {
    slots: Box<[MaybeUninit<T>]>,
    occupied: Box<[u64]>, // one bit per slot
    free: Vec<u32>,       // recycled indices (pop from end)
}
```

- Capacity chosen at construction (e.g. 256). Insertion when full = spawn error / backpressure.
- All slots pinned in memory — `Box<[T]>` never grows or shrinks.
- Runtime must read the queue len, then poll that many tasks (they themselves may spawn other tasks after all), then poll completed IO events, repeat.

```rust
struct Task<F: Future> {
    ready: bool,
    io: IoState,
    future: F,
}
```

**Insertion**: pop free list; if empty, scan `occupied` bitmap for a zero bit. Set bit. The slot position *is* the task index — no need to store it in the task.

**Removal**: clear the bit in `occupied`, push index to `free`. Tasks are
removed only when their future returns `Poll::Ready` — at that point all IOs
are complete and buffers are safe to drop. External task cancellation is not
supported by the runtime. IO cancellation (via `IORING_OP_ASYNC_CANCEL`) is
for internal future use only, e.g. implementing select between multiple IOs.

**Access**: `&slots[index]` — O(1), direct, no hashing or version checking.

---

## IO State

Tracks submitted IOs and their results in a single enum. The inline variant
handles ≤64 concurrent IOs (common case) with zero heap allocation. The heap
variant kicks in when more are needed.

```rust
enum IoState {
    Inline {
        submitted: u64,        // bitmap: bit set = SQE submitted, awaiting CQE
        results: [i32; 64],    // one slot per bit, written when CQE arrives
    },
    Heap {
        submitted: Vec<u64>,   // bitmap: one bit per slot, results.len() == submitted.len() * 64
        results: Vec<i32>,
    },
}
```

Lifecycle per slot:
- **SQE pushed**: future finds the first free bit via
  `(!submitted).trailing_zeros()`, remembers the slot index, sets the bit.
- **CQE arrives**: runtime clears the `submitted` bit, writes `cqe.result()`
  to `results[slot]`, marks the task ready.
- **Future polls**: checks `submitted` — any clear bit has a valid result in
  `results[slot]`. The future reads it and can reuse the slot for a new IO.

**Allocation**: `(!submitted).trailing_zeros()` gives first free bit. Set it.
**Overflow**: when the inline bitmap is all ones, switch to heap-allocated
`Vec<u64>` + `Vec<i32>`. No downgrade back to inline (stability over churn).

### Future-side pattern

IO-issuing futures store the `io_slot` (bit position) they are using. On
each poll they check `submitted` to determine whether their IO has completed:

```rust
struct ReadFuture {
    fd: RawFd,
    buf: Vec<u8>,
    io_slot: Option<u32>,  // found by future on first submit
}

impl Future for ReadFuture {
    type Output = i32;

    fn poll(self: Pin<&mut Self>, cx: &Context<'_>) -> Poll<i32> {
        let ti = task_index(cx);
        let rt = unsafe { (*addr_of_mut!(RUNTIME)).as_ref().unwrap() };
        let task = &rt.tasks[ti];

        if let Some(slot) = self.io_slot {
            // Already submitted — check if CQE arrived
            if task.io.submitted & (1 << slot) != 0 {
                return Poll::Pending;  // still awaiting CQE
            }
            // CQE arrived — read result, slot is now free for reuse
            let result = task.io.results[slot as usize];
            return Poll::Ready(result);
        }

        // First poll — find free slot, submit SQE
        let slot = (!task.io.submitted).trailing_zeros();
        task.io.submitted |= 1 << slot;
        self.io_slot = Some(slot);

        let sq = unsafe { &mut *addr_of_mut!(RUNTIME) }.ring.submission();
        let user_data = IoUserData { task_index: ti, io_slot: slot }.into();
        unsafe {
            push_read(sq, self.fd, &mut self.buf, user_data)
                .expect("SQ full — need retry");
        }
        Poll::Pending
    }
}
```

The future owns the slot lifecycle: it finds a free bit, sets it, and later
reads the result when the bit is clear. The runtime never assigns slots —
it only clears `submitted` bits when CQEs arrive.

---

## `user_data` Encoding

A `#[repr(C)]` struct with two `u32` fields, transmuted to/from `u64`:

```rust
#[repr(C)]
struct IoUserData {
    task_index: u32,   // slab slot position
    io_slot: u32,      // bit position in IoState.submitted
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

On completion: decode via `IoUserData::from(cqe.user_data())`, write
`cqe.result()` into `task.io.results[ud.io_slot]`, clear the `submitted`
bit, mark the task ready. Cancel CQEs are processed identically — the
cancel result overwrites `results[slot]` which is harmless because the
future already has the winner's result.

---

## Context

The waker encodes the task index in its `data` pointer. `wake_by_ref` checks
`task.ready` — if the task is not already enqueued, it sets `ready = true` and
pushes the index into the ready queue. This is O(1) with no contains check on
the queue itself.

```rust
fn waker(task_index: u32) -> Waker {
    unsafe extern "C" fn wake_by_ref(data: *const ()) {
        let index = data as u32;
        let rt = (*addr_of_mut!(RUNTIME)).as_mut().unwrap();
        let task = &mut rt.tasks[index];
        if !task.ready {
            task.ready = true;
            rt.ready.push(index);
        }
    }
    unsafe extern "C" fn wake(data: *const ()) {
        wake_by_ref(data);
    }
    const VTABLE: RawWakerVTable = RawWakerVTable::new(
        |data| RawWaker::new(data, &VTABLE), // clone
        wake_by_ref,
        wake,
        |_| {},                               // drop
    );
    let ptr = core::ptr::without_provenance(task_index as usize);
    unsafe { Waker::from_raw(RawWaker::new(ptr, &VTABLE)) }
}

fn task_index(cx: &Context) -> u32 {
    cx.waker().as_raw().data() as u32
}
```

---

## Event Loop

```rust
loop {
    // Phase 1: poll ready tasks (snapshot count — new tasks from polling
    // wait for next cycle to avoid starvation)
    let n = ready.len();
    for _ in 0..n {
        let index = ready.pop().unwrap();
        poll_one(index);
    }

    // Phase 2: wait for and drain IO completions
    match ring.submit_and_wait(1) {
        Ok(_) => {}
        Err(ref e) if e.raw_os_error() == Some(libc::EBUSY) => {}
        Err(e) => return Err(e),
    }
    drain_cqes();

    // Phase 3: retry submission if EBUSY blocked SQEs
    ring.submit()?;
}
```

```rust
fn poll_one(index: u32) {
    let task = &mut tasks[index];
    task.ready = false;
    let w = waker(index);
    let mut cx = Context::from_waker(&w);
    match task.future.poll(cx) {
        Poll::Ready(()) => tasks.remove(index),
        Poll::Pending => {}
    }
}
```

`ready` is cleared **before** calling `poll`. If the task calls
`wake_by_ref` during the poll (e.g. via `yield_now`), `ready` is `false` so
the waker enqueues it for the next cycle. If `wake_by_ref` is called a second
time in the same poll, `ready` is already `true` — the push is skipped.

---

## Completion Queue Overflow

The CQ ring is fixed-size. When it fills up, the kernel buffers overflowed
CQEs internally (requires `IORING_FEAT_NODROP`, default since Linux 5.11).
If the application doesn't drain the CQ, these buffered CQEs stay stuck —
the kernel won't refill the ring until the application advances the CQ head
by consuming entries.

When `submit_and_wait` is called with a full CQ, it returns `-EBUSY` and the
SQEs remain **unsubmitted**. The `io-uring` crate handles this transparently:
its submit logic checks the `IORING_SQ_CQ_OVERFLOW` SQ ring flag (set by the
kernel when CQEs are buffered) and adds `IORING_ENTER_GETEVENTS` to the
`io_uring_enter()` flags, which tells the kernel to flush the overflow buffer
into the CQ ring.

The only thing the runtime must do is **drain the CQ** before retrying
submission. Consuming CQEs advances the head pointer, freeing ring slots.
The next `submit_and_wait` call then triggers the kernel to refill those slots
from the overflow buffer.

This is entirely transparent to tasks — they never interact with overflow
handling. Tasks submit SQEs and read results from `task.io.results`; the runtime
deals with CQ management.

```rust
fn drain_cqes() {
    for cqe in ring.completion() {
        let ud = IoUserData::from(cqe.user_data());
        let task = &mut tasks[ud.task_index];
        task.io.results[ud.io_slot as usize] = cqe.result();
        task.io.submitted &= !(1 << ud.io_slot);
        if !task.ready {
            task.ready = true;
            ready.push(ud.task_index);
        }
    }
}
```

`cqueue::overflow()` can be checked for diagnostics — it returns the number
of CQEs that were dropped (only relevant if `IORING_FEAT_NODROP` is not
available, which is unlikely on modern kernels).

### CQ sizing

The CQ should be large enough to hold completions for every in-flight IO
simultaneously. A common heuristic:

```
CQ_ENTRIES >= max_concurrent_ios
```

A CQ that is too small causes frequent `EBUSY` stalls (each stall requires a
full drain + resubmit round-trip). A CQ that is too large wastes memory
(every CQE is 16 bytes). Matching the CQ size to the in-flight IO capacity
avoids both problems and eliminates the `EBUSY` path in steady state.

---

## IO Cancellation

Futures can cancel their own in-flight IOs via `IORING_OP_ASYNC_CANCEL`.
This is needed for select-style patterns — racing multiple IOs and canceling
the losers when one wins.

A cancel SQE uses the **same `user_data`** as the original IO (same
`task_index` + `io_slot`). The `drain_cqes` function processes it
identically to a normal CQE — clears `submitted`, writes the (meaningless)
cancel result to `results[slot]`, marks the task ready. The future doesn't
need to distinguish between normal and cancel CQEs. Both mean the same thing:
**the kernel is done with that slot**.

Two outcomes when a cancel is submitted:
- **Cancel succeeds**: original IO was aborted. Cancel CQE arrives, clears
  `submitted`. No original CQE will arrive.
- **Original IO completes first**: original CQE arrives normally (clears
  `submitted`, writes result, sets `ready`). Cancel CQE also arrives
  (no-op, overwrites result with cancel status). The original result was
  already written and the future already has it.

Both cases are idempotent — the `submitted` bit ends up clear either way.

### CancelFuture

A standalone future that cancels one in-flight IO. Reusable outside select
— any future can create one and `.await` it to cancel a pending operation.

```rust
struct CancelFuture {
    io_slot: u32,
    submitted: bool,
}

impl CancelFuture {
    fn new(io_slot: u32) -> Self {
        Self { io_slot, submitted: false }
    }
}

impl Future for CancelFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &Context<'_>) -> Poll<()> {
        let ti = task_index(cx);
        let rt = unsafe { (*addr_of_mut!(RUNTIME)).as_ref().unwrap() };
        let task = &rt.tasks[ti];

        if !self.submitted {
            let sq = unsafe { &mut *addr_of_mut!(RUNTIME) }.ring.submission();
            let ud = IoUserData { task_index: ti, io_slot: self.io_slot };
            unsafe { push_cancel(sq, ud.into()) };
            self.submitted = true;
            return Poll::Pending;
        }

        // Cancel CQE arrived — bit is clear
        if task.io.submitted & (1 << self.io_slot) != 0 {
            return Poll::Pending;
        }

        Poll::Ready(())
    }
}
```

### Select pattern

Races two IOs, runs a closure on the winner's result immediately, then
cancels the loser and waits for the cancel to complete before returning.

```rust
struct SelectFuture<F, T> {
    slot_a: u32,
    slot_b: u32,
    cancel: CancelFuture,
    closure: F,
    closure_result: MaybeUninit<T>,
    phase: SelectPhase,
}

enum SelectPhase {
    Racing,
    CancelPending,
}

impl<F, T> Future for SelectFuture<F, T>
where
    F: FnOnce(&i32) -> T,
{
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &Context<'_>) -> Poll<T> {
        // SAFETY: we only project to fields, never move the future
        let this = unsafe { self.get_unchecked_mut() };
        let ti = task_index(cx);
        let rt = unsafe { (*addr_of_mut!(RUNTIME)).as_ref().unwrap() };
        let task = &rt.tasks[ti];

        match this.phase {
            Racing => {
                let a_done = task.io.submitted & (1 << this.slot_a) == 0;
                let b_done = task.io.submitted & (1 << this.slot_b) == 0;

                if !a_done && !b_done {
                    return Poll::Pending;
                }

                // Winner found — read result, run closure, submit cancel
                let winner = if a_done { this.slot_a } else { this.slot_b };
                let loser = if a_done { this.slot_b } else { this.slot_a };
                let result = task.io.results[winner as usize];
                let closure_result = (this.closure)(&result);
                this.closure_result = MaybeUninit::new(closure_result);

                // Submit cancel for loser
                this.cancel = CancelFuture::new(loser);
                let cancel = unsafe { Pin::new_unchecked(&mut this.cancel) };
                let _ = cancel.poll(cx); // always returns Pending (submits SQE)

                this.phase = CancelPending;
                Poll::Pending
            }

            CancelPending => {
                let cancel = unsafe { Pin::new_unchecked(&mut this.cancel) };
                match cancel.poll(cx) {
                    Poll::Ready(()) => {
                        // Cancel done — safe to drop both buffers
                        let result = unsafe {
                            this.closure_result.assume_init_read()
                        };
                        Poll::Ready(result)
                    }
                    Poll::Pending => Poll::Pending,
                }
            }
        }
    }
}
```

Usage:

```rust
let result = select(read(fd1, buf1), read(fd2, buf2))
    .then(|result| process(result))
    .await;
```

### Select flow

```
poll 1 (Racing):
  slot_a submitted clear → winner found
  → run closure(&result) → store closure_result
  → create CancelFuture for slot_b, poll → submits cancel SQE
  → transition to CancelPending
  → return Pending

poll 2 (CancelPending):
  → poll CancelFuture → check slot_b submitted bit
  → if clear → return Ready(closure_result) — safe to drop both buffers
  → if set → return Pending (wait for cancel CQE)
```

The closure runs **immediately** when the winner is detected — not after the
cancel completes. The cancel is submitted after the closure returns. By the
time we re-poll, the cancel CQE has almost certainly arrived (submitted and
collected in the same `submit_and_wait` call). The `CancelPending` phase is
a defensive safety net.

The future cannot return `Poll::Ready` until the losing slot's `submitted`
bit is clear — otherwise the kernel may still own that buffer. The cancel
CQE clears the bit, allowing the future to safely complete and drop both
buffers.

---

## Yield

`yield_now().await` is a runtime-specific reschedule point. It returns
`Pending` on the first poll (yielding the current turn) and re-enqueues the
task via the waker. On the next scheduling cycle the task is polled again and
`Yield` returns `Ready`.

```rust
struct Yield {
    polled: bool,
}

impl Future for Yield {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &Context<'_>) -> Poll<()> {
        if self.polled {
            Poll::Ready(())
        } else {
            self.polled = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

fn yield_now() -> Yield {
    Yield { polled: false }
}
```

When `Yield::poll` calls `wake_by_ref`, `task.ready` is `false` (cleared by
`poll_one` before the poll started), so the waker sets `ready = true` and
pushes the index into the ready queue. Because the queue is FIFO, all tasks
that were already ready (enqueued before this task was popped) are polled
first. The yielded task gets its turn after them.
