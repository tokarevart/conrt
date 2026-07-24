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

IO-issuing futures are created via an intermediate future (`ReadOp`) that
reserves the slot on its first poll (when the task index is available from
the waker) and returns `(ReadFuture, CancelFuture)`. No allocation, no
shared state — both get the slot as a plain `u32`.

```rust
// Intermediate future — reserves slot, returns (ReadFuture, CancelFuture)
struct ReadOp<'a, T> {
    fd: RawFd,
    buf: &'a mut [u8],
    _marker: PhantomData<T>,
}

impl<'a, T> Future for ReadOp<'a, T> {
    type Output = (ReadFuture<'a, T>, CancelFuture);

    fn poll(self: Pin<&mut Self>, cx: &Context<'_>) -> Poll<Self::Output> {
        let ti = task_index(cx);
        let rt = unsafe { (*addr_of_mut!(RUNTIME)).as_ref().unwrap() };
        let task = &rt.tasks[ti];

        // Reserve slot — set bit in submitted, SQE not pushed yet
        let io_slot = (!task.io.submitted).trailing_zeros();
        task.io.submitted |= 1 << io_slot;

        Poll::Ready((
            ReadFuture {
                fd: self.fd, buf: self.buf, task_index: ti,
                io_slot, sqe_pushed: false, _marker: PhantomData,
            },
            CancelFuture { io_slot, submitted: false },
        ))
    }
}

fn read<'a, T>(fd: RawFd, buf: &'a mut [u8]) -> ReadOp<'a, T> {
    ReadOp { fd, buf, _marker: PhantomData }
}
```

The actual IO future — pushes the SQE on first poll, checks CQE on
subsequent polls. The slot is already reserved by `ReadOp`.

```rust
struct ReadFuture<'a, T> {
    fd: RawFd,
    buf: &'a mut [u8],
    task_index: u32,
    io_slot: u32,
    sqe_pushed: bool,
    _marker: PhantomData<T>,
}

impl<T> Future for ReadFuture<'_, T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &Context<'_>) -> Poll<T> {
        let ti = task_index(cx);
        let rt = unsafe { (*addr_of_mut!(RUNTIME)).as_ref().unwrap() };
        let task = &rt.tasks[ti];

        if !self.sqe_pushed {
            // First poll — push SQE (slot already reserved)
            let sq = unsafe { &mut *addr_of_mut!(RUNTIME) }.ring.submission();
            let user_data = IoUserData { task_index: ti, io_slot: self.io_slot }.into();
            unsafe {
                push_read(sq, self.fd, self.buf, user_data)
                    .expect("SQ full — need retry");
            }
            self.sqe_pushed = true;
            return Poll::Pending;
        }

        // Subsequent polls — check if CQE arrived
        if task.io.submitted & (1 << self.io_slot) != 0 {
            return Poll::Pending;
        }

        let result = task.io.results[self.io_slot as usize];
        Poll::Ready(unsafe { transmute_copy(&result) })
    }
}

impl<T> Drop for ReadFuture<'_, T> {
    fn drop(&mut self) {
        if !self.sqe_pushed {
            // SQE never pushed — clear the reserved bit
            let rt = unsafe { (*addr_of_mut!(RUNTIME)).as_ref().unwrap() };
            let task = &mut rt.tasks[self.task_index];
            task.io.submitted &= !(1 << self.io_slot);
        }
    }
}
```

The `ReadOp` future owns the slot lifecycle: it reserves a free bit on its
first poll, then `ReadFuture` pushes the SQE. The runtime only clears
`submitted` bits when CQEs arrive.

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

The `io_slot` is known at creation time (reserved by `ReadOp`). No shared
state needed.

```rust
struct CancelFuture {
    io_slot: u32,
    submitted: bool,
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

        if task.io.submitted & (1 << self.io_slot) != 0 {
            return Poll::Pending;
        }

        Poll::Ready(())
    }
}
```

### Select

Generic future combinator. Races any two futures, optionally applies a
closure to the winner's result, then cancels the loser and waits for the
cancel future to complete before returning.

Accepts `(future, cancel_token)` tuples — each arm carries its own future
and cancellation mechanism. The cancel tokens are generic `Future<Output = ()>`,
not tied to any specific runtime. IO futures return `CancelFuture`; other
futures may return no-op or custom cancel tokens.

```rust
fn select<A, B, CA, CB>(
    a: (A, CA),
    b: (B, CB),
) -> Select<A, B, CA, CB>
where
    A: Future,
    B: Future<Output = A::Output>,
    CA: Future<Output = ()>,
    CB: Future<Output = ()>,
{
    Select { a: a.0, b: b.0, cancel_a: a.1, cancel_b: b.1, result: MaybeUninit::uninit(), phase: Racing, winner: 0 }
}
```

Usage:

```rust
// Simple read
let result = read::<i32>(fd, buf).await;

// Select between two reads
let result = select(
    read::<i32>(fd1, buf1).await,
    read::<i32>(fd2, buf2).await,
).await;

// With closure
let result = select(
    read::<i32>(fd1, buf1).await,
    read::<i32>(fd2, buf2).await,
).then(|result| process(result)).await;
```

#### Select (no closure)

```rust
struct Select<A, B, CA, CB> {
    a: A,
    b: B,
    cancel_a: CA,
    cancel_b: CB,
    result: MaybeUninit<A::Output>,
    phase: SelectPhase,
    winner: u8,
}

enum SelectPhase {
    Racing,
    CancelPending,
}

impl<A, B, CA, CB> Future for Select<A, B, CA, CB>
where
    A: Future,
    B: Future<Output = A::Output>,
    CA: Future<Output = ()>,
    CB: Future<Output = ()>,
{
    type Output = A::Output;

    fn poll(self: Pin<&mut Self>, cx: &Context<'_>) -> Poll<A::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        match this.phase {
            Racing => {
                let a = unsafe { Pin::new_unchecked(&mut this.a) };
                let b = unsafe { Pin::new_unchecked(&mut this.b) };

                let winner_result = if let Poll::Ready(result) = a.poll(cx) {
                    this.winner = 0;
                    let cancel = unsafe { Pin::new_unchecked(&mut this.cancel_b) };
                    let _ = cancel.poll(cx); // submits cancel SQE
                    result
                } else if let Poll::Ready(result) = b.poll(cx) {
                    this.winner = 1;
                    let cancel = unsafe { Pin::new_unchecked(&mut this.cancel_a) };
                    let _ = cancel.poll(cx);
                    result
                } else {
                    return Poll::Pending;
                };

                this.result = MaybeUninit::new(winner_result);
                this.phase = CancelPending;
                Poll::Pending
            }

            CancelPending => {
                let cancel = if this.winner == 0 {
                    unsafe { Pin::new_unchecked(&mut this.cancel_b) }
                } else {
                    unsafe { Pin::new_unchecked(&mut this.cancel_a) }
                };
                match cancel.poll(cx) {
                    Poll::Ready(()) => {
                        Poll::Ready(unsafe { this.result.assume_init_read() })
                    }
                    Poll::Pending => Poll::Pending,
                }
            }
        }
    }
}

impl<A, B, CA, CB> Select<A, B, CA, CB>
where
    A: Future,
    B: Future<Output = A::Output>,
    CA: Future<Output = ()>,
    CB: Future<Output = ()>,
{
    fn then<F, T>(self, closure: F) -> SelectThen<A, B, CA, CB, F, T>
    where
        F: FnOnce(A::Output) -> T,
    {
        SelectThen {
            a: self.a,
            b: self.b,
            cancel_a: self.cancel_a,
            cancel_b: self.cancel_b,
            closure,
            closure_result: MaybeUninit::uninit(),
            phase: self.phase,
            winner: self.winner,
        }
    }
}
```

#### SelectThen (with closure)

```rust
struct SelectThen<A, B, CA, CB, F, T> {
    a: A,
    b: B,
    cancel_a: CA,
    cancel_b: CB,
    closure: F,
    closure_result: MaybeUninit<T>,
    phase: SelectPhase,
    winner: u8,
}

impl<A, B, CA, CB, F, T> Future for SelectThen<A, B, CA, CB, F, T>
where
    A: Future,
    B: Future<Output = A::Output>,
    CA: Future<Output = ()>,
    CB: Future<Output = ()>,
    F: FnOnce(A::Output) -> T,
{
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &Context<'_>) -> Poll<T> {
        let this = unsafe { self.get_unchecked_mut() };

        match this.phase {
            Racing => {
                let a = unsafe { Pin::new_unchecked(&mut this.a) };
                let b = unsafe { Pin::new_unchecked(&mut this.b) };

                let winner_result = if let Poll::Ready(result) = a.poll(cx) {
                    this.winner = 0;
                    let cancel = unsafe { Pin::new_unchecked(&mut this.cancel_b) };
                    let _ = cancel.poll(cx);
                    result
                } else if let Poll::Ready(result) = b.poll(cx) {
                    this.winner = 1;
                    let cancel = unsafe { Pin::new_unchecked(&mut this.cancel_a) };
                    let _ = cancel.poll(cx);
                    result
                } else {
                    return Poll::Pending;
                };

                let closure_result = (this.closure)(winner_result);
                this.closure_result = MaybeUninit::new(closure_result);
                this.phase = CancelPending;
                Poll::Pending
            }

            CancelPending => {
                let cancel = if this.winner == 0 {
                    unsafe { Pin::new_unchecked(&mut this.cancel_b) }
                } else {
                    unsafe { Pin::new_unchecked(&mut this.cancel_a) }
                };
                match cancel.poll(cx) {
                    Poll::Ready(()) => {
                        Poll::Ready(unsafe { this.closure_result.assume_init_read() })
                    }
                    Poll::Pending => Poll::Pending,
                }
            }
        }
    }
}
```

#### Select flow

```
poll 1 (Racing):
  → poll both futures
  → a returns Ready(result)
  → poll b's cancel future → submits cancel SQE
  → run closure(result) → store closure_result (SelectThen only)
  → transition to CancelPending
  → return Pending

poll 2 (CancelPending):
  → poll cancel future → check submitted bit
  → if clear → return Ready(result / closure_result)
  → if set → return Pending
```

The closure (if any) runs **immediately** when the winner is detected, not
after the cancel completes. The cancel is submitted in the same poll. By the
time we re-poll, the cancel CQE has almost certainly arrived (submitted and
collected in the same `submit_and_wait` call). The `CancelPending` phase is
a defensive safety net.

The future cannot return `Poll::Ready` until the losing future's IOs are
complete — otherwise the kernel may still own those buffers. The cancel
future ensures this by waiting for the cancel CQE.

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
