# Custom io_uring Async Runtime Design

## Goals

- Spawn `Future`s directly (no per-future `Box` allocation)
- Runtime has a single task future type; different behaviors are composed into one future (e.g. an enum dispatch)
- Zero extra allocation for IO state tracking (inline u64 submitted bitmap + `[i32; 64]` results, heap-overflow variant for >64 IOs)
- Fast completion dispatch via `user_data` encoding
- Per-task bump arena for zero-allocation buffer management — each allocation returns an `ArenaAlloc` guard that deallocates on drop with O(1) pop from the top
- Zero-cost direct `&mut Task` access passed to tasks on spawn (eliminates Thread-Local Storage TLS lookups during poll)
- Zero-hazard cancellation safety — buffer ownership moves into the task on SQE submission and is retained until CQE completion

---

## Runtime Storage

Single `static mut Option<Runtime>`. Single-threaded — no contention.

```rust
static mut RUNTIME: Option<Runtime> = None;
```

Waker reads `RUNTIME` directly (unsafe, single-threaded, no data race). Caller `take`s the `Runtime` on teardown, drops it, can install a new one.

---

## Fixed-Capacity Tasks Slab

Pre-allocated at runtime, **never reallocated**. Guarantees pinning soundness (task addresses never move). The slab *is* the index allocator — slot position = task index.

Tasks and futures are stored in **parallel arrays** — a `Task` is always fully initialized (no `MaybeUninit<Task>`), while futures live in their own `MaybeUninit<F>` slots. This solves the chicken-and-egg problem: `spawn_fn` needs `&mut Task` to create the future, so `Task` must be valid before the future exists.

```rust
struct Runtime<T, F: Future<Output = ()>, S: Fn(&mut Task, T) -> F> {
    tasks: TaskSlab<F>,
    ready: Vec<u32>,
    ring: IoUring,
    spawn_fn: S,
    _phantom: core::marker::PhantomData<T>,
}

struct TaskSlab<F> {
    tasks: Box<[MaybeUninit<Task>]>,   // task data (always valid when occupied)
    futures: Box<[MaybeUninit<F>]>,    // futures (parallel array, same index)
    occupied: Box<[u64]>,              // one bit per slot
    free: Vec<u32>,                     // recycled indices (pop from end)
}
```

- Capacity chosen at construction (e.g. 256). Insertion when full = spawn error / backpressure.
- All slots pinned in memory — `Box<[...]>` never grows or shrinks.
- `Slab<T>` (generic, single-array) is used separately for `IoBuffers` inflight tracking.

```rust
struct Task {
    index: u32,
    ready: bool,
    io: IoState,
    buffers: IoBuffers,
}
```

`Task` has no generic parameter — it is always fully initialized. The future `F` lives in `TaskSlab.futures[index]`, in its own `MaybeUninit<F>` slot.

**Insertion**: pop free list; if empty, scan `occupied` bitmap for a zero bit. Set bit. Store the index as `task.index` — the task knows its own slot position.

**Removal**: clear the bit in `occupied`, push index to `free`. Drop the `Task` and the future separately. Tasks are removed only when their future returns `Poll::Ready` — at that point all IOs are complete and buffers are safe to drop. External task cancellation is not supported by the runtime. IO cancellation (via `IORING_OP_ASYNC_CANCEL`) is for internal future use only, e.g. implementing select between multiple IOs.

**Access**: `&tasks[index]` / `&futures[index]` — O(1), direct, no hashing or version checking.

### TaskSlab methods

```rust
impl<F> TaskSlab<F> {
    fn new(capacity: u32) -> Self { ... }
    fn insert_vacant(&mut self) -> Option<u32> { ... }
    fn init_task(&mut self, index: u32, task: Task) { ... }
    fn init_future(&mut self, index: u32, future: F) { ... }
    fn task_mut(&mut self, index: u32) -> Option<&mut Task> { ... }
    fn future_mut(&mut self, index: u32) -> Option<&mut F> { ... }
    fn remove(&mut self, index: u32) -> (Task, F) { ... }
}
```

`insert_vacant` marks the slot occupied and returns the index, but does NOT initialize the task or future — the caller (Runtime::spawn) initializes both via `init_task` and `init_future` after calling `insert_vacant`. This keeps `TaskSlab` generic over `F` without requiring `F: Default`.

### Generic Slab

A general-purpose, resizable `Slab<T>` used for small collections within `IoBuffers` (inflight arena guards and inflight vecs) and any other spot that needs indexed, reusable slots. Unlike `TaskSlab`, this slab does NOT require pinned memory — it uses `Vec<MaybeUninit<T>>` and grows on demand.

```rust
struct Slab<T> {
    slots: Vec<MaybeUninit<T>>,
    occupied: Box<[u64]>,
    free: Vec<u32>,
}
```

- **No pre-allocation required** — `new()` starts empty, grows as items are inserted.
- **Resizes automatically** — when all slots are occupied and the free list is empty, the slots vec doubles in capacity. The occupied bitmap is extended to match.
- **Not pinned** — addresses may move on resize. Suitable for `IoBuffers` inflight tracking (guards and vecs) where pointers are not stored long-term. NOT suitable for `TaskSlab` (which needs pinning for future pinning soundness).

Methods: `new()`, `insert(value) -> Option<u32>`, `remove(index) -> T`, `get(index) -> Option<&T>`, `get_mut(index) -> Option<&mut T>`, `insert_at(index, value)` (for indexed insertion by io_slot), `contains(index) -> bool`.

---

## Task Arena

Each task gets a fixed-size bump arena for zero-allocation buffer management.
All IO buffers, intermediate strings, and temporaries live in the arena — freed
on task completion via O(1) offset rewind. No per-allocation `malloc`/`free`,
no fragmentation, no deallocation cost.

Each allocation returns an `ArenaAlloc` guard that marks the allocation as
deallocated on drop. If the allocation is at the top of the arena, the guard
pops it and all consecutive deallocated allocations below it until hitting a
live allocation. If it's not at the top, it just marks it deallocated — the
space is reclaimed later when upper allocations are dropped.

### Arena

```rust
const ARENA_MAX: u32 = u32::MAX;
const FOOTER_SIZE: u32 = core::mem::size_of::<AllocFooter>() as u32;

#[repr(C, packed)]
struct AllocFooter {
    size: u32,        // span from previous footer end to this footer start (includes alignment padding)
    deallocated: u8,  // 0 = live, 1 = dead
}

struct Arena {
    base_ptr: *mut u8,
    offset: u32,       // next free byte (end of last footer)
    max_capacity: u32, // arena size in bytes (≤ u32::MAX)
}
```

Three fields (16 bytes with pointer), zero heap allocation for metadata. Arena
capacity is capped at `u32::MAX` (4GB) — plenty for per-task IO buffers. Every
allocation's metadata lives inside the arena buffer itself, right after the
allocated bytes:

```
[alloc A bytes][footer A] [alloc B bytes][footer B] [alloc C bytes][footer C] ...
                                                           ^
                                                     offset
```

The footer of the current top allocation is always at
`base_ptr + offset - FOOTER_SIZE`. Walking backward from any footer to the
previous one: read `footer.size` (includes alignment padding) → previous footer
at `current - FOOTER_SIZE - size` → check its `deallocated` flag → repeat.

### ArenaAlloc

RAII guard returned by every allocation. On drop, marks the allocation as
deallocated and pops from the top of the arena if possible.

```rust
struct ArenaAlloc<'a> {
    arena: &'a mut Arena,
    alloc_offset: u32, // where user data starts in the arena buffer
    alloc_size: u32,   // byte count of the allocation (needed to find its footer)
}
```

16 bytes. `ArenaAlloc` is `!Send` and `!Sync` — it cannot leave the task.

```rust
impl<'a> Drop for ArenaAlloc<'a> {
    #[inline(always)]
    fn drop(&mut self) {
        // Mark this allocation's footer as deallocated
        let footer = unsafe {
            &mut *(self.arena.base_ptr
                .add(self.alloc_offset as usize)
                .add(self.alloc_size as usize) as *mut AllocFooter)
        };
        footer.deallocated = 1;

        // Pop from top while consecutive deallocated footers
        while self.arena.offset > FOOTER_SIZE {
            let top = unsafe {
                &mut *(self.arena.base_ptr
                    .add((self.arena.offset - FOOTER_SIZE) as usize) as *mut AllocFooter)
            };
            if top.deallocated == 0 { break; }
            let size = top.size;
            self.arena.offset -= FOOTER_SIZE + size;
            // Clear flag — clean state for reused memory
            let new_top = unsafe {
                &mut *(self.arena.base_ptr
                    .add((self.arena.offset - FOOTER_SIZE) as usize) as *mut AllocFooter)
            };
            new_top.deallocated = 0;
        }
    }
}
```

### Allocation methods

```rust
impl Arena {
    fn alloc(&mut self, layout: Layout) -> Option<ArenaAlloc<'_>> {
        let align = layout.align() as u32;
        let size = layout.size() as u32;
        let aligned = (self.offset + align - 1) & !(align - 1);
        let padded = aligned + size;
        // Check with u32 arithmetic — no overflow since max_capacity ≤ u32::MAX
        if padded + FOOTER_SIZE > self.max_capacity || padded + FOOTER_SIZE < padded {
            return None; // arena exhausted
        }
        // Write footer — size includes alignment padding so pop can walk backward
        let footer = unsafe {
            &mut *(self.base_ptr.add(padded as usize) as *mut AllocFooter)
        };
        footer.size = padded - self.offset;
        footer.deallocated = 0;
        self.offset = padded + FOOTER_SIZE;
        Some(ArenaAlloc {
            arena: self,
            alloc_offset: aligned,
            alloc_size: size,
        })
    }

    fn alloc_bytes(&mut self, len: u32) -> Option<ArenaAlloc<'_>> {
        self.alloc(Layout::from_size_align(len as usize, align_of::<u8>()).unwrap())
    }

    fn alloc_type<T>(&mut self) -> Option<ArenaAlloc<'_>> {
        self.alloc(Layout::new::<T>())
    }
}
```

### Pop behavior — examples

```
Arena layout after 3 allocations:
  [pad A][A:100B][fA] [pad B][B:64B][fB] [C:32B][fC] ...
  fA.size = 100 + pad A (includes alignment gap)
  fB.size = 64 + pad B
  fC.size = 32 (no padding needed)
  offset = end of fC

Drop B (not at top):
  mark fB.deallocated = 1
  offset stays — fC is still live

Drop C (at top):
  mark fC.deallocated = 1
  check top footer → fC deallocated → pop: offset -= 8 + 32
  check new top footer → fB deallocated → pop: offset -= 8 + fB.size
  check new top footer → fA live → stop
  offset now points right after fA

Drop A (now at top):
  mark fA.deallocated = 1
  pop: offset -= 8 + fA.size
  offset = 0 — full arena reclaim
```

## Dual-Mode Buffer Management (`IoBuffers`)

While `Arena` provides $O(1)$ stack rewind for task-local IO temporaries, some workloads require buffers that outlive the task or are transferred across tasks. `IoBuffers` encapsulates both bump allocation and owned heap vector tracking under a single zero-cost interface.

`IoBuffers` uses the **generic `Slab<T>`** (single-array, for small fixed-capacity collections) — NOT `TaskSlab` (which is for task+future storage).

```rust
pub struct IoBuffers {
    pub arena: Arena,
    pub inflight_arena_guards: Slab<ArenaAlloc>,
    pub inflight_vecs: Slab<Vec<u8>>,
}

impl IoBuffers {
    pub fn new(arena_capacity: u32) -> Self {
        Self {
            arena: Arena::new(arena_capacity),
            inflight_arena_guards: Slab::new(),
            inflight_vecs: Slab::new(),
        }
    }
}

/// Flexible input source for IO operations
pub enum BufferInput {
    /// Arena-backed allocation with top-of-stack rewind on drop
    Arena(ArenaAlloc),
    /// Standard heap-allocated vector
    Vector(Vec<u8>),
}
```

### Cancellation safety & In-Flight Ownership Transfer

io_uring is completion-based — the kernel holds raw pointers to arena memory
while operations are in-flight. 

To guarantee cancellation safety when a user-facing `Future` is dropped
mid-`await` (e.g. inside `select!` or timeouts), **move ownership of the
`ArenaAlloc` guard directly into `task.buffers.inflight_arena_guards` upon SQE
submission**.

1. **Submission Phase**: When pushing an SQE, the future moves its `ArenaAlloc`
   guard into `task.buffers.inflight_arena_guards` mapped to the assigned `io_slot`.
2. **Cancellation Phase**: If the user-facing `Future` is dropped before the IO
   finishes, the `ArenaAlloc` guard **remains safely owned by the `Task`**.
3. **CQE Arrival**: When the kernel posts the CQE for that `io_slot`,
   `drain_cqes()` removes the guard from `task.buffers.inflight_arena_guards` and drops
   it.
4. **Rewind Phase**: The `ArenaAlloc::drop` handler executes *only after* the
   kernel has released its pointer, marking the slot deallocated and unwinding
   the top of the arena safely.

This completely eliminates data races caused by early future drops.

### I/O Submission Flow

When a task needs to perform IO, it allocates a buffer (from the arena or a heap
`Vec<u8>`) and submits the IO directly through `Task` methods. The buffer's
ownership is moved into the appropriate inflight slab on submission, guaranteeing
it stays alive until the CQE arrives.

```rust
impl Task {
    /// Submits a read IO operation accepting either an ArenaAlloc guard or a heap Vec<u8>
    pub fn submit_read(&mut self, fd: RawFd, buf: BufferInput, len: u32) -> u32 {
        let io_slot = (!self.io.submitted).trailing_zeros();
        self.io.submitted |= 1 << io_slot;

        // Resolve raw pointer and stash ownership into the appropriate inflight slab
        let ptr = match buf {
            BufferInput::Arena(alloc) => {
                let p = unsafe { self.buffers.arena.base_ptr().add(alloc.alloc_offset as usize) };
                self.buffers.inflight_arena_guards.insert_at(io_slot as usize, alloc);
                p
            }
            BufferInput::Vector(vec) => {
                let p = vec.as_ptr() as *mut u8;
                self.buffers.inflight_vecs.insert_at(io_slot as usize, vec);
                p
            }
        };

        // Submit SQE to io_uring ring (accessed via RUNTIME static, single-threaded)
        let user_data = IoUserData { index: self.index, io_slot }.into();
        let rt = unsafe { (*addr_of_mut!(RUNTIME)).as_mut().unwrap() };
        unsafe { push_read(rt.ring.submission(), fd, ptr, len, user_data) };

        io_slot
    }

    /// Called during CQE draining
    pub fn on_cqe_completion(&mut self, io_slot: u32, result: i32) {
        self.io.results[io_slot as usize] = result;
        self.io.submitted &= !(1 << io_slot);

        // Remove and drop from whichever slab held ownership for this io_slot.
        // Dropping an ArenaAlloc triggers O(1) top-of-stack rewind.
        // Dropping a Vec<u8> deallocates the heap buffer.
        if self.buffers.inflight_arena_guards.contains(io_slot as usize) {
            self.buffers.inflight_arena_guards.remove(io_slot as usize);
        } else if self.buffers.inflight_vecs.contains(io_slot as usize) {
            self.buffers.inflight_vecs.remove(io_slot as usize);
        }
    }
}
```

Usage:

```rust
async fn socket_worker(task: &mut Task, fd: RawFd) {
    loop {
        // Fast path: Zero-allocation Arena Read
        if let Some(alloc) = task.buffers.arena.alloc_bytes(4096) {
            let io_slot = task.submit_read(fd, BufferInput::Arena(alloc), 4096);
            yield_now().await;
            if task.io.results[io_slot as usize] < 0 { break; }
        } else {
            // Fallback path: Heap vector allocation if arena is full
            let vec_buf = vec![0u8; 4096];
            let io_slot = task.submit_read(fd, BufferInput::Vector(vec_buf), 4096);
            yield_now().await;
            if task.io.results[io_slot as usize] < 0 { break; }
        }
    }
}
```

### Spawn integration

The `Fn(&mut Task, T) -> F` closure is passed to the `Runtime` at
construction time. `spawn` only accepts user data — it creates a fully
initialized `Task` (with its `index` set), calls the stored closure
to get the future, then stores both in `TaskSlab`.

`Task` is always initialized before `spawn_fn` runs — no uninitialized
memory, no `MaybeUninit`, no UB. The closure receives `&mut Task` with valid
`ready`, `io`, and `buffers` fields.

```rust
impl<T, F: Future<Output = ()>, S: Fn(&mut Task, T) -> F> Runtime<T, F, S> {
    fn spawn(&mut self, user_data: T) -> Option<u32> {
        let index = self.tasks.insert_vacant()?;
        // Initialize Task — always valid before spawn_fn runs
        let task = Task {
            index: index,
            ready: true,
            io: IoState::new(),
            buffers: IoBuffers::new(4096),
        };
        self.tasks.init_task(index, task);
        // spawn_fn receives a valid &mut Task and returns the future
        let future = (self.spawn_fn)(self.tasks.task_mut(index).unwrap(), user_data);
        self.tasks.init_future(index, future);
        self.ready.push(index);
        Some(index)
    }
}
```

Tasks receive `&mut Task` which gives direct access to `buffers` (arena + inflight guards),
`submit_read()`. No TLS, no `current_arena()` lookup — the task
reference is passed directly into the future's state via the spawn closure.

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

IO-issuing futures call `task.submit_read()` which handles slot allocation, guard
transfer, and SQE submission. The future then polls `task.io` to check for
completion — no `ReadOp`/`ReadFuture` intermediaries needed.

```rust
async fn socket_worker(task: &mut Task, fd: RawFd) {
    loop {
        if let Some(alloc) = task.buffers.arena.alloc_bytes(4096) {
            let io_slot = task.submit_read(fd, BufferInput::Arena(alloc), 4096);
            yield_now().await;
            if task.io.results[io_slot as usize] < 0 { break; }
        } else {
            let vec_buf = vec![0u8; 4096];
            let io_slot = task.submit_read(fd, BufferInput::Vector(vec_buf), 4096);
            yield_now().await;
            if task.io.results[io_slot as usize] < 0 { break; }
        }
    }
}
```

---

## `user_data` Encoding

A `#[repr(C)]` struct with two `u32` fields, transmuted to/from `u64`:

```rust
#[repr(C)]
struct IoUserData {
    index: u32,   // slab slot position
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
fn waker(index: u32) -> Waker {
    unsafe extern "C" fn wake_by_ref(data: *const ()) {
        let index = data as u32;
        let rt = (*addr_of_mut!(RUNTIME)).as_mut().unwrap();
        let task = rt.tasks.task_mut(index).unwrap();
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
    let ptr = core::ptr::without_provenance(index as usize);
    unsafe { Waker::from_raw(RawWaker::new(ptr, &VTABLE)) }
}

fn index(cx: &Context) -> u32 {
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
    let task = tasks.task_mut(index).unwrap();
    task.ready = false;
    let w = waker(index);
    let mut cx = Context::from_waker(&w);
    let future = unsafe { Pin::new_unchecked(tasks.future_mut(index).unwrap()) };
    match future.poll(&mut cx) {
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
        let task = tasks.task_mut(ud.index).unwrap();
        task.on_cqe_completion(ud.io_slot, cqe.result());
        if !task.ready {
            task.ready = true;
            ready.push(ud.index);
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
`index` + `io_slot`). The `drain_cqes` function processes it
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

### Cancel trait

Implemented by IO futures. Provides a `cancel()` method that returns a future
which submits a cancel SQE and waits for the CQE. Users can implement `Cancel`
for composed futures to make entire compositions cancellable.

```rust
trait Cancel: Future {
    type CancelFuture: Future<Output = ()>;
    fn cancel(&mut self) -> Self::CancelFuture;
}
```

### CancelFuture

Concrete future returned by `Cancel::cancel()`. Submits a cancel SQE on first
poll, checks the CQE on subsequent polls. Created by IO futures when they
implement `Cancel`.

```rust
struct CancelFuture {
    io_slot: u32,
    submitted: bool,
}

impl Future for CancelFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &Context<'_>) -> Poll<()> {
        let ti = index(cx);
        let rt = unsafe { (*addr_of_mut!(RUNTIME)).as_ref().unwrap() };
        let task = &rt.tasks[ti];

        if !self.submitted {
            let sq = unsafe { &mut *addr_of_mut!(RUNTIME) }.ring.submission();
            let ud = IoUserData { index: ti, io_slot: self.io_slot };
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

### Implementing Cancel for IO futures

Any IO future that holds an `io_slot` can implement `Cancel`:

```rust
impl Cancel for MyIoFuture {
    type CancelFuture = CancelFuture;

    fn cancel(&mut self) -> CancelFuture {
        CancelFuture { io_slot: self.io_slot, submitted: false }
    }
}
```

Composed futures implement `Cancel` by canceling all sub-operations:

```rust
struct ComposedIo {
    slot_a: u32,
    slot_b: u32,
}

impl Cancel for ComposedIo {
    type CancelFuture = ComposedCancelFuture;

    fn cancel(&mut self) -> ComposedCancelFuture {
        ComposedCancelFuture {
            cancel_a: CancelFuture { io_slot: self.slot_a, submitted: false },
            cancel_b: CancelFuture { io_slot: self.slot_b, submitted: false },
        }
    }
}

struct ComposedCancelFuture {
    cancel_a: CancelFuture,
    cancel_b: CancelFuture,
}

impl Future for ComposedCancelFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &Context<'_>) -> Poll<()> {
        let this = unsafe { self.get_unchecked_mut() };
        let a = unsafe { Pin::new_unchecked(&mut this.cancel_a) };
        let b = unsafe { Pin::new_unchecked(&mut this.cancel_b) };
        if a.poll(cx).is_pending() || b.poll(cx).is_pending() {
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }
}
```

### Select

Generic future combinator. Races any two cancellable futures, then cancels the
loser and waits for the cancel to complete before returning.

Both futures must implement `Cancel` — when one wins, the other's `cancel()`
is called to produce a cancel future that is polled until completion.

```rust
fn select<A, B>(a: A, b: B) -> Select<A, B>
where
    A: Cancel,
    B: Cancel<Output = A::Output>,
{
    Select { a, b, cancel_fut: None, result: MaybeUninit::uninit(), phase: Racing, winner: 0 }
}
```

Usage:

```rust
// Simple read — allocate from arena, submit, wait for CQE
let alloc = task.buffers.arena.alloc_bytes(4096).unwrap();
let io_slot = task.submit_read(fd, BufferInput::Arena(alloc), 4096);
yield_now().await;
let result = task.io.results[io_slot as usize];

// Select between two reads — both futures implement Cancel
select(
    read_task_a(task_a, fd1, buf1),
    read_task_b(task_b, fd2, buf2),
).await;
```

#### Select (no closure)

```rust
enum CancelFut<A: Cancel, B: Cancel> {
    A(A::CancelFuture),
    B(B::CancelFuture),
}

struct Select<A, B>
where
    A: Cancel,
    B: Cancel,
{
    a: A,
    b: B,
    cancel_fut: Option<CancelFut<A, B>>,
    result: MaybeUninit<A::Output>,
    phase: SelectPhase,
    winner: u8,
}

enum SelectPhase {
    Racing,
    CancelPending,
}

impl<A, B> Future for Select<A, B>
where
    A: Cancel,
    B: Cancel<Output = A::Output>,
{
    type Output = A::Output;

    fn poll(self: Pin<&mut Self>, cx: &Context<'_>) -> Poll<A::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        match this.phase {
            Racing => {
                // Poll a — release borrow before calling cancel on b
                let winner_result = {
                    let a = unsafe { Pin::new_unchecked(&mut this.a) };
                    match a.poll(cx) {
                        Poll::Ready(result) => {
                            this.winner = 0;
                            Some(result)
                        }
                        Poll::Pending => None,
                    }
                };

                if let Some(result) = winner_result {
                    this.result = MaybeUninit::new(result);
                    let cancel = this.b.cancel();
                    this.cancel_fut = Some(CancelFut::B(cancel));
                    this.phase = CancelPending;
                    return Poll::Pending;
                }

                // Poll b
                let winner_result = {
                    let b = unsafe { Pin::new_unchecked(&mut this.b) };
                    match b.poll(cx) {
                        Poll::Ready(result) => {
                            this.winner = 1;
                            Some(result)
                        }
                        Poll::Pending => None,
                    }
                };

                if let Some(result) = winner_result {
                    this.result = MaybeUninit::new(result);
                    let cancel = this.a.cancel();
                    this.cancel_fut = Some(CancelFut::A(cancel));
                    this.phase = CancelPending;
                    return Poll::Pending;
                }

                Poll::Pending
            }

            CancelPending => {
                let cancel_fut = this.cancel_fut.as_mut().unwrap();
                let done = match cancel_fut {
                    CancelFut::A(f) => unsafe { Pin::new_unchecked(f) }.poll(cx).is_ready(),
                    CancelFut::B(f) => unsafe { Pin::new_unchecked(f) }.poll(cx).is_ready(),
                };
                if done {
                    Poll::Ready(unsafe { this.result.assume_init_read() })
                } else {
                    Poll::Pending
                }
            }
        }
    }
}

impl<A, B> Select<A, B>
where
    A: Cancel,
    B: Cancel<Output = A::Output>,
{
    fn then<F, T>(self, closure: F) -> SelectThen<A, B, F, T>
    where
        F: FnOnce(A::Output) -> T,
    {
        SelectThen {
            a: self.a,
            b: self.b,
            cancel_fut: self.cancel_fut,
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
struct SelectThen<A, B, F, T>
where
    A: Cancel,
    B: Cancel,
{
    a: A,
    b: B,
    cancel_fut: Option<CancelFut<A, B>>,
    closure: F,
    closure_result: MaybeUninit<T>,
    phase: SelectPhase,
    winner: u8,
}

impl<A, B, F, T> Future for SelectThen<A, B, F, T>
where
    A: Cancel,
    B: Cancel<Output = A::Output>,
    F: FnOnce(A::Output) -> T,
{
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &Context<'_>) -> Poll<T> {
        let this = unsafe { self.get_unchecked_mut() };

        match this.phase {
            Racing => {
                let winner_result = {
                    let a = unsafe { Pin::new_unchecked(&mut this.a) };
                    match a.poll(cx) {
                        Poll::Ready(result) => {
                            this.winner = 0;
                            Some(result)
                        }
                        Poll::Pending => None,
                    }
                };

                if let Some(result) = winner_result {
                    let closure_result = (this.closure)(result);
                    this.closure_result = MaybeUninit::new(closure_result);
                    let cancel = this.b.cancel();
                    this.cancel_fut = Some(CancelFut::B(cancel));
                    this.phase = CancelPending;
                    return Poll::Pending;
                }

                let winner_result = {
                    let b = unsafe { Pin::new_unchecked(&mut this.b) };
                    match b.poll(cx) {
                        Poll::Ready(result) => {
                            this.winner = 1;
                            Some(result)
                        }
                        Poll::Pending => None,
                    }
                };

                if let Some(result) = winner_result {
                    let closure_result = (this.closure)(result);
                    this.closure_result = MaybeUninit::new(closure_result);
                    let cancel = this.a.cancel();
                    this.cancel_fut = Some(CancelFut::A(cancel));
                    this.phase = CancelPending;
                    return Poll::Pending;
                }

                Poll::Pending
            }

            CancelPending => {
                let cancel_fut = this.cancel_fut.as_mut().unwrap();
                let done = match cancel_fut {
                    CancelFut::A(f) => unsafe { Pin::new_unchecked(f) }.poll(cx).is_ready(),
                    CancelFut::B(f) => unsafe { Pin::new_unchecked(f) }.poll(cx).is_ready(),
                };
                if done {
                    Poll::Ready(unsafe { this.closure_result.assume_init_read() })
                } else {
                    Poll::Pending
                }
            }
        }
    }
}
```

#### Select flow

```
poll 1 (Racing):
  → poll a → Ready(result)
  → call b.cancel() → stores CancelFuture in cancel_fut
  → run closure(result) → store closure_result (SelectThen only)
  → transition to CancelPending
  → return Pending

poll 2 (CancelPending):
  → poll cancel_fut → check submitted bit
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
