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
    ptr: *mut u8,      // pointer to allocated memory (computed once during alloc)
    alloc_size: u32,   // byte count of the allocation
}
```

16 bytes. `ArenaAlloc` is `!Send` and `!Sync` — it cannot leave the task.
Fields are private — access via `ptr(&self) -> *mut u8` and `size(&self) -> u32`.

```rust
impl ArenaAlloc<'_> {
    pub fn ptr(&self) -> *mut u8 { self.ptr }
    pub fn size(&self) -> u32 { self.alloc_size }
}

impl<'a> Drop for ArenaAlloc<'a> {
    #[inline(always)]
    fn drop(&mut self) {
        // Footer is right after the allocated bytes
        let footer = unsafe {
            &mut *(self.ptr.add(self.alloc_size as usize) as *mut AllocFooter)
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
        let ptr = unsafe { self.base_ptr.add(aligned as usize) };
        Some(ArenaAlloc {
            arena: self,
            ptr,
            alloc_size: size,
        })
    }

    fn alloc_type<T>(&mut self) -> Option<ArenaAlloc<'_>> {
        self.alloc(Layout::new::<T>())
    }

    fn alloc_write<T>(&mut self, value: T) -> Option<ArenaAlloc<'_>> {
        let alloc = self.alloc_type::<T>()?;
        unsafe { core::ptr::write(alloc.ptr as *mut T, value); }
        Some(alloc)
    }
}
```

`alloc_write` allocates space for `T` in the arena, writes the value via
`ptr::write` (forgets the original — no drop), and returns the guard.
This is the primary API for serializing buffer types into the arena.

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

## Buffer Traits & IO Pipeline

The buffer system uses a **trait-driven execution pipeline**. Instead of passing raw memory references or wrapper types directly to `io_uring`, the buffer type itself functions as a localized state machine.

By accepting `&mut Task` during execution, each buffer type encapsulates its own memory layout, temporary arena staging, and completion hooks. The higher-level operation runners (`read_bytes`, `write_bytes`) orchestrate the lifecycle without needing to know the underlying allocation strategy.

```
       +-------------------------------------------------------------+
       |                        Driver Loop                          |
       |  (e.g., read_bytes(task, fd, &mut stack_buf[..]).await)    |
       +------------------------------+------------------------------+
                                      |
         1. prepare_read(&mut Task)   |  4. complete_read(&mut Task, bytes)
                                      v
       +-------------------------------------------------------------+
       |                      Buffer Trait Impl                      |
       |  - Staging: Allocates temp space in Task Arena if needed    |
       |  - Finalize: Copies bytes back, rewinds arena, moves ptrs   |
       +------------------------------+------------------------------+
                                      |
         2. Submit SQE                |  3. Poll CQE
                                      v
       +-------------------------------------------------------------+
       |                      io_uring Kernel                        |
       +-------------------------------------------------------------+
```

### In-Future Staging & Automatic Arena Rewinding

When borrowing temporary buffers (such as stack slices `&[u8]` or `&mut [u8]`), raw stack pointers cannot safely remain in-flight with the kernel across suspension points.

To handle this safely:

1. `prepare_read` / `prepare_write` serializes the buffer's metadata (the fat pointer) into the task arena via `ptr::write`. The original is forgotten — no drop is called. The returned data pointer is the original slice pointer (not the arena pointer).
2. Once the CQE completes, `complete_read` / `complete_write` deserializes the metadata from the arena via `ptr::read`, drops the `ArenaAlloc` guard (triggering arena rewind), and returns the buffer.
3. The caller must guarantee the original memory is still valid when `complete_*` is called (unsafe contract).

### Buffer Traits

```rust
pub trait IoReadBuffer: Sized {
    /// Serializes self into the task arena as bytes, stores an ArenaAlloc guard
    /// in inflight_arena_guards, returns (slot, data_ptr, data_len).
    /// The data_ptr/data_len are for SQE submission.
    fn prepare_read(self, task: &mut Task) -> Option<(u32, *mut u8, u32)>;
}

pub trait IoWriteBuffer: Sized {
    /// Serializes self into the task arena as bytes, stores an ArenaAlloc guard
    /// in inflight_arena_guards, returns (slot, data_ptr, data_len).
    /// The data_ptr/data_len are for SQE submission.
    fn prepare_write(self, task: &mut Task) -> Option<(u32, *const u8, u32)>;
}

/// Reads the serialized B back from the arena via ptr::read, drops the guard
/// (rewinds arena), returns B.
///
/// # Safety
/// Slot must correspond to a completed IO operation.
/// Caller must guarantee the buffer's underlying memory is still valid.
pub unsafe fn complete_read<B: Sized>(task: &mut Task, slot: u32) -> Option<B> {
    let ptr = task.buffers.inflight_arena_guards.get(slot)?.ptr();
    let value = core::ptr::read(ptr as *const B);
    task.buffers.inflight_arena_guards.remove(slot);
    Some(value)
}

/// Reads the serialized B back from the arena via ptr::read, drops the guard
/// (rewinds arena), returns B.
///
/// # Safety
/// Slot must correspond to a completed IO operation.
/// Caller must guarantee the buffer's underlying memory is still valid.
pub unsafe fn complete_write<B: Sized>(task: &mut Task, slot: u32) -> Option<B> {
    let ptr = task.buffers.inflight_arena_guards.get(slot)?.ptr();
    let value = core::ptr::read(ptr as *const B);
    task.buffers.inflight_arena_guards.remove(slot);
    Some(value)
}
```

**Serialization pattern** (same for all buffer types):

```rust
fn prepare_read(self, task: &mut Task) -> Option<(u32, *mut u8, u32)> {
    let slot = task.io.free_slot()?;
    task.io.set_submitted(slot, true);
    let buf_ptr = self.as_mut_ptr(); // or self.ptr() for ArenaAlloc
    let buf_len = self.len() as u32; // or self.size() for ArenaAlloc
    // alloc_write: allocates space, ptr::write self into arena (no drop), returns guard
    let arena_ptr: *mut Arena = &mut task.buffers.arena;
    let alloc = unsafe { (*arena_ptr).alloc_write(self)? };
    // Store guard — transmuted to 'static, dropped by complete_* or drain_cqes
    let guards_ptr: *mut Slab<ArenaAlloc<'static>> = &mut task.buffers.inflight_arena_guards;
    let alloc_static: ArenaAlloc<'static> = unsafe { core::mem::transmute(alloc) };
    unsafe { (*guards_ptr).insert_at(slot, alloc_static) };
    Some((slot, buf_ptr, buf_len))
}
```

`alloc_write` serializes the buffer into the arena via `ptr::write` (the original is forgotten — no drop). The arena stores the buffer's metadata (e.g. `Vec` struct, fat pointer). The data pointer returned is for SQE submission.

### Core Implementations

| Buffer Type | Prepare Behavior | Complete Phase |
| --- | --- | --- |
| **`Vec<u8>`** | Serializes `Vec` struct into arena. Returns heap pointer for SQE. | Deserializes `Vec` from arena. Heap buffer still valid. |
| **`ArenaAlloc<'static>`** | Serializes `ArenaAlloc` struct into arena. Returns arena-resolved pointer for SQE. | Deserializes `ArenaAlloc` from arena. Drops guard (rewinds arena). |
| **`&'a [u8]`** | Serializes fat pointer into arena. Returns original pointer for SQE. *(Write-only)* | Deserializes fat pointer. Caller guarantees original memory valid. |
| **`&'a mut [u8]`** | Serializes fat pointer into arena. Returns original pointer for SQE. | Deserializes fat pointer. Caller guarantees original memory valid. |

### Cancellation Safety

io_uring is completion-based — the kernel holds raw pointers to buffer memory while operations are in-flight. If a future is dropped mid-`await` (e.g. inside `select!` or timeouts), the buffer must remain valid until the CQE arrives.

The trait approach handles this naturally:

1. `prepare_read` / `prepare_write` serializes the buffer's metadata (e.g. `Vec` struct, fat pointer) into the task arena and stores an `ArenaAlloc` guard in `inflight_arena_guards[slot]`.
2. If the future is dropped before the IO finishes, the serialized metadata and the arena guard **remain safely held by the `Task`**.
3. `drain_cqes()` removes the guard from `inflight_arena_guards[slot]` when the CQE arrives, triggering arena rewind. For `Vec<u8>`, the heap buffer is leaked (the `Vec` metadata in the arena is freed, but the heap data was separately allocated). For references (`&[u8]`, `&mut [u8]`), the caller is responsible for the original memory.

This completely eliminates use-after-free caused by early future drops.

### Driver Helpers

Because preparation and completion are encapsulated within the trait hooks, I/O operations are decoupled into lightweight free functions:

```rust
pub async fn read_bytes<B: IoReadBuffer>(
    task: &mut Task,
    fd: RawFd,
    buf: B,
) -> (std::io::Result<usize>, B) {
    let (slot, ptr, len) = match buf.prepare_read(task) {
        Some(res) => res,
        None => return (Err(std::io::ErrorKind::OutOfMemory.into()), buf),
    };

    push_read(task, fd, ptr, len, user_data);
    let res = await_cqe(task, slot).await;

    let bytes = if res > 0 { res as usize } else { 0 };
    let completed_buf = unsafe { complete_read::<B>(task, slot) }
        .unwrap_or_else(|| todo!("handle missing guard"));

    let io_result = if res >= 0 {
        Ok(res as usize)
    } else {
        Err(std::io::Error::from_raw_os_error(-res))
    };

    (io_result, completed_buf)
}
```

### Usage

```rust
async fn echo_server(task: &mut Task, client_fd: RawFd) {
    let mut stack_buf = [0u8; 1024];

    loop {
        let (read_res, stack_buf_ref) = read_bytes(task, client_fd, &mut stack_buf[..]).await;
        let bytes_read = match read_res {
            Ok(0) => break, // Connection closed
            Ok(n) => n,
            Err(_) => break,
        };

        let (write_res, stack_buf_ref) = write_bytes(task, client_fd, &stack_buf_ref[..bytes_read]).await;
        if write_res.is_err() { break; }
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

Tasks receive `&mut Task` which gives direct access to `buffers` (arena).
No TLS, no `current_arena()` lookup — the task
reference is passed directly into the future's state via the spawn closure.

---

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
        task.io.set_result(ud.io_slot, cqe.result());
        task.io.set_submitted(ud.io_slot, false);
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
let mut buf = vec![0u8; 4096];
let (result, buf) = read_bytes(task, fd, buf).await;

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
