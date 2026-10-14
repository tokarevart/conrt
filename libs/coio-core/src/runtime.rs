//! The backend-generic callback runtime.
//!
//! Owns the op slabs, their intrusive free lists, the cancel protocol and the
//! caller's global state `G`. The completion mechanism — an `io_uring` ring
//! today — is a [`Backend`], and the runtime cannot reach into it, so a backend
//! can neither invoke a completion callback nor touch `G` or an op's local.

use core::cell::Cell;
use core::mem::ManuallyDrop;
use core::mem::MaybeUninit;
use core::num::NonZeroU32;
use core::num::NonZeroU64;
use std::io;

use crate::backend::Backend;
use crate::backend::Completion;
use crate::buf::IoBufErased;

thread_local! {
    static RUNNING: Cell<bool> = const { Cell::new(false) };
    static ACTIVE_GEN: Cell<NonZeroU32> = const { Cell::new(NonZeroU32::new(1).unwrap()) };
    /// Per-thread, never-reset counter of dispatched op ids. A handle stamped
    /// with an id from this counter can only ever name one op across the whole
    /// lifetime of the thread — including across runtime restarts — so a stale
    /// `Op` can never alias a later op's slot. Mirrors `NEXT_TASK_ID`.
    static NEXT_OP_ID: Cell<NonZeroU64> = const { Cell::new(NonZeroU64::new(1).unwrap()) };
}

pub fn is_running() -> bool {
    RUNNING.with(|c| c.get())
}

pub fn active_gen_matches(generation: NonZeroU32) -> bool {
    active_gen() == Some(generation)
}

pub fn active_gen() -> Option<NonZeroU32> {
    if !RUNNING.with(|c| c.get()) {
        return None;
    }
    Some(ACTIVE_GEN.with(|c| c.get()))
}

pub fn exit_active_gen() {
    RUNNING.with(|c| c.set(false));
}

pub fn enter_active_gen() -> ActiveGenGuard {
    assert!(!RUNNING.with(|c| c.get()));
    RUNNING.with(|c| c.set(true));
    ACTIVE_GEN.with(|c| {
        c.update(|x| x.checked_add(1).expect("active gen overflow"));
    });
    ActiveGenGuard
}

/// RAII guard that marks the runtime active for as long as it is driven, so
/// buffer views taken during a run stay valid and stale ones are detected
/// across restarts. Held by the backend's entry point.
pub struct ActiveGenGuard;

impl Drop for ActiveGenGuard {
    fn drop(&mut self) {
        exit_active_gen();
    }
}

/// Allocates a fresh, never-reused op id from the per-thread counter.
pub(crate) fn next_op_id() -> NonZeroU64 {
    NEXT_OP_ID.with(|c| {
        let id = c.get();
        assert!(id.get() < MAX_OP_ID, "op id space exhausted");
        c.set(id.checked_add(1).expect("op id overflow"));
        id
    })
}

// ── Slot state encoding ─────────────────────────────────────────────
//
// A slab slot is in exactly one of three states, all encoded in a single `u64`
// `id` so no `Option` discriminant is needed and `Vec<IoSlot>` stays safe to
// index:
//
//   id == 0                       free — on the intrusive free stack, `body`
//                                 holds the link to the next free slot
//   id == n                       live — `local` is initialized and owned here
//   id == n | CANCEL_PENDING      live, with a cancellation submitted whose
//                                 completion has not been seen yet
//
// There is no "local taken" state: a slot's local is moved out only in
// `complete_op`, which frees the slot in the same breath, so nothing can
// observe the gap.
//
// The never-reset per-thread `NEXT_OP_ID` counter makes `n` unique for the
// whole lifetime of the thread, so an `Op` handle can never name a slot that a
// later op has taken over, nor an op of a later runtime. Because real ids start
// at 1, a free slot (id 0) never satisfies an `Op`'s id check.
//
// The flag rides along in the same word rather than costing a field. The id
// space shrinks to 63 bits, which the never-reset counter will not reach.

/// A cancellation of this op has been submitted. The op's own completion is
/// what decides the outcome, so this only records that the cancel's completion
/// may still arrive and must not be mistaken for a fresh cancel.
const CANCEL_PENDING: u64 = 1 << 62;

/// Highest id the counter may hand out; both flag bits must stay free.
const MAX_OP_ID: u64 = CANCEL_PENDING;

const ID_FLAGS: u64 = CANCEL_PENDING;

/// The op id carried in `id`, with the state bits stripped. Zero for a free
/// slot, which therefore never matches a real [`Op`].
fn id_bits(id: u64) -> u64 {
    id & !ID_FLAGS
}

/// Whether the slot is on the free stack (and so holds a [`FreeLink`]).
fn is_free(id: u64) -> bool {
    id == 0
}

/// Whether a cancellation of this op is still in flight.
fn is_cancel_pending(id: u64) -> bool {
    id & CANCEL_PENDING != 0
}

// ── Slots ─────────────────────────────────────────────────────────────

// ── Buffer arena ──────────────────────────────────────────────────────

/// Sentinel for "this op carries no caller buffer". Also the index just past
/// the arena's reachable range, so a live cursor can never collide with it.
const BUF_NONE: u32 = u32::MAX;

/// Block sizes, in buffers. `1` covers a plain read or write and `1024` is
/// Linux's `IOV_MAX`, the largest a single `readv`/`writev` may be given. A
/// request for another size rounds up to the next class.
const BUF_CLASSES: [usize; 8] = [1, 2, 4, 8, 16, 64, 256, 1024];

/// Contiguous per-op storage for erased caller buffers.
///
/// The runtime has to hold a buffer of a type only the call site knows, so it
/// stores the erased form; and a shim needs all of an op's buffers as one
/// slice so gather and scatter ops need no runtime length negotiation. Both
/// together mean one op's buffers must occupy a contiguous run of `blocks`.
///
/// Runs are carved out of the same `blocks` vector and recycled through a
/// per-size-class intrusive free list, so nothing is allocated after startup —
/// the same shape as the buffer pools themselves. An op with no buffer
/// allocates nothing at all.
struct BufArena {
    blocks: Vec<IoBufErased>,
    /// Head of each size class's free list, as a block index, or [`BUF_NONE`].
    free: [u32; BUF_CLASSES.len()],
}

impl Default for BufArena {
    fn default() -> Self {
        Self::new()
    }
}

impl BufArena {
    const fn new() -> Self {
        Self {
            blocks: Vec::new(),
            free: [BUF_NONE; BUF_CLASSES.len()],
        }
    }

    /// Reserves a run of `len` contiguous blocks, returning its first index.
    ///
    /// `len` must be non-zero and no greater than `BUF_CLASSES`'s largest
    /// entry; ops are built from const-generic array lengths, so both are known
    /// at the call site and checked here rather than trusted.
    fn alloc(&mut self, len: usize) -> u32 {
        assert!(len > 0, "a buffer run must hold at least one buffer");
        assert!(
            len <= *BUF_CLASSES.last().expect("class table is non-empty"),
            "no buffer run can hold {len} buffers",
        );
        let class = class_for(len);

        let head = self.free[class];
        if head != BUF_NONE {
            // The head is a block index whose run is exactly `BUF_CLASSES[class]`
            // long, which `release` recorded and `alloc` is asking for.
            self.free[class] = next_link(&self.blocks, head);
            return head;
        }

        // No run of this size is free, so extend the tail. Reusing smaller
        // blocks would need splitting, which never pays off here: the classes
        // are powers of two apart precisely so a run is never wasted.
        let base = self.blocks.len();
        assert!(
            base + BUF_CLASSES[class] < BUF_NONE as usize,
            "buffer arena exhausted",
        );
        self.blocks
            .extend(core::iter::repeat_n(IoBufErased::NONE, BUF_CLASSES[class]));
        base as u32
    }

    /// Links a run of `len` buffers, starting at `start`, back onto its size
    /// class's free list.
    ///
    /// `len` comes from the caller because the run length is not recoverable
    /// from the arena: every byte of a block belongs to the caller's buffer, so
    /// there is nowhere in here to keep a side note. The op slot already holds
    /// it.
    fn release(&mut self, start: u32, len: usize) {
        let class = class_for(len);
        self.blocks[start as usize].a = u64::from(self.free[class]);
        self.free[class] = start;
    }
}

/// The size class that serves a run of `len` buffers.
fn class_for(len: usize) -> usize {
    BUF_CLASSES
        .iter()
        .position(|&n| n >= len)
        .expect("every permitted run length is covered by a class")
}

/// The link stored in a free block's first word. Only read while the block is
/// on a free list, where its payload is meaningless.
fn next_link(blocks: &[IoBufErased], index: u32) -> u32 {
    blocks[index as usize].a as u32
}

/// How an op finished.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpOutcome {
    /// The op ran. `result` is the backend's raw result — a byte count, or a
    /// negated errno — and `flags` are its completion flags.
    Completed { result: i32, flags: u32 },
    /// A cancel won the race and the op did not run. When the cancel was
    /// accepted its own callback ran first, holding `&mut` to this op's local.
    Cancelled,
}

/// Everything a completion shim needs, bundled so the call site reads by name
/// rather than by position.
pub struct OpShimArgs<'a, L> {
    /// The op's per-dispatch local, moved back to the callback.
    pub local: L,
    /// The caller's erased buffers, one op's run and therefore contiguous.
    /// Empty for ops the backend fills itself, such as a provided read; `K`
    /// entries for an operation built on a `const K` array.
    pub bufs: &'a [IoBufErased],
    pub outcome: OpOutcome,
    /// The erased callback, cast back by the shim. Null on the teardown path,
    /// which rebuilds the buffers and drops them without calling back.
    pub done: *const (),
    /// Dispatch-time metadata (the read size class), or 0 when unused.
    pub class: u8,
}

/// The completion shim stored per op.
///
/// It is monomorphized per operation kind, which is what lets one function
/// pointer type serve reads, writes, accepts and multi-buffer gathers alike:
/// each shim knows statically how many buffers its op kind carries, so it never
/// has to negotiate a length with the runtime.
///
/// The `B: Backend` bound is needed to name `Runtime<G, L, B>` at all. Alias
/// bounds are not re-checked at each use, but every use here is inside a block
/// that already requires it.
#[allow(type_alias_bounds)]
type OpShim<G, L, B: Backend> = fn(&mut Runtime<G, L, B>, OpShimArgs<'_, L>);

/// A handle to one in-flight op. `id` is stamped from the never-reset
/// per-thread counter (`next_op_id`), so the pair `(slot, id)` uniquely
/// identifies this op for the whole lifetime of the thread: a stale handle can
/// neither name a slot the runtime has since recycled (the new occupant has a
/// fresh id) nor be mistaken for an op of a later runtime (the counter never
/// resets). Copyable and inert.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Op {
    pub(crate) slot: u32,
    /// The op's per-dispatch `class` (e.g. the read size class), kept in the
    /// handle (free in the padding) so `cancel` can rebuild the victim's exact
    /// `user_data` for `ASYNC_CANCEL`.
    pub(crate) class: u8,
    id: NonZeroU64,
}

/// The payload of a live op: the shim that decodes the CQE and the erased
/// caller callback.
struct Live<G, L, B: Backend> {
    shim: OpShim<G, L, B>,
    done: *const (),
}

/// The intrusive free stack: each free slot holds the index of the next free
/// slot, so the stack needs no side allocation — only a head index in the
/// runtime. Stored in the slot's own dead `body` union arm.
struct FreeLink {
    next: u32,
}

/// Sentinel for "end of the free stack".
const FREE_NONE: u32 = u32::MAX;

/// What a slot holds, which depends entirely on its `id`: a live op's dispatch
/// state, or — while free — the link to the next free slot. `local` is
/// deliberately *outside* this union so that moving a slot between live and
/// free never touches the caller's state.
union SlotBody<G, L, B: Backend> {
    live: ManuallyDrop<Live<G, L, B>>,
    free: ManuallyDrop<FreeLink>,
}

/// One op's slot in the slab.
///
/// `local` is the caller's per-op state, moved back into the shim on completion
/// (first-wins with a winning cancel). It is [`MaybeUninit`] rather than
/// `Option` so the slot stays a plain `Sized` value with no discriminant: it is
/// initialized whenever `!is_free(id)` and must not be touched
/// otherwise.
///
/// The `Vec<IoSlot>` invariant is that every slot is in exactly one of the
/// states described above. Every state transition re-checks it with a plain
/// `assert!` rather than a `debug_assert!`: each check is one comparison on a
/// word already in a register, against an op that costs a syscall, and the
/// alternative to catching a violated transition is reading an uninitialized
/// `local` or handing one slot to two ops.
struct IoSlot<G, L, B: Backend> {
    /// See the slot-state table above.
    id: u64,
    local: MaybeUninit<L>,
    /// First block of this op's erased caller buffers, or [`BUF_NONE`]. Sits
    /// outside the union beside `local`, so a free slot — which holds only a
    /// four-byte free link — does not pay for buffer space it never uses.
    buf: u32,
    /// Buffers in that run. Adjacent to `buf` so the pair fills the alignment
    /// padding the slot already had, rather than adding a word.
    buf_len: u32,
    body: SlotBody<G, L, B>,
}

impl<G, L, B: Backend> IoSlot<G, L, B> {
    /// A fresh slot, already on the free stack.
    fn init_free(next: u32) -> Self {
        Self {
            id: 0,
            local: MaybeUninit::uninit(),
            buf: BUF_NONE,
            buf_len: 0,
            body: SlotBody {
                free: ManuallyDrop::new(FreeLink { next }),
            },
        }
    }

    /// Claims this slot for a new op, installing its dispatch state.
    ///
    /// `buf` and `buf_len` describe a run already carved from the arena, or
    /// `BUF_NONE` and 0 for an op that carries no caller buffer.
    fn claim(
        &mut self,
        id: NonZeroU64,
        shim: OpShim<G, L, B>,
        done: *const (),
        local: L,
        buf: u32,
        buf_len: u32,
    ) {
        assert!(is_free(self.id), "claiming a slot that is not free");
        assert!(
            buf != BUF_NONE || buf_len == 0,
            "a buffer-free slot must not name a run",
        );
        self.id = id.get();
        self.local = MaybeUninit::new(local);
        self.buf = buf;
        self.buf_len = buf_len;
        self.body = SlotBody {
            live: ManuallyDrop::new(Live { shim, done }),
        };
    }

    /// Moves the local out, leaving the slot inert.
    fn take_local(&mut self) -> L {
        assert!(!is_free(self.id), "slot does not own a local");
        // SAFETY: `!is_free` guarantees the slot owns an initialized `local`;
        // `assume_init_read` moves it to the caller and leaves the field
        // uninitialized. The caller frees the slot before anyone can observe
        // the gap — nothing runs between those two steps.
        unsafe { self.local.assume_init_read() }
    }

    /// Puts the slot back on the free stack, linking it above `head`.
    ///
    /// Writes only `id` and `body`, never `local`, so this is a two-register
    /// operation that cannot panic or be observed half-applied.
    fn push_free(&mut self, head: u32) {
        assert!(!is_free(self.id), "slot already free");
        // A run still owned here would never be released back to the arena, so
        // its absence is checked rather than assumed. The shim clears `buf` as
        // it takes the buffers, so a normal completion always satisfies this.
        assert!(
            self.buf == BUF_NONE,
            "slot freed while it still owns {} buffer(s)",
            self.buf_len,
        );
        self.id = 0;
        self.buf = BUF_NONE;
        self.buf_len = 0;
        self.body = SlotBody {
            free: ManuallyDrop::new(FreeLink { next: head }),
        };
    }

    /// This op's run of erased buffers, clearing the slot's claim so that
    /// freeing it cannot be forgotten.
    fn take_buf_run(&mut self) -> Option<(u32, usize)> {
        let (start, len) = (self.buf, self.buf_len as usize);
        self.buf = BUF_NONE;
        self.buf_len = 0;
        (start != BUF_NONE).then_some((start, len))
    }

    /// The next free slot above this one; only valid while free.
    fn free_next(&self) -> u32 {
        assert!(is_free(self.id), "not a free slot");
        // SAFETY: `is_free` guarantees the `free` arm of the union is the live
        // one, written by `init_free`/`push_free`.
        unsafe { self.body.free.next }
    }
}

/// One in-flight cancel. `target`/`target_id` name the op slot; the body holds
/// the caller's cancel callback, receiving the cancel's own `local`.
struct CancelSlot<G, L, B: Backend> {
    /// See the slot-state table above; a cancel slot never holds a `local`
    /// that outlives the ack, so only the free/live distinction is needed.
    id: u64,
    target: u32,
    target_id: NonZeroU64,
    local: MaybeUninit<L>,
    body: CancelBody<G, L, B>,
}

/// A cancel's live payload, or — while free — its place on the free stack.
union CancelBody<G, L, B: Backend> {
    live: ManuallyDrop<CancelDone<G, L, B>>,
    free: ManuallyDrop<FreeLink>,
}

struct CancelDone<G, L, B: Backend> {
    done: fn(&mut Runtime<G, L, B>, &mut L, L),
}

impl<G, L, B: Backend> CancelSlot<G, L, B> {
    fn init_free(next: u32) -> Self {
        Self {
            id: 0,
            target: 0,
            target_id: NonZeroU64::new(1).expect("nonzero"),
            local: MaybeUninit::uninit(),
            body: CancelBody {
                free: ManuallyDrop::new(FreeLink { next }),
            },
        }
    }

    fn claim(
        &mut self,
        id: NonZeroU64,
        target: u32,
        target_id: NonZeroU64,
        done: fn(&mut Runtime<G, L, B>, &mut L, L),
        local: L,
    ) {
        assert!(is_free(self.id), "claiming a cancel slot that is not free");
        self.id = id.get();
        self.target = target;
        self.target_id = target_id;
        self.local = MaybeUninit::new(local);
        self.body = CancelBody {
            live: ManuallyDrop::new(CancelDone { done }),
        };
    }

    fn take_local(&mut self) -> L {
        assert!(!is_free(self.id), "cancel slot already freed");
        // SAFETY: a live cancel slot always owns an initialized `local`; it is
        // taken exactly once, immediately before the slot is freed.
        unsafe { self.local.assume_init_read() }
    }

    fn push_free(&mut self, head: u32) {
        assert!(!is_free(self.id), "cancel slot already free");
        self.id = 0;
        self.body = CancelBody {
            free: ManuallyDrop::new(FreeLink { next: head }),
        };
    }

    fn free_next(&self) -> u32 {
        assert!(is_free(self.id), "not a free cancel slot");
        // SAFETY: `is_free` guarantees the `free` arm is live.
        unsafe { self.body.free.next }
    }
}

/// An op completion, as the runtime needs it: the raw result and flags, the
/// dispatch-time class, and whether it is a cancellation rather than a run.
struct CompletedOp {
    result: i32,
    flags: u32,
    class: u8,
    cancelled: bool,
}

// ── Runtime ───────────────────────────────────────────────────────────

/// The callback runtime. Owns the op slabs, the free lists, the cancel protocol
/// and the caller's global state `G`; a [`Backend`] owns the completion
/// mechanism and the buffers. Each op carries its own local state `L`, moved
/// into the op on dispatch and back to a shim on completion.
///
/// The io_uring backend lives in `coio-uring`, which re-exports this as a
/// two-parameter alias; this crate cannot depend on it, so `B` is spelled
/// out here.
pub struct Runtime<G, L, B: Backend> {
    /// The completion mechanism. Deliberately opaque to the rest of the
    /// runtime: a backend cannot reach the slabs, the global, or a callback.
    pub(crate) backend: B,
    /// One entry per op slot ever used. A slot is free, live, or live with its
    /// local taken out; the free ones form an intrusive stack threaded through
    /// `IoSlot::body`, linked from `free_slots_head`.
    io_slab: Vec<IoSlot<G, L, B>>,
    /// Head of the intrusive free-op stack, or [`FREE_NONE`].
    free_slots_head: u32,
    /// One entry per cancel slot ever used; free ones stack the same way.
    cancel_slab: Vec<CancelSlot<G, L, B>>,
    /// Head of the intrusive free-cancel stack, or [`FREE_NONE`].
    free_cancels_head: u32,
    /// Number of ops submitted but not yet completed (their slot is `Some`).
    inflight_ops: u32,
    /// Number of cancel requests submitted but not yet completed.
    inflight_cancels: u32,
    /// Erased caller buffers, carved into one contiguous run per buffered op.
    buf_arena: BufArena,
    /// The caller's global state, taken back out by
    /// [`Self::into_global`] on return.
    global: Option<G>,
}

impl<G, L, B: Backend> Runtime<G, L, B> {
    /// Builds a runtime around `backend`, holding `global` until it is taken
    /// back by [`Self::into_global`].
    ///
    /// Nothing is submitted yet: the caller dispatches the initial ops through
    /// the `&mut Runtime` it gets from its own `boot`, then calls
    /// [`Self::run`].
    pub fn new(backend: B, global: G) -> Self {
        Self {
            backend,
            io_slab: Vec::new(),
            free_slots_head: FREE_NONE,
            cancel_slab: Vec::new(),
            free_cancels_head: FREE_NONE,
            inflight_ops: 0,
            inflight_cancels: 0,
            buf_arena: BufArena::new(),
            global: Some(global),
        }
    }

    /// The backend this runtime drives.
    ///
    /// Only the backend itself is reachable — never a slab, a local, or the
    /// global — so handing this out does not weaken the isolation a
    /// [`Backend`] relies on.
    pub fn backend(&self) -> &B {
        &self.backend
    }

    /// See [`Self::backend`].
    pub fn backend_mut(&mut self) -> &mut B {
        &mut self.backend
    }

    /// Carves a run of `bufs.len()` blocks and copies the caller's buffers
    /// into it, releasing the run if `arm` then fails.
    fn alloc_buf_run(&mut self, bufs: &[IoBufErased]) -> Option<(u32, usize)> {
        if bufs.is_empty() {
            return None;
        }
        let start = self.buf_arena.alloc(bufs.len());
        self.buf_arena.blocks[start as usize..start as usize + bufs.len()].copy_from_slice(bufs);
        Some((start, bufs.len()))
    }

    /// Hands one op's erased buffers to its shim as a slice.
    ///
    /// The arena is moved aside for the duration of the shim, so `bufs` — which
    /// borrows it — and the `&mut Runtime` the shim receives do not alias. The
    /// run is released once the shim returns. A shim that dispatches further
    /// ops allocates from the empty arena left behind, which is correct and
    /// rare enough not to matter.
    fn dispatch_shim(
        &mut self,
        shim: OpShim<G, L, B>,
        run: Option<(u32, usize)>,
        outcome: OpOutcome,
        done: *const (),
        local: L,
        class: u8,
    ) {
        const NONE: [IoBufErased; 0] = [];

        let arena = core::mem::take(&mut self.buf_arena);
        let bufs: &[IoBufErased] = match run {
            Some((start, len)) => &arena.blocks[start as usize..start as usize + len],
            None => &NONE,
        };

        shim(self, OpShimArgs {
            local,
            bufs,
            outcome,
            done,
            class,
        });

        self.buf_arena = arena;
        if let Some((start, len)) = run {
            self.buf_arena.release(start, len);
        }
    }

    /// Runs completions until nothing is in flight.
    pub fn run(&mut self) {
        self.drive();
    }

    /// Takes the caller's global state back out, ending the runtime's use of
    /// it. Dropping the runtime afterwards tears the backend down in order.
    pub fn into_global(mut self) -> G {
        self.global.take().expect("global already taken")
    }

    /// Allocates an op slab slot and marks it live. Pops the intrusive free
    /// stack when it is non-empty, otherwise grows the slab.
    fn alloc_op_slot(&mut self) -> u32 {
        self.inflight_ops += 1;
        if self.free_slots_head != FREE_NONE {
            let index = self.free_slots_head;
            // SAFETY: the head is a slot on the free stack, so its `body` holds
            // the live `free` arm written by `init_free`/`push_free`.
            self.free_slots_head = self.io_slab[index as usize].free_next();
            return index;
        }
        self.io_slab.push(IoSlot::init_free(FREE_NONE));
        (self.io_slab.len() - 1) as u32
    }

    /// Returns `index` to the free stack, dropping any local the slot still
    /// owns. Only call once the CQE for the op has been consumed, so a late
    /// completion can never land on a recycled slot.
    ///
    /// The local is dropped rather than leaked: `local` is a [`MaybeUninit`],
    /// so forgetting the slot no longer runs its destructor.
    pub(crate) fn free_op_slot(&mut self, index: u32) {
        self.inflight_ops -= 1;
        let head = self.free_slots_head;
        let slot = &mut self.io_slab[index as usize];
        // SAFETY: a live slot owns an initialized `local`, and this is the
        // single place that reclaims a slot whose op was never dispatched.
        unsafe { slot.local.assume_init_drop() };
        slot.push_free(head);
        self.free_slots_head = index;
    }

    /// Like [`Self::free_op_slot`], but the local has already been moved out
    /// by the completion, so only `id` and the free link are written.
    fn free_op_slot_taken(&mut self, index: u32) {
        self.inflight_ops -= 1;
        let head = self.free_slots_head;
        self.io_slab[index as usize].push_free(head);
        self.free_slots_head = index;
    }

    fn alloc_cancel_slot(&mut self) -> u32 {
        self.inflight_cancels += 1;
        if self.free_cancels_head != FREE_NONE {
            let index = self.free_cancels_head;
            // SAFETY: the head is a slot on the free stack.
            self.free_cancels_head = self.cancel_slab[index as usize].free_next();
            return index;
        }
        self.cancel_slab.push(CancelSlot::init_free(FREE_NONE));
        (self.cancel_slab.len() - 1) as u32
    }

    /// Frees a cancel slot whose local has been moved out by the ack.
    fn free_cancel_slot(&mut self, index: u32) {
        self.inflight_cancels -= 1;
        let head = self.free_cancels_head;
        self.cancel_slab[index as usize].push_free(head);
        self.free_cancels_head = index;
    }

    /// Frees a cancel slot that is still holding its local, dropping it.
    ///
    /// Used when the `ASYNC_CANCEL` could not be pushed to a full submission
    /// queue: the cancel never reached the kernel, so nothing will ack it.
    fn free_cancel_slot_dropping_local(&mut self, index: u32) {
        self.inflight_cancels -= 1;
        let head = self.free_cancels_head;
        let slot = &mut self.cancel_slab[index as usize];
        // SAFETY: a live cancel slot owns an initialized `local`, and the SQ
        // push failure means this ack will never arrive.
        unsafe { slot.local.assume_init_drop() };
        slot.push_free(head);
        self.free_cancels_head = index;
    }

    /// Registers an op and submits it in one step: takes the caller's `local`,
    /// installs its completion `shim` and erased `done` on a freshly allocated
    /// slot, and hands `req` to the backend to arm.
    ///
    /// `class` is dispatch-time metadata recorded on the returned [`Op`] and
    /// passed back to the backend, which encodes it so the shim can read it on
    /// completion.
    ///
    /// If the backend cannot submit, the slot is released again and the error
    /// is returned; no completion will arrive for it.
    pub fn register_op(
        &mut self,
        shim: OpShim<G, L, B>,
        done: *const (),
        local: L,
        class: u8,
        bufs: &[IoBufErased],
        req: B::Request,
    ) -> io::Result<Op> {
        let index = self.alloc_op_slot();
        let id = next_op_id();
        let run = self.alloc_buf_run(bufs);
        let (buf, buf_len) = run.map_or((BUF_NONE, 0), |(s, n)| (s, n as u32));
        self.io_slab[index as usize].claim(id, shim, done, local, buf, buf_len);
        let op = Op {
            slot: index,
            class,
            id,
        };
        match self.backend.arm(index, class, req) {
            Ok(()) => Ok(op),
            Err(e) => {
                // The request never reached the kernel, so nothing will ever
                // complete this slot and neither it nor its buffer run may
                // linger. `free_op_slot` drops the local and asserts the run has
                // been released, so release it here first.
                if let Some((start, len)) = self.io_slab[index as usize].take_buf_run() {
                    self.buf_arena.release(start, len);
                }
                self.free_op_slot(index);
                Err(e)
            }
        }
    }

    /// Immutable access to the caller's global state.
    pub fn global(&self) -> &G {
        self.global.as_ref().expect("global already taken")
    }

    /// Mutable access to the caller's global state.
    ///
    /// This is sound because the runtime is single-threaded and exactly one
    /// callback runs at a time: a completion callback is invoked from the
    /// drive loop with `&mut self`, and the borrow of the global ends when the
    /// callback returns. Dispatching a further op while holding it is fine —
    /// the op cannot complete until the callback yields back to the drive loop,
    /// by which point the borrow is gone.
    ///
    /// A callback must still not retain this reference past its own body; the
    /// next callback is free to mutate the same state.
    pub fn global_mut(&mut self) -> &mut G {
        self.global.as_mut().expect("global already taken")
    }

    /// Cancels an in-flight op, best effort.
    ///
    /// Returns nothing, because at submission time it cannot know which
    /// callback will run first. Exactly one of these happens next:
    ///
    /// * the op's own callback, always, receiving the moved-back local and an
    ///   [`OpOutcome::Completed`] if the op finished for real, or
    ///   [`OpOutcome::Cancelled`] if this cancel won;
    /// * `done`, only if this cancel won — and then it runs *before* the op's
    ///   callback, holding `&mut` to the local the op slot still owns, so it
    ///   can leave the op's callback something to see.
    ///
    /// If the op has already finished, or the cancellation could not be
    /// submitted, `done` never runs and `local` is dropped: nothing at the call
    /// site can observe that, which is why the op's callback — which always
    /// runs — is where the outcome is reported.
    pub fn cancel(&mut self, op: Op, local: L, done: fn(&mut Runtime<G, L, B>, &mut L, L)) {
        let valid = matches!(
            self.io_slab.get(op.slot as usize).map(|slot| slot.id),
            // A free slot has id 0, which never equals a real op id, so this
            // single comparison rejects both free and recycled slots. A slot
            // already taken by its own completion, or one already cancelled,
            // is rejected too — there is nothing left to cancel.
            Some(id) if id_bits(id) == op.id.get() && !is_cancel_pending(id),
        );
        if !valid {
            return;
        }
        if !self.submit_cancel(op, done, local) {
            // Submission failed, so no cancellation is pending and the op slot
            // keeps no `CANCEL_PENDING` mark. `submit_cancel` has already
            // dropped the cancel's local and freed its slot.
            return;
        }
        self.io_slab[op.slot as usize].id |= CANCEL_PENDING;
    }

    /// Submits a cancellation of `victim`, carrying the cancel's own `local`
    /// for `done`. Returns whether it was submitted.
    fn submit_cancel(
        &mut self,
        victim: Op,
        done: fn(&mut Runtime<G, L, B>, &mut L, L),
        local: L,
    ) -> bool {
        let index = self.alloc_cancel_slot();
        self.cancel_slab[index as usize].claim(next_op_id(), victim.slot, victim.id, done, local);
        match self.backend.cancel(index, victim.slot, victim.class) {
            Ok(true) => true,
            Ok(false) | Err(_) => {
                // Ring full: the victim's natural completion still recycles its
                // slot; give up the cancel slot and report no cancellation. No
                // completion will ever arrive, so this path drops the cancel's
                // own local.
                self.free_cancel_slot_dropping_local(index);
                false
            }
        }
    }

    /// Dispatches one op completion.
    ///
    /// This is where a cancellation is decided, because the op's own completion
    /// is the only thing that settles it: a backend reports success for a
    /// cancel only when it actually cancelled the target, which fixes the
    /// target's result at `-ECANCELED`, and reports failure when the target
    /// had already gone. So a cancel that wins is recognised here, without
    /// waiting for the cancel's own completion — which is what lets the
    /// cancel's callback run first.
    fn complete_op(&mut self, index: u32, completion: CompletedOp) {
        let Some(slot) = self.io_slab.get(index as usize) else {
            return;
        };
        if is_free(slot.id) {
            // Slot already recycled; a stale completion can never name a live op.
            return;
        }
        let cancel_pending = is_cancel_pending(slot.id);
        let cancelled = cancel_pending && completion.cancelled;

        // Read the dispatch state out of the live union arm, then move the
        // local and the buffers. Both are guarded by the `is_free` check above.
        let (shim, done) = {
            let body = &self.io_slab[index as usize].body;
            // SAFETY: `!is_free` guarantees the `live` arm is the live one,
            // written by `claim` and untouched since.
            let live = unsafe { &body.live };
            // SAFETY: `Live` is `Copy` data (two function pointers).
            (live.shim, live.done)
        };
        let run = self.io_slab[index as usize].take_buf_run();
        let mut local = self.io_slab[index as usize].take_local();

        if cancel_pending {
            // The op's own completion settles any cancellation of it, so the
            // cancel slot is spent either way — but exactly one of these two
            // runs, and which one depends on whether the cancel won. Doing the
            // losing case first would free the slot the winning one still needs
            // to read its callback out of.
            if cancelled {
                // The local is still in the slot, so the cancel's callback can
                // reach through `&mut` and leave something for the op's callback.
                self.dispatch_cancel_of(index, &mut local);
            } else {
                self.drop_cancel_of(index);
            }
            self.io_slab[index as usize].id &= !CANCEL_PENDING;
        }

        self.free_op_slot_taken(index);

        let outcome = if cancelled {
            OpOutcome::Cancelled
        } else {
            OpOutcome::Completed {
                result: completion.result,
                flags: completion.flags,
            }
        };
        // The shim is monomorphized per op kind, so it knows how many buffers it
        // was given and asserts that itself; the runtime only hands over the run
        // as a slice.
        self.dispatch_shim(shim, run, outcome, done, local, completion.class);
    }

    /// Runs the cancellation callback of the cancel targeting `index`, if there
    /// is one, and consumes its slot.
    ///
    /// The op's local is borrowed for the duration, so the cancel can set
    /// something the op's callback will then observe.
    fn dispatch_cancel_of(&mut self, index: u32, victim_local: &mut L) {
        let Some(cancel) = self.find_cancel_of(index) else {
            return;
        };
        let done = {
            let cancel = &self.cancel_slab[cancel as usize];
            // SAFETY: `find_cancel_of` only returns a live slot, whose `live`
            // arm was written by `claim` and is not touched since. `CancelDone`
            // is a single function pointer (Copy).
            unsafe { cancel.body.live.done }
        };
        let clocal = self.cancel_slab[cancel as usize].take_local();
        self.free_cancel_slot(cancel);
        done(self, victim_local, clocal);
    }

    /// Drops the cancellation of `index` without running its callback: the op
    /// completed for real, so the cancellation lost.
    fn drop_cancel_of(&mut self, index: u32) {
        let Some(cancel) = self.find_cancel_of(index) else {
            return;
        };
        self.free_cancel_slot_dropping_local(cancel);
    }

    /// The live cancel slot targeting `index`, if one is still outstanding.
    ///
    /// The cancel slab holds one entry per in-flight cancellation, so this is a
    /// scan over a handful of entries and it only runs when a cancellation is
    /// actually pending. The match is exact — target slot and target id — so a
    /// cancel recycled onto this slot can never be mistaken for the one that
    /// belongs here.
    fn find_cancel_of(&self, index: u32) -> Option<u32> {
        let target_id = {
            let slot = self.io_slab.get(index as usize)?;
            if is_free(slot.id) {
                return None;
            }
            slot.id & !CANCEL_PENDING
        };
        self.cancel_slab
            .iter()
            .position(|c| !is_free(c.id) && c.target == index && c.target_id.get() == target_id)
            .map(|i| i as u32)
    }

    /// Settles a cancellation whose own completion has been seen.
    ///
    /// A zero result means the cancellation took effect, and then this does
    /// nothing at all: the op's own completion is what reports the outcome, and
    /// it is what dispatches the cancel's callback before the op's. Consuming
    /// the slot here would race that and lose the callback whenever the
    /// cancellation's completion lands first, which is the common case.
    fn complete_cancel(&mut self, index: u32, result: i32) {
        let Some(cancel) = self.cancel_slab.get(index as usize) else {
            return;
        };
        if is_free(cancel.id) {
            // The op's own completion already consumed this cancellation and
            // freed its slot, so this completion has nothing left to report.
            return;
        }
        if result == 0 {
            return;
        }

        // The cancellation never took effect — the target had already gone — so
        // the callback is suppressed, its local dropped, and the op's
        // `CANCEL_PENDING` mark cleared. Left standing, the op would go on
        // looking for a cancellation that no longer exists.
        let (target, target_id) = (cancel.target, cancel.target_id);
        // SAFETY: a live cancel slot owns its initialized local, taken exactly
        // once here, immediately before the slot is freed.
        drop(self.cancel_slab[index as usize].take_local());
        self.free_cancel_slot(index);

        if let Some(slot) = self.io_slab.get_mut(target as usize)
            && is_cancel_pending(slot.id)
            && id_bits(slot.id) == target_id.get()
        {
            slot.id &= !CANCEL_PENDING;
        }
    }

    /// Checks that a slot index handed back by the backend actually names a
    /// live slot of the right slab.
    ///
    /// `Backend` is public and implementable from outside, so every field that
    /// crosses the boundary is untrusted input. This is a single comparison on
    /// data already in cache, against an op that costs a syscall, so it is an
    /// `assert!` rather than a `debug_assert!`: the whole point of publishing
    /// the trait is that a wrong implementation fails loudly instead of handing
    /// one op's `local` to another. A release build therefore panics on a
    /// misbehaving backend rather than silently dropping a completion, which
    /// would surface much later as a hang.
    ///
    /// The path runs in the drive loop and never inside [`Drop`], so a panic
    /// here unwinds normally and the ordered teardown still runs.
    fn assert_names_live(slot: u32, slab_len: usize, free: bool) {
        assert!(
            (slot as usize) < slab_len,
            "backend returned out-of-range slot {slot}",
        );
        assert!(
            !free,
            "backend completed slot {slot}, which is on the free stack",
        );
    }

    /// Routes one completion to the slab it names — the cancel slab for a
    /// cancel request, the op slab otherwise. The two index spaces are
    /// independent, so each is checked against its own slab.
    fn take_completion(&mut self, c: Completion) {
        if c.cancel {
            // Range-checked, but deliberately *not* free-checked. A cancel slot
            // is legitimately consumed before its own completion arrives: the
            // op's completion can win the race, dispatch the cancel's callback
            // and free the slot, leaving this completion with nothing to do.
            // That is the common ordering, not a misbehaving backend, and
            // `complete_cancel` already handles a free slot by dropping it.
            assert!(
                (c.slot as usize) < self.cancel_slab.len(),
                "backend returned out-of-range cancel slot {}",
                c.slot,
            );
            self.complete_cancel(c.slot, c.result);
        } else {
            let idx = c.slot as usize;
            Self::assert_names_live(c.slot, self.io_slab.len(), is_free(self.io_slab[idx].id));
            self.complete_op(c.slot, CompletedOp {
                result: c.result,
                flags: c.flags,
                class: c.class,
                cancelled: c.cancelled,
            });
        }
    }

    fn drain_cqes(&mut self) {
        // One completion at a time, with no buffering: see
        // `Uring::next_completion` for why the queue guard must not outlive the
        // pop.
        //
        // The loop terminates because dispatch only pushes to the submission
        // queue: nothing reaches the kernel until `drive` submits, so the set
        // of completions arriving here is finite.
        while let Some(c) = self.backend.next_completion() {
            self.take_completion(c);
        }
    }

    fn has_inflight(&self) -> bool {
        self.inflight_ops != 0 || self.inflight_cancels != 0
    }

    /// The main loop: run completions until nothing is in flight.
    fn drive(&mut self) {
        loop {
            self.drain_cqes();
            if !self.has_inflight() {
                break;
            }
            // A backend error ends the loop with ops still in flight; `Drop`
            // then tears them down in order.
            if self.backend.wait().is_err() {
                break;
            }
        }
    }
}

/// Teardown order matters and is load-bearing.
///
/// The runtime holds memory a backend can still be writing into: for
/// `io_uring` the pools back every read's provided-buffer ring and every
/// `write_fixed` buffer, and an op's `L` may own a buffer pointing at that
/// same slab. So the teardown must be strictly:
///
/// 1. [`Backend::begin_teardown`] — unregister the buffer rings (needs the fd
///    open), then close it. The kernel cancels every in-flight request and does
///    not return until any worker mid-execution has finished, so after this
///    returns **no backend op can touch pool or local memory**;
/// 2. only then drop the un-consumed op locals (which may return pooled
///    buffers);
/// 3. finally let the backend free the pools.
///
/// Steps 2 and 3 cannot be left to field order: `backend` is declared first,
/// so it would be dropped *before* step 2 ran and would free the slab while
/// locals still point into it. Hence the explicit call here, and the reason the
/// pools are freed by a plain field drop afterwards.
///
/// The `io-uring` crate encodes the same invariant for its own maps by
/// dropping `MemoryMap` before `fd`; we do the same for the slab by closing
/// the fd before touching `L` or the pools.
///
/// A normal `drive()` drains to quiescence, so steps 2–3 are no-ops there;
/// they matter only when a panic unwinds out of `boot`/`drive`, or when
/// `drive` breaks early on a backend error with ops still in flight.
impl<G, L, B: Backend> Drop for Runtime<G, L, B> {
    fn drop(&mut self) {
        // 1 + 2. Hand the backend its chance to quiesce while it still owns
        //    everything.
        self.backend.begin_teardown();

        // 3. Safe now: no backend op is live, so drop the locals of every op that never
        //    completed. Free slots hold a `FreeLink`, not a live payload, so their
        //    uninitialized `local` must not be touched.
        for slot in self.io_slab.iter_mut() {
            if !is_free(slot.id) {
                // SAFETY: a live op slot still owns its initialized local, and no
                // other path will drop it now.
                unsafe { slot.local.assume_init_drop() };
            }
        }
        for slot in self.cancel_slab.iter_mut() {
            if !is_free(slot.id) {
                // SAFETY: a live cancel slot whose ack never arrived still
                // owns its initialized local.
                unsafe { slot.local.assume_init_drop() };
            }
        }
        // 4. `backend` (and the slabs, which are plain `Vec`s of POD-ish slots)
        //    drops here, after the locals.
    }
}

#[cfg(test)]
mod slot_layout_tests {
    use super::*;

    /// A backend that is never driven, so the slot layout can be measured
    /// without an `io_uring` ring. Its presence is also the point: `Backend` is
    /// public, and implementing it needs nothing from this crate.
    struct Unused;

    impl Backend for Unused {
        type Config = ();
        type Request = ();
        fn attach(_: &()) -> io::Result<Self> {
            unimplemented!()
        }
        fn arm(&mut self, _: u32, _: u8, _: ()) -> io::Result<()> {
            unimplemented!()
        }
        fn wait(&mut self) -> io::Result<()> {
            unimplemented!()
        }
        fn next_completion(&mut self) -> Option<Completion> {
            unimplemented!()
        }
        fn cancel(&mut self, _: u32, _: u32, _: u8) -> io::Result<bool> {
            unimplemented!()
        }
        fn begin_teardown(&mut self) {
            unimplemented!()
        }
    }

    type TestSlot<G, L> = IoSlot<G, L, Unused>;

    #[test]
    fn io_slot_has_no_option_overhead() {
        // Overhead must be exactly id(8) + buf(4) + buf_len(4) + body union(16)
        // + L, i.e. the local is stored inline with no discriminant, no
        // `Option` wrapper, and no separate state byte. This is the whole point
        // of encoding free-ness in `id` and threading the free stack through the
        // slot body.
        //
        // The two `u32`s are the buffer cursor and its length. They sit outside
        // the union precisely so a free slot — which holds only a four-byte free
        // link — does not grow to pay for buffer space it never uses; what this
        // asserts is that together they cost exactly one word, by landing in the
        // alignment padding `id` and `local` already left.
        struct L(#[allow(dead_code)] u64, #[allow(dead_code)] u64);
        assert_eq!(
            size_of::<TestSlot<(), L>>(),
            32 + size_of::<L>(),
            "IoSlot should be exactly 32 bytes of overhead plus L"
        );
        // The slab is a plain `Vec<IoSlot>`. Because `id` is a bare `u64`
        // (0 == free) rather than `NonZeroU64`, `Option` has no niche to borrow
        // and would now cost a full discriminant word — which is exactly why
        // free-ness moved into `id` rather than wrapping the slot.
        assert!(
            size_of::<Option<TestSlot<(), L>>>() > size_of::<TestSlot<(), L>>(),
            "Option would no longer be free, so the slab must not use it"
        );
    }

    #[test]
    fn cancel_slot_has_no_option_overhead() {
        struct L(#[allow(dead_code)] u64, #[allow(dead_code)] u64);
        // id(8) + target(4, padded to 8) + target_id(8) + body union(8) + L.
        assert_eq!(
            size_of::<CancelSlot<(), L, Unused>>(),
            32 + size_of::<L>(),
            "CancelSlot should be 32 bytes of overhead plus L"
        );
    }

    #[test]
    fn free_slot_reports_link_but_is_never_read_as_live() {
        // The free stack is threaded through the slot's own body, so walking it
        // must never require a side allocation and must survive reuse.
        struct L(#[allow(dead_code)] u64);
        let mut a: TestSlot<(), L> = IoSlot::init_free(FREE_NONE);
        let b: TestSlot<(), L> = IoSlot::init_free(7);

        assert!(is_free(a.id));
        assert!(is_free(b.id));

        // Claim one, then re-free it on top of the stack.
        a.claim(
            NonZeroU64::new(1).unwrap(),
            |_, _| {},
            core::ptr::null(),
            L(1),
            BUF_NONE,
            0,
        );
        assert!(!is_free(a.id));

        let local = a.take_local();
        assert_eq!(local.0, 1);
        a.push_free(b.free_next());
        assert!(is_free(a.id));
        assert_eq!(a.free_next(), 7);
    }

    #[test]
    fn buf_arena_hands_out_contiguous_runs_and_recycles_them() {
        let mut arena = BufArena::new();

        // A run of three rounds up to the four-block class, and the three
        // buffers it was given stay adjacent.
        let run = arena.alloc(3);
        for i in 0..3 {
            arena.blocks[run as usize + i] = IoBufErased {
                a: 0x100 + i as u64,
                b: 0x200,
            };
        }
        assert_eq!(&arena.blocks[run as usize..run as usize + 3], &[
            IoBufErased { a: 0x100, b: 0x200 },
            IoBufErased { a: 0x101, b: 0x200 },
            IoBufErased { a: 0x102, b: 0x200 },
        ],);

        // Releasing returns the whole class-sized run, not just what was used.
        arena.release(run, 3);

        // So the next request for that class gets the same memory back, with no
        // growth at all — the whole point of the free lists.
        let again = arena.alloc(3);
        assert_eq!(again, run, "a freed run must be reused");
        assert_eq!(arena.blocks.len(), 4, "reuse must not grow the arena");
    }

    #[test]
    fn buf_arena_keeps_classes_separate() {
        let mut arena = BufArena::new();
        let one = arena.alloc(1);
        let four = arena.alloc(4);
        assert_ne!(one, four);

        // Four runs of one block fit the 1-class; the 4-class is untouched.
        let more: Vec<u32> = (0..8).map(|_| arena.alloc(1)).collect();
        assert!(
            !more.contains(&four),
            "the 1-class must not hand out the 4-class's block",
        );
        // 1 (the first single) + 4 (the class-sized run) + 8 more singles.
        assert_eq!(arena.blocks.len(), 13);
    }

    #[test]
    fn buf_arena_rejects_a_run_it_cannot_serve() {
        let mut arena = BufArena::new();
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| arena.alloc(0))).is_err());
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                arena.alloc(BUF_CLASSES.last().unwrap() + 1)
            }))
            .is_err(),
        );
    }

    #[test]
    fn an_unclaimed_slot_owns_no_buffers() {
        struct L(#[allow(dead_code)] u64);
        let mut slot: TestSlot<(), L> = IoSlot::init_free(FREE_NONE);
        assert_eq!(slot.buf, BUF_NONE);
        assert_eq!(slot.buf_len, 0);
        assert_eq!(slot.take_buf_run(), None);

        slot.claim(
            NonZeroU64::new(1).unwrap(),
            |_, _| {},
            core::ptr::null(),
            L(1),
            4,
            2,
        );
        assert_eq!(slot.take_buf_run(), Some((4, 2)));
        assert_eq!(slot.take_buf_run(), None, "a run is taken exactly once");

        // Freeing a slot that still owns a run would strand it, so that is
        // rejected rather than quietly dropped.
        slot.push_free(FREE_NONE);
        slot.claim(
            NonZeroU64::new(2).unwrap(),
            |_, _| {},
            core::ptr::null(),
            L(2),
            8,
            1,
        );
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| slot.push_free(FREE_NONE)))
                .is_err(),
            "a slot still owning a buffer run must not be freed",
        );
    }

    #[test]
    fn op_class_rides_in_padding() {
        // slot(4) + class(1) + 3 padding + id(8) = 16, class is free.
        assert_eq!(size_of::<Op>(), 16);
    }
}
