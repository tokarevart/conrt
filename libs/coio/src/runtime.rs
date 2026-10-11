use core::cell::Cell;
use core::mem::ManuallyDrop;
use core::mem::MaybeUninit;
use core::num::NonZeroU32;
use core::num::NonZeroU64;
use std::alloc::Layout;
use std::alloc::alloc;
use std::alloc::dealloc;
use std::alloc::handle_alloc_error;
use std::ptr::NonNull;

use crate::buf::FixedPool;
use crate::buf::FixedSlab;
use crate::buf::ProvidedPool;
use crate::buf::ProvidedSlab;
use crate::buf::pool::Slab;
use crate::classes::BUFFER_MAX_ALIGN;
use crate::classes::DEFAULT_SIZE_CLASSES;
use crate::classes::SizeClass;
use crate::classes::bid_class;
use crate::classes::bid_local;
use crate::classes::bid_provided;
use crate::classes::layout_classes;

thread_local! {
    static RUNNING: Cell<bool> = const { Cell::new(false) };
    static ACTIVE_GEN: Cell<NonZeroU32> = const { Cell::new(NonZeroU32::new(1).unwrap()) };
    static CURRENT_POOLS: Cell<*mut Pools> = const { Cell::new(core::ptr::null_mut()) };
    /// Per-thread, never-reset counter of dispatched op ids. A handle stamped
    /// with an id from this counter can only ever name one op across the whole
    /// lifetime of the thread — including across runtime restarts — so a stale
    /// `Op` can never alias a later op's slot. Mirrors `NEXT_TASK_ID`.
    static NEXT_OP_ID: Cell<NonZeroU64> = const { Cell::new(NonZeroU64::new(1).unwrap()) };
}

pub(crate) fn is_running() -> bool {
    RUNNING.with(|c| c.get())
}

pub(crate) fn active_gen_matches(generation: NonZeroU32) -> bool {
    active_gen() == Some(generation)
}

pub(crate) fn active_gen() -> Option<NonZeroU32> {
    if !RUNNING.with(|c| c.get()) {
        return None;
    }
    Some(ACTIVE_GEN.with(|c| c.get()))
}

pub(crate) fn exit_active_gen() {
    RUNNING.with(|c| c.set(false));
}

pub(crate) fn enter_active_gen() -> ActiveGenGuard {
    assert!(!RUNNING.with(|c| c.get()));
    RUNNING.with(|c| c.set(true));
    ACTIVE_GEN.with(|c| {
        c.update(|x| x.checked_add(1).expect("active gen overflow"));
    });
    ActiveGenGuard
}

/// RAII guard that marks the runtime active for the duration of a
/// [`block_on`] call, so buffer views taken during it stay valid and stale
/// ones are detected across restarts.
pub(crate) struct ActiveGenGuard;

impl Drop for ActiveGenGuard {
    fn drop(&mut self) {
        exit_active_gen();
    }
}

pub(crate) fn set_current_pools(pools: *mut Pools) {
    CURRENT_POOLS.with(|c| c.set(pools));
}

pub(crate) fn clear_current_pools() {
    CURRENT_POOLS.with(|c| c.set(core::ptr::null_mut()));
}

/// Gives `f` mutable access to the active runtime's buffer pools. `Pools` is
/// the non-generic half of a runtime, so buffer views can reach their slabs
/// without naming the runtime's `G`/`L`.
pub(crate) fn with_pools<R>(f: impl FnOnce(&mut Pools) -> R) -> R {
    assert!(is_running(), "with_pools called outside an active runtime");
    let ptr = CURRENT_POOLS.with(|c| c.get());
    assert!(!ptr.is_null(), "no active runtime");
    unsafe { f(&mut *ptr) }
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

// ── View routing ─────────────────────────────────────────────────────
//
// The [`crate::buf`] view types never reach into the pools directly: every
// borrow operation is dispatched here on the packed `bid` (provided vs. fixed
// pool). A view whose generation no longer matches the active runtime is
// stale — its runtime has shut down and its slot may have been freed — so
// drop and clone are no-ops (the slot is leaked rather than touched), while
// resolve and the exclusive transitions panic.

/// Resolves a view's guarded slot to the start of its memory plus the view's
/// sub-slot byte `offset`. Panics when called outside the runtime that owns
/// the slot.
pub(crate) fn resolve_ptr(bid: u32, generation: NonZeroU32, offset: u32) -> NonNull<u8> {
    assert!(
        active_gen_matches(generation),
        "resolve_ptr called outside the runtime that owns this buffer"
    );
    with_pools(|p| {
        let base = p.with_slab(bid, |slab, local| slab.slot_ptr(local));
        unsafe { base.add(offset as usize) }
    })
}

/// Registers one more shared borrower: a cloned [`crate::buf::Ref`]. A stale
/// view is dropped after its runtime shut down and leaks its slot.
pub(crate) fn clone_view(bid: u32, generation: NonZeroU32) {
    if !active_gen_matches(generation) {
        return;
    }
    with_pools(|p| p.with_slab(bid, |slab, local| slab.tracker_mut().clone_shared(local)))
}

/// Releases one borrower of a view's slot, recycling the slot when the last
/// view drops. `exclusive` selects the exclusive (`RefMut`/`SliceMut`) vs.
/// shared (`Ref`/`Slice`) release. A stale view leaks its slot instead.
pub(crate) fn drop_view(bid: u32, generation: NonZeroU32, exclusive: bool) {
    if !active_gen_matches(generation) {
        return;
    }
    with_pools(|p| p.with_slab(bid, |slab, local| slab.drop_view(exclusive, local)))
}

/// Flips a sole shared `Ref` to an exclusive `RefMut`. Panics unless the slot
/// has exactly one shared holder.
pub(crate) fn upgrade_view(bid: u32, generation: NonZeroU32) {
    assert!(
        active_gen_matches(generation),
        "upgrade_view called outside the runtime that owns this buffer"
    );
    with_pools(|p| p.with_slab(bid, |slab, local| slab.tracker_mut().upgrade(local)))
}

/// Flips a sole exclusive `RefMut` to a shared `Ref`. Panics unless the slot
/// has exactly one exclusive holder.
pub(crate) fn downgrade_view(bid: u32, generation: NonZeroU32) {
    assert!(
        active_gen_matches(generation),
        "downgrade_view called outside the runtime that owns this buffer"
    );
    with_pools(|p| p.with_slab(bid, |slab, local| slab.tracker_mut().downgrade(local)))
}

/// Runs a closure exactly once when dropped, even on panic unwind.
struct DropGuard<F: FnOnce()>(Option<F>);

impl<F: FnOnce()> DropGuard<F> {
    fn new(f: F) -> Self {
        Self(Some(f))
    }
}

impl<F: FnOnce()> Drop for DropGuard<F> {
    fn drop(&mut self) {
        if let Some(f) = self.0.take() {
            f()
        }
    }
}

/// The buffer-pool half of a runtime: the shared buffer slab plus the provided
/// and fixed pools built over it. Deliberately non-generic — buffer views
/// reach it through [`with_pools`] without naming the runtime's `G`/`L`.
pub struct Pools {
    /// Start of the shared buffer slab, allocated aligned to at most one page.
    pub slab: NonNull<u8>,
    /// The layout of the slab allocation, used to free it on drop.
    slab_layout: Layout,
    pub(crate) provided_pool: ProvidedPool,
    pub(crate) fixed_pool: FixedPool,
}

impl Drop for Pools {
    fn drop(&mut self) {
        unsafe { dealloc(self.slab.as_ptr(), self.slab_layout) };
    }
}

impl Pools {
    /// Hands the closure the slab of the `bid`'s direction and class, plus its
    /// slot local id, decoding `bid` once for all borrow operations.
    pub(crate) fn with_slab<R>(&mut self, bid: u32, f: impl FnOnce(&mut dyn Slab, u32) -> R) -> R {
        let class = bid_class(bid);
        let local = bid_local(bid);
        if bid_provided(bid) {
            f(self.provided_pool.slab_mut(class), local)
        } else {
            f(self.fixed_pool.slab_mut(class), local)
        }
    }
}

// ── user_data encoding ────────────────────────────────────────────────

#[repr(C)]
#[derive(Clone, Copy)]
struct UserData {
    index: u32,
    /// High bit set marks a cancel completion; the rest is zero. The handler
    /// routes on this bit: an op CQE invokes the target slot's `shim`, a
    /// cancel CQE runs the cancel handshake for that cancel slot.
    tag: u32,
}

const CANCEL_TAG: u32 = 1 << 31;

impl From<UserData> for u64 {
    fn from(ud: UserData) -> u64 {
        ud.index as u64 | ((ud.tag as u64) << 32)
    }
}

impl From<u64> for UserData {
    fn from(raw: u64) -> Self {
        UserData {
            index: raw as u32,
            tag: (raw >> 32) as u32,
        }
    }
}

// ── Slot state encoding ─────────────────────────────────────────────
//
// A slab slot is in exactly one of three states, all encoded in a single `u64`
// `id` so no `Option` discriminant is needed and `Vec<IoSlot>` stays safe to
// index:
//
//   id == 0                    free — on the intrusive free stack, `body` holds
//                              the link to the next free slot
//   id == n      (1 <= n < 2^63) live — `local` is initialized and owned here
//   id == n | LOCAL_TAKEN      live, but `local` has been moved out (a winning
//                              cancel took it, or `complete_op` did) and so
//                              must never be read or dropped again
//
// The never-reset per-thread `NEXT_OP_ID` counter makes `n` unique for the
// whole lifetime of the thread, so an `Op` handle can never name a slot that a
// later op has taken over, nor an op of a later runtime. Because real ids start
// at 1, a free slot (id 0) never satisfies an `Op`'s id check.
//
// `LOCAL_TAKEN` rides along in the same word rather than costing a field, and
// doubles as the first-wins cancel mark: the victim's later CQE sees it and
// suppresses its callback.

const LOCAL_TAKEN: u64 = 1 << 63;

/// Highest id the counter may hand out; [`LOCAL_TAKEN`] must stay free.
const MAX_OP_ID: u64 = LOCAL_TAKEN;

/// The op id carried in `id`, with the [`LOCAL_TAKEN`] bit stripped. Zero for a
/// free slot, which therefore never matches a real [`Op`].
fn id_bits(id: u64) -> u64 {
    id & !LOCAL_TAKEN
}

/// Whether the slot is on the free stack (and so holds a [`FreeLink`]).
fn is_free(id: u64) -> bool {
    id == 0
}

/// Whether this op's `local` has already been moved out of the slot.
fn is_taken(id: u64) -> bool {
    id & LOCAL_TAKEN != 0
}

// ── Slots ─────────────────────────────────────────────────────────────

/// The completion shim stored per op. Receives the moved-back `local`, the raw
/// CQE (`result` — negative errno — and `flags`), the erased `done` fn pointer
/// (which the shim casts back to its concrete type before invoking), and the
/// slot's `state` byte (op metadata, e.g. the read size class).
type OpShim<G, L> = fn(&mut Runtime<G, L>, L, i32, u32, *const (), u8);

/// A handle to one in-flight op. `id` is stamped from the never-reset
/// per-thread counter [`next_op_id`], so the pair `(slot, id)` uniquely
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

/// The result a `cancel` reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CancelStatus {
    /// The cancel was accepted; `done` fires — receiving the target op's
    /// `local` — only if the cancellation beats the op's own completion.
    Accepted,
    /// The op had already completed (recycled slot or mismatched id), or the
    /// submission failed. `done` is not called and nothing is pending.
    AlreadyCompleted,
}

/// The payload of a live op: the shim that decodes the CQE and the erased
/// caller callback.
struct Live<G, L> {
    shim: OpShim<G, L>,
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
union SlotBody<G, L> {
    live: ManuallyDrop<Live<G, L>>,
    free: ManuallyDrop<FreeLink>,
}

/// One op's slot in the slab.
///
/// `local` is the caller's per-op state, moved back into the shim on completion
/// (first-wins with a winning cancel). It is [`MaybeUninit`] rather than
/// `Option` so the slot stays a plain `Sized` value with no discriminant: it is
/// initialized whenever `!is_free(id) && !is_taken(id)` and must not be touched
/// otherwise.
///
/// The `Vec<IoSlot>` invariant is that every slot is in exactly one of the
/// states described above. Every state transition re-checks it with a plain
/// `assert!` rather than a `debug_assert!`: each check is one comparison on a
/// word already in a register, against an op that costs a syscall, and the
/// alternative to catching a violated transition is reading an uninitialized
/// `local` or handing one slot to two ops.
struct IoSlot<G, L> {
    /// See the slot-state table above.
    id: u64,
    local: MaybeUninit<L>,
    body: SlotBody<G, L>,
}

impl<G, L> IoSlot<G, L> {
    /// A fresh slot, already on the free stack.
    fn init_free(next: u32) -> Self {
        Self {
            id: 0,
            local: MaybeUninit::uninit(),
            body: SlotBody {
                free: ManuallyDrop::new(FreeLink { next }),
            },
        }
    }

    /// Claims this slot for a new op, installing its dispatch state.
    fn claim(&mut self, id: NonZeroU64, shim: OpShim<G, L>, done: *const (), local: L) {
        assert!(is_free(self.id), "claiming a slot that is not free");
        self.id = id.get();
        self.local = MaybeUninit::new(local);
        self.body = SlotBody {
            live: ManuallyDrop::new(Live { shim, done }),
        };
    }

    /// Moves the local out, leaving the slot inert.
    ///
    /// Only valid while `!is_taken(id)`.
    fn take_local(&mut self) -> L {
        assert!(
            !is_free(self.id) && !is_taken(self.id),
            "slot does not own a local"
        );
        // SAFETY: `!is_free && !is_taken` guarantees the slot owns an
        // initialized `local`; `assume_init_read` moves it to the caller and
        // leaves the field uninitialized. The caller is responsible for either
        // marking `LOCAL_TAKEN` or freeing the slot before anyone can observe
        // the gap — nothing runs between those two steps.
        unsafe { self.local.assume_init_read() }
    }

    /// Puts the slot back on the free stack, linking it above `head`.
    ///
    /// Writes only `id` and `body`, never `local`, so this is a two-register
    /// operation that cannot panic or be observed half-applied.
    fn push_free(&mut self, head: u32) {
        assert!(!is_free(self.id), "slot already free");
        self.id = 0;
        self.body = SlotBody {
            free: ManuallyDrop::new(FreeLink { next: head }),
        };
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
struct CancelSlot<G, L> {
    /// See the slot-state table above; a cancel slot never holds a `local`
    /// that outlives the ack, so only the free/live distinction is needed.
    id: u64,
    target: u32,
    target_id: NonZeroU64,
    local: MaybeUninit<L>,
    body: CancelBody<G, L>,
}

/// A cancel's live payload, or — while free — its place on the free stack.
union CancelBody<G, L> {
    live: ManuallyDrop<CancelDone<G, L>>,
    free: ManuallyDrop<FreeLink>,
}

struct CancelDone<G, L> {
    done: fn(&mut Runtime<G, L>, L),
}

impl<G, L> CancelSlot<G, L> {
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
        done: fn(&mut Runtime<G, L>, L),
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

// ── Runtime ───────────────────────────────────────────────────────────

/// The callback runtime. Owns the `io_uring` ring, the buffer pools, the op
/// slabs, and the caller's global state `G`. Each op carries its own local
/// state `L`, moved into the op on dispatch and back to a shim on completion.
pub struct Runtime<G, L> {
    /// Held in an `Option` only so [`Drop`] can close it at the exact point we
    /// choose — see the `Drop` impl, which relies on this being the first
    /// thing torn down. It is `Some` for the entire lifetime of the runtime
    /// except inside `drop`.
    pub(crate) ring: Option<io_uring::IoUring>,
    pub(crate) pools: Pools,
    /// One entry per op slot ever used. A slot is free, live, or live with its
    /// local taken out; the free ones form an intrusive stack threaded through
    /// `IoSlot::body`, linked from `free_slots_head`.
    io_slab: Vec<IoSlot<G, L>>,
    /// Head of the intrusive free-op stack, or [`FREE_NONE`].
    free_slots_head: u32,
    /// One entry per cancel slot ever used; free ones stack the same way.
    cancel_slab: Vec<CancelSlot<G, L>>,
    /// Head of the intrusive free-cancel stack, or [`FREE_NONE`].
    free_cancels_head: u32,
    /// Number of ops submitted but not yet completed (their slot is `Some`).
    inflight_ops: u32,
    /// Number of cancel requests submitted but not yet completed.
    inflight_cancels: u32,
    /// The caller's global state, taken back out by [`block_on`] on return.
    global: Option<G>,
}

impl<G, L> Runtime<G, L> {
    /// The live ring. Only ever `None` inside [`Drop`], after it was closed.
    pub fn ring_mut(&mut self) -> &mut io_uring::IoUring {
        self.ring.as_mut().expect("ring closed")
    }
}

impl<G, L> Runtime<G, L> {
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
        if !is_taken(slot.id) {
            // SAFETY: a live, untaken slot owns an initialized `local`, and
            // this is the single place that reclaims a slot whose op was never
            // dispatched to completion.
            unsafe { slot.local.assume_init_drop() };
        }
        slot.push_free(head);
        self.free_slots_head = index;
    }

    /// Like [`Self::free_op_slot`], but the local has already been moved out
    /// by the completion or a winning cancel, so only `id` and the free link
    /// are written.
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

    /// Registers a dispatched op: takes the caller's `local`, installs its
    /// completion `shim` and erased `done`, and returns the op's handle. The
    /// caller submits the SQE with [`Self::op_user_data`]. `class` is
    /// dispatch-time metadata carried in the handle and in the op's
    /// `user_data`, read back by the shim on completion.
    pub(crate) fn register_op(
        &mut self,
        shim: OpShim<G, L>,
        done: *const (),
        local: L,
        class: u8,
    ) -> Op {
        let index = self.alloc_op_slot();
        let id = next_op_id();
        self.io_slab[index as usize].claim(id, shim, done, local);
        Op {
            slot: index,
            class,
            id,
        }
    }

    /// The `user_data` for the SQE completing the op in `slot`. `class` is
    /// dispatch-time metadata the shim reads back on completion. The handler
    /// never checks the op `id` (a stale CQE is impossible by construction);
    /// the `id` lives in the slot for `cancel` to validate the handle.
    pub(crate) fn op_user_data(slot: u32, class: u8) -> u64 {
        UserData {
            index: slot,
            tag: u32::from(class),
        }
        .into()
    }

    /// Submits an `ASYNC_CANCEL` naming `victim`'s `user_data`, carrying the
    /// cancel's own `local` for `done`. Returns whether it was accepted.
    fn submit_cancel(
        &mut self,
        victim: Op,
        target_id: NonZeroU64,
        done: fn(&mut Runtime<G, L>, L),
        local: L,
    ) -> bool {
        let index = self.alloc_cancel_slot();
        self.cancel_slab[index as usize].claim(next_op_id(), victim.slot, target_id, done, local);
        let entry =
            io_uring::opcode::AsyncCancel::new(Self::op_user_data(victim.slot, victim.class))
                .build()
                .user_data(
                    UserData {
                        index,
                        tag: CANCEL_TAG,
                    }
                    .into(),
                );
        let mut sq = self.ring_mut().submission();
        let pushed = unsafe { sq.push(&entry) };
        drop(sq);
        if pushed.is_err() {
            // Ring full: the victim's natural completion still recycles its
            // slot; give up the cancel slot and report no cancellation. No ack
            // will ever arrive, so this path drops the cancel's own local.
            self.free_cancel_slot_dropping_local(index);
            return false;
        }
        true
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

    /// Cancels an in-flight op. On acceptance an `ASYNC_CANCEL` is submitted
    /// and `done` fires — receiving the cancel's own `local` — only if the
    /// cancellation beats the op's own completion. If the op already finished
    /// (recycled slot or mismatched id), returns
    /// [`CancelStatus::AlreadyCompleted`] and `done` is never called.
    pub fn cancel(&mut self, op: Op, local: L, done: fn(&mut Runtime<G, L>, L)) -> CancelStatus {
        let valid = matches!(
            self.io_slab.get(op.slot as usize).map(|slot| slot.id),
            // A free slot has id 0, which never equals a real op id, so this
            // single comparison rejects both free and recycled slots.
            Some(id) if id_bits(id) == op.id.get()
        );
        if !valid {
            return CancelStatus::AlreadyCompleted;
        }
        if self.submit_cancel(op, op.id, done, local) {
            CancelStatus::Accepted
        } else {
            CancelStatus::AlreadyCompleted
        }
    }

    fn complete_op(&mut self, index: u32, result: i32, flags: u32, class: u8) {
        let Some(slot) = self.io_slab.get(index as usize) else {
            return;
        };
        if is_free(slot.id) {
            // Slot already recycled; a stale CQE can never name a live op.
            return;
        }
        if is_taken(slot.id) {
            // A winning cancel took the local out and ran its own `done`. The
            // local is already gone, so it must not be taken again.
            self.free_op_slot_taken(index);
            return;
        }

        // Read the dispatch state out of the live union arm, then move the
        // local. Both are guarded by the `is_free`/`is_taken` checks above.
        let (shim, done) = {
            let body = &self.io_slab[index as usize].body;
            // SAFETY: `!is_free` guarantees the `live` arm is the live one,
            // written by `claim` and untouched since (a taken local does not
            // affect `body`).
            let live = unsafe { &body.live };
            // SAFETY: `Live` is `Copy` data (two function pointers).
            (live.shim, live.done)
        };
        let local = self.io_slab[index as usize].take_local();
        self.free_op_slot_taken(index);
        shim(self, local, result, flags, done, class);
    }

    fn complete_cancel(&mut self, index: u32) {
        let Some(cancel) = self.cancel_slab.get(index as usize) else {
            return;
        };
        if is_free(cancel.id) {
            return;
        }
        let (target, target_id, done) = {
            let cancel = &self.cancel_slab[index as usize];
            // SAFETY: `!is_free` guarantees the `live` arm is live, written by
            // `claim`. `CancelDone` is a single function pointer (Copy).
            let done = unsafe { cancel.body.live.done };
            (cancel.target, cancel.target_id, done)
        };
        let clocal = self.cancel_slab[index as usize].take_local();
        self.free_cancel_slot(index);

        // First-wins: take the target's local only if the op's own completion
        // hasn't already moved it out. Marking the target LOCAL_TAKEN makes its
        // later CQE suppress its callback.
        let mut won = false;
        if let Some(ts) = self.io_slab.get_mut(target as usize)
            && !is_free(ts.id)
            && !is_taken(ts.id)
            && id_bits(ts.id) == target_id.get()
        {
            // The local moves out here; the slot stays in the slab until its
            // own CQE, which will see LOCAL_TAKEN and free it without touching
            // the local again. The mark is set *after* the move, because
            // `take_local` requires the slot to still own its local.
            let _victim_local = ts.take_local();
            ts.id |= LOCAL_TAKEN;
            won = true;
        }
        if won {
            done(self, clocal);
        }
        // If the op already ran its own callback, `done` is suppressed and
        // `clocal` drops here.
    }

    fn drain_cqes(&mut self) {
        // One completion at a time, with no buffering.
        //
        // `IoUring::completion()` returns a guard that mutably borrows the ring
        // for its whole lifetime, and dropping it publishes the consumed `head`
        // back to shared memory. A shim dispatched from here needs `&mut self`
        // again (to push to the submission queue, or to cancel), so the guard
        // must not outlive the pop. Borrowing, popping one entry and dropping
        // per completion costs three relaxed/acquire/release accesses and
        // nothing else — no allocation, no copy — and each entry is still
        // consumed exactly once, since `head` is published before the next
        // borrow. Re-reading `tail` each time also picks up completions that
        // land mid-drain.
        //
        // The loop terminates because dispatch only pushes to the submission
        // queue: nothing reaches the kernel until `drive` submits, so the set
        // of completions arriving here is finite.
        while let Some(cqe) = {
            let mut cq = self.ring_mut().completion();
            cq.next()
        } {
            let ud = UserData::from(cqe.user_data());
            if ud.tag & CANCEL_TAG != 0 {
                self.complete_cancel(ud.index);
            } else {
                let class = (ud.tag & 0x7f) as u8;
                self.complete_op(ud.index, cqe.result(), cqe.flags(), class);
            }
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
            match self.ring_mut().submit_and_wait(1) {
                Ok(_) => {}
                Err(ref e) if e.raw_os_error() == Some(libc::EBUSY) => {}
                Err(_) => break,
            }
        }
    }
}

/// Teardown order matters and is load-bearing.
///
/// `Runtime` holds memory the kernel can still be writing into: `pools` backs
/// every read's provided-buffer ring and every `write_fixed` buffer, and an
/// op's `L` may own a `Bytes` pointing at that same slab. So the teardown must
/// be strictly:
///
/// 1. unregister the buffer rings (needs the fd open),
/// 2. close the ring fd — the kernel cancels every in-flight request and does
///    not return until any worker mid-execution has finished, so after this
///    returns **no kernel op can touch pool or local memory**,
/// 3. only then drop the un-consumed op locals (which may return pooled
///    buffers),
/// 4. finally let `pools` free the slab.
///
/// The `io-uring` crate encodes the same invariant for its own maps by
/// dropping `MemoryMap` before `fd`; we do the same for the slab by closing the
/// fd before touching `L` or `pools`.
///
/// A normal `drive()` drains to quiescence, so steps 3–4 are no-ops there;
/// they matter only when a panic unwinds out of `boot`/`drive`, or when
/// `drive` breaks early on a ring error with ops still in flight.
impl<G, L> Drop for Runtime<G, L> {
    fn drop(&mut self) {
        // 1. Unregister every provided buffer ring and the fixed write buffer slab
        //    while the io_uring fd is still open.
        if let Some(ring) = self.ring.as_ref() {
            let _ = self.pools.provided_pool.unregister_all(ring);
            let _ = ring.submitter().unregister_buffers();
        }

        // 2. Close the ring. `IoUring::drop` releases its own SQ/CQ maps and then
        //    closes the fd, which tears down the ring and does not return until every
        //    in-flight request is cancelled and any worker that was mid-execution has
        //    finished.
        drop(self.ring.take());

        // 3. Safe now: no kernel op is live, so drop the locals of every op that never
        //    completed. Free slots hold a `FreeLink`, not a live payload, so their
        //    uninitialized `local` must not be touched.
        for slot in self.io_slab.iter_mut() {
            if !is_free(slot.id) && !is_taken(slot.id) {
                // SAFETY: a live, untaken op slot still owns its initialized
                // local, and no other path will drop it now.
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
        // 4. `pools` (and the slabs, which are plain `Vec`s of POD-ish slots)
        //    drop here, after the ring is closed.
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct RuntimeParams<'a> {
    /// A hint for how many ops may be in flight at once; the slab grows if
    /// more are dispatched.
    pub ops_capacity: u32,
    pub ring_entries: u32,
    /// Provided-buffer size classes for reads. Each `count` must be a power of
    /// two no larger than 32768.
    pub provided_size_classes: &'a [SizeClass],
    /// Fixed-buffer size classes for writes and op-argument allocations.
    pub size_classes: &'a [SizeClass],
}

impl<'a> Default for RuntimeParams<'a> {
    fn default() -> Self {
        Self {
            ops_capacity: 1024,
            ring_entries: 1024,
            provided_size_classes: &DEFAULT_SIZE_CLASSES,
            size_classes: &DEFAULT_SIZE_CLASSES,
        }
    }
}

/// Builds both directions' pools over one shared slab: the provided classes
/// occupy the start of the slab, the fixed classes follow them. Each provided
/// class gets a provided-buffer ring registered under `bgid = class index`;
/// the fixed classes get free-stack slabs, and the fixed region alone is
/// registered as fixed buffer index 0 so writes can address any slot by its
/// absolute offset. Returns the slab and both pools.
///
/// The slab is allocated aligned to at most one page (4 KiB) and the classes
/// are laid out on offsets aligned to their own sizes capped at that page
/// alignment, so every buffer is aligned to `min(size, 4096)` and no buffer
/// claims an alignment beyond a page. Class sizes must be powers of two (the
/// allocator only guarantees power-of-two alignment).
fn build_pools(
    ring: &io_uring::IoUring,
    provided_spec: &[SizeClass],
    fixed_spec: &[SizeClass],
) -> Pools {
    let provided_layout = layout_classes(provided_spec, true);
    let fixed_layout = layout_classes(fixed_spec, false);
    let provided_total = provided_layout.last().map(|l| l.total).unwrap();
    let align = provided_spec
        .iter()
        .chain(fixed_spec)
        .map(|class| class.size.min(BUFFER_MAX_ALIGN) as usize)
        .max()
        .expect("at least one size class is required");
    assert!(
        align.is_power_of_two(),
        "buffer size class sizes must be powers of two so buffers can be aligned to their size"
    );
    let fixed_start = provided_total.div_ceil(u64::from(align as u32)) * u64::from(align as u32);
    let total = fixed_start + fixed_layout.last().map(|l| l.total).unwrap();
    assert!(
        total <= u64::from(u32::MAX),
        "the combined provided+fixed slab exceeds the 4 GiB offset range"
    );

    let layout = Layout::from_size_align(total as usize, align).unwrap();
    let slab = unsafe { alloc(layout) };
    if slab.is_null() {
        handle_alloc_error(layout);
    }
    let base = unsafe { NonNull::new_unchecked(slab) };

    let provided_classes: Vec<SizeClass> = provided_layout
        .iter()
        .map(|l| SizeClass {
            size: l.size,
            count: l.count,
        })
        .collect();
    let mut provided_slabs = Vec::with_capacity(provided_layout.len());
    for (i, l) in provided_layout.iter().enumerate() {
        let class_base = unsafe { base.as_ptr().add(l.base_offset as usize) };
        let slab = ProvidedSlab::new(
            unsafe { NonNull::new_unchecked(class_base) },
            l.count as u16,
            l.size,
            i as _,
        );
        provided_slabs.push(slab);
    }
    let provided_pool = ProvidedPool::new(provided_classes, provided_slabs);
    provided_pool
        .register_all(ring)
        .expect("failed to register the provided buffer rings");

    let fixed_classes: Vec<SizeClass> = fixed_layout
        .iter()
        .map(|l| SizeClass {
            size: l.size,
            count: l.count,
        })
        .collect();
    let fixed_slabs: Vec<FixedSlab> = fixed_layout
        .iter()
        .enumerate()
        .map(|(i, l)| {
            let class_base = unsafe {
                base.as_ptr()
                    .add((fixed_start + u64::from(l.base_offset)) as usize)
            };
            FixedSlab::new(
                unsafe { NonNull::new_unchecked(class_base) },
                l.size,
                l.count,
                i as u8,
            )
        })
        .collect();
    let fixed_pool = FixedPool::new(fixed_classes, fixed_slabs);

    // Only the fixed region is registered as fixed buffer index 0: writes
    // address their slots by absolute offset within that region, while the
    // provided region's slots are handed to the kernel one op at a time via
    // `IORING_OP_PROVIDE_BUFFERS`. Registering the whole slab would pin the
    // provided region's pages against the process's locked-memory limit for
    // no benefit (each registered page counts toward `RLIMIT_MEMLOCK`), which
    // caps how many daemons can run at once.
    let iovec = libc::iovec {
        iov_base: unsafe { base.as_ptr().add(fixed_start as usize) }.cast(),
        iov_len: (total - fixed_start) as usize,
    };
    unsafe { ring.submitter().register_buffers(&[iovec]) }
        .expect("failed to register the fixed buffer region");

    Pools {
        slab: base,
        slab_layout: layout,
        provided_pool,
        fixed_pool,
    }
}

pub fn block_on_default<G, L, F>(global: G, boot: F) -> G
where
    F: FnOnce(&mut Runtime<G, L>),
{
    block_on(RuntimeParams::default(), global, boot)
}

/// Runs the callback runtime to completion (until no op is in flight) and
/// returns the caller's global state `G`. `boot` is invoked once with the
/// runtime to dispatch the initial ops; it is the one place a closure is
/// allowed (it runs a single time, at startup).
pub fn block_on<G, L, F>(params: RuntimeParams<'_>, global: G, boot: F) -> G
where
    F: FnOnce(&mut Runtime<G, L>),
{
    let _gen_guard = enter_active_gen();

    let ring = io_uring::IoUring::builder()
        .setup_single_issuer()
        .setup_defer_taskrun()
        .build(params.ring_entries)
        .unwrap_or_else(|_| {
            io_uring::IoUring::new(params.ring_entries).expect("failed to create io_uring")
        });

    let pools = build_pools(&ring, params.provided_size_classes, params.size_classes);

    // Declared *before* `rt` so it drops *after* it: locals can own pooled bytes,
    // and `Runtime::drop` must be able to route their release through the live
    // pools. Clearing the pointer first would abort that drop.
    let _rt_guard = DropGuard::new(clear_current_pools);

    let mut rt = Runtime {
        pools,
        ring: Some(ring),
        io_slab: Vec::new(),
        free_slots_head: FREE_NONE,
        cancel_slab: Vec::new(),
        free_cancels_head: FREE_NONE,
        inflight_ops: 0,
        inflight_cancels: 0,
        global: Some(global),
    };

    set_current_pools(&raw mut rt.pools);

    // A panic in `boot` or `drive` simply unwinds: dropping `rt` runs the
    // ordered teardown in `Runtime::drop`, which closes the ring before any
    // local or pool memory is released.
    boot(&mut rt);
    rt.drive();

    rt.global.take().expect("global already taken")
}

#[cfg(test)]
mod slot_layout_tests {
    use super::*;

    #[test]
    fn io_slot_has_no_option_overhead() {
        // Overhead must be exactly id(8) + body union(16) + L, i.e. the local
        // is stored inline with no discriminant, no `Option` wrapper, and no
        // separate state byte. This is the whole point of encoding free-ness in
        // `id` and threading the free stack through the slot body.
        struct L(#[allow(dead_code)] u64, #[allow(dead_code)] u64);
        assert_eq!(
            size_of::<IoSlot<(), L>>(),
            24 + size_of::<L>(),
            "IoSlot should be exactly 24 bytes of overhead plus L"
        );
        // The slab is a plain `Vec<IoSlot>`. Because `id` is a bare `u64`
        // (0 == free) rather than `NonZeroU64`, `Option` has no niche to borrow
        // and would now cost a full discriminant word — which is exactly why
        // free-ness moved into `id` rather than wrapping the slot.
        assert!(
            size_of::<Option<IoSlot<(), L>>>() > size_of::<IoSlot<(), L>>(),
            "Option would no longer be free, so the slab must not use it"
        );
    }

    #[test]
    fn cancel_slot_has_no_option_overhead() {
        struct L(#[allow(dead_code)] u64, #[allow(dead_code)] u64);
        // id(8) + target(4, padded to 8) + target_id(8) + body union(8) + L.
        assert_eq!(
            size_of::<CancelSlot<(), L>>(),
            32 + size_of::<L>(),
            "CancelSlot should be 32 bytes of overhead plus L"
        );
    }

    #[test]
    fn free_slot_reports_link_but_is_never_read_as_live() {
        // The free stack is threaded through the slot's own body, so walking it
        // must never require a side allocation and must survive reuse.
        struct L(#[allow(dead_code)] u64);
        let mut a: IoSlot<(), L> = IoSlot::init_free(FREE_NONE);
        let b: IoSlot<(), L> = IoSlot::init_free(7);

        assert!(is_free(a.id));
        assert!(is_free(b.id));

        // Claim one, then re-free it on top of the stack.
        a.claim(
            NonZeroU64::new(1).unwrap(),
            |_, _, _, _, _, _| {},
            core::ptr::null(),
            L(1),
        );
        assert!(!is_free(a.id));
        assert!(!is_taken(a.id));

        let local = a.take_local();
        assert_eq!(local.0, 1);
        a.push_free(b.free_next());
        assert!(is_free(a.id));
        assert_eq!(a.free_next(), 7);
    }

    #[test]
    fn op_class_rides_in_padding() {
        // slot(4) + class(1) + 3 padding + id(8) = 16, class is free.
        assert_eq!(size_of::<Op>(), 16);
    }
}
