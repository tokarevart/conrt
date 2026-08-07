use core::cell::Cell;
use core::future::Future;
use core::num::NonZeroU32;
use core::pin::Pin;
use core::task::Context;
use core::task::Poll;
use core::task::Waker;
use std::alloc::Layout;
use std::alloc::alloc;
use std::alloc::dealloc;
use std::alloc::handle_alloc_error;
use std::ptr::NonNull;

use crate::buf::BufferPool;
use crate::classes::BUFFER_MAX_ALIGN;
use crate::classes::DEFAULT_SIZE_CLASSES;
use crate::classes::SizeClass;
use crate::classes::bid_class;
use crate::classes::bid_local;
use crate::classes::bid_provided;
use crate::classes::layout_classes;
use crate::pbuf::ProvidedBufferPool;
use crate::task::IoSlot;
use crate::task::IoUserData;
use crate::task::IoVec;
use crate::task::JoinHandle;
use crate::task::NO_JOINER;
use crate::task::Task;
use crate::task::TaskContext;
use crate::task::TaskSlab;

thread_local! {
    static RUNNING: Cell<bool> = const { Cell::new(false) };
    static ACTIVE_GEN: Cell<NonZeroU32> = const { Cell::new(NonZeroU32::new(1).unwrap()) };
    static CURRENT_RUNTIME: Cell<*mut Runtime> = const { Cell::new(core::ptr::null_mut()) };
    static CURRENT_TASK_INDEX: Cell<Option<u32>> = const { Cell::new(None) };
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

pub(crate) fn enter_active_gen() -> ActiveGenGuard {
    assert!(!RUNNING.with(|c| c.get()));
    RUNNING.with(|c| c.set(true));
    ACTIVE_GEN.with(|c| {
        c.update(|x| x.checked_add(1).expect("active gen overflow"));
        ActiveGenGuard(c.get())
    })
}

pub(crate) fn exit_active_gen() {
    RUNNING.with(|c| c.set(false));
}

pub(crate) struct ActiveGenGuard(NonZeroU32);

impl ActiveGenGuard {
    pub fn get(&self) -> NonZeroU32 {
        self.0
    }
}

impl Drop for ActiveGenGuard {
    fn drop(&mut self) {
        exit_active_gen();
    }
}

pub(crate) fn set_current_runtime(rt: *mut Runtime) {
    CURRENT_RUNTIME.with(|c| c.set(rt));
}

pub(crate) fn clear_current_runtime() {
    CURRENT_RUNTIME.with(|c| c.set(core::ptr::null_mut()));
}

pub(crate) fn with_runtime<R>(f: impl FnOnce(&mut Runtime) -> R) -> R {
    assert!(
        is_running(),
        "with_runtime called outside an active runtime"
    );
    let ptr = CURRENT_RUNTIME.with(|c| c.get());
    assert!(!ptr.is_null(), "no active runtime");
    unsafe { f(&mut *ptr) }
}

/// The index of the task currently being polled by the runtime loop, if any.
/// Join futures use it to register their waiter in the target's waiter list.
pub(crate) fn current_task_index() -> Option<u32> {
    CURRENT_TASK_INDEX.with(|c| c.get())
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
pub(crate) fn resolve_ptr(bid: u32, generation: NonZeroU32, offset: u32) -> *mut u8 {
    assert!(
        active_gen_matches(generation),
        "resolve_ptr called outside the runtime that owns this buffer"
    );
    with_runtime(|r| {
        let class = usize::from(bid_class(bid));
        let local = bid_local(bid);
        let base = if bid_provided(bid) {
            r.provided_pools[class].slot_ptr(local as u16).as_ptr()
        } else {
            r.fixed_pools[class].slot_ptr(local).as_ptr()
        };
        unsafe { base.add(offset as usize) }
    })
}

/// Registers one more shared borrower: a cloned [`crate::buf::Ref`]. A stale
/// view is dropped after its runtime shut down and leaks its slot.
pub(crate) fn clone_view(bid: u32, generation: NonZeroU32) {
    if !active_gen_matches(generation) {
        return;
    }
    with_runtime(|r| {
        let class = usize::from(bid_class(bid));
        let local = bid_local(bid);
        if bid_provided(bid) {
            r.provided_pools[class].clone_shared(local as u16);
        } else {
            r.fixed_pools[class].clone_shared(local);
        }
    })
}

/// Releases one borrower of a view's slot, recycling the slot when the last
/// view drops. `exclusive` selects the exclusive (`RefMut`/`SliceMut`) vs.
/// shared (`Ref`/`Slice`) release. A stale view leaks its slot instead.
pub(crate) fn drop_view(bid: u32, generation: NonZeroU32, exclusive: bool) {
    if !active_gen_matches(generation) {
        return;
    }
    with_runtime(|r| {
        let class = usize::from(bid_class(bid));
        let local = bid_local(bid);
        if bid_provided(bid) {
            r.provided_pools[class].drop_view(exclusive, local as u16);
        } else {
            r.fixed_pools[class].drop_view(exclusive, local);
        }
    })
}

/// Flips a sole shared `Ref` to an exclusive `RefMut`. Panics unless the slot
/// has exactly one shared holder.
pub(crate) fn upgrade_view(bid: u32, generation: NonZeroU32) {
    assert!(
        active_gen_matches(generation),
        "upgrade_view called outside the runtime that owns this buffer"
    );
    with_runtime(|r| {
        let class = usize::from(bid_class(bid));
        let local = bid_local(bid);
        if bid_provided(bid) {
            r.provided_pools[class].upgrade(local as u16);
        } else {
            r.fixed_pools[class].upgrade(local);
        }
    })
}

/// Flips a sole exclusive `RefMut` to a shared `Ref`. Panics unless the slot
/// has exactly one exclusive holder.
pub(crate) fn downgrade_view(bid: u32, generation: NonZeroU32) {
    assert!(
        active_gen_matches(generation),
        "downgrade_view called outside the runtime that owns this buffer"
    );
    with_runtime(|r| {
        let class = usize::from(bid_class(bid));
        let local = bid_local(bid);
        if bid_provided(bid) {
            r.provided_pools[class].downgrade(local as u16);
        } else {
            r.fixed_pools[class].downgrade(local);
        }
    })
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

pub(crate) struct Runtime {
    pub tasks: TaskSlab,
    pub wakeups: Vec<u32>,
    /// Runtime-wide slab of per-op io states; a slot lives while the op it
    /// tracks is in flight, then is recycled via `free_io_slots`.
    pub io_slab: Vec<IoSlot>,
    /// Indices of reusable io slab slots.
    pub free_io_slots: Vec<u32>,
    /// Start of the shared buffer slab, allocated aligned to at most one page.
    pub slab: NonNull<u8>,
    /// The layout of the slab allocation, used to free it on drop.
    slab_layout: Layout,
    pub provided_classes: Vec<SizeClass>,
    pub provided_pools: Vec<ProvidedBufferPool>,
    pub fixed_classes: Vec<SizeClass>,
    pub fixed_pools: Vec<BufferPool>,
    pub ring: io_uring::IoUring,
    pub make_fut: *const (),
}

impl Drop for Runtime {
    fn drop(&mut self) {
        // Unregister every provided buffer ring and the fixed write buffer
        // slab while the io_uring fd is still open; the pools and slabs are
        // dropped afterwards along with the other fields.
        for pool in &self.provided_pools {
            let _ = pool.unregister(&self.ring);
        }
        let _ = self.ring.submitter().unregister_buffers();
        unsafe { dealloc(self.slab.as_ptr(), self.slab_layout) };
    }
}

impl Runtime {
    /// Allocates an io slab slot and marks it submitted. The slot is reused
    /// from the free list when available, otherwise the slab grows; it only
    /// ever holds in-flight ops, so it stays bounded by peak concurrency.
    pub(crate) fn alloc_io_slot(&mut self) -> u32 {
        let index = match self.free_io_slots.pop() {
            Some(index) => index,
            None => {
                self.io_slab.push(IoSlot::default());
                (self.io_slab.len() - 1) as u32
            }
        };
        self.io_slab[index as usize].submitted = 1;
        index
    }

    /// Returns `index` to the free list. Only call once the CQE for the op is
    /// consumed, so a late completion can never land on a recycled slot.
    pub(crate) fn free_io_slot(&mut self, index: u32) {
        self.io_slab[index as usize] = IoSlot::default();
        self.free_io_slots.push(index);
    }

    /// Whether any io op is still in flight. Scans the slab for submitted
    /// slots that are not yet ready; a finished task's ops keep this true
    /// until their CQEs are drained.
    fn has_io_in_flight(&self) -> bool {
        self.io_slab
            .iter()
            .any(|slot| slot.submitted != 0 && slot.ready == 0)
    }

    /// Recycles the io slots in `io` whose ops have already completed (ready),
    /// removing them from `io`. Slots still in flight are left for their
    /// pending CQE, which recycles them through [`Self::drain_cqes`].
    fn reap_io_slots(&mut self, io: &mut IoVec) {
        let mut i = 0;
        while i < io.len() {
            let slot = {
                let mut it = io.iter();
                it.nth(i).expect("index in bounds")
            };
            if self.io_slab[slot as usize].submitted != 0 && self.io_slab[slot as usize].ready != 0
            {
                self.free_io_slot(slot);
                io.remove_value(slot);
            } else {
                i += 1;
            }
        }
    }

    fn context_for(&mut self, task_idx: u32) -> TaskContext {
        set_current_runtime(self as *mut Runtime);
        self.tasks.context_for(task_idx)
    }

    /// Finalizes a finished task: recycles io slots whose ops already
    /// completed, cancels the rest if [`JoinHandle::cancel`] was called on the
    /// task, and removes the task once its io has fully drained, waking any
    /// joiners. A task that finished on its own keeps its in-flight io running
    /// to completion instead of issuing cancel requests. The task stays in its
    /// slot until its io drains, so a CQE can never land on a recycled slot
    /// and the finished-task branch of [`Self::drain_cqes`] is airtight.
    fn finalize_task<F: 'static, R: 'static>(&mut self, idx: u32) {
        let task_ptr = self.tasks.task_ptr(idx);
        unsafe {
            assert!((*task_ptr).finished);
        }

        // Recycle slots whose ops already completed; their CQEs are consumed,
        // so only this path can free them.
        self.reap_io_slots(unsafe { &mut (*task_ptr).io });

        if unsafe { (*task_ptr).cancel_requested } {
            // Cancel the still-in-flight ops. `push_cancel` only appends its
            // cancel slots, so the first `n` entries stay the victims and the
            // loop never touches the slots it adds.
            let n = unsafe { (*task_ptr).io.len() };
            for i in 0..n {
                let slot = {
                    let mut it = unsafe { (*task_ptr).io.iter() };
                    it.nth(i).expect("index in bounds")
                };
                self.push_cancel(idx, slot);
            }
        }

        if unsafe { (*task_ptr).io.is_empty() } {
            self.remove_task::<F, R>(idx);
        }
    }

    /// Submits an `IORING_OP_ASYNC_CANCEL` for one in-flight io slot of a
    /// finished task. The cancel request occupies its own io slot, so its CQE
    /// is recycled by the same finished-task drain path as the victim's.
    fn push_cancel(&mut self, idx: u32, victim_slot: u32) {
        let cancel_slot = self.alloc_io_slot();
        self.tasks.task_mut(idx).io.push(cancel_slot);

        let victim_ud = IoUserData {
            index: idx,
            io_slot: victim_slot,
        };
        let cancel_ud = IoUserData {
            index: idx,
            io_slot: cancel_slot,
        };
        let entry = io_uring::opcode::AsyncCancel::new(victim_ud.into())
            .build()
            .user_data(cancel_ud.into());

        let mut sq = self.ring.submission();
        let pushed = unsafe { sq.push(&entry) };
        drop(sq);
        if pushed.is_err() {
            // Ring full: the victim's natural CQE will still recycle its slot,
            // so give up the cancel slot.
            self.free_io_slot(cancel_slot);
            self.tasks.task_mut(idx).io.remove_value(cancel_slot);
        }
    }

    /// Removes a finished task whose io has fully drained, waking the task
    /// blocked on its [`JoinHandle`], if any.
    fn remove_task<F: 'static, R: 'static>(&mut self, idx: u32) {
        assert!(self.tasks.task(idx).finished);
        let joiner = self.tasks.task_mut(idx).joiner;
        if joiner != NO_JOINER {
            self.wakeups.push(joiner);
        }
        self.tasks.remove::<F, R>(idx);
    }

    fn drain_cqes(&mut self, ready: &mut Vec<u32>) {
        for cqe in self.ring.completion() {
            let raw = cqe.user_data();
            let task_index = (raw >> 32) as u32;
            let io_slot = raw as u32;
            let result = cqe.result();

            if io_slot as usize >= self.io_slab.len() {
                continue;
            }

            if !self.tasks.is_occupied(task_index) {
                continue;
            }
            if self.tasks.task(task_index).finished {
                // The task is done and only waiting for its io to drain:
                // recycle the slot now and let the loop finalize the
                // removal once the last slot is gone. `free_io_slot` is
                // inlined since the ring borrow rules it out here.
                self.tasks.task_mut(task_index).io.remove_value(io_slot);
                self.io_slab[io_slot as usize] = IoSlot::default();
                self.free_io_slots.push(io_slot);
                if self.tasks.task(task_index).io.is_empty() {
                    ready.push(task_index);
                }
                continue;
            }
            let slot = &mut self.io_slab[io_slot as usize];
            slot.result = result;
            // Only reads report a selected buffer via the CQE flags;
            // writes store their own slot id in `bids`, so leave it
            // untouched when no buffer flag is present.
            if let Some(bid) = io_uring::cqueue::buffer_select(cqe.flags()) {
                slot.bid = bid;
            }
            slot.ready = 1;
            let task = self.tasks.task_mut(task_index);
            if !task.ready {
                task.ready = true;
                ready.push(task_index);
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct RuntimeParams<'a> {
    pub tasks_capacity: u32,
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
            tasks_capacity: 1024,
            ring_entries: 1024,
            provided_size_classes: &DEFAULT_SIZE_CLASSES,
            size_classes: &DEFAULT_SIZE_CLASSES,
        }
    }
}

/// The shared buffer slab allocation plus the class tables and pools built
/// over it.
type BuiltPools = (
    NonNull<u8>,
    Layout,
    Vec<SizeClass>,
    Vec<ProvidedBufferPool>,
    Vec<SizeClass>,
    Vec<BufferPool>,
);

/// Builds both directions' pools over one shared slab: the provided classes
/// occupy the start of the slab, the fixed classes follow them. Each provided
/// class gets a provided-buffer ring registered under `bgid = class index`;
/// the fixed classes get free-stack pools, and the whole slab is registered
/// once as fixed buffer index 0 so writes can address any slot by its absolute
/// offset. Returns the slab, both sorted class tables, and both pool lists.
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
) -> BuiltPools {
    let provided_layout = layout_classes(provided_spec, true);
    let fixed_layout = layout_classes(fixed_spec, false);
    let provided_total = provided_layout.last().map(|l| l.total).unwrap();
    // The slab is aligned to at most one page: larger classes still land on
    // page-aligned offsets, so no buffer ever exceeds a page's alignment.
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
    // Start the fixed region on an offset aligned to `align` (hence to every
    // class size): the provided region's total need not be a multiple of the
    // fixed class sizes.
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
    let mut provided_pools = Vec::with_capacity(provided_layout.len());
    for (i, l) in provided_layout.iter().enumerate() {
        let pool =
            ProvidedBufferPool::new(base, l.base_offset as usize, l.count as u16, l.size, i as _);
        pool.register(ring)
            .expect("failed to register the provided buffer ring");
        provided_pools.push(pool);
    }

    let fixed_classes: Vec<SizeClass> = fixed_layout
        .iter()
        .map(|l| SizeClass {
            size: l.size,
            count: l.count,
        })
        .collect();
    let fixed_pools = fixed_layout
        .iter()
        .enumerate()
        .map(|(i, l)| {
            BufferPool::new(
                base,
                fixed_start as usize + l.base_offset as usize,
                l.size,
                l.count,
                i as u8,
            )
        })
        .collect();

    let iovec = libc::iovec {
        iov_base: base.as_ptr().cast::<libc::c_void>(),
        iov_len: total as usize,
    };
    unsafe { ring.submitter().register_buffers(&[iovec]) }
        .expect("failed to register the buffer slab");

    (
        base,
        layout,
        provided_classes,
        provided_pools,
        fixed_classes,
        fixed_pools,
    )
}

pub fn block_on_default<S, F, T, R>(make_fut: S, user_data: T) -> R
where
    S: Fn(TaskContext, RuntimeContext<T, R>, T) -> F,
    F: Future<Output = R> + 'static,
    R: 'static,
{
    block_on(RuntimeParams::default(), make_fut, user_data)
}

pub fn block_on<S, F, T, R>(params: RuntimeParams<'_>, make_fut: S, user_data: T) -> R
where
    S: Fn(TaskContext, RuntimeContext<T, R>, T) -> F,
    F: Future<Output = R> + 'static,
    R: 'static,
{
    let _gen_guard = enter_active_gen();
    let generation = _gen_guard.get();

    let ring = io_uring::IoUring::builder()
        .setup_single_issuer()
        .setup_defer_taskrun()
        .build(params.ring_entries)
        .unwrap_or_else(|_| {
            io_uring::IoUring::new(params.ring_entries).expect("failed to create io_uring")
        });

    let (slab, slab_layout, provided_classes, provided_pools, fixed_classes, fixed_pools) =
        build_pools(&ring, params.provided_size_classes, params.size_classes);

    let mut rt = Runtime {
        tasks: TaskSlab::new::<F, R>(params.tasks_capacity),
        wakeups: Vec::new(),
        io_slab: Vec::new(),
        free_io_slots: Vec::new(),
        slab,
        slab_layout,
        provided_classes,
        provided_pools,
        fixed_classes,
        fixed_pools,
        ring,
        make_fut: &raw const make_fut as *const (),
    };

    let rt_ptr: *mut Runtime = &mut rt;
    set_current_runtime(rt_ptr);
    let _rt_guard = DropGuard::new(clear_current_runtime);
    let _drop_guard = DropGuard::new(move || unsafe {
        TaskSlab::drop_futures_raw::<F>(&mut (*rt_ptr).tasks);
        TaskSlab::drop_outputs_raw::<R>(&mut (*rt_ptr).tasks);
    });

    let spawn: unsafe fn(RuntimeContext<T, R>, T) -> Option<JoinHandle<R>> = spawn::<S, F, T, R>;

    let ctx = RuntimeContext { generation, spawn };

    let mut task = Task::new();
    task.ready = true;
    let task_idx = rt
        .tasks
        .insert(task, |task_ctx| (make_fut)(task_ctx, ctx, user_data))
        .expect("failed to insert vacant task");
    rt.wakeups.push(task_idx);

    let mut ready_tasks = Vec::new();
    let mut main_output: Option<R> = None;

    loop {
        core::mem::swap(&mut rt.wakeups, &mut ready_tasks);
        assert!(rt.wakeups.is_empty());

        rt.drain_cqes(&mut ready_tasks);

        for &idx in &ready_tasks {
            if !rt.tasks.is_occupied(idx) {
                continue;
            }

            if rt.tasks.task(idx).finished {
                rt.finalize_task::<F, R>(idx);
                continue;
            }

            rt.tasks.task_mut(idx).ready = false;

            CURRENT_TASK_INDEX.with(|c| c.set(Some(idx)));
            let mut cx = Context::from_waker(Waker::noop());
            let future_ptr = rt.tasks.future_ptr::<F>(idx);
            let future = unsafe { Pin::new_unchecked(&mut *future_ptr) };
            let poll_result = future.poll(&mut cx);
            CURRENT_TASK_INDEX.with(|c| c.set(None));

            if let Poll::Ready(output) = poll_result {
                if idx == task_idx {
                    main_output = Some(output);
                } else {
                    let joiner = rt.tasks.task_mut(idx).joiner;
                    if joiner == NO_JOINER {
                        drop(output);
                    } else {
                        rt.tasks.init_output::<R>(joiner, output);
                    }
                }
                rt.tasks.task_mut(idx).finished = true;
                rt.finalize_task::<F, R>(idx);
            }
        }

        ready_tasks.clear();

        if !rt.has_io_in_flight() {
            if rt.wakeups.is_empty() {
                break;
            }
            continue;
        }

        match rt.ring.submit_and_wait(1) {
            Ok(_) => {}
            Err(ref e) if e.raw_os_error() == Some(libc::EBUSY) => {}
            Err(_) => {
                return main_output
                    .expect("block_on: submit failed before the main future completed");
            }
        }
    }

    main_output.expect(
        "block_on: the main future ended Pending with no pending io or wakeups and can never \
         complete",
    )
}

unsafe fn spawn<S, F, T, R>(ctx: RuntimeContext<T, R>, user_data: T) -> Option<JoinHandle<R>>
where
    S: Fn(TaskContext, RuntimeContext<T, R>, T) -> F,
    F: Future<Output = R> + 'static,
    R: 'static,
{
    with_runtime(|data| {
        let closure = unsafe { &*(data.make_fut as *const S) };

        let mut task = Task::new();
        task.ready = true;
        let index = data
            .tasks
            .insert(task, |task_ctx| closure(task_ctx, ctx, user_data))?;

        let handle = JoinHandle::new(data.context_for(index));

        data.wakeups.push(index);

        Some(handle)
    })
}

#[derive(Debug)]
pub struct RuntimeContext<T, R> {
    generation: NonZeroU32,
    spawn: unsafe fn(RuntimeContext<T, R>, T) -> Option<JoinHandle<R>>,
}

impl<T, R> Clone for RuntimeContext<T, R> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T, R> Copy for RuntimeContext<T, R> {}

impl<T, R> RuntimeContext<T, R> {
    pub fn spawn(&self, user_data: T) -> Option<JoinHandle<R>> {
        assert!(
            active_gen_matches(self.generation),
            "RuntimeContext used outside the runtime it belongs to"
        );
        unsafe { (self.spawn)(*self, user_data) }
    }
}

#[cfg(test)]
mod tests {
    use core::future::Ready;
    use core::mem::MaybeUninit;
    use core::pin::pin;
    use core::task::Context;
    use core::task::Poll;
    use core::task::Waker;

    use super::*;
    use crate::buf::Bytes;
    use crate::buf::BytesMut;
    use crate::buf::Ref;
    use crate::buf::RefMut;
    use crate::buf::Slice;
    use crate::buf::SliceMut;
    use crate::classes::SizeClass;
    use crate::classes::pack_bid;
    use crate::io::MAX_CTRL_CAP;
    use crate::io::MAX_IOV_CAP;
    use crate::io::Msg;
    use crate::io::MsgMut;
    use crate::io::await_cqe;
    use crate::io::cmsg_space;
    use crate::io::yield_now;
    use crate::task::IoUserData;
    use crate::task::IoVec;
    use crate::task::Task;
    use crate::task::TaskSlab;

    fn test_runtime_data(capacity: u32) -> Runtime {
        let ring = io_uring::IoUring::new(8).unwrap();
        // Class 0 (16) and 1 (64) feed the small-slot tests; class 2 (2048)
        // fits MAX_CTRL_CAP and class 3 (16384) fits the MAX_IOV_CAP iov array
        // that a non-generic Msg always allocates.
        let (slab, slab_layout, provided_classes, provided_pools, fixed_classes, fixed_pools) =
            build_pools(&ring, &[SizeClass { size: 16, count: 4 }], &[
                SizeClass { size: 16, count: 4 },
                SizeClass { size: 64, count: 4 },
                SizeClass {
                    size: 2048,
                    count: 4,
                },
                SizeClass {
                    size: 16384,
                    count: 2,
                },
            ]);
        Runtime {
            tasks: TaskSlab::new::<Ready<()>, ()>(capacity),
            wakeups: Vec::new(),
            io_slab: Vec::new(),
            free_io_slots: Vec::new(),
            slab,
            slab_layout,
            provided_classes,
            provided_pools,
            fixed_classes,
            fixed_pools,
            ring,
            make_fut: core::ptr::null(),
        }
    }

    // ── io slab ───────────────────────────────────────────────────────

    #[test]
    fn io_slot_alloc_marks_submitted() {
        let mut data = test_runtime_data(64);
        let slot = data.alloc_io_slot();
        assert_eq!(slot, 0);
        assert_eq!(data.io_slab[slot as usize].submitted, 1);
        assert!(data.has_io_in_flight());
    }

    #[test]
    fn io_slot_free_clears_and_reuses() {
        let mut data = test_runtime_data(64);
        let a = data.alloc_io_slot();
        let b = data.alloc_io_slot();
        assert_eq!(a, 0);
        assert_eq!(b, 1);
        data.free_io_slot(a);
        assert_eq!(data.io_slab[a as usize].submitted, 0);
        // The freed slot is reused before the slab grows.
        let c = data.alloc_io_slot();
        assert_eq!(c, 0);
        assert_eq!(data.io_slab.len(), 2);
    }

    #[test]
    fn io_slab_reaps_completed_unreaped_slot() {
        let mut data = test_runtime_data(64);
        let mut io = IoVec::new();
        let slot = data.alloc_io_slot();
        io.push(slot);
        // The op completed but the task never reaped it.
        data.io_slab[slot as usize].ready = 1;
        data.reap_io_slots(&mut io);
        assert_eq!(data.io_slab[slot as usize].submitted, 0);
        assert_eq!(data.free_io_slots, vec![slot]);
        assert!(!data.has_io_in_flight());
    }

    #[test]
    fn io_slab_leaves_in_flight_slot_orphaned() {
        let mut data = test_runtime_data(64);
        let mut io = IoVec::new();
        let slot = data.alloc_io_slot();
        io.push(slot);
        // Still in flight: reaping must leave it for the late CQE.
        data.reap_io_slots(&mut io);
        assert_eq!(data.io_slab[slot as usize].submitted, 1);
        assert!(data.free_io_slots.is_empty());
        assert!(data.has_io_in_flight());
    }

    #[test]
    fn shared_slab_fixed_region_follows_provided_region() {
        let ring = io_uring::IoUring::new(8).unwrap();
        let (slab, _layout, _pc, provided_pools, _fc, fixed_pools) =
            build_pools(&ring, &[SizeClass { size: 16, count: 4 }], &[SizeClass {
                size: 32,
                count: 2,
            }]);
        // The provided region is 4 x 16 = 64 bytes; the fixed class starts
        // right after it in the same slab.
        assert_eq!(provided_pools[0].slot_offset(3), 48);
        assert_eq!(fixed_pools[0].slot_offset(0), 64);
        assert_eq!(fixed_pools[0].slot_offset(1), 96);
        // Every slot address is aligned to its class's buffer size.
        let base = slab.as_ptr() as usize;
        assert_eq!((base + provided_pools[0].slot_offset(3) as usize) % 16, 0);
        assert_eq!((base + fixed_pools[0].slot_offset(0) as usize) % 32, 0);
        assert_eq!((base + fixed_pools[0].slot_offset(1) as usize) % 32, 0);
    }

    #[test]
    fn build_pools_aligns_every_slot_to_its_size() {
        let ring = io_uring::IoUring::new(8).unwrap();
        let (slab, _layout, provided_classes, provided_pools, fixed_classes, fixed_pools) =
            build_pools(&ring, &[SizeClass { size: 16, count: 4 }], &[
                SizeClass { size: 64, count: 2 },
                SizeClass {
                    size: 128,
                    count: 2,
                },
            ]);
        // The provided region is 4 x 16 = 64 bytes, not a multiple of the
        // fixed classes' sizes: the fixed region must start padded to the
        // largest size, or the 128-byte class would land 64 bytes off.
        let base = slab.as_ptr() as usize;
        assert_eq!(base % 128, 0, "slab base is aligned to the largest class");
        for (class, pool) in provided_classes.iter().zip(&provided_pools) {
            for local in 0..class.count {
                let addr = base + pool.slot_offset(local as u16) as usize;
                assert_eq!(
                    addr % class.size as usize,
                    0,
                    "provided slot {local} of size {} is misaligned",
                    class.size
                );
            }
        }
        for (class, pool) in fixed_classes.iter().zip(&fixed_pools) {
            for local in 0..class.count {
                let addr = base + pool.slot_offset(local) as usize;
                assert_eq!(
                    addr % class.size.min(BUFFER_MAX_ALIGN) as usize,
                    0,
                    "fixed slot {local} of size {} is misaligned",
                    class.size
                );
            }
        }
    }

    #[test]
    fn build_pools_caps_alignment_at_one_page() {
        let ring = io_uring::IoUring::new(8).unwrap();
        let (slab, _layout, _pc, provided_pools, _fc, fixed_pools) =
            build_pools(&ring, &[SizeClass { size: 16, count: 4 }], &[SizeClass {
                size: 8192,
                count: 2,
            }]);
        // The 16-byte provided region is 64 bytes; the fixed class must start
        // padded to a page, and the 8 KiB slots are page-aligned — not aligned
        // to their full size, since alignment never exceeds one page.
        let base = slab.as_ptr() as usize;
        assert_eq!(base % 4096, 0, "slab base is aligned to at most one page");
        assert_eq!(fixed_pools[0].slot_offset(0), 4096);
        assert_eq!(fixed_pools[0].slot_offset(1), 12288);
        for local in 0..2 {
            let addr = base + fixed_pools[0].slot_offset(local) as usize;
            assert_eq!(addr % 4096, 0, "slot {local} is page-aligned");
        }
        // Small classes still get full alignment to their own size.
        for local in 0..4 {
            let addr = base + provided_pools[0].slot_offset(local as u16) as usize;
            assert_eq!(addr % 16, 0, "provided slot {local} is 16-byte aligned");
        }
    }

    // ── Bytes (provided pool) ─────────────────────────────────────────

    #[test]
    fn provided_buffer_drop_recycles_slot() {
        let task = Task::new();
        let mut data = test_runtime_data(64);
        let index = data
            .tasks
            .insert(task, |_| core::future::ready(()))
            .unwrap();

        let _gen = enter_active_gen();
        let _ctx = data.context_for(index);

        let local = 1u16;
        let buf = data.provided_pools[0].select(local, 5);

        assert_eq!(data.provided_pools[0].ring_tail(), 4);
        assert_eq!(data.provided_pools[0].borrows(local), 1);
        drop(buf);
        assert_eq!(data.provided_pools[0].borrows(local), 0);
        assert_eq!(data.provided_pools[0].ring_tail(), 5);
    }

    #[test]
    fn provided_buffer_stale_generation_skips_recycle() {
        let task = Task::new();
        let mut data = test_runtime_data(64);
        let index = data
            .tasks
            .insert(task, |_| core::future::ready(()))
            .unwrap();

        let _gen = enter_active_gen();
        let _ctx = data.context_for(index);

        let generation = NonZeroU32::new(_gen.get().get() + 1).unwrap();
        let local = 1u16;
        data.provided_pools[0].mark_selected(local);
        // SAFETY: mark_selected above registered a shared borrow on the slot,
        // so 5 bytes stay within the class-0 slot size.
        let buf: Slice<u8> = unsafe {
            Slice::new(
                Ref::new(pack_bid(true, 0, u32::from(local)), generation, 0),
                5,
            )
        };

        assert_eq!(data.provided_pools[0].ring_tail(), 4);
        drop(buf);
        assert_eq!(data.provided_pools[0].ring_tail(), 4);
    }

    #[test]
    fn provided_buffer_into_vec_recycles_slot() {
        let task = Task::new();
        let mut data = test_runtime_data(64);
        let index = data
            .tasks
            .insert(task, |_| core::future::ready(()))
            .unwrap();

        let _gen = enter_active_gen();
        let _ctx = data.context_for(index);

        let local = 1u16;
        let buf = data.provided_pools[0].select(local, 5);

        assert_eq!(data.provided_pools[0].ring_tail(), 4);
        let bytes = buf.into_vec();
        assert_eq!(bytes.len(), 5);
        assert_eq!(data.provided_pools[0].ring_tail(), 5);
    }

    #[test]
    fn provided_bytes_into_mut_and_into_bytes_roundtrip() {
        let task = Task::new();
        let mut data = test_runtime_data(64);
        let index = data
            .tasks
            .insert(task, |_| core::future::ready(()))
            .unwrap();

        let _gen = enter_active_gen();
        let _ctx = data.context_for(index);

        let local = 1u16;
        let buf = data.provided_pools[0].select(local, 5);
        assert_eq!(data.provided_pools[0].borrows(local), 1);

        // Upgrade to an exclusive, writable view.
        let mut buf = buf.into_mut();
        assert_eq!(data.provided_pools[0].borrows(local), -1);
        assert_eq!(buf.capacity(), 16);
        buf.as_mut()[..5].copy_from_slice(b"hello");
        buf.set_len(5);

        // Downgrade back to a shared read buffer and read the data back.
        let buf = buf.into_bytes();
        assert_eq!(data.provided_pools[0].borrows(local), 1);
        assert_eq!(buf.as_ref(), b"hello");

        drop(buf);
        assert_eq!(data.provided_pools[0].borrows(local), 0);
        assert_eq!(data.provided_pools[0].ring_tail(), 5);
    }

    #[test]
    fn provided_bytesmut_split_at_recycles_after_both_halves_drop() {
        let task = Task::new();
        let mut data = test_runtime_data(64);
        let index = data
            .tasks
            .insert(task, |_| core::future::ready(()))
            .unwrap();

        let _gen = enter_active_gen();
        let _ctx = data.context_for(index);

        let local = 1u16;
        let buf = data.provided_pools[0].select(local, 5);
        let buf = buf.into_mut();
        let (head, tail) = buf.split_at(2).unwrap();
        assert_eq!(data.provided_pools[0].borrows(local), -2);
        drop(head);
        assert_eq!(data.provided_pools[0].borrows(local), -1);
        assert_eq!(data.provided_pools[0].ring_tail(), 4);
        drop(tail);
        assert_eq!(data.provided_pools[0].borrows(local), 0);
        assert_eq!(data.provided_pools[0].ring_tail(), 5);
    }

    #[test]
    fn provided_upgrade_panics_unless_sole_shared_holder() {
        let task = Task::new();
        let mut data = test_runtime_data(64);
        let index = data
            .tasks
            .insert(task, |_| core::future::ready(()))
            .unwrap();

        let _gen = enter_active_gen();
        let _ctx = data.context_for(index);

        let local = 1u16;
        let buf = data.provided_pools[0].select(local, 5);
        data.provided_pools[0].clone_shared(local);
        assert_eq!(data.provided_pools[0].borrows(local), 2);

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| buf.into_mut()));
        assert!(result.is_err());
        // The failed upgrade left the borrows intact; the unwind dropped
        // `buf`, releasing one of the two shared holders.
        assert_eq!(data.provided_pools[0].borrows(local), 1);
    }

    #[test]
    fn provided_select_mut_borrows_exclusive_and_recycles() {
        let task = Task::new();
        let mut data = test_runtime_data(64);
        let index = data
            .tasks
            .insert(task, |_| core::future::ready(()))
            .unwrap();

        let _gen = enter_active_gen();
        let _ctx = data.context_for(index);

        let local = 1u16;
        let mut buf = data.provided_pools[0].select_mut(local, 5);
        assert_eq!(data.provided_pools[0].borrows(local), -1);
        assert_eq!(buf.capacity(), 16);
        assert_eq!(buf.len(), 5);
        buf.as_mut()[..5].copy_from_slice(b"hello");

        drop(buf);
        assert_eq!(data.provided_pools[0].borrows(local), 0);
        assert_eq!(data.provided_pools[0].ring_tail(), 5);
    }

    #[test]
    fn provided_select_double_selection_panics() {
        let task = Task::new();
        let mut data = test_runtime_data(64);
        let index = data
            .tasks
            .insert(task, |_| core::future::ready(()))
            .unwrap();

        let _gen = enter_active_gen();
        let _ctx = data.context_for(index);

        let buf = data.provided_pools[0].select(1, 5);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            data.provided_pools[0].select(1, 5);
        }));
        assert!(result.is_err());
        drop(buf);
        assert_eq!(data.provided_pools[0].borrows(1), 0);
    }

    // ── BytesMut ─────────────────────────────────────────────────────

    #[test]
    fn buffer_bytes_drop_recycles_slot() {
        let task = Task::new();
        let mut data = test_runtime_data(64);
        let index = data
            .tasks
            .insert(task, |_| core::future::ready(()))
            .unwrap();

        let _gen = enter_active_gen();
        let _ctx = data.context_for(index);

        let buf = data.fixed_pools[0].acquire_bytes_mut().unwrap();

        assert_eq!(data.fixed_pools[0].free_count(), 3);
        drop(buf);
        assert_eq!(data.fixed_pools[0].free_count(), 4);
    }

    #[test]
    fn buffer_bytes_stale_generation_skips_recycle() {
        let task = Task::new();
        let mut data = test_runtime_data(64);
        let index = data
            .tasks
            .insert(task, |_| core::future::ready(()))
            .unwrap();

        let _gen = enter_active_gen();
        let _ctx = data.context_for(index);

        let local = data.fixed_pools[0].acquire_slot().unwrap();
        let generation = NonZeroU32::new(_gen.get().get() + 1).unwrap();
        // SAFETY: the borrow is registered via acquire, but the generation is
        // deliberately stale so drop leaks the slot instead of recycling it.
        let buf: SliceMut<u8> =
            unsafe { SliceMut::new(RefMut::new(pack_bid(false, 0, local), generation, 0), 16) };

        assert_eq!(data.fixed_pools[0].free_count(), 3);
        drop(buf);
        assert_eq!(data.fixed_pools[0].free_count(), 3);
    }

    #[test]
    fn acquire_ref_shared_clone_and_drop() {
        let task = Task::new();
        let mut data = test_runtime_data(64);
        let index = data
            .tasks
            .insert(task, |_| core::future::ready(()))
            .unwrap();

        let _gen = enter_active_gen();
        let _ctx = data.context_for(index);

        let before = data.fixed_pools[0].free_count();
        let view: Ref<[u8; 16]> = data.fixed_pools[0].acquire_ref().unwrap();
        assert_eq!(data.fixed_pools[0].free_count(), before - 1);
        let cloned = view.clone();
        assert_eq!(data.fixed_pools[0].borrows(0), 2);
        drop(cloned);
        assert_eq!(data.fixed_pools[0].borrows(0), 1);
        drop(view);
        assert_eq!(data.fixed_pools[0].free_count(), before);
    }

    #[test]
    fn acquire_mut_exclusive_and_drop() {
        let task = Task::new();
        let mut data = test_runtime_data(64);
        let index = data
            .tasks
            .insert(task, |_| core::future::ready(()))
            .unwrap();

        let _gen = enter_active_gen();
        let _ctx = data.context_for(index);

        let before = data.fixed_pools[0].free_count();
        let view: RefMut<[u8; 16]> = data.fixed_pools[0].acquire_mut().unwrap();
        assert_eq!(data.fixed_pools[0].free_count(), before - 1);
        assert_eq!(data.fixed_pools[0].borrows(0), -1);
        drop(view);
        assert_eq!(data.fixed_pools[0].free_count(), before);
    }

    #[test]
    fn acquire_slices_cover_the_whole_slot() {
        let task = Task::new();
        let mut data = test_runtime_data(64);
        let index = data
            .tasks
            .insert(task, |_| core::future::ready(()))
            .unwrap();

        let _gen = enter_active_gen();
        let _ctx = data.context_for(index);

        let before = data.fixed_pools[0].free_count();
        let bytes: Bytes = data.fixed_pools[0].acquire_bytes().unwrap();
        assert_eq!(bytes.len(), 16);
        drop(bytes);

        let bytes_mut: BytesMut = data.fixed_pools[0].acquire_bytes_mut().unwrap();
        assert_eq!(bytes_mut.len(), 16);
        assert_eq!(bytes_mut.capacity(), 16);
        drop(bytes_mut);

        let slice: Slice<u32> = data.fixed_pools[0].acquire_slice().unwrap();
        assert_eq!(slice.len(), 4);
        drop(slice);

        let slice_mut: SliceMut<u64> = data.fixed_pools[0].acquire_slice_mut().unwrap();
        assert_eq!(slice_mut.len(), 2);
        drop(slice_mut);

        assert_eq!(data.fixed_pools[0].free_count(), before);
    }

    #[test]
    fn alloc_bytes_helper_returns_slot_capacity() {
        let task = Task::new();
        let mut data = test_runtime_data(64);
        let index = data
            .tasks
            .insert(task, |_| core::future::ready(()))
            .unwrap();

        let _gen = enter_active_gen();
        let ctx = data.context_for(index);

        let before = data.fixed_pools[0].free_count();
        let buf = ctx.alloc_bytes(5).unwrap();
        assert_eq!(buf.capacity(), 16);
        assert_eq!(buf.len(), 16);
        assert_eq!(data.fixed_pools[0].free_count(), before - 1);
        drop(buf);
        assert_eq!(data.fixed_pools[0].free_count(), before);
    }

    #[test]
    fn alloc_bytes_as_mut_set_len_clear() {
        let task = Task::new();
        let mut data = test_runtime_data(64);
        let index = data
            .tasks
            .insert(task, |_| core::future::ready(()))
            .unwrap();

        let _gen = enter_active_gen();
        let ctx = data.context_for(index);

        let mut buf = ctx.alloc_bytes(5).unwrap();
        assert_eq!(buf.as_mut().len(), 16);
        buf.as_mut()[..5].copy_from_slice(b"hello");
        buf.set_len(5);
        assert_eq!(buf.len(), 5);
        assert_eq!(buf.as_ref(), b"hello");

        buf.set_len(16);
        assert_eq!(buf.len(), 16);

        buf.clear();
        assert_eq!(buf.len(), 0);
        assert!(buf.is_empty());
    }

    #[test]
    #[should_panic(expected = "exceeds capacity")]
    fn alloc_bytes_set_len_above_capacity_panics() {
        let task = Task::new();
        let mut data = test_runtime_data(64);
        let index = data
            .tasks
            .insert(task, |_| core::future::ready(()))
            .unwrap();

        let _gen = enter_active_gen();
        let ctx = data.context_for(index);

        let mut buf = ctx.alloc_bytes(5).unwrap();
        buf.set_len(17);
    }

    // ── IoUserData ────────────────────────────────────────────────────

    #[test]
    fn io_user_data_u64_roundtrip() {
        for raw in [0u64, 1, u64::MAX, 0xDEAD_BEEF, 0x1234_5678_9ABC_DEF0] {
            let ud: IoUserData = raw.into();
            let back: u64 = ud.into();
            assert_eq!(raw, back);
        }
    }

    #[test]
    fn io_user_data_struct_roundtrip() {
        let ud = IoUserData {
            index: 42,
            io_slot: 7,
        };
        let raw: u64 = ud.into();
        let back: IoUserData = raw.into();
        assert_eq!(back.index, 42);
        assert_eq!(back.io_slot, 7);
    }

    // ── Runtime::has_io_in_flight ────────────────────────────────────

    #[test]
    fn has_io_in_flight_empty_slab() {
        let data = test_runtime_data(64);
        assert!(!data.has_io_in_flight());
    }

    #[test]
    fn has_io_in_flight_true_while_submitted_not_ready() {
        let mut data = test_runtime_data(64);
        data.alloc_io_slot();
        assert!(data.has_io_in_flight());
    }

    #[test]
    fn has_io_in_flight_false_when_ready() {
        let mut data = test_runtime_data(64);
        let slot = data.alloc_io_slot();
        data.io_slab[slot as usize].ready = 1;
        assert!(!data.has_io_in_flight());
    }

    #[test]
    fn has_io_in_flight_false_after_free() {
        let mut data = test_runtime_data(64);
        let slot = data.alloc_io_slot();
        data.free_io_slot(slot);
        assert!(!data.has_io_in_flight());
    }

    // ── await_cqe ─────────────────────────────────────────────────────

    #[test]
    fn await_cqe_immediate_ready() {
        let mut data = test_runtime_data(64);
        let slot = data.alloc_io_slot();
        data.io_slab[slot as usize].result = 42;
        data.io_slab[slot as usize].ready = 1;

        let index = data
            .tasks
            .insert(Task::new(), |_| core::future::ready(()))
            .unwrap();

        let _gen = enter_active_gen();
        let ctx = data.context_for(index);

        let fut = await_cqe(ctx, slot);
        let mut fut = pin!(fut);
        let mut cx = Context::from_waker(Waker::noop());

        assert_eq!(fut.as_mut().poll(&mut cx), Poll::Ready(42));
    }

    #[test]
    fn await_cqe_delayed_ready() {
        let mut data = test_runtime_data(64);
        let slot = data.alloc_io_slot();
        // NOT setting ready yet

        let index = data
            .tasks
            .insert(Task::new(), |_| core::future::ready(()))
            .unwrap();

        let _gen = enter_active_gen();
        let ctx = data.context_for(index);

        let fut = await_cqe(ctx, slot);
        let mut fut = pin!(fut);
        let mut cx = Context::from_waker(Waker::noop());

        // First poll: not ready → Yield's first poll → Pending
        assert_eq!(fut.as_mut().poll(&mut cx), Poll::Pending);

        // Now set ready externally
        data.io_slab[slot as usize].result = 99;
        data.io_slab[slot as usize].ready = 1;

        // Second poll: Yield's second poll → Ready, loop sees ready → Ready(99)
        assert_eq!(fut.as_mut().poll(&mut cx), Poll::Ready(99));
    }

    // ── Yield ─────────────────────────────────────────────────────────

    #[test]
    fn yield_first_poll_returns_pending() {
        let task = Task::new();
        let mut data = test_runtime_data(64);
        let index = data
            .tasks
            .insert(task, |_| core::future::ready(()))
            .unwrap();

        let _gen = enter_active_gen();
        let ctx = data.context_for(index);
        let mut y = yield_now(ctx);
        let mut y = pin!(&mut y);
        let mut cx = Context::from_waker(Waker::noop());

        assert_eq!(y.as_mut().poll(&mut cx), Poll::Pending);
        assert!(y.polled);
    }

    #[test]
    fn yield_second_poll_returns_ready() {
        let task = Task::new();
        let mut data = test_runtime_data(64);
        let index = data
            .tasks
            .insert(task, |_| core::future::ready(()))
            .unwrap();

        let _gen = enter_active_gen();
        let ctx = data.context_for(index);
        let mut y = yield_now(ctx);
        let mut y = pin!(&mut y);
        let mut cx = Context::from_waker(Waker::noop());

        assert_eq!(y.as_mut().poll(&mut cx), Poll::Pending);
        assert_eq!(y.as_mut().poll(&mut cx), Poll::Ready(()));
    }

    #[test]
    fn yield_calls_wake_on_first_poll() {
        let task = Task::new();
        let mut data = test_runtime_data(64);
        let index = data
            .tasks
            .insert(task, |_| core::future::ready(()))
            .unwrap();

        let _gen = enter_active_gen();
        let ctx = data.context_for(index);
        let mut y = yield_now(ctx);
        let mut y = pin!(&mut y);
        let mut cx = Context::from_waker(Waker::noop());

        let _ = y.as_mut().poll(&mut cx);
        // wake should have pushed index into wakeups
        assert_eq!(data.wakeups, vec![index]);
    }

    // ── TaskContext ───────────────────────────────────────────────────

    #[test]
    fn task_context_with_task_reads_correct_task() {
        let task = Task::new();
        let mut data = test_runtime_data(64);
        let index = data
            .tasks
            .insert(task, |_| core::future::ready(()))
            .unwrap();

        let _gen = enter_active_gen();
        let ctx = data.context_for(index);

        let ready = ctx.with_task(|t| t.ready);
        assert!(!ready);
    }

    #[test]
    fn task_context_with_task_modifies() {
        let task = Task::new();
        let mut data = test_runtime_data(64);
        let index = data
            .tasks
            .insert(task, |_| core::future::ready(()))
            .unwrap();

        let _gen = enter_active_gen();
        let ctx = data.context_for(index);

        ctx.with_task(|t| t.ready = true);
        assert!(data.tasks.task(index).ready);
    }

    #[test]
    fn task_context_with_runtime_reads_buffer_pool() {
        let task = Task::new();
        let mut data = test_runtime_data(64);
        let index = data
            .tasks
            .insert(task, |_| core::future::ready(()))
            .unwrap();

        let _gen = enter_active_gen();
        let ctx = data.context_for(index);

        let bgid = ctx.with_runtime(|r| r.provided_pools[0].bgid());
        assert_eq!(bgid, 0);
    }

    #[test]
    fn task_context_with_runtime_after_removal_panics() {
        let task = Task::new();
        let mut data = test_runtime_data(64);
        let index = data
            .tasks
            .insert(task, |_| core::future::ready(()))
            .unwrap();

        let _gen = enter_active_gen();
        let ctx = data.context_for(index);

        let _ = data.tasks.remove::<Ready<()>, ()>(index);

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            ctx.with_runtime(|_r| ());
        }));
        assert!(result.is_err());
    }

    #[test]
    fn task_context_wake_pushes_index() {
        let task = Task::new();
        let mut data = test_runtime_data(64);
        let index = data
            .tasks
            .insert(task, |_| core::future::ready(()))
            .unwrap();

        let _gen = enter_active_gen();
        let ctx = data.context_for(index);

        ctx.wake();
        ctx.wake();
        assert_eq!(data.wakeups, vec![index, index]);
    }

    #[test]
    fn task_context_after_removal_panics() {
        let task = Task::new();
        let mut data = test_runtime_data(64);
        let index = data
            .tasks
            .insert(task, |_| core::future::ready(()))
            .unwrap();

        let _gen = enter_active_gen();
        let ctx = data.context_for(index);

        let _ = data.tasks.remove::<Ready<()>, ()>(index);

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            ctx.with_task(|_t| ());
        }));
        assert!(result.is_err());
    }

    #[test]
    fn task_context_after_slot_reuse_panics_without_touching_new_task() {
        let task_a = Task::new();
        let mut data = test_runtime_data(64);
        let index = data
            .tasks
            .insert(task_a, |_| core::future::ready(()))
            .unwrap();

        let _gen = enter_active_gen();
        let ctx = data.context_for(index);

        let _ = data.tasks.remove::<Ready<()>, ()>(index);

        let mut task_b = Task::new();
        task_b.ready = true;
        let index = data
            .tasks
            .insert(task_b, |_| core::future::ready(()))
            .expect("slot should be free");

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            ctx.with_task(|t| t.ready = false);
        }));

        assert!(result.is_err());
        assert!(data.tasks.task(index).ready);
    }

    #[test]
    fn task_context_of_live_task_usable_alongside_other_tasks() {
        let task_a = Task::new();
        let mut data = test_runtime_data(64);
        let index_a = data
            .tasks
            .insert(task_a, |_| core::future::ready(()))
            .unwrap();

        let mut task_b = Task::new();
        task_b.ready = true;
        let index_b = data
            .tasks
            .insert(task_b, |_| core::future::ready(()))
            .unwrap();

        let _gen = enter_active_gen();
        let ctx_a = data.context_for(index_a);
        let _ctx_b = data.context_for(index_b);

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            ctx_a.with_task(|t| t.ready = true);
        }));

        assert!(result.is_ok());
        assert!(data.tasks.task(index_a).ready);
    }

    // ── Ref ───────────────────────────────────────────────────────────

    #[test]
    fn alloc_drop_recycles_slot() {
        let task = Task::new();
        let mut data = test_runtime_data(64);
        let index = data
            .tasks
            .insert(task, |_| core::future::ready(()))
            .unwrap();

        let _gen = enter_active_gen();
        let ctx = data.context_for(index);

        let before = data.fixed_pools[0].free_count();
        let alloc = ctx.alloc::<[u8; 8]>().unwrap();
        assert_eq!(data.fixed_pools[0].free_count(), before - 1);
        drop(alloc);
        assert_eq!(data.fixed_pools[0].free_count(), before);
    }

    #[test]
    fn alloc_cast_preserves_slot() {
        let task = Task::new();
        let mut data = test_runtime_data(64);
        let index = data
            .tasks
            .insert(task, |_| core::future::ready(()))
            .unwrap();

        let _gen = enter_active_gen();
        let ctx = data.context_for(index);

        // Casting strips the outer `MaybeUninit` but names the same slot.
        let orig = ctx.alloc::<[u8; 16]>().unwrap();
        let orig_ptr = orig.as_ptr();
        let before = data.fixed_pools[0].free_count();
        let casted: Ref<[u8; 16]> = unsafe { orig.cast() };
        assert_eq!(orig_ptr.cast::<u8>(), casted.as_ptr().cast::<u8>());

        // The cast transfers the slot: it must not recycle it (which would
        // hand the same slot out twice on the next alloc).
        assert_eq!(data.fixed_pools[0].free_count(), before);

        // The slot still recycles on drop after the cast, exactly once.
        drop(casted);
        assert_eq!(data.fixed_pools[0].free_count(), before + 1);
    }

    #[test]
    #[should_panic(expected = "zero-sized")]
    fn alloc_zst_panics() {
        let task = Task::new();
        let mut data = test_runtime_data(64);
        let index = data
            .tasks
            .insert(task, |_| core::future::ready(()))
            .unwrap();

        let _gen = enter_active_gen();
        let ctx = data.context_for(index);

        let _ = ctx.alloc::<()>();
    }

    #[test]
    fn alloc_stale_generation_leaks_on_drop() {
        let task = Task::new();
        let mut data = test_runtime_data(64);
        let index = data
            .tasks
            .insert(task, |_| core::future::ready(()))
            .unwrap();

        let gen_guard = enter_active_gen();
        let ctx = data.context_for(index);

        let alloc = ctx.alloc::<[u8; 8]>().unwrap();
        let before = data.fixed_pools[0].free_count();
        // The runtime shuts down: the drop must leak, not recycle.
        drop(gen_guard);
        drop(alloc);
        assert_eq!(data.fixed_pools[0].free_count(), before);
    }

    #[test]
    #[should_panic(expected = "outside the runtime")]
    fn alloc_as_ptr_stale_generation_panics() {
        let task = Task::new();
        let mut data = test_runtime_data(64);
        let index = data
            .tasks
            .insert(task, |_| core::future::ready(()))
            .unwrap();

        let _gen = enter_active_gen();
        // context_for sets the current runtime; only its side effect is needed.
        let _ctx = data.context_for(index);

        let stale = NonZeroU32::new(_gen.get().get() + 1).unwrap();
        // SAFETY: a deliberately fabricated view over a never-borrowed slot
        // with a stale generation: resolve panics before touching memory and
        // drop would leak the slot rather than corrupt the pool.
        let alloc: Ref<MaybeUninit<[u8; 16]>> =
            unsafe { Ref::new(pack_bid(false, 0, 0), stale, 0) };
        let _ = alloc.as_ptr();
    }

    #[test]
    fn ref_clone_adds_and_drops_shared_borrows() {
        let task = Task::new();
        let mut data = test_runtime_data(64);
        let index = data
            .tasks
            .insert(task, |_| core::future::ready(()))
            .unwrap();

        let _gen = enter_active_gen();
        let ctx = data.context_for(index);

        let before = data.fixed_pools[0].free_count();
        let alloc = ctx.alloc::<[u8; 8]>().unwrap();
        assert_eq!(data.fixed_pools[0].borrows(0), 1);
        let clone = alloc.clone();
        assert_eq!(data.fixed_pools[0].borrows(0), 2);
        assert_eq!(data.fixed_pools[0].free_count(), before - 1);
        drop(alloc);
        // One borrower left: the slot is still held by the clone.
        assert_eq!(data.fixed_pools[0].borrows(0), 1);
        assert_eq!(data.fixed_pools[0].free_count(), before - 1);
        drop(clone);
        assert_eq!(data.fixed_pools[0].borrows(0), 0);
        assert_eq!(data.fixed_pools[0].free_count(), before);
    }

    #[test]
    fn ref_upgrade_and_downgrade_sole_holder() {
        let task = Task::new();
        let mut data = test_runtime_data(64);
        let index = data
            .tasks
            .insert(task, |_| core::future::ready(()))
            .unwrap();

        let _gen = enter_active_gen();
        let ctx = data.context_for(index);

        let before = data.fixed_pools[0].free_count();
        let alloc = ctx.alloc::<[u8; 8]>().unwrap();
        assert_eq!(data.fixed_pools[0].borrows(0), 1);
        let exclusive = alloc.into_mut();
        assert_eq!(data.fixed_pools[0].borrows(0), -1);
        let shared: Ref<[u8; 8]> = unsafe { exclusive.into_ref().cast() };
        assert_eq!(data.fixed_pools[0].borrows(0), 1);
        drop(shared);
        assert_eq!(data.fixed_pools[0].free_count(), before);
    }

    // ── Msg / MsgMut ─────────────────────────────────────────────────

    #[test]
    fn msg_new_wires_iov_and_ctrl_to_pooled_memory() {
        let task = Task::new();
        let mut data = test_runtime_data(64);
        let index = data
            .tasks
            .insert(task, |_| core::future::ready(()))
            .unwrap();

        let _gen = enter_active_gen();
        let ctx = data.context_for(index);

        // A Msg always allocates the mandatory msghdr plus the full iov array
        // and control buffer, all in pooled memory, starting empty.
        let mut slot = Msg::new(ctx).unwrap();
        {
            let msg = slot.msg();
            assert_eq!(msg.msg_iovlen, 0);
            assert_eq!(msg.msg_controllen, 0);
            assert!(!msg.msg_iov.is_null());
            assert!(!msg.msg_control.is_null());
        }
        assert!(slot.iov(0).is_none());
        assert!(slot.ctrl().unwrap().is_empty());
    }

    #[test]
    fn msg_drop_recycles_all_slots() {
        let task = Task::new();
        let mut data = test_runtime_data(64);
        let index = data
            .tasks
            .insert(task, |_| core::future::ready(()))
            .unwrap();

        let _gen = enter_active_gen();
        let ctx = data.context_for(index);

        // The msghdr (56 B) lands in class 1 (64), the ctrl array
        // (MAX_CTRL_CAP) in class 2 (2048) and the iov array (MAX_IOV_CAP *
        // 16) in class 3 (16384).
        let (free1, free2, free3) = (
            data.fixed_pools[1].free_count(),
            data.fixed_pools[2].free_count(),
            data.fixed_pools[3].free_count(),
        );
        let slot = Msg::new(ctx).unwrap();
        assert_eq!(data.fixed_pools[1].free_count(), free1 - 1);
        assert_eq!(data.fixed_pools[2].free_count(), free2 - 1);
        assert_eq!(data.fixed_pools[3].free_count(), free3 - 1);
        drop(slot);
        assert_eq!(data.fixed_pools[1].free_count(), free1);
        assert_eq!(data.fixed_pools[2].free_count(), free2);
        assert_eq!(data.fixed_pools[3].free_count(), free3);
    }

    #[test]
    fn msg_slot_wires_iov_to_pooled_memory() {
        let task = Task::new();
        let mut data = test_runtime_data(64);
        let index = data
            .tasks
            .insert(task, |_| core::future::ready(()))
            .unwrap();

        let _gen = enter_active_gen();
        let ctx = data.context_for(index);

        let mut slot = Msg::new(ctx).unwrap();
        let msg_iov = {
            let msg = slot.msg();
            assert_eq!(msg.msg_iovlen, 0);
            assert_eq!(msg.msg_controllen, 0);
            msg.msg_iov as *mut libc::iovec
        };
        // Nothing pushed yet: the accessor exposes no entries.
        assert!(slot.iov(0).is_none());
        assert!(!msg_iov.is_null());
        // Pushes land in the pooled array and advance msg_iovlen.
        assert!(slot.push_iov(libc::iovec {
            iov_base: std::ptr::null_mut(),
            iov_len: 5,
        }));
        assert!(slot.push_iov(libc::iovec {
            iov_base: std::ptr::null_mut(),
            iov_len: 6,
        }));
        assert_eq!(msg_iov, slot.iov(0).unwrap() as *mut libc::iovec);
        assert_eq!(slot.msg().msg_iovlen, 2);
        assert!(slot.iov(1).is_some());
        assert!(slot.iov(2).is_none());
        // Writes through the accessor land in the pooled memory.
        slot.iov(0).unwrap().iov_len = 7;
        assert_eq!(unsafe { (*msg_iov).iov_len }, 7);
    }

    #[test]
    fn msg_slot_push_iov_rejects_past_capacity() {
        let task = Task::new();
        let mut data = test_runtime_data(64);
        let index = data
            .tasks
            .insert(task, |_| core::future::ready(()))
            .unwrap();

        let _gen = enter_active_gen();
        let ctx = data.context_for(index);

        let mut slot = Msg::new(ctx).unwrap();
        for _ in 0..MAX_IOV_CAP {
            assert!(slot.push_iov(libc::iovec {
                iov_base: std::ptr::null_mut(),
                iov_len: 1,
            }));
        }
        assert!(!slot.push_iov(libc::iovec {
            iov_base: std::ptr::null_mut(),
            iov_len: 2,
        }));
        assert_eq!(slot.msg().msg_iovlen, MAX_IOV_CAP);
    }

    #[test]
    fn msg_slot_wires_ctrl_to_pooled_memory() {
        let task = Task::new();
        let mut data = test_runtime_data(64);
        let index = data
            .tasks
            .insert(task, |_| core::future::ready(()))
            .unwrap();

        let _gen = enter_active_gen();
        let ctx = data.context_for(index);

        let mut slot = Msg::new(ctx).unwrap();
        let msg_control = {
            let msg = slot.msg();
            assert_eq!(msg.msg_controllen, 0);
            msg.msg_control as *mut u8
        };
        assert_eq!(msg_control, slot.ctrl().unwrap().as_mut_ptr());
        // push_cmsg wires msg_controllen to CMSG_SPACE of the pushed payload.
        assert!(slot.push_cmsg(libc::SOL_SOCKET, libc::SCM_RIGHTS, &[0u8; 4]));
        assert_eq!(slot.msg().msg_controllen, cmsg_space(4));
        assert!(slot.msg().msg_control as usize != 0);
    }

    #[test]
    fn msg_slot_push_cmsg_accumulates_and_overflows() {
        let task = Task::new();
        let mut data = test_runtime_data(64);
        let index = data
            .tasks
            .insert(task, |_| core::future::ready(()))
            .unwrap();

        let _gen = enter_active_gen();
        let ctx = data.context_for(index);

        let mut slot = Msg::new(ctx).unwrap();
        let payload = [1u8, 2, 3, 4];
        assert!(slot.push_cmsg(libc::SOL_SOCKET, libc::SCM_RIGHTS, &payload));
        // The cmsg bytes land at the start of the pooled control buffer.
        let ctrl = slot.ctrl().unwrap();
        let hdr = ctrl.as_ptr() as *const libc::cmsghdr;
        unsafe {
            assert_eq!((*hdr).cmsg_level, libc::SOL_SOCKET);
            assert_eq!((*hdr).cmsg_type, libc::SCM_RIGHTS);
            assert_eq!((*hdr).cmsg_len, size_of::<libc::cmsghdr>() + payload.len());
        }
        // A second cmsg of the same size fits and accumulates its space.
        assert!(slot.push_cmsg(libc::SOL_SOCKET, libc::SCM_CREDENTIALS, &payload));
        assert_eq!(slot.msg().msg_controllen, 2 * cmsg_space(4));
        assert_eq!(slot.ctrl().unwrap().len(), 2 * cmsg_space(4));
        // A cmsg that would exceed MAX_CTRL_CAP is rejected, lengths unchanged.
        let huge = vec![0u8; MAX_CTRL_CAP];
        assert!(!slot.push_cmsg(1, 2, &huge));
        assert_eq!(slot.msg().msg_controllen, 2 * cmsg_space(4));
    }

    #[test]
    fn msg_caps_and_sizes_are_kernel_maxima() {
        // The caps are the kernel's limits: UIO_MAXIOV (1024 iovecs) and
        // CMSG_SPACE of SCM_MAX_FD (253 fds).
        assert_eq!(MAX_IOV_CAP, 1024);
        assert_eq!(MAX_CTRL_CAP, cmsg_space(253 * size_of::<libc::c_int>()));
        assert_eq!(size_of::<Msg>(), 36);
        assert_eq!(size_of::<MsgMut>(), 36);
    }

    #[test]
    fn msgmut_take_iov_copies_and_resets() {
        let task = Task::new();
        let mut data = test_runtime_data(64);
        let index = data
            .tasks
            .insert(task, |_| core::future::ready(()))
            .unwrap();

        let _gen = enter_active_gen();
        let ctx = data.context_for(index);

        let mut slot = MsgMut::new(ctx).unwrap();
        assert!(slot.push_iov(libc::iovec {
            iov_base: std::ptr::null_mut(),
            iov_len: 3,
        }));
        assert!(slot.push_iov(libc::iovec {
            iov_base: std::ptr::null_mut(),
            iov_len: 4,
        }));
        let iovs = slot.take_iov();
        assert_eq!(iovs.len(), 2);
        assert_eq!(iovs[0].iov_len, 3);
        assert_eq!(iovs[1].iov_len, 4);
        // The pooled array is reset, so nothing remains to offer the kernel.
        assert_eq!(slot.msg().msg_iovlen, 0);
        assert!(slot.iov(0).is_none());
    }
}
