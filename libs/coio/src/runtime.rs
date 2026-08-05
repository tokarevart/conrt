use core::cell::Cell;
use core::future::Future;
use core::num::NonZeroU32;
use core::pin::Pin;
use core::task::Context;
use core::task::Poll;
use core::task::Waker;
use std::io;
use std::os::fd::RawFd;
use std::ptr::NonNull;

use crate::levels::DEFAULT_LEVELS;
use crate::levels::Level;
use crate::levels::layout_levels;
use crate::levels::level_for;
use crate::levels::pack_bid;
use crate::pbuf::ProvidedBufferPool;
use crate::pbuf::ReadBuffer;
use crate::task::IoSlot;
use crate::task::IoUserData;
use crate::task::IoVec;
use crate::task::JoinHandle;
use crate::task::NO_JOINER;
use crate::task::Task;
use crate::task::TaskContext;
use crate::task::TaskSlab;
use crate::wbuf::WriteBuffer;
use crate::wbuf::WriteBufferPool;

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

/// Reads up to `max_len` bytes from `fd` into a buffer selected from the
/// runtime's provided buffer pools. The buffer is drawn from the smallest
/// level whose slot size is at least `max_len`; `max_len` larger than the
/// largest level's slot size fails with `EFBIG`.
///
/// The returned [`ReadBuffer`] borrows memory from the pool: its slot is
/// recycled when the buffer is dropped, so the runtime must still be alive
/// while the buffer is in use.
pub async fn read(ctx: TaskContext, fd: RawFd, max_len: usize) -> io::Result<ReadBuffer> {
    let bgid = ctx.with_runtime(|r| -> io::Result<u16> {
        let level = level_for(&r.read_levels, max_len)
            .ok_or_else(|| io::Error::from_raw_os_error(libc::EFBIG))?;
        Ok(level as u16)
    })?;

    let slot = ctx.with_runtime(|r| r.alloc_io_slot());
    ctx.with_task(|task| task.io.push(slot));

    let entry = io_uring::opcode::Read::new(
        io_uring::types::Fd(fd),
        std::ptr::null_mut(),
        max_len as u32,
    )
    .buf_group(bgid)
    .build()
    .flags(io_uring::squeue::Flags::BUFFER_SELECT);
    if let Err(e) = ctx.push_io(entry, slot) {
        ctx.with_runtime(|r| r.free_io_slot(slot));
        ctx.with_task(|task| task.io.remove_value(slot));
        return Err(e);
    }

    let result = await_cqe(ctx, slot).await;
    if result < 0 {
        ctx.with_runtime(|r| r.free_io_slot(slot));
        ctx.with_task(|task| task.io.remove_value(slot));
        return Err(io::Error::from_raw_os_error(-result));
    }

    let generation = active_gen().expect("read called outside an active runtime");
    let level = bgid as usize;
    let (offset, bid) = ctx.with_runtime(|r| {
        let slot_ref = &r.io_slab[slot as usize];
        let local = slot_ref.bid;
        let offset = r.buffer_pools[level].slot_offset(local);
        let bid = pack_bid(level as u32, u32::from(local));
        r.free_io_slot(slot);
        (offset, bid)
    });
    ctx.with_task(|task| task.io.remove_value(slot));

    Ok(ReadBuffer::new(offset, bid, result as u32, generation))
}

/// Acquires a zero-copy write buffer backed by a slot from the runtime's fixed
/// write buffer slab. The buffer is drawn from the smallest level whose slot
/// size is at least `size`; `size` larger than the largest level's slot size
/// fails with `EFBIG`. The buffer's [`WriteBuffer::capacity`] is the chosen
/// level's slot size; the caller fills it via [`WriteBuffer::as_mut`] and
/// records the length with [`WriteBuffer::set_len`] before passing it to
/// [`write`]. The slot is recycled when the buffer is dropped.
pub fn write_buffer(ctx: TaskContext, size: usize) -> io::Result<WriteBuffer> {
    let generation = active_gen().expect("write_buffer called outside an active runtime");
    let (bid, offset) = ctx.with_runtime(|r| -> io::Result<(u32, u32)> {
        let level = level_for(&r.write_levels, size)
            .ok_or_else(|| io::Error::from_raw_os_error(libc::EFBIG))?;
        let local = r.write_pools[level]
            .acquire()
            .ok_or_else(|| io::Error::from_raw_os_error(libc::ENOMEM))?;
        let offset = r.write_pools[level].slot_offset(local);
        Ok((pack_bid(level as u32, local), offset))
    })?;
    Ok(WriteBuffer::new(offset, bid, generation))
}

/// Writes the contents of `wb` to `fd` via `IORING_OP_WRITE_FIXED`. The
/// buffer's slot is held until the kernel finishes with it and recycled when
/// `wb` is dropped.
pub async fn write(ctx: TaskContext, fd: RawFd, wb: WriteBuffer) -> io::Result<usize> {
    let slot = ctx.with_runtime(|r| r.alloc_io_slot());
    ctx.with_task(|task| task.io.push(slot));

    let len = wb.len();
    let addr =
        ctx.with_runtime(|r| unsafe { r.slab.as_ptr().cast_mut().add(wb.offset() as usize) });

    let entry = io_uring::opcode::WriteFixed::new(
        io_uring::types::Fd(fd),
        addr,
        len as u32,
        0, // the whole slab is registered as fixed buffer index 0
    )
    .build();
    if let Err(e) = ctx.push_io(entry, slot) {
        ctx.with_runtime(|r| r.free_io_slot(slot));
        ctx.with_task(|task| task.io.remove_value(slot));
        return Err(e);
    }

    let result = await_cqe(ctx, slot).await;
    if result < 0 {
        ctx.with_runtime(|r| r.free_io_slot(slot));
        ctx.with_task(|task| task.io.remove_value(slot));
        return Err(io::Error::from_raw_os_error(-result));
    }

    ctx.with_runtime(|r| r.free_io_slot(slot));
    ctx.with_task(|task| task.io.remove_value(slot));
    Ok(result as usize)
}

pub async fn await_cqe(ctx: TaskContext, slot: u32) -> i32 {
    loop {
        let result = ctx.with_runtime(|r| {
            let slot_ref = &r.io_slab[slot as usize];
            if slot_ref.ready != 0 {
                Some(slot_ref.result)
            } else {
                None
            }
        });
        if let Some(result) = result {
            return result;
        }
        yield_now(ctx).await;
    }
}

pub fn yield_now(ctx: TaskContext) -> Yield {
    Yield { ctx, polled: false }
}

pub struct Yield {
    ctx: TaskContext,
    polled: bool,
}

impl Future for Yield {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        if self.polled {
            Poll::Ready(())
        } else {
            self.polled = true;
            self.ctx.wake();
            Poll::Pending
        }
    }
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
    pub slab: Vec<u8>,
    pub read_levels: Vec<Level>,
    pub buffer_pools: Vec<ProvidedBufferPool>,
    pub write_levels: Vec<Level>,
    pub write_pools: Vec<WriteBufferPool>,
    pub ring: io_uring::IoUring,
    pub make_fut: *const (),
}

impl Drop for Runtime {
    fn drop(&mut self) {
        // Unregister every provided buffer ring and the fixed write buffer
        // slab while the io_uring fd is still open; the pools and slabs are
        // dropped afterwards along with the other fields.
        for pool in &self.buffer_pools {
            let _ = pool.unregister(&self.ring);
        }
        let _ = self.ring.submitter().unregister_buffers();
    }
}

impl Runtime {
    /// Allocates an io slab slot and marks it submitted. The slot is reused
    /// from the free list when available, otherwise the slab grows; it only
    /// ever holds in-flight ops, so it stays bounded by peak concurrency.
    fn alloc_io_slot(&mut self) -> u32 {
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
    fn free_io_slot(&mut self, index: u32) {
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
    /// Per-direction buffer levels for reads. Each `count` must be a power of
    /// two no larger than 32768.
    pub read_levels: &'a [Level],
    /// Per-direction buffer levels for writes.
    pub write_levels: &'a [Level],
}

impl<'a> Default for RuntimeParams<'a> {
    fn default() -> Self {
        Self {
            tasks_capacity: 1024,
            ring_entries: 1024,
            read_levels: &DEFAULT_LEVELS,
            write_levels: &DEFAULT_LEVELS,
        }
    }
}

/// The shared buffer slab plus the level tables and pools built over it.
type BuiltPools = (
    Vec<u8>,
    Vec<Level>,
    Vec<ProvidedBufferPool>,
    Vec<Level>,
    Vec<WriteBufferPool>,
);

/// Builds both directions' pools over one shared slab: the read levels occupy
/// the start of the slab, the write levels follow them. Each read level gets a
/// provided-buffer ring registered under `bgid = level index`; the write
/// levels get free-stack pools, and the whole slab is registered once as fixed
/// buffer index 0 so writes can address any slot by its absolute offset.
/// Returns the slab, both sorted level tables, and both pool lists.
fn build_pools(ring: &io_uring::IoUring, read_spec: &[Level], write_spec: &[Level]) -> BuiltPools {
    let read_layout = layout_levels(read_spec, true);
    let write_layout = layout_levels(write_spec, false);
    let read_total = read_layout.last().map(|l| l.total).unwrap();
    let total = read_total + write_layout.last().map(|l| l.total).unwrap();
    assert!(
        total <= u64::from(u32::MAX),
        "the combined read+write slab exceeds the 4 GiB offset range"
    );
    let mut slab = vec![0u8; total as usize];
    let base = unsafe { NonNull::new_unchecked(slab.as_mut_ptr()) };

    let read_levels: Vec<Level> = read_layout
        .iter()
        .map(|l| Level {
            size: l.size,
            count: l.count,
        })
        .collect();
    let mut buffer_pools = Vec::with_capacity(read_layout.len());
    for (i, l) in read_layout.iter().enumerate() {
        let pool = ProvidedBufferPool::new(
            base,
            l.base_offset as usize,
            l.count as u16,
            l.size,
            i as u16,
        );
        pool.register(ring)
            .expect("failed to register the provided buffer ring");
        buffer_pools.push(pool);
    }

    let write_levels: Vec<Level> = write_layout
        .iter()
        .map(|l| Level {
            size: l.size,
            count: l.count,
        })
        .collect();
    let write_pools = write_layout
        .iter()
        .map(|l| {
            WriteBufferPool::new(
                base,
                read_total as usize + l.base_offset as usize,
                l.size,
                l.count,
            )
        })
        .collect();

    let iovec = libc::iovec {
        iov_base: slab.as_mut_ptr().cast::<libc::c_void>(),
        iov_len: slab.len(),
    };
    unsafe { ring.submitter().register_buffers(&[iovec]) }
        .expect("failed to register the buffer slab");

    (slab, read_levels, buffer_pools, write_levels, write_pools)
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

    let (slab, read_levels, buffer_pools, write_levels, write_pools) =
        build_pools(&ring, params.read_levels, params.write_levels);

    let mut rt = Runtime {
        tasks: TaskSlab::new::<F, R>(params.tasks_capacity),
        wakeups: Vec::new(),
        io_slab: Vec::new(),
        free_io_slots: Vec::new(),
        slab,
        read_levels,
        buffer_pools,
        write_levels,
        write_pools,
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
    use core::pin::pin;
    use core::task::Context;
    use core::task::Poll;
    use core::task::Waker;

    use super::*;
    use crate::levels::Level;
    use crate::levels::pack_bid;
    use crate::pbuf::ReadBuffer;
    use crate::task::IoUserData;
    use crate::task::IoVec;
    use crate::task::Task;
    use crate::task::TaskSlab;
    use crate::wbuf::WriteBuffer;

    fn test_runtime_data(capacity: u32) -> Runtime {
        let ring = io_uring::IoUring::new(8).unwrap();
        let (slab, read_levels, buffer_pools, write_levels, write_pools) =
            build_pools(&ring, &[Level { size: 16, count: 4 }], &[Level {
                size: 16,
                count: 4,
            }]);
        Runtime {
            tasks: TaskSlab::new::<Ready<()>, ()>(capacity),
            wakeups: Vec::new(),
            io_slab: Vec::new(),
            free_io_slots: Vec::new(),
            slab,
            read_levels,
            buffer_pools,
            write_levels,
            write_pools,
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
    fn shared_slab_write_region_follows_read_region() {
        let ring = io_uring::IoUring::new(8).unwrap();
        let (_slab, _rl, read_pools, _wl, write_pools) =
            build_pools(&ring, &[Level { size: 16, count: 4 }], &[Level {
                size: 32,
                count: 2,
            }]);
        // The read region is 4 x 16 = 64 bytes; the write level starts right
        // after it in the same slab.
        assert_eq!(read_pools[0].slot_offset(3), 48);
        assert_eq!(write_pools[0].slot_offset(0), 64);
        assert_eq!(write_pools[0].slot_offset(1), 96);
    }

    // ── ReadBuffer ────────────────────────────────────────────────────

    #[test]
    fn read_buffer_drop_recycles_slot() {
        let task = Task::new();
        let mut data = test_runtime_data(64);
        let index = data
            .tasks
            .insert(task, |_| core::future::ready(()))
            .unwrap();

        let _gen = enter_active_gen();
        let _ctx = data.context_for(index);

        let generation = _gen.get();
        let local = 1u16;
        let offset = data.buffer_pools[0].slot_offset(local);
        let buf = ReadBuffer::new(offset, pack_bid(0, u32::from(local)), 5, generation);

        assert_eq!(data.buffer_pools[0].ring_tail(), 4);
        drop(buf);
        assert_eq!(data.buffer_pools[0].ring_tail(), 5);
    }

    #[test]
    fn read_buffer_stale_generation_skips_recycle() {
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
        let offset = data.buffer_pools[0].slot_offset(local);
        let buf = ReadBuffer::new(offset, pack_bid(0, u32::from(local)), 5, generation);

        assert_eq!(data.buffer_pools[0].ring_tail(), 4);
        drop(buf);
        assert_eq!(data.buffer_pools[0].ring_tail(), 4);
    }

    #[test]
    fn read_buffer_into_vec_recycles_slot() {
        let task = Task::new();
        let mut data = test_runtime_data(64);
        let index = data
            .tasks
            .insert(task, |_| core::future::ready(()))
            .unwrap();

        let _gen = enter_active_gen();
        let _ctx = data.context_for(index);

        let generation = _gen.get();
        let local = 1u16;
        let offset = data.buffer_pools[0].slot_offset(local);
        let buf = ReadBuffer::new(offset, pack_bid(0, u32::from(local)), 5, generation);

        assert_eq!(data.buffer_pools[0].ring_tail(), 4);
        let bytes = buf.into_vec();
        assert_eq!(bytes.len(), 5);
        assert_eq!(data.buffer_pools[0].ring_tail(), 5);
    }

    // ── WriteBuffer ───────────────────────────────────────────────────

    #[test]
    fn write_buffer_drop_recycles_slot() {
        let task = Task::new();
        let mut data = test_runtime_data(64);
        let index = data
            .tasks
            .insert(task, |_| core::future::ready(()))
            .unwrap();

        let _gen = enter_active_gen();
        let _ctx = data.context_for(index);

        let generation = _gen.get();
        let local = data.write_pools[0].acquire().unwrap();
        let offset = data.write_pools[0].slot_offset(local);
        let buf = WriteBuffer::new(offset, pack_bid(0, local), generation);

        assert_eq!(data.write_pools[0].free_count(), 3);
        drop(buf);
        assert_eq!(data.write_pools[0].free_count(), 4);
    }

    #[test]
    fn write_buffer_stale_generation_skips_recycle() {
        let task = Task::new();
        let mut data = test_runtime_data(64);
        let index = data
            .tasks
            .insert(task, |_| core::future::ready(()))
            .unwrap();

        let _gen = enter_active_gen();
        let _ctx = data.context_for(index);

        let local = data.write_pools[0].acquire().unwrap();
        let offset = data.write_pools[0].slot_offset(local);
        let generation = NonZeroU32::new(_gen.get().get() + 1).unwrap();
        let buf = WriteBuffer::new(offset, pack_bid(0, local), generation);

        assert_eq!(data.write_pools[0].free_count(), 3);
        drop(buf);
        assert_eq!(data.write_pools[0].free_count(), 3);
    }

    #[test]
    fn write_buffer_helper_returns_slot_capacity() {
        let task = Task::new();
        let mut data = test_runtime_data(64);
        let index = data
            .tasks
            .insert(task, |_| core::future::ready(()))
            .unwrap();

        let _gen = enter_active_gen();
        let ctx = data.context_for(index);

        let before = data.write_pools[0].free_count();
        let buf = write_buffer(ctx, 5).unwrap();
        assert_eq!(buf.capacity(), 16);
        assert_eq!(buf.len(), 0);
        assert_eq!(data.write_pools[0].free_count(), before - 1);
        drop(buf);
        assert_eq!(data.write_pools[0].free_count(), before);
    }

    #[test]
    fn write_buffer_as_mut_set_len_clear() {
        let task = Task::new();
        let mut data = test_runtime_data(64);
        let index = data
            .tasks
            .insert(task, |_| core::future::ready(()))
            .unwrap();

        let _gen = enter_active_gen();
        let ctx = data.context_for(index);

        let mut buf = write_buffer(ctx, 5).unwrap();
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
    fn write_buffer_set_len_above_capacity_panics() {
        let task = Task::new();
        let mut data = test_runtime_data(64);
        let index = data
            .tasks
            .insert(task, |_| core::future::ready(()))
            .unwrap();

        let _gen = enter_active_gen();
        let ctx = data.context_for(index);

        let mut buf = write_buffer(ctx, 5).unwrap();
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

        let bgid = ctx.with_runtime(|r| r.buffer_pools[0].bgid());
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
}
