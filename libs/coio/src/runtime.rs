use core::cell::Cell;
use core::future::Future;
use core::pin::Pin;
use core::task::Context;
use core::task::Poll;
use core::task::Waker;
use std::io;
use std::os::fd::RawFd;

use crate::pbuf::ProvidedBufferPool;
use crate::pbuf::ReadBuffer;
use crate::task::IoState;
use crate::task::Task;
use crate::task::TaskContext;
use crate::task::TaskSlab;
use crate::wbuf::WriteBuffer;
use crate::wbuf::WriteBufferPool;

thread_local! {
    static RUNNING: Cell<bool> = const { Cell::new(false) };
    static ACTIVE_GEN: Cell<u64> = const { Cell::new(0) };
    static CURRENT_RUNTIME: Cell<*mut Runtime> = const { Cell::new(core::ptr::null_mut()) };
}

pub(crate) fn is_running() -> bool {
    RUNNING.with(|c| c.get())
}

pub(crate) fn active_gen_matches(generation: u64) -> bool {
    active_gen() == Some(generation)
}

pub(crate) fn active_gen() -> Option<u64> {
    RUNNING
        .with(|c| c.get())
        .then_some(ACTIVE_GEN.with(|c| c.get()))
}

pub(crate) fn enter_active_gen() -> ActiveGenGuard {
    assert!(!RUNNING.with(|c| c.get()));
    RUNNING.with(|c| c.set(true));
    ACTIVE_GEN.with(|c| {
        let g = c.get().wrapping_add(1);
        c.set(g);
        ActiveGenGuard(g)
    })
}

pub(crate) fn exit_active_gen() {
    RUNNING.with(|c| c.set(false));
}

pub(crate) struct ActiveGenGuard(u64);

impl ActiveGenGuard {
    pub fn get(&self) -> u64 {
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

/// Reads up to `max_len` bytes from `fd` into a buffer selected from the
/// runtime's provided buffer pool. Returns at most `buf_size` bytes (the
/// kernel caps the transfer at the selected buffer's size).
///
/// The returned [`ReadBuffer`] borrows memory from the pool: its slot is
/// recycled when the buffer is dropped, so the runtime must still be alive
/// while the buffer is in use.
pub async fn read(ctx: TaskContext, fd: RawFd, max_len: usize) -> io::Result<ReadBuffer> {
    let bgid = ctx.with_runtime(|r| r.buffer_pool.bgid());

    let slot = ctx
        .with_task(|task| {
            let slot = task.io.free_slot()?;
            task.io.set_submitted(slot, true);
            Some(slot)
        })
        .ok_or_else(|| io::Error::from_raw_os_error(libc::ENOMEM))?;

    let entry = io_uring::opcode::Read::new(
        io_uring::types::Fd(fd),
        std::ptr::null_mut(),
        max_len as u32,
    )
    .buf_group(bgid)
    .build()
    .flags(io_uring::squeue::Flags::BUFFER_SELECT);
    if let Err(e) = ctx.push_io(entry, slot) {
        ctx.with_task(|task| task.io.reset_slot(slot));
        return Err(e);
    }

    let result = await_cqe(ctx, slot).await;
    if result < 0 {
        ctx.with_task(|task| task.io.reset_slot(slot));
        return Err(io::Error::from_raw_os_error(-result));
    }

    let generation = active_gen().expect("read called outside an active runtime");
    let bid = ctx.with_task(|task| task.io.bid(slot));
    let ptr = ctx.with_runtime(|r| r.buffer_pool.slot_ptr(bid));
    ctx.with_task(|task| task.io.reset_slot(slot));

    Ok(ReadBuffer::new(ptr, result as _, bid, generation))
}

/// Acquires a zero-copy write buffer backed by a slot from the runtime's fixed
/// write buffer slab. The buffer's [`WriteBuffer::capacity`] is the pool's
/// slot size; the caller fills it via [`WriteBuffer::as_mut`] and records the
/// length with [`WriteBuffer::set_len`] before passing it to [`write`]. The
/// slot is recycled when the buffer is dropped.
pub fn write_buffer(ctx: TaskContext) -> io::Result<WriteBuffer> {
    let generation = active_gen().expect("write_buffer called outside an active runtime");
    let bid = ctx
        .with_runtime(|r| r.write_pool.acquire())
        .ok_or_else(|| io::Error::from_raw_os_error(libc::ENOMEM))?;
    let ptr = ctx.with_runtime(|r| r.write_pool.slot_ptr(bid));
    Ok(WriteBuffer::new(ptr, bid, generation))
}

/// Writes the contents of `wb` to `fd` via `IORING_OP_WRITE_FIXED`. The
/// buffer's slot is held until the kernel finishes with it and recycled when
/// `wb` is dropped.
pub async fn write(ctx: TaskContext, fd: RawFd, wb: WriteBuffer) -> io::Result<usize> {
    let slot = ctx
        .with_task(|task| task.io.free_slot())
        .ok_or_else(|| io::Error::from_raw_os_error(libc::ENOMEM))?;

    let len = wb.len();
    let ptr = wb.ptr().as_ptr();

    ctx.with_task(|task| task.io.set_submitted(slot, true));

    let entry = io_uring::opcode::WriteFixed::new(
        io_uring::types::Fd(fd),
        ptr,
        len as u32,
        0, // the whole slab is registered as fixed buffer index 0
    )
    .build();
    if let Err(e) = ctx.push_io(entry, slot) {
        ctx.with_task(|task| task.io.reset_slot(slot));
        return Err(e);
    }

    let result = await_cqe(ctx, slot).await;
    if result < 0 {
        ctx.with_task(|task| task.io.reset_slot(slot));
        return Err(io::Error::from_raw_os_error(-result));
    }

    ctx.with_task(|task| task.io.reset_slot(slot));
    Ok(result as usize)
}

pub async fn await_cqe(ctx: TaskContext, slot: u32) -> i32 {
    loop {
        let ready = ctx.with_task(|task| task.io.is_ready(slot));
        if ready {
            return ctx.with_task(|task| task.io.result(slot));
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
    pub buffer_pool: ProvidedBufferPool,
    pub write_pool: WriteBufferPool,
    pub ring: io_uring::IoUring,
    pub make_fut: *const (),
}

impl Drop for Runtime {
    fn drop(&mut self) {
        // Unregister the provided buffer ring and the fixed write buffer slab
        // while the io_uring fd is still open; the pools are dropped
        // afterwards along with the other fields.
        let _ = self.buffer_pool.unregister(&self.ring);
        let _ = self.write_pool.unregister(&self.ring);
    }
}

impl Runtime {
    fn context_for(&mut self, index: u32) -> TaskContext {
        assert!(
            self.tasks.is_occupied(index),
            "cannot build a context for an uninitialized slot"
        );
        let task_id = unsafe { self.tasks.task_unchecked(index) }.id;
        set_current_runtime(self as *mut Runtime);
        TaskContext::new(index, task_id)
    }

    fn drain_cqes(&mut self, ready: &mut Vec<u32>) {
        for cqe in self.ring.completion() {
            let raw = cqe.user_data();
            let task_index = (raw >> 32) as u32;
            let io_slot = raw as u32;
            let result = cqe.result();

            unsafe {
                if self.tasks.is_occupied(task_index) {
                    let task = self.tasks.task_mut_unchecked(task_index);
                    task.io.set_result(io_slot, result);
                    // Only reads report a selected buffer via the CQE flags;
                    // writes store their own slot id in `bids`, so leave it
                    // untouched when no buffer flag is present.
                    if let Some(bid) = io_uring::cqueue::buffer_select(cqe.flags()) {
                        task.io.set_bid(io_slot, bid);
                    }
                    task.io.set_ready(io_slot, true);
                    if !task.ready {
                        task.ready = true;
                        ready.push(task_index);
                    }
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct RuntimeParams {
    pub tasks_capacity: u32,
    pub ring_entries: u32,
    pub buf_count: u16,
    pub buf_size: u32,
}

impl Default for RuntimeParams {
    fn default() -> Self {
        Self {
            tasks_capacity: 1024,
            ring_entries: 1024,
            buf_count: 1024,
            buf_size: 4096,
        }
    }
}

pub fn block_on_default<S, F, T>(make_fut: S, user_data: T)
where
    S: Fn(TaskContext, RuntimeContext<T>, T) -> F,
    F: Future<Output = ()> + 'static,
{
    block_on(RuntimeParams::default(), make_fut, user_data)
}

pub fn block_on<S, F, T>(params: RuntimeParams, make_fut: S, user_data: T)
where
    S: Fn(TaskContext, RuntimeContext<T>, T) -> F,
    F: Future<Output = ()> + 'static,
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

    let pool = ProvidedBufferPool::new(params.buf_count, params.buf_size);
    let write_pool = WriteBufferPool::new(params.buf_count, params.buf_size);

    let mut rt = Runtime {
        tasks: TaskSlab::new::<F>(params.tasks_capacity),
        wakeups: Vec::new(),
        buffer_pool: pool,
        write_pool,
        ring,
        make_fut: &raw const make_fut as *const (),
    };

    rt.buffer_pool
        .register(&rt.ring)
        .expect("failed to register the provided buffer pool");
    rt.write_pool
        .register(&rt.ring)
        .expect("failed to register the write buffer pool");

    let rt_ptr: *mut Runtime = &mut rt;
    set_current_runtime(rt_ptr);
    let _rt_guard = DropGuard::new(clear_current_runtime);
    let _drop_guard =
        DropGuard::new(move || unsafe { TaskSlab::drop_futures_raw::<F>(&mut (*rt_ptr).tasks) });

    let spawn: unsafe fn(RuntimeContext<T>, T) -> Option<u32> = spawn::<S, F, T>;

    let ctx = RuntimeContext { generation, spawn };

    let index = rt
        .tasks
        .insert_vacant()
        .expect("failed to insert vacant task");

    let task = Task {
        ready: true,
        io: IoState::new(),
        id: 0,
    };
    unsafe { rt.tasks.init_task_unchecked(index, task) };

    let task_ctx = rt.context_for(index);

    let future = (make_fut)(task_ctx, ctx, user_data);
    unsafe { rt.tasks.init_future_unchecked(index, future) };
    rt.wakeups.push(index);

    let mut ready_tasks = Vec::new();

    loop {
        core::mem::swap(&mut rt.wakeups, &mut ready_tasks);
        assert!(rt.wakeups.is_empty());

        rt.drain_cqes(&mut ready_tasks);

        for &idx in &ready_tasks {
            if !rt.tasks.is_occupied(idx) {
                continue;
            }

            unsafe { rt.tasks.task_mut_unchecked(idx).ready = false };

            let mut cx = Context::from_waker(Waker::noop());
            let future_ptr = rt.tasks.future_ptr_unchecked::<F>(idx);
            let future = unsafe { Pin::new_unchecked(&mut *future_ptr) };

            match future.poll(&mut cx) {
                Poll::Ready(()) => {
                    unsafe { rt.tasks.remove_unchecked::<F>(idx) };
                }
                Poll::Pending => {}
            }
        }

        ready_tasks.clear();

        if !rt.tasks.has_io_in_flight() {
            if rt.wakeups.is_empty() {
                break;
            }
            continue;
        }

        match rt.ring.submit_and_wait(1) {
            Ok(_) => {}
            Err(ref e) if e.raw_os_error() == Some(libc::EBUSY) => {}
            Err(_) => {
                return;
            }
        }
    }
}

unsafe fn spawn<S, F, T>(ctx: RuntimeContext<T>, user_data: T) -> Option<u32>
where
    S: Fn(TaskContext, RuntimeContext<T>, T) -> F,
    F: Future<Output = ()> + 'static,
{
    with_runtime(|data| {
        let closure = unsafe { &*(data.make_fut as *const S) };

        let index = data.tasks.insert_vacant()?;
        let task = Task {
            ready: true,
            io: IoState::new(),
            id: 0,
        };
        unsafe { data.tasks.init_task_unchecked(index, task) };

        let task_ctx = data.context_for(index);

        let future = closure(task_ctx, ctx, user_data);
        unsafe {
            data.tasks.init_future_unchecked(index, future);
        }

        data.wakeups.push(index);

        Some(index)
    })
}

#[derive(Debug)]
pub struct RuntimeContext<T> {
    generation: u64,
    spawn: unsafe fn(RuntimeContext<T>, T) -> Option<u32>,
}

impl<T> Clone for RuntimeContext<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for RuntimeContext<T> {}

impl<T> RuntimeContext<T> {
    pub fn spawn(&self, user_data: T) -> Option<u32> {
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
    use crate::task::IoUserData;
    use crate::task::Task;
    use crate::task::TaskSlab;

    fn test_runtime_data(capacity: u32) -> Runtime {
        let ring = io_uring::IoUring::new(8).unwrap();
        let pool = ProvidedBufferPool::new(4, 16);
        pool.register(&ring).unwrap();
        let write_pool = WriteBufferPool::new(4, 16);
        write_pool.register(&ring).unwrap();
        Runtime {
            tasks: TaskSlab::new::<Ready<()>>(capacity),
            wakeups: Vec::new(),
            buffer_pool: pool,
            write_pool,
            ring,
            make_fut: core::ptr::null(),
        }
    }

    // ── IoState (inline) ──────────────────────────────────────────────

    #[test]
    fn io_state_free_slot_exhaustion() {
        let mut s = IoState::new();
        for i in 0..64 {
            assert_eq!(s.free_slot(), Some(i));
            s.set_submitted(i, true);
        }
        assert_eq!(s.free_slot(), None);
    }

    #[test]
    fn io_state_free_slot_reuses_freed() {
        let mut s = IoState::new();
        s.set_submitted(0, true);
        s.set_submitted(1, true);
        s.set_submitted(2, true);
        // free slot 1
        s.set_submitted(1, false);
        assert_eq!(s.free_slot(), Some(1));
    }

    // ── IoState (heap variant) ────────────────────────────────────────

    fn make_heap_state(capacity: usize) -> IoState {
        IoState::Heap {
            submitted: vec![0; capacity],
            ready: vec![0; capacity],
            results: vec![0; capacity * 64],
            bids: vec![0; capacity * 64],
        }
    }

    #[test]
    fn io_state_heap_free_slot() {
        let mut s = make_heap_state(1);
        assert_eq!(s.free_slot(), Some(0));
        s.set_submitted(0, true);
        // After submitting slot 0, '!submitted[0]' has bit 0 cleared => free_slot = 1
        assert_eq!(s.free_slot(), Some(1));
    }

    #[test]
    fn io_state_heap_free_slot_exhausted() {
        let mut s = make_heap_state(1);
        for i in 0..64 {
            s.set_submitted(i, true);
        }
        assert_eq!(s.free_slot(), None);
    }

    #[test]
    fn io_state_heap_beyond_64() {
        let mut s = make_heap_state(2);
        // submitted[0] covers slots 0..63, submitted[1] covers 64..127
        assert_eq!(s.free_slot(), Some(0));
        s.set_submitted(64, true);
        assert!(s.is_submitted(64));
        assert_eq!(s.free_slot(), Some(0));
    }

    #[test]
    fn io_state_heap_ready_and_result() {
        let mut s = make_heap_state(1);
        s.set_submitted(10, true);
        s.set_result(10, -1);
        s.set_ready(10, true);
        assert!(s.is_ready(10));
        assert_eq!(s.result(10), -1);
    }

    // ── IoState bid storage ───────────────────────────────────────────

    #[test]
    fn io_state_bid_inline_roundtrip() {
        let mut s = IoState::new();
        assert_eq!(s.bid(0), 0);
        s.set_bid(0, 3);
        assert_eq!(s.bid(0), 3);
        s.set_bid(0, 5);
        assert_eq!(s.bid(0), 5);
    }

    #[test]
    fn io_state_bid_heap_roundtrip() {
        let mut s = make_heap_state(2);
        assert_eq!(s.bid(64), 0);
        s.set_bid(64, 7);
        assert_eq!(s.bid(64), 7);
        s.set_bid(64, 9);
        assert_eq!(s.bid(64), 9);
    }

    #[test]
    fn io_state_reset_slot_clears_state() {
        let mut s = IoState::new();
        s.set_submitted(5, true);
        s.set_ready(5, true);
        s.set_bid(5, 2);
        s.reset_slot(5);
        assert!(!s.is_submitted(5));
        assert!(!s.is_ready(5));
        // bids is not cleared: like `results`, it is only read once `ready`
        // is set and is overwritten before the next use of the slot.
        assert_eq!(s.bid(5), 2);
    }

    // ── ReadBuffer ────────────────────────────────────────────────────

    #[test]
    fn read_buffer_drop_recycles_slot() {
        let task = Task {
            ready: false,
            io: IoState::new(),
            id: 0,
        };
        let mut data = test_runtime_data(64);
        let index = data.tasks.insert_vacant().unwrap();
        unsafe { data.tasks.init_task_unchecked(index, task) };

        let _gen = enter_active_gen();
        let _ctx = data.context_for(index);

        let generation = _gen.get();
        let ptr = data.buffer_pool.slot_ptr(1);
        let buf = crate::pbuf::ReadBuffer::new(ptr, 5, 1, generation);

        assert_eq!(data.buffer_pool.ring_tail(), 4);
        drop(buf);
        assert_eq!(data.buffer_pool.ring_tail(), 5);
    }

    #[test]
    fn read_buffer_stale_generation_skips_recycle() {
        let task = Task {
            ready: false,
            io: IoState::new(),
            id: 0,
        };
        let mut data = test_runtime_data(64);
        let index = data.tasks.insert_vacant().unwrap();
        unsafe { data.tasks.init_task_unchecked(index, task) };

        let _gen = enter_active_gen();
        let _ctx = data.context_for(index);

        let generation = _gen.get() + 1;
        let ptr = data.buffer_pool.slot_ptr(1);
        let buf = crate::pbuf::ReadBuffer::new(ptr, 5, 1, generation);

        assert_eq!(data.buffer_pool.ring_tail(), 4);
        drop(buf);
        assert_eq!(data.buffer_pool.ring_tail(), 4);
    }

    #[test]
    fn read_buffer_into_vec_recycles_slot() {
        let task = Task {
            ready: false,
            io: IoState::new(),
            id: 0,
        };
        let mut data = test_runtime_data(64);
        let index = data.tasks.insert_vacant().unwrap();
        unsafe { data.tasks.init_task_unchecked(index, task) };

        let _gen = enter_active_gen();
        let _ctx = data.context_for(index);

        let generation = _gen.get();
        let ptr = data.buffer_pool.slot_ptr(1);
        let buf = crate::pbuf::ReadBuffer::new(ptr, 5, 1, generation);

        assert_eq!(data.buffer_pool.ring_tail(), 4);
        let bytes = buf.into_vec();
        assert_eq!(bytes.len(), 5);
        assert_eq!(data.buffer_pool.ring_tail(), 5);
    }

    // ── WriteBuffer ───────────────────────────────────────────────────

    #[test]
    fn write_buffer_drop_recycles_slot() {
        let task = Task {
            ready: false,
            io: IoState::new(),
            id: 0,
        };
        let mut data = test_runtime_data(64);
        let index = data.tasks.insert_vacant().unwrap();
        unsafe { data.tasks.init_task_unchecked(index, task) };

        let _gen = enter_active_gen();
        let _ctx = data.context_for(index);

        let generation = _gen.get();
        let bid = data.write_pool.acquire().unwrap();
        let buf = crate::wbuf::WriteBuffer::new(data.write_pool.slot_ptr(bid), bid, generation);

        assert_eq!(data.write_pool.free_count(), 3);
        drop(buf);
        assert_eq!(data.write_pool.free_count(), 4);
    }

    #[test]
    fn write_buffer_stale_generation_skips_recycle() {
        let task = Task {
            ready: false,
            io: IoState::new(),
            id: 0,
        };
        let mut data = test_runtime_data(64);
        let index = data.tasks.insert_vacant().unwrap();
        unsafe { data.tasks.init_task_unchecked(index, task) };

        let _gen = enter_active_gen();
        let _ctx = data.context_for(index);

        let bid = data.write_pool.acquire().unwrap();
        let generation = _gen.get() + 1;
        let buf = crate::wbuf::WriteBuffer::new(data.write_pool.slot_ptr(bid), bid, generation);

        assert_eq!(data.write_pool.free_count(), 3);
        drop(buf);
        assert_eq!(data.write_pool.free_count(), 3);
    }

    #[test]
    fn write_buffer_helper_returns_slot_capacity() {
        let task = Task {
            ready: false,
            io: IoState::new(),
            id: 0,
        };
        let mut data = test_runtime_data(64);
        let index = data.tasks.insert_vacant().unwrap();
        unsafe { data.tasks.init_task_unchecked(index, task) };

        let _gen = enter_active_gen();
        let ctx = data.context_for(index);

        let before = data.write_pool.free_count();
        let buf = write_buffer(ctx).unwrap();
        assert_eq!(buf.capacity(), 16);
        assert_eq!(buf.len(), 0);
        assert_eq!(data.write_pool.free_count(), before - 1);
        drop(buf);
        assert_eq!(data.write_pool.free_count(), before);
    }

    #[test]
    fn write_buffer_as_mut_set_len_clear() {
        let task = Task {
            ready: false,
            io: IoState::new(),
            id: 0,
        };
        let mut data = test_runtime_data(64);
        let index = data.tasks.insert_vacant().unwrap();
        unsafe { data.tasks.init_task_unchecked(index, task) };

        let _gen = enter_active_gen();
        let ctx = data.context_for(index);

        let mut buf = write_buffer(ctx).unwrap();
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
        let task = Task {
            ready: false,
            io: IoState::new(),
            id: 0,
        };
        let mut data = test_runtime_data(64);
        let index = data.tasks.insert_vacant().unwrap();
        unsafe { data.tasks.init_task_unchecked(index, task) };

        let _gen = enter_active_gen();
        let ctx = data.context_for(index);

        let mut buf = write_buffer(ctx).unwrap();
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

    // ── IoState::has_io_in_flight ─────────────────────────────────────

    #[test]
    fn io_state_no_io_in_flight_when_empty() {
        let s = IoState::new();
        assert!(!s.has_io_in_flight());
    }

    #[test]
    fn io_state_has_io_in_flight_when_submitted_not_ready() {
        let mut s = IoState::new();
        s.set_submitted(3, true);
        // submitted=3, ready=0 => submitted & !ready != 0
        assert!(s.has_io_in_flight());
    }

    #[test]
    fn io_state_no_io_in_flight_when_submitted_and_ready() {
        let mut s = IoState::new();
        s.set_submitted(3, true);
        s.set_ready(3, true);
        // submitted=3, ready=3 => submitted & !ready == 0
        assert!(!s.has_io_in_flight());
    }

    #[test]
    fn io_state_no_io_in_flight_after_clearing_submitted() {
        let mut s = IoState::new();
        s.set_submitted(3, true);
        s.set_submitted(3, false);
        assert!(!s.has_io_in_flight());
    }

    #[test]
    fn io_state_heap_has_io_in_flight() {
        let mut s = make_heap_state(2);
        s.set_submitted(64, true);
        assert!(s.has_io_in_flight());
        s.set_ready(64, true);
        assert!(!s.has_io_in_flight());
    }

    // ── await_cqe ─────────────────────────────────────────────────────

    #[test]
    fn await_cqe_immediate_ready() {
        let mut task = Task {
            ready: false,
            io: IoState::new(),
            id: 0,
        };
        let slot = task.io.free_slot().unwrap();
        task.io.set_submitted(slot, true);
        task.io.set_result(slot, 42);
        task.io.set_ready(slot, true);

        let mut data = test_runtime_data(64);
        let index = data.tasks.insert_vacant().unwrap();
        unsafe { data.tasks.init_task_unchecked(index, task) };

        let _gen = enter_active_gen();
        let ctx = data.context_for(index);

        let fut = await_cqe(ctx, slot);
        let mut fut = pin!(fut);
        let mut cx = Context::from_waker(Waker::noop());

        assert_eq!(fut.as_mut().poll(&mut cx), Poll::Ready(42));
    }

    #[test]
    fn await_cqe_delayed_ready() {
        let mut task = Task {
            ready: false,
            io: IoState::new(),
            id: 0,
        };
        let slot = task.io.free_slot().unwrap();
        task.io.set_submitted(slot, true);
        // NOT setting ready yet

        let mut data = test_runtime_data(64);
        let index = data.tasks.insert_vacant().unwrap();
        unsafe { data.tasks.init_task_unchecked(index, task) };

        let _gen = enter_active_gen();
        let ctx = data.context_for(index);

        let fut = await_cqe(ctx, slot);
        let mut fut = pin!(fut);
        let mut cx = Context::from_waker(Waker::noop());

        // First poll: not ready → Yield's first poll → Pending
        assert_eq!(fut.as_mut().poll(&mut cx), Poll::Pending);

        // Now set ready externally
        let task = unsafe { data.tasks.task_mut_unchecked(index) };
        task.io.set_ready(slot, true);
        task.io.set_result(slot, 99);

        // Second poll: Yield's second poll → Ready, loop sees ready → Ready(99)
        assert_eq!(fut.as_mut().poll(&mut cx), Poll::Ready(99));
    }

    // ── Yield ─────────────────────────────────────────────────────────

    #[test]
    fn yield_first_poll_returns_pending() {
        let task = Task {
            ready: false,
            io: IoState::new(),
            id: 0,
        };
        let mut data = test_runtime_data(64);
        let index = data.tasks.insert_vacant().unwrap();
        unsafe { data.tasks.init_task_unchecked(index, task) };

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
        let task = Task {
            ready: false,
            io: IoState::new(),
            id: 0,
        };
        let mut data = test_runtime_data(64);
        let index = data.tasks.insert_vacant().unwrap();
        unsafe { data.tasks.init_task_unchecked(index, task) };

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
        let task = Task {
            ready: false,
            io: IoState::new(),
            id: 0,
        };
        let mut data = test_runtime_data(64);
        let index = data.tasks.insert_vacant().unwrap();
        unsafe { data.tasks.init_task_unchecked(index, task) };

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
        let task = Task {
            ready: false,
            io: IoState::new(),
            id: 0,
        };
        let mut data = test_runtime_data(64);
        let index = data.tasks.insert_vacant().unwrap();
        unsafe { data.tasks.init_task_unchecked(index, task) };

        let _gen = enter_active_gen();
        let ctx = data.context_for(index);

        let ready = ctx.with_task(|t| t.ready);
        assert!(!ready);
    }

    #[test]
    fn task_context_with_task_modifies() {
        let task = Task {
            ready: false,
            io: IoState::new(),
            id: 0,
        };
        let mut data = test_runtime_data(64);
        let index = data.tasks.insert_vacant().unwrap();
        unsafe { data.tasks.init_task_unchecked(index, task) };

        let _gen = enter_active_gen();
        let ctx = data.context_for(index);

        ctx.with_task(|t| t.ready = true);
        assert!(unsafe { data.tasks.task_unchecked(index) }.ready);
    }

    #[test]
    fn task_context_with_runtime_reads_buffer_pool() {
        let task = Task {
            ready: false,
            io: IoState::new(),
            id: 0,
        };
        let mut data = test_runtime_data(64);
        let index = data.tasks.insert_vacant().unwrap();
        unsafe { data.tasks.init_task_unchecked(index, task) };

        let _gen = enter_active_gen();
        let ctx = data.context_for(index);

        let bgid = ctx.with_runtime(|r| r.buffer_pool.bgid());
        assert_eq!(bgid, 0);
    }

    #[test]
    fn task_context_with_runtime_after_removal_panics() {
        let task = Task {
            ready: false,
            io: IoState::new(),
            id: 0,
        };
        let mut data = test_runtime_data(64);
        let index = data.tasks.insert_vacant().unwrap();
        unsafe { data.tasks.init_task_unchecked(index, task) };
        unsafe {
            data.tasks
                .init_future_unchecked(index, core::future::ready(()))
        };

        let _gen = enter_active_gen();
        let ctx = data.context_for(index);

        let _ = unsafe { data.tasks.remove_unchecked::<Ready<()>>(index) };

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            ctx.with_runtime(|_r| ());
        }));
        assert!(result.is_err());
    }

    #[test]
    fn task_context_wake_pushes_index() {
        let task = Task {
            ready: false,
            io: IoState::new(),
            id: 0,
        };
        let mut data = test_runtime_data(64);
        let index = data.tasks.insert_vacant().unwrap();
        unsafe { data.tasks.init_task_unchecked(index, task) };

        let _gen = enter_active_gen();
        let ctx = data.context_for(index);

        ctx.wake();
        ctx.wake();
        assert_eq!(data.wakeups, vec![index, index]);
    }

    #[test]
    fn task_context_after_removal_panics() {
        let task = Task {
            ready: false,
            io: IoState::new(),
            id: 0,
        };
        let mut data = test_runtime_data(64);
        let index = data.tasks.insert_vacant().unwrap();
        unsafe { data.tasks.init_task_unchecked(index, task) };
        unsafe {
            data.tasks
                .init_future_unchecked(index, core::future::ready(()))
        };

        let _gen = enter_active_gen();
        let ctx = data.context_for(index);

        let _ = unsafe { data.tasks.remove_unchecked::<Ready<()>>(index) };

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            ctx.with_task(|_t| ());
        }));
        assert!(result.is_err());
    }

    #[test]
    fn task_context_after_slot_reuse_panics_without_touching_new_task() {
        let task_a = Task {
            ready: false,
            io: IoState::new(),
            id: 0,
        };
        let mut data = test_runtime_data(64);
        let index = data.tasks.insert_vacant().unwrap();
        unsafe { data.tasks.init_task_unchecked(index, task_a) };
        unsafe {
            data.tasks
                .init_future_unchecked(index, core::future::ready(()))
        };

        let _gen = enter_active_gen();
        let ctx = data.context_for(index);

        let _ = unsafe { data.tasks.remove_unchecked::<Ready<()>>(index) };

        let task_b = Task {
            ready: true,
            io: IoState::new(),
            id: 0,
        };
        unsafe { data.tasks.init_task_unchecked(index, task_b) };
        unsafe {
            data.tasks
                .init_future_unchecked(index, core::future::ready(()))
        };

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            ctx.with_task(|t| t.ready = false);
        }));

        assert!(result.is_err());
        assert!(unsafe { data.tasks.task_unchecked(index) }.ready);
    }

    #[test]
    fn task_context_of_live_task_usable_alongside_other_tasks() {
        let task_a = Task {
            ready: false,
            io: IoState::new(),
            id: 0,
        };
        let mut data = test_runtime_data(64);
        let index_a = data.tasks.insert_vacant().unwrap();
        unsafe { data.tasks.init_task_unchecked(index_a, task_a) };

        let task_b = Task {
            ready: true,
            io: IoState::new(),
            id: 0,
        };
        let index_b = data.tasks.insert_vacant().unwrap();
        unsafe { data.tasks.init_task_unchecked(index_b, task_b) };

        let _gen = enter_active_gen();
        let ctx_a = data.context_for(index_a);
        let _ctx_b = data.context_for(index_b);

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            ctx_a.with_task(|t| t.ready = true);
        }));

        assert!(result.is_ok());
        assert!(unsafe { data.tasks.task_unchecked(index_a) }.ready);
    }
}
