use core::cell::Cell;
use core::future::Future;
use core::pin::Pin;
use core::task::Context;
use core::task::Poll;
use core::task::Waker;
use std::io;
use std::os::fd::RawFd;

use crate::arena::Arena;
use crate::arena::ArenaAlloc;
use crate::buffer::IoReadBuffer;
use crate::buffer::IoWriteBuffer;
use crate::buffer::complete_read;
use crate::buffer::complete_write;
use crate::task::Task;
use crate::task::TaskContext;
use crate::task::TaskSlab;

thread_local! {
    static RUNNING: Cell<bool> = const { Cell::new(false) };
    static ACTIVE_GEN: Cell<u64> = const { Cell::new(0) };
}

pub(crate) fn active_gen_matches(generation: u64) -> bool {
    active_gen() == Some(generation)
}

pub(crate) fn active_gen() -> Option<u64> {
    RUNNING
        .with(|c| c.get())
        .then_some(ACTIVE_GEN.with(|c| c.get()))
}

pub(crate) fn enter_active_gen() -> u64 {
    assert!(!RUNNING.with(|c| c.get()));
    RUNNING.with(|c| c.set(true));
    ACTIVE_GEN.with(|c| {
        let g = c.get().wrapping_add(1);
        c.set(g);
        g
    })
}

pub(crate) fn exit_active_gen() {
    RUNNING.with(|c| c.set(false));
}

#[allow(clippy::large_enum_variant)]
pub enum IoState {
    Inline {
        submitted: u64,
        ready: u64,
        results: [i32; 64],
        allocs: [Option<ArenaAlloc>; 64],
    },
    Heap {
        submitted: Vec<u64>,
        ready: Vec<u64>,
        results: Vec<i32>,
        allocs: Vec<Option<ArenaAlloc>>,
    },
}

impl Default for IoState {
    fn default() -> Self {
        Self::new()
    }
}

impl IoState {
    pub fn new() -> Self {
        const NONE_ALLOC: Option<ArenaAlloc> = None;
        Self::Inline {
            submitted: 0,
            ready: 0,
            results: [0; 64],
            allocs: [NONE_ALLOC; 64],
        }
    }

    pub fn is_submitted(&self, bit: u32) -> bool {
        match self {
            Self::Inline { submitted, .. } => *submitted & (1 << bit) != 0,
            Self::Heap { submitted, .. } => {
                let word = bit as usize / 64;
                let bit = bit as usize % 64;
                submitted[word] & (1 << bit) != 0
            }
        }
    }

    pub fn set_submitted(&mut self, bit: u32, value: bool) {
        match self {
            Self::Inline { submitted, .. } => {
                if value {
                    *submitted |= 1 << bit;
                } else {
                    *submitted &= !(1 << bit);
                }
            }
            Self::Heap { submitted, .. } => {
                let word = bit as usize / 64;
                let bit = bit as usize % 64;
                if value {
                    submitted[word] |= 1 << bit;
                } else {
                    submitted[word] &= !(1 << bit);
                }
            }
        }
    }

    pub fn is_ready(&self, bit: u32) -> bool {
        match self {
            Self::Inline { ready, .. } => *ready & (1 << bit) != 0,
            Self::Heap { ready, .. } => {
                let word = bit as usize / 64;
                let bit = bit as usize % 64;
                word < ready.len() && ready[word] & (1 << bit) != 0
            }
        }
    }

    pub fn set_ready(&mut self, bit: u32, value: bool) {
        match self {
            Self::Inline { ready, .. } => {
                if value {
                    *ready |= 1 << bit;
                } else {
                    *ready &= !(1 << bit);
                }
            }
            Self::Heap { ready, .. } => {
                let word = bit as usize / 64;
                let bit = bit as usize % 64;
                if value {
                    ready[word] |= 1 << bit;
                } else {
                    ready[word] &= !(1 << bit);
                }
            }
        }
    }

    pub fn result(&self, slot: u32) -> i32 {
        match self {
            Self::Inline { results, .. } => results[slot as usize],
            Self::Heap { results, .. } => results[slot as usize],
        }
    }

    pub fn set_result(&mut self, slot: u32, value: i32) {
        match self {
            Self::Inline { results, .. } => results[slot as usize] = value,
            Self::Heap { results, .. } => results[slot as usize] = value,
        }
    }

    pub fn set_alloc(&mut self, slot: u32, alloc: ArenaAlloc) {
        match self {
            Self::Inline { allocs, .. } => allocs[slot as usize] = Some(alloc),
            Self::Heap { allocs, .. } => {
                let idx = slot as usize;
                if idx >= allocs.len() {
                    allocs.resize(idx + 1, None);
                }
                allocs[idx] = Some(alloc);
            }
        }
    }

    pub fn take_alloc(&mut self, slot: u32) -> ArenaAlloc {
        match self {
            Self::Inline { allocs, .. } => allocs[slot as usize].take().unwrap(),
            Self::Heap { allocs, .. } => allocs[slot as usize].take().unwrap(),
        }
    }

    pub fn alloc(&self, slot: u32) -> ArenaAlloc {
        match self {
            Self::Inline { allocs, .. } => allocs[slot as usize].unwrap(),
            Self::Heap { allocs, .. } => allocs[slot as usize].unwrap(),
        }
    }

    pub fn free_slot(&self) -> Option<u32> {
        let bits = match self {
            Self::Inline { submitted, .. } => !submitted,
            Self::Heap { submitted, .. } => !submitted[0],
        };

        if bits == 0 {
            None
        } else {
            Some(bits.trailing_zeros())
        }
    }

    pub fn has_io_in_flight(&self) -> bool {
        match self {
            Self::Inline {
                submitted, ready, ..
            } => *submitted & !*ready != 0,
            Self::Heap {
                submitted, ready, ..
            } => {
                for (s, r) in submitted.iter().zip(ready.iter()) {
                    if s & !r != 0 {
                        return true;
                    }
                }
                false
            }
        }
    }
}

#[repr(C)]
pub struct IoUserData {
    pub index: u32,
    pub io_slot: u32,
}

impl From<IoUserData> for u64 {
    fn from(ud: IoUserData) -> u64 {
        unsafe { core::mem::transmute(ud) }
    }
}

impl From<u64> for IoUserData {
    fn from(raw: u64) -> Self {
        unsafe { core::mem::transmute(raw) }
    }
}

pub async fn read(ctx: TaskContext, fd: RawFd, buf: Vec<u8>) -> io::Result<Vec<u8>> {
    let slot = ctx
        .with_task(|task| buf.prepare_read(task))
        .ok_or_else(|| io::Error::from_raw_os_error(libc::ENOMEM))?;

    let (ptr, len) = ctx.with_task(|task| {
        let alloc = task.io.alloc(slot);
        let vec: Vec<u8> = task.arena.read(&alloc);
        let (ptr, len, _) = vec.into_raw_parts();
        (ptr, len as u32)
    });

    let entry = io_uring::opcode::Read::new(io_uring::types::Fd(fd), ptr, len).build();
    if let Err(e) = ctx.push_io(entry, slot) {
        let _ = unsafe { ctx.with_task(|task| complete_read::<Vec<u8>>(task, slot)) };
        return Err(e);
    }

    let result = await_cqe(ctx, slot).await;
    if result < 0 {
        let _ = unsafe { ctx.with_task(|task| complete_read::<Vec<u8>>(task, slot)) };
        return Err(io::Error::from_raw_os_error(-result));
    }

    let mut out = unsafe { ctx.with_task(|task| complete_read::<Vec<u8>>(task, slot)) }.unwrap();
    out.truncate(result.min(out.len() as i32) as usize);
    Ok(out)
}

pub async fn write(ctx: TaskContext, fd: RawFd, buf: Vec<u8>) -> io::Result<usize> {
    let slot = ctx
        .with_task(|task| buf.prepare_write(task))
        .ok_or_else(|| io::Error::from_raw_os_error(libc::ENOMEM))?;

    let (ptr, len) = ctx.with_task(|task| {
        let alloc = task.io.alloc(slot);
        let vec: Vec<u8> = task.arena.read(&alloc);
        let (ptr, len, _) = vec.into_raw_parts();
        (ptr as *const _, len as u32)
    });

    let entry = io_uring::opcode::Write::new(io_uring::types::Fd(fd), ptr, len).build();
    if let Err(e) = ctx.push_io(entry, slot) {
        let _ = unsafe { ctx.with_task(|task| complete_write::<Vec<u8>>(task, slot)) };
        return Err(e);
    }

    let result = await_cqe(ctx, slot).await;
    if result < 0 {
        let _ = unsafe { ctx.with_task(|task| complete_write::<Vec<u8>>(task, slot)) };
        return Err(io::Error::from_raw_os_error(-result));
    }

    let _ = unsafe { ctx.with_task(|task| complete_write::<Vec<u8>>(task, slot)) };
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

pub(crate) struct RuntimeData {
    pub(crate) tasks: TaskSlab,
    pub(crate) wakeups: Vec<u32>,
    pub(crate) ring: io_uring::IoUring,
}

impl RuntimeData {
    pub(crate) fn context_for(&mut self, index: u32) -> TaskContext {
        assert!(
            self.tasks.is_occupied(index),
            "cannot build a context for an uninitialized slot"
        );
        let task_id = unsafe { self.tasks.task_unchecked(index) }.id;
        TaskContext::new(self as *mut RuntimeData, index, task_id)
    }

    pub(crate) fn drain_cqes(&mut self, ready: &mut Vec<u32>) {
        for cqe in self.ring.completion() {
            let raw = cqe.user_data();
            let task_index = (raw >> 32) as u32;
            let io_slot = raw as u32;
            let result = cqe.result();

            unsafe {
                if self.tasks.is_occupied(task_index) {
                    let task = self.tasks.task_mut_unchecked(task_index);
                    task.io.set_result(io_slot, result);
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

pub struct Runtime {
    tasks_capacity: u32,
    ring_entries: u32,
}

impl Runtime {
    pub fn new(tasks_capacity: u32, ring_entries: u32) -> std::io::Result<Self> {
        if ring_entries == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "ring_entries must be greater than 0",
            ));
        }

        Ok(Self {
            tasks_capacity,
            ring_entries,
        })
    }

    pub fn block_on<S, F, T>(self, make_fut: S, user_data: T)
    where
        S: Fn(TaskContext, RuntimeContext<T>, T) -> F,
        F: Future<Output = ()> + 'static,
    {
        let generation = enter_active_gen();

        let ring = io_uring::IoUring::builder()
            .setup_single_issuer()
            .setup_defer_taskrun()
            .build(self.ring_entries)
            .unwrap_or_else(|_| io_uring::IoUring::new(self.ring_entries).unwrap());

        let mut data = RuntimeData {
            tasks: TaskSlab::new::<F>(self.tasks_capacity),
            wakeups: Vec::new(),
            ring,
        };

        let data_ptr: *mut RuntimeData = &mut data;
        let _drop_guard = DropGuard::new(move || unsafe {
            TaskSlab::drop_futures_raw::<F>(&mut (*data_ptr).tasks)
        });

        let spawn: unsafe fn(RuntimeContext<T>, T) -> Option<u32> = Self::spawn::<S, F, T>;

        let ctx = RuntimeContext {
            generation,
            data: data_ptr,
            spawn,
            make_fut: &raw const make_fut as *const (),
        };

        let index = match data.tasks.insert_vacant() {
            Some(i) => i,
            None => {
                exit_active_gen();
                return;
            }
        };

        let task = Task {
            ready: true,
            io: IoState::new(),
            arena: Arena::new(4096),
            id: 0,
        };
        unsafe { data.tasks.init_task_unchecked(index, task) };

        let task_ctx = data.context_for(index);

        let future = (make_fut)(task_ctx, ctx, user_data);
        unsafe { data.tasks.init_future_unchecked(index, future) };
        data.wakeups.push(index);

        let mut ready_tasks = Vec::new();

        loop {
            core::mem::swap(&mut data.wakeups, &mut ready_tasks);
            assert!(data.wakeups.is_empty());

            data.drain_cqes(&mut ready_tasks);

            for &idx in &ready_tasks {
                if !data.tasks.is_occupied(idx) {
                    continue;
                }

                unsafe { data.tasks.task_mut_unchecked(idx).ready = false };

                let mut cx = Context::from_waker(Waker::noop());
                let future_ptr = data.tasks.future_ptr_unchecked::<F>(idx);
                let future = unsafe { Pin::new_unchecked(&mut *future_ptr) };

                match future.poll(&mut cx) {
                    Poll::Ready(()) => {
                        unsafe { data.tasks.remove_unchecked::<F>(idx) };
                    }
                    Poll::Pending => {}
                }
            }

            ready_tasks.clear();

            if !data.tasks.has_io_in_flight() {
                if data.wakeups.is_empty() {
                    break;
                }
                continue;
            }

            match data.ring.submit_and_wait(1) {
                Ok(_) => {}
                Err(ref e) if e.raw_os_error() == Some(libc::EBUSY) => {}
                Err(_) => {
                    exit_active_gen();
                    return;
                }
            }
        }

        exit_active_gen();
    }

    unsafe fn spawn<S, F, T>(ctx: RuntimeContext<T>, user_data: T) -> Option<u32>
    where
        S: Fn(TaskContext, RuntimeContext<T>, T) -> F,
        F: Future<Output = ()> + 'static,
    {
        let data = unsafe { &mut *ctx.data };
        let closure = unsafe { &*(ctx.make_fut as *const S) };

        let index = data.tasks.insert_vacant()?;
        let task = Task {
            ready: true,
            io: IoState::new(),
            arena: Arena::new(4096),
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
    }
}

#[derive(Debug)]
pub struct RuntimeContext<T> {
    generation: u64,
    spawn: unsafe fn(RuntimeContext<T>, T) -> Option<u32>,
    data: *mut RuntimeData,
    make_fut: *const (),
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
    use core::num::NonZeroU32;
    use core::pin::pin;
    use core::task::Context;
    use core::task::Poll;
    use core::task::Waker;

    use super::*;
    use crate::arena::Arena;
    use crate::arena::ArenaAlloc;
    use crate::task::Task;
    use crate::task::TaskSlab;

    fn test_runtime_data(capacity: u32) -> RuntimeData {
        RuntimeData {
            tasks: TaskSlab::new::<Ready<()>>(capacity),
            wakeups: Vec::new(),
            ring: io_uring::IoUring::new(8).unwrap(),
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
            allocs: vec![None; capacity * 64],
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
        s.set_alloc(10, ArenaAlloc {
            size: NonZeroU32::new(4).unwrap(),
            offset: 0,
        });
        s.set_result(10, -1);
        s.set_ready(10, true);
        assert!(s.is_ready(10));
        assert_eq!(s.result(10), -1);
        let alloc = s.take_alloc(10);
        assert_eq!(alloc.size.get(), 4);
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
            arena: Arena::new(4096),
            id: 0,
        };
        let slot = task.io.free_slot().unwrap();
        task.io.set_submitted(slot, true);
        task.io.set_result(slot, 42);
        task.io.set_ready(slot, true);

        let mut data = test_runtime_data(64);
        let index = data.tasks.insert_vacant().unwrap();
        unsafe { data.tasks.init_task_unchecked(index, task) };

        enter_active_gen();
        let ctx = data.context_for(index);

        let fut = await_cqe(ctx, slot);
        let mut fut = pin!(fut);
        let mut cx = Context::from_waker(Waker::noop());

        assert_eq!(fut.as_mut().poll(&mut cx), Poll::Ready(42));
        exit_active_gen();
    }

    #[test]
    fn await_cqe_delayed_ready() {
        let mut task = Task {
            ready: false,
            io: IoState::new(),
            arena: Arena::new(4096),
            id: 0,
        };
        let slot = task.io.free_slot().unwrap();
        task.io.set_submitted(slot, true);
        // NOT setting ready yet

        let mut data = test_runtime_data(64);
        let index = data.tasks.insert_vacant().unwrap();
        unsafe { data.tasks.init_task_unchecked(index, task) };

        enter_active_gen();
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
        exit_active_gen();
    }

    // ── Yield ─────────────────────────────────────────────────────────

    #[test]
    fn yield_first_poll_returns_pending() {
        let task = Task {
            ready: false,
            io: IoState::new(),
            arena: Arena::new(4096),
            id: 0,
        };
        let mut data = test_runtime_data(64);
        let index = data.tasks.insert_vacant().unwrap();
        unsafe { data.tasks.init_task_unchecked(index, task) };

        enter_active_gen();
        let ctx = data.context_for(index);
        let mut y = yield_now(ctx);
        let mut y = pin!(&mut y);
        let mut cx = Context::from_waker(Waker::noop());

        assert_eq!(y.as_mut().poll(&mut cx), Poll::Pending);
        assert!(y.polled);
        exit_active_gen();
    }

    #[test]
    fn yield_second_poll_returns_ready() {
        let task = Task {
            ready: false,
            io: IoState::new(),
            arena: Arena::new(4096),
            id: 0,
        };
        let mut data = test_runtime_data(64);
        let index = data.tasks.insert_vacant().unwrap();
        unsafe { data.tasks.init_task_unchecked(index, task) };

        enter_active_gen();
        let ctx = data.context_for(index);
        let mut y = yield_now(ctx);
        let mut y = pin!(&mut y);
        let mut cx = Context::from_waker(Waker::noop());

        assert_eq!(y.as_mut().poll(&mut cx), Poll::Pending);
        assert_eq!(y.as_mut().poll(&mut cx), Poll::Ready(()));
        exit_active_gen();
    }

    #[test]
    fn yield_calls_wake_on_first_poll() {
        let task = Task {
            ready: false,
            io: IoState::new(),
            arena: Arena::new(4096),
            id: 0,
        };
        let mut data = test_runtime_data(64);
        let index = data.tasks.insert_vacant().unwrap();
        unsafe { data.tasks.init_task_unchecked(index, task) };

        enter_active_gen();
        let ctx = data.context_for(index);
        let mut y = yield_now(ctx);
        let mut y = pin!(&mut y);
        let mut cx = Context::from_waker(Waker::noop());

        let _ = y.as_mut().poll(&mut cx);
        // wake should have pushed index into wakeups
        assert_eq!(data.wakeups, vec![index]);
        exit_active_gen();
    }

    // ── TaskContext ───────────────────────────────────────────────────

    #[test]
    fn task_context_with_task_reads_correct_task() {
        let task = Task {
            ready: false,
            io: IoState::new(),
            arena: Arena::new(4096),
            id: 0,
        };
        let mut data = test_runtime_data(64);
        let index = data.tasks.insert_vacant().unwrap();
        unsafe { data.tasks.init_task_unchecked(index, task) };

        enter_active_gen();
        let ctx = data.context_for(index);

        let ready = ctx.with_task(|t| t.ready);
        exit_active_gen();
        assert!(!ready);
    }

    #[test]
    fn task_context_with_task_modifies() {
        let task = Task {
            ready: false,
            io: IoState::new(),
            arena: Arena::new(4096),
            id: 0,
        };
        let mut data = test_runtime_data(64);
        let index = data.tasks.insert_vacant().unwrap();
        unsafe { data.tasks.init_task_unchecked(index, task) };

        enter_active_gen();
        let ctx = data.context_for(index);

        ctx.with_task(|t| t.ready = true);
        exit_active_gen();
        assert!(unsafe { data.tasks.task_unchecked(index) }.ready);
    }

    #[test]
    fn task_context_wake_pushes_index() {
        let task = Task {
            ready: false,
            io: IoState::new(),
            arena: Arena::new(4096),
            id: 0,
        };
        let mut data = test_runtime_data(64);
        let index = data.tasks.insert_vacant().unwrap();
        unsafe { data.tasks.init_task_unchecked(index, task) };

        enter_active_gen();
        let ctx = data.context_for(index);

        ctx.wake();
        ctx.wake();
        exit_active_gen();
        assert_eq!(data.wakeups, vec![index, index]);
    }

    #[test]
    fn task_context_after_removal_panics() {
        let task = Task {
            ready: false,
            io: IoState::new(),
            arena: Arena::new(4096),
            id: 0,
        };
        let mut data = test_runtime_data(64);
        let index = data.tasks.insert_vacant().unwrap();
        unsafe { data.tasks.init_task_unchecked(index, task) };
        unsafe {
            data.tasks
                .init_future_unchecked(index, core::future::ready(()))
        };

        enter_active_gen();
        let ctx = data.context_for(index);

        let _ = unsafe { data.tasks.remove_unchecked::<Ready<()>>(index) };

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            ctx.with_task(|_t| ());
        }));
        exit_active_gen();
        assert!(result.is_err());
    }

    #[test]
    fn task_context_after_slot_reuse_panics_without_touching_new_task() {
        let task_a = Task {
            ready: false,
            io: IoState::new(),
            arena: Arena::new(4096),
            id: 0,
        };
        let mut data = test_runtime_data(64);
        let index = data.tasks.insert_vacant().unwrap();
        unsafe { data.tasks.init_task_unchecked(index, task_a) };
        unsafe {
            data.tasks
                .init_future_unchecked(index, core::future::ready(()))
        };

        enter_active_gen();
        let ctx = data.context_for(index);

        let _ = unsafe { data.tasks.remove_unchecked::<Ready<()>>(index) };

        let task_b = Task {
            ready: true,
            io: IoState::new(),
            arena: Arena::new(4096),
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
        exit_active_gen();

        assert!(result.is_err());
        assert!(unsafe { data.tasks.task_unchecked(index) }.ready);
    }

    #[test]
    fn task_context_of_live_task_usable_alongside_other_tasks() {
        let task_a = Task {
            ready: false,
            io: IoState::new(),
            arena: Arena::new(4096),
            id: 0,
        };
        let mut data = test_runtime_data(64);
        let index_a = data.tasks.insert_vacant().unwrap();
        unsafe { data.tasks.init_task_unchecked(index_a, task_a) };

        let task_b = Task {
            ready: true,
            io: IoState::new(),
            arena: Arena::new(4096),
            id: 0,
        };
        let index_b = data.tasks.insert_vacant().unwrap();
        unsafe { data.tasks.init_task_unchecked(index_b, task_b) };

        enter_active_gen();
        let ctx_a = data.context_for(index_a);
        let _ctx_b = data.context_for(index_b);

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            ctx_a.with_task(|t| t.ready = true);
        }));
        exit_active_gen();

        assert!(result.is_ok());
        assert!(unsafe { data.tasks.task_unchecked(index_a) }.ready);
    }
}
