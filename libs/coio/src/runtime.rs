use core::future::Future;
use core::marker::PhantomData;
use core::pin::Pin;
use core::task::Context;
use core::task::Poll;
use core::task::Waker;
use std::os::fd::RawFd;

use crate::arena::Arena;
use crate::arena::ArenaAlloc;
use crate::task::Task;
use crate::task::TaskContext;
use crate::task::TaskSlab;

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

/// # Safety
/// `sq` must be a valid submission queue. `buf` must point to valid memory of
/// at least `len` bytes.
#[allow(dead_code)]
pub unsafe fn push_read(
    sq: &mut io_uring::squeue::SubmissionQueue,
    fd: RawFd,
    buf: *mut u8,
    len: u32,
    user_data: u64,
) {
    let entry = io_uring::opcode::Read::new(io_uring::types::Fd(fd), buf, len)
        .build()
        .user_data(user_data);
    unsafe {
        sq.push(&entry).ok();
    }
}

/// # Safety
/// `sq` must be a valid submission queue. `buf` must point to valid memory of
/// at least `len` bytes.
#[allow(dead_code)]
pub unsafe fn push_write(
    sq: &mut io_uring::squeue::SubmissionQueue,
    fd: RawFd,
    buf: *const u8,
    len: u32,
    user_data: u64,
) {
    let entry = io_uring::opcode::Write::new(io_uring::types::Fd(fd), buf, len)
        .build()
        .user_data(user_data);
    unsafe {
        sq.push(&entry).ok();
    }
}

pub async fn await_cqe(ctx: &TaskContext, slot: u32) -> i32 {
    loop {
        let ready = ctx.with_task(|task| task.io.is_ready(slot));
        if ready {
            return ctx.with_task(|task| task.io.result(slot));
        }
        yield_now(ctx).await;
    }
}

pub fn yield_now(ctx: &TaskContext) -> Yield {
    Yield {
        ctx: ctx as *const TaskContext,
        polled: false,
    }
}

pub struct Yield {
    ctx: *const TaskContext,
    polled: bool,
}

impl Future for Yield {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        if self.polled {
            Poll::Ready(())
        } else {
            self.polled = true;
            unsafe { (*self.ctx).wake() };
            Poll::Pending
        }
    }
}

pub struct Runtime<T, F, S>
where
    F: Future<Output = ()>,
    S: Fn(&TaskContext, &RuntimeContext<T>, T) -> F,
{
    tasks: TaskSlab<F>,
    wakeups: Vec<u32>,
    ring: io_uring::IoUring,
    spawn_fn: S,
    _phantom: PhantomData<T>,
}

impl<T, F, S> Runtime<T, F, S>
where
    F: Future<Output = ()>,
    S: Fn(&TaskContext, &RuntimeContext<T>, T) -> F,
{
    pub fn new(task_capacity: u32, ring_entries: u32, spawn_fn: S) -> std::io::Result<Self> {
        Ok(Self {
            tasks: TaskSlab::new(task_capacity),
            wakeups: Vec::new(),
            ring: io_uring::IoUring::new(ring_entries)?,
            spawn_fn,
            _phantom: PhantomData,
        })
    }

    pub fn block_on(mut self, user_data: T) {
        let tasks_ptr: *mut TaskSlab<F> = &mut self.tasks;
        let wakeups_ptr: *mut Vec<u32> = &mut self.wakeups;

        let spawn_fn_ptr: unsafe fn(*const RuntimeContext<T>, T) -> Option<u32> = Self::spawn;

        let ctx = RuntimeContext {
            tasks: tasks_ptr as *mut (),
            spawn_fn: spawn_fn_ptr,
            wakeups: wakeups_ptr,
            spawn_closure: &self.spawn_fn as *const S as *const (),
            _phantom: PhantomData,
        };

        let index = match unsafe { TaskSlab::insert_vacant(tasks_ptr) } {
            Some(i) => i,
            None => return,
        };

        let task = Task {
            ready: true,
            io: IoState::new(),
            arena: Arena::new(4096),
        };
        unsafe { TaskSlab::init_task_unchecked(tasks_ptr, index, task) };

        let task_ctx = TaskContext::new(
            index,
            unsafe { TaskSlab::task_ptr_unchecked(tasks_ptr, index) },
            wakeups_ptr,
        );
        unsafe { TaskSlab::init_context_unchecked(tasks_ptr, index, task_ctx) };
        let task_ctx_ref =
            unsafe { &*TaskSlab::context_ptr_unchecked(tasks_ptr as *const _, index) };

        let future = (self.spawn_fn)(task_ctx_ref, &ctx, user_data);
        unsafe { TaskSlab::init_future_unchecked(tasks_ptr, index, future) };
        self.wakeups.push(index);

        let mut ready_tasks = Vec::new();

        loop {
            core::mem::swap(&mut self.wakeups, &mut ready_tasks);

            self.drain_cqes(&mut ready_tasks);

            for &idx in &ready_tasks {
                let task_ptr = unsafe { TaskSlab::task_ptr_unchecked(tasks_ptr, idx) };
                unsafe { (*task_ptr).ready = false };

                let mut cx = Context::from_waker(Waker::noop());
                let future_ptr = unsafe { TaskSlab::future_ptr_unchecked(tasks_ptr, idx) };
                let future = unsafe { Pin::new_unchecked(&mut *future_ptr) };

                match future.poll(&mut cx) {
                    Poll::Ready(()) => {
                        unsafe { TaskSlab::remove_unchecked(tasks_ptr, idx) };
                    }
                    Poll::Pending => {}
                }
            }

            ready_tasks.clear();

            match self.ring.submit_and_wait(1) {
                Ok(_) => {}
                Err(ref e) if e.raw_os_error() == Some(libc::EBUSY) => {}
                Err(_) => return,
            }
        }
    }

    fn drain_cqes(&mut self, ready: &mut Vec<u32>) {
        for cqe in self.ring.completion() {
            let raw = cqe.user_data();
            let task_index = (raw >> 32) as u32;
            let io_slot = raw as u32;
            let result = cqe.result();

            unsafe {
                if TaskSlab::is_occupied(&self.tasks as *const TaskSlab<F>, task_index) {
                    let task_ptr = TaskSlab::task_ptr_unchecked(
                        &mut self.tasks as *mut TaskSlab<F>,
                        task_index,
                    );
                    (*task_ptr).io.set_result(io_slot, result);
                    (*task_ptr).io.set_ready(io_slot, true);
                    if !(*task_ptr).ready {
                        (*task_ptr).ready = true;
                        (*ready).push(task_index);
                    }
                }
            }
        }
    }

    unsafe fn spawn(ctx: *const RuntimeContext<T>, user_data: T) -> Option<u32> {
        let ctx = unsafe { &*ctx };
        let tasks = ctx.tasks as *mut TaskSlab<F>;
        let closure = unsafe { &*(ctx.spawn_closure as *const S) };

        let index = unsafe { TaskSlab::insert_vacant(tasks)? };
        let task = Task {
            ready: true,
            io: IoState::new(),
            arena: Arena::new(4096),
        };
        unsafe { TaskSlab::init_task_unchecked(tasks, index, task) };

        let task_ctx = TaskContext::new(
            index,
            unsafe { TaskSlab::task_ptr_unchecked(tasks, index) },
            ctx.wakeups,
        );
        unsafe { TaskSlab::init_context_unchecked(tasks, index, task_ctx) };
        let task_ctx_ref = unsafe { &*TaskSlab::context_ptr_unchecked(tasks as *const _, index) };

        let future = closure(task_ctx_ref, ctx, user_data);
        unsafe {
            TaskSlab::init_future_unchecked(tasks, index, future);
            (*ctx.wakeups).push(index);
        }
        Some(index)
    }
}

pub struct RuntimeContext<T> {
    spawn_fn: unsafe fn(*const RuntimeContext<T>, T) -> Option<u32>,
    tasks: *mut (),
    wakeups: *mut Vec<u32>,
    spawn_closure: *const (),
    _phantom: PhantomData<T>,
}

impl<T> RuntimeContext<T> {
    pub fn spawn(&self, user_data: T) -> Option<u32> {
        unsafe { (self.spawn_fn)(self, user_data) }
    }
}
