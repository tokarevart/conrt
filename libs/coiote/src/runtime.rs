use core::cell::Cell;
use core::future::Future;
use core::pin::Pin;
use core::task::Context;
use core::task::Poll;
use core::task::RawWaker;
use core::task::RawWakerVTable;
use core::task::Waker;
use std::os::fd::RawFd;

use crate::arena::Arena;
use crate::arena::ArenaAlloc;
use crate::task::Task;
use crate::task::TaskSlab;

thread_local! {
    static RUNTIME: Cell<Option<*mut ()>> = const { Cell::new(None) };
    static WAKE_QUEUE_PTR: Cell<*mut Vec<u32>> = const { Cell::new(core::ptr::null_mut()) };
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

pub async fn await_cqe(task: &mut Task, slot: u32) -> i32 {
    while !task.io.is_ready(slot) {
        yield_now().await;
    }
    task.io.result(slot)
}

pub fn yield_now() -> Yield {
    Yield { polled: false }
}

pub struct Yield {
    polled: bool,
}

impl Future for Yield {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.polled {
            Poll::Ready(())
        } else {
            self.polled = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
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

pub struct Runtime<T, F, S>
where
    F: Future<Output = ()>,
    S: Fn(&mut Task, T) -> F,
{
    pub tasks: TaskSlab<F>,
    pub ready: Vec<u32>,
    pub wake_queue: Vec<u32>,
    pub ring: io_uring::IoUring,
    spawn_fn: S,
    _phantom: core::marker::PhantomData<T>,
}

impl<T, F, S> Runtime<T, F, S>
where
    F: Future<Output = ()>,
    S: Fn(&mut Task, T) -> F,
{
    pub fn new(task_capacity: u32, ring_entries: u32, spawn_fn: S) -> Self {
        Self {
            tasks: TaskSlab::new(task_capacity),
            ready: Vec::new(),
            wake_queue: Vec::new(),
            ring: io_uring::IoUring::new(ring_entries).expect("failed to create io_uring"),
            spawn_fn,
            _phantom: core::marker::PhantomData,
        }
    }

    pub fn spawn(&mut self, user_data: T) -> Option<u32> {
        let index = self.tasks.insert_vacant()?;
        let task = Task {
            ready: true,
            io: IoState::new(),
            arena: Arena::new(4096),
        };
        self.tasks.init_task(index, task);
        let future = (self.spawn_fn)(self.tasks.task_mut(index).unwrap(), user_data);
        self.tasks.init_future(index, future);
        self.ready.push(index);
        Some(index)
    }

    pub fn run(&mut self) {
        WAKE_QUEUE_PTR.with(|p| p.set(&mut self.wake_queue));
        RUNTIME.with(|r| r.set(Some(self as *mut _ as *mut ())));

        loop {
            let n = self.ready.len();
            for _ in 0..n {
                let index = self.ready.remove(0);
                self.poll_one(index);
            }

            for index in self.wake_queue.drain(..) {
                if !self.ready.contains(&index) {
                    self.ready.push(index);
                }
            }

            match self.ring.submit_and_wait(1) {
                Ok(_) => {}
                Err(ref e) if e.raw_os_error() == Some(libc::EBUSY) => {}
                Err(_) => return,
            }
            self.drain_cqes();

            let _ = self.ring.submit();
        }
    }

    fn poll_one(&mut self, index: u32) {
        let task = match self.tasks.task_mut(index) {
            Some(t) => t,
            None => return,
        };
        task.ready = false;
        let w = unsafe { waker(index) };
        let mut cx = Context::from_waker(&w);
        let future = unsafe { Pin::new_unchecked(self.tasks.future_mut(index).unwrap()) };
        match future.poll(&mut cx) {
            Poll::Ready(()) => {
                self.tasks.remove(index);
            }
            Poll::Pending => {}
        }
    }

    fn drain_cqes(&mut self) {
        for cqe in self.ring.completion() {
            let raw = cqe.user_data();
            let task_index = (raw >> 32) as u32;
            let io_slot = raw as u32;
            let result = cqe.result();

            if let Some(task) = self.tasks.task_mut(task_index) {
                task.io.set_result(io_slot, result);
                task.io.set_ready(io_slot, true);
                if !task.ready {
                    task.ready = true;
                    self.ready.push(task_index);
                }
            }
        }
    }
}

unsafe fn waker(task_index: u32) -> Waker {
    unsafe fn wake_by_ref(data: *const ()) {
        let index = data as u32;
        WAKE_QUEUE_PTR.with(|p| unsafe {
            let queue = &mut *p.get();
            queue.push(index);
        });
    }
    unsafe fn wake(data: *const ()) {
        unsafe { wake_by_ref(data) };
    }
    const VTABLE: RawWakerVTable = RawWakerVTable::new(
        |data| RawWaker::new(data, &VTABLE),
        wake_by_ref,
        wake,
        |_| {},
    );
    let ptr = core::ptr::without_provenance(task_index as usize);
    unsafe { Waker::from_raw(RawWaker::new(ptr, &VTABLE)) }
}
