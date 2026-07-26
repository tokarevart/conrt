use core::future::Future;
use core::pin::Pin;
use core::task::Context;
use core::task::Poll;
use core::task::RawWaker;
use core::task::RawWakerVTable;
use core::task::Waker;
use std::cell::UnsafeCell;
use std::os::fd::RawFd;

use crate::arena::Arena;
use crate::slab::Slab;
use crate::task::Task;
use crate::task::TaskSlab;

pub static mut RUNTIME: Option<*mut ()> = None;
pub static mut WAKE_QUEUE: Vec<u32> = Vec::new();

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
    yield_now().await;
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

pub struct IoState {
    inner: UnsafeCell<IoStateInner>,
}

#[allow(clippy::large_enum_variant)]
enum IoStateInner {
    Inline {
        submitted: u64,
        results: [i32; 64],
    },
    Heap {
        submitted: Vec<u64>,
        results: Vec<i32>,
    },
}

impl Default for IoState {
    fn default() -> Self {
        Self::new()
    }
}

impl IoState {
    pub fn new() -> Self {
        Self {
            inner: UnsafeCell::new(IoStateInner::Inline {
                submitted: 0,
                results: [0; 64],
            }),
        }
    }

    pub fn is_submitted(&self, bit: u32) -> bool {
        let inner = unsafe { &*self.inner.get() };
        match inner {
            IoStateInner::Inline { submitted, .. } => *submitted & (1 << bit) != 0,
            IoStateInner::Heap { submitted, .. } => {
                let word = bit as usize / 64;
                let bit = bit as usize % 64;
                word < submitted.len() && (submitted[word] & (1 << bit)) != 0
            }
        }
    }

    pub fn set_submitted(&self, bit: u32, value: bool) {
        let inner = unsafe { &mut *self.inner.get() };

        if bit >= 64 && matches!(inner, IoStateInner::Inline { .. }) {
            self.promote_to_heap();
        }

        let inner = unsafe { &mut *self.inner.get() };
        match inner {
            IoStateInner::Inline { submitted, .. } => {
                if value {
                    *submitted |= 1 << bit;
                } else {
                    *submitted &= !(1 << bit);
                }
            }
            IoStateInner::Heap { submitted, .. } => {
                let word = bit as usize / 64;
                let bit_offset = bit as usize % 64;
                if word >= submitted.len() {
                    submitted.resize(word + 1, 0);
                }
                if value {
                    submitted[word] |= 1 << bit_offset;
                } else {
                    submitted[word] &= !(1 << bit_offset);
                }
            }
        }
    }

    pub fn result(&self, slot: u32) -> i32 {
        let inner = unsafe { &*self.inner.get() };
        match inner {
            IoStateInner::Inline { results, .. } => results[slot as usize],
            IoStateInner::Heap { results, .. } => results[slot as usize],
        }
    }

    pub fn set_result(&self, slot: u32, value: i32) {
        let inner = unsafe { &mut *self.inner.get() };

        if slot >= 64 && matches!(inner, IoStateInner::Inline { .. }) {
            self.promote_to_heap();
        }

        let inner = unsafe { &mut *self.inner.get() };
        match inner {
            IoStateInner::Inline { results, .. } => results[slot as usize] = value,
            IoStateInner::Heap { results, .. } => {
                let idx = slot as usize;
                if idx >= results.len() {
                    results.resize(idx + 1, 0);
                }
                results[idx] = value;
            }
        }
    }

    pub fn free_slot(&self) -> Option<u32> {
        let inner = unsafe { &*self.inner.get() };
        match inner {
            IoStateInner::Inline { submitted, .. } => {
                let bits = !submitted;
                if bits == 0 {
                    None
                } else {
                    Some(bits.trailing_zeros())
                }
            }
            IoStateInner::Heap { submitted, .. } => {
                for (word_idx, &word) in submitted.iter().enumerate() {
                    if word != u64::MAX {
                        let bit = (!word).trailing_zeros();
                        return Some((word_idx * 64) as u32 + bit);
                    }
                }
                Some((submitted.len() * 64) as u32)
            }
        }
    }

    fn promote_to_heap(&self) {
        let inner = unsafe { &mut *self.inner.get() };
        if let IoStateInner::Inline { submitted, results } = inner {
            let heap_submitted = vec![*submitted];
            let heap_results = results.to_vec();

            unsafe {
                core::ptr::write(self.inner.get(), IoStateInner::Heap {
                    submitted: heap_submitted,
                    results: heap_results,
                });
            }
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
            ring: io_uring::IoUring::new(ring_entries).expect("failed to create io_uring"),
            spawn_fn,
            _phantom: core::marker::PhantomData,
        }
    }

    pub fn spawn(&mut self, user_data: T) -> Option<u32> {
        let index = self.tasks.insert_vacant()?;
        let task = Task {
            index,
            ready: true,
            io: IoState::new(),
            arena: Arena::new(4096),
            inflight: Slab::new(),
        };
        self.tasks.init_task(index, task);
        let future = (self.spawn_fn)(self.tasks.task_mut(index).unwrap(), user_data);
        self.tasks.init_future(index, future);
        self.ready.push(index);
        Some(index)
    }

    pub fn run(&mut self) {
        loop {
            let n = self.ready.len();
            for _ in 0..n {
                let index = self.ready.remove(0);
                self.poll_one(index);
            }

            // Drain wake queue from wakers into ready queue
            unsafe {
                let queue = &mut *core::ptr::addr_of_mut!(WAKE_QUEUE);
                for index in queue.drain(..) {
                    if !self.ready.contains(&index) {
                        self.ready.push(index);
                    }
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
                task.io.set_submitted(io_slot, false);
                let alloc = task.inflight.remove(io_slot);
                unsafe {
                    task.arena.dealloc(alloc);
                }
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
        unsafe { (*core::ptr::addr_of_mut!(WAKE_QUEUE)).push(index) };
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
