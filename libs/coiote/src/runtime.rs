use core::future::Future;
use core::pin::Pin;
use core::task::Context;
use core::task::Poll;
use core::task::RawWaker;
use core::task::RawWakerVTable;
use core::task::Waker;

use crate::arena::Arena;
use crate::arena::ArenaAlloc;
use crate::slab::Slab;
use crate::task::Task;
use crate::task::TaskSlab;

#[allow(clippy::large_enum_variant)]
pub enum IoState {
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
        Self::Inline {
            submitted: 0,
            results: [0; 64],
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

pub struct IoBuffers {
    pub arena: Arena,
    pub inflight_arena_guards: Slab<ArenaAlloc<'static>>,
    pub inflight_vecs: Slab<Vec<u8>>,
}

impl IoBuffers {
    pub fn new(arena_capacity: u32) -> Self {
        Self {
            arena: Arena::new(arena_capacity),
            inflight_arena_guards: Slab::new(),
            inflight_vecs: Slab::new(),
        }
    }
}

#[allow(dead_code)]
pub enum BufferInput {
    Arena(ArenaAlloc<'static>),
    Vector(Vec<u8>),
}

pub struct Runtime<T, F: Future<Output = ()>, S: Fn(&mut Task, T) -> F> {
    tasks: TaskSlab<F>,
    ready: Vec<u32>,
    ring: io_uring::IoUring,
    spawn_fn: S,
    _phantom: core::marker::PhantomData<T>,
}

impl<T, F: Future<Output = ()>, S: Fn(&mut Task, T) -> F> Runtime<T, F, S> {
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
        // Initialize Task — always valid before spawn_fn runs
        let task = Task {
            index,
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

    pub fn run(&mut self) {
        loop {
            let n = self.ready.len();
            for _ in 0..n {
                let index = self.ready.remove(0);
                self.poll_one(index);
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
                if !task.ready {
                    task.ready = true;
                    self.ready.push(task_index);
                }
            }
        }
    }
}

unsafe fn waker(task_index: u32) -> Waker {
    unsafe fn wake_by_ref(_data: *const ()) {
        // TODO: push to ready queue
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
