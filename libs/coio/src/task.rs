use core::mem::MaybeUninit;

use crate::arena::Arena;
use crate::runtime::IoState;

pub struct Task {
    pub ready: bool,
    pub io: IoState,
    pub arena: Arena,
}

pub struct TaskContext {
    task_index: u32,
    task: *mut Task,
    wakeups: *mut Vec<u32>,
}

impl TaskContext {
    pub fn new(task_index: u32, task: *mut Task, wakeups: *mut Vec<u32>) -> Self {
        Self {
            task_index,
            task,
            wakeups,
        }
    }

    pub fn with_task<R>(&self, f: impl FnOnce(&mut Task) -> R) -> R {
        unsafe { f(&mut *self.task) }
    }

    pub fn wake(&self) {
        unsafe { (*self.wakeups).push(self.task_index) };
    }
}

pub struct TaskSlab<F> {
    tasks: Box<[MaybeUninit<Task>]>,
    futures: Box<[MaybeUninit<F>]>,
    contexts: Box<[MaybeUninit<TaskContext>]>,
    free: Box<[u64]>,
}

impl<F> TaskSlab<F> {
    pub fn new(capacity: u32) -> Self {
        let cap = capacity as usize;
        let mut tasks = Vec::with_capacity(cap);
        tasks.resize_with(cap, MaybeUninit::uninit);
        let mut futures = Vec::with_capacity(cap);
        futures.resize_with(cap, MaybeUninit::uninit);
        let mut contexts = Vec::with_capacity(cap);
        contexts.resize_with(cap, MaybeUninit::uninit);
        let words = cap.div_ceil(64);
        Self {
            tasks: tasks.into_boxed_slice(),
            futures: futures.into_boxed_slice(),
            contexts: contexts.into_boxed_slice(),
            free: vec![u64::MAX; words].into_boxed_slice(),
        }
    }

    /// # Safety
    /// `this` must be a valid, aligned pointer to a `TaskSlab`.
    pub unsafe fn insert_vacant(this: *mut Self) -> Option<u32> {
        let this = unsafe { &mut *this };
        for (word_idx, word) in this.free.iter().enumerate() {
            if *word != 0 {
                let bit = word.trailing_zeros();
                let index = word_idx as u32 * 64 + bit;
                if index as usize >= this.tasks.len() {
                    return None;
                }
                this.free[word_idx] &= !(1 << bit);
                return Some(index);
            }
        }
        None
    }

    /// # Safety
    /// `this` must be valid.
    pub unsafe fn init_task_unchecked(this: *mut Self, index: u32, task: Task) {
        unsafe { (*this).tasks[index as usize] = MaybeUninit::new(task) };
    }

    /// # Safety
    /// `this` must be valid.
    pub unsafe fn init_future_unchecked(this: *mut Self, index: u32, future: F) {
        unsafe { (*this).futures[index as usize] = MaybeUninit::new(future) };
    }

    /// # Safety
    /// `this` must be valid.
    pub unsafe fn init_context_unchecked(this: *mut Self, index: u32, ctx: TaskContext) {
        unsafe { (*this).contexts[index as usize] = MaybeUninit::new(ctx) };
    }

    /// # Safety
    /// `this` must be valid.
    pub unsafe fn task_ptr_unchecked(this: *mut Self, index: u32) -> *mut Task {
        unsafe { (*this).tasks[index as usize].as_mut_ptr() }
    }

    /// # Safety
    /// `this` must be valid.
    pub unsafe fn future_ptr_unchecked(this: *mut Self, index: u32) -> *mut F {
        unsafe { (*this).futures[index as usize].as_mut_ptr() }
    }

    /// # Safety
    /// `this` must be valid.
    pub unsafe fn context_ptr_unchecked(this: *const Self, index: u32) -> *const TaskContext {
        unsafe { (*this).contexts[index as usize].as_ptr() }
    }

    /// # Safety
    /// `this` must be valid.
    pub unsafe fn is_occupied(this: *const Self, index: u32) -> bool {
        let this = unsafe { &*this };
        let word = index as usize / 64;
        let bit = index as usize % 64;
        word < this.free.len() && this.free[word] & (1 << bit) == 0
    }

    /// # Safety
    /// `this` must be valid and `index` must be an in-bounds, initialized slot
    /// that has not already been removed.
    pub unsafe fn remove_unchecked(this: *mut Self, index: u32) -> (Task, F) {
        let this = unsafe { &mut *this };
        let word = index as usize / 64;
        let bit = index as usize % 64;
        this.free[word] |= 1 << bit;
        unsafe {
            let task = this.tasks[index as usize].assume_init_read();
            let future = this.futures[index as usize].assume_init_read();
            (task, future)
        }
    }
}
