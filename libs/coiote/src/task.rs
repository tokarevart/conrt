use core::mem::MaybeUninit;

use crate::arena::Arena;
use crate::runtime::IoState;

pub struct Task {
    pub ready: bool,
    pub io: IoState,
    pub arena: Arena,
}

pub struct TaskSlab<F> {
    tasks: Box<[MaybeUninit<Task>]>,
    futures: Box<[MaybeUninit<F>]>,
    free: Box<[u64]>,
}

impl<F> TaskSlab<F> {
    pub fn new(capacity: u32) -> Self {
        let cap = capacity as usize;
        let mut tasks = Vec::with_capacity(cap);
        tasks.resize_with(cap, MaybeUninit::uninit);
        let mut futures = Vec::with_capacity(cap);
        futures.resize_with(cap, MaybeUninit::uninit);
        let words = cap.div_ceil(64);
        Self {
            tasks: tasks.into_boxed_slice(),
            futures: futures.into_boxed_slice(),
            free: vec![u64::MAX; words].into_boxed_slice(),
        }
    }

    pub fn insert_vacant(&mut self) -> Option<u32> {
        for (word_idx, word) in self.free.iter().enumerate() {
            if *word != 0 {
                let bit = word.trailing_zeros();
                let index = word_idx as u32 * 64 + bit;
                if index as usize >= self.tasks.len() {
                    return None;
                }
                self.free[word_idx] &= !(1 << bit);
                return Some(index);
            }
        }
        None
    }

    pub fn init_task(&mut self, index: u32, task: Task) {
        self.tasks[index as usize] = MaybeUninit::new(task);
    }

    pub fn init_future(&mut self, index: u32, future: F) {
        self.futures[index as usize] = MaybeUninit::new(future);
    }

    pub fn task_mut(&mut self, index: u32) -> Option<&mut Task> {
        if self.is_occupied(index) {
            Some(unsafe { self.tasks[index as usize].assume_init_mut() })
        } else {
            None
        }
    }

    pub fn future_mut(&mut self, index: u32) -> Option<&mut F> {
        if self.is_occupied(index) {
            Some(unsafe { self.futures[index as usize].assume_init_mut() })
        } else {
            None
        }
    }

    pub fn remove(&mut self, index: u32) -> (Task, F) {
        let word = index as usize / 64;
        let bit = index as usize % 64;
        self.free[word] |= 1 << bit;
        let task = unsafe { self.tasks[index as usize].assume_init_read() };
        let future = unsafe { self.futures[index as usize].assume_init_read() };
        (task, future)
    }

    fn is_occupied(&self, index: u32) -> bool {
        let word = index as usize / 64;
        let bit = index as usize % 64;
        word < self.free.len() && self.free[word] & (1 << bit) == 0
    }
}
