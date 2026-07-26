use core::mem::MaybeUninit;

use crate::arena::Arena;
use crate::arena::ArenaAlloc;
use crate::runtime::IoState;
use crate::slab::Slab;

pub struct Task {
    pub index: u32,
    pub ready: bool,
    pub io: IoState,
    pub arena: Arena,
    pub inflight: Slab<ArenaAlloc>,
}

pub struct TaskSlab<F> {
    tasks: Box<[MaybeUninit<Task>]>,
    futures: Box<[MaybeUninit<F>]>,
    occupied: Box<[u64]>,
    free: Vec<u32>,
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
            occupied: vec![0u64; words].into_boxed_slice(),
            free: Vec::new(),
        }
    }

    pub fn insert_vacant(&mut self) -> Option<u32> {
        let index = if let Some(index) = self.free.pop() {
            index
        } else {
            let word = self.occupied.iter().position(|&w| w != u64::MAX)?;
            let bit = (!self.occupied[word]).trailing_zeros();
            let index = word as u32 * 64 + bit;
            if index as usize >= self.tasks.len() {
                return None;
            }
            index
        };
        self.set_occupied(index, true);
        Some(index)
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
        self.set_occupied(index, false);
        self.free.push(index);
        let task = unsafe { self.tasks[index as usize].assume_init_read() };
        let future = unsafe { self.futures[index as usize].assume_init_read() };
        (task, future)
    }

    fn is_occupied(&self, index: u32) -> bool {
        let word = index as usize / 64;
        let bit = index as usize % 64;
        word < self.occupied.len() && self.occupied[word] & (1 << bit) != 0
    }

    fn set_occupied(&mut self, index: u32, occupied: bool) {
        let word = index as usize / 64;
        let bit = index as usize % 64;
        if occupied {
            self.occupied[word] |= 1 << bit;
        } else {
            self.occupied[word] &= !(1 << bit);
        }
    }
}
