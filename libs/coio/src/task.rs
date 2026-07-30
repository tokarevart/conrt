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

#[cfg(test)]
mod tests {
    use core::future::Ready;

    use super::*;
    use crate::arena::Arena;
    use crate::runtime::IoState;

    // ── TaskSlab ──────────────────────────────────────────────────────

    #[test]
    fn slab_new_large_capacity() {
        let slab = TaskSlab::<Ready<()>>::new(128);
        assert_eq!(slab.tasks.len(), 128);
        assert_eq!(slab.free.len(), 2); // 128 / 64 = 2 words
        assert_eq!(slab.free[0], u64::MAX);
        assert_eq!(slab.free[1], u64::MAX);
    }

    #[test]
    fn insert_vacant_sequential() {
        let mut slab = TaskSlab::<Ready<()>>::new(10);
        unsafe {
            assert_eq!(TaskSlab::insert_vacant(&mut slab), Some(0));
            assert_eq!(TaskSlab::insert_vacant(&mut slab), Some(1));
            assert_eq!(TaskSlab::insert_vacant(&mut slab), Some(2));
        }
    }

    #[test]
    fn insert_vacant_exhaustion() {
        let mut slab = TaskSlab::<Ready<()>>::new(3);
        unsafe {
            assert_eq!(TaskSlab::insert_vacant(&mut slab), Some(0));
            assert_eq!(TaskSlab::insert_vacant(&mut slab), Some(1));
            assert_eq!(TaskSlab::insert_vacant(&mut slab), Some(2));
            assert_eq!(TaskSlab::insert_vacant(&mut slab), None);
        }
    }

    #[test]
    fn init_and_retrieve_task() {
        let mut slab = TaskSlab::<Ready<()>>::new(10);
        unsafe {
            let idx = TaskSlab::insert_vacant(&mut slab).unwrap();
            let task = Task {
                ready: true,
                io: IoState::new(),
                arena: Arena::new(4096),
            };
            TaskSlab::init_task_unchecked(&mut slab, idx, task);

            let ptr = TaskSlab::task_ptr_unchecked(&mut slab, idx);
            assert_eq!((*ptr).ready, true);
        }
    }

    #[test]
    fn init_and_retrieve_context() {
        let mut slab = TaskSlab::<Ready<()>>::new(10);
        unsafe {
            let idx = TaskSlab::insert_vacant(&mut slab).unwrap();
            let task = Task {
                ready: true,
                io: IoState::new(),
                arena: Arena::new(4096),
            };
            TaskSlab::init_task_unchecked(&mut slab, idx, task);
            let mut wakeups = Vec::new();
            let ctx = TaskContext::new(
                idx,
                TaskSlab::task_ptr_unchecked(&mut slab, idx),
                &mut wakeups,
            );
            TaskSlab::init_context_unchecked(&mut slab, idx, ctx);

            let cptr = TaskSlab::context_ptr_unchecked(&slab as *const _, idx);
            assert_eq!((*cptr).task_index, idx);
        }
    }

    #[test]
    fn is_occupied_initially_false() {
        let slab = TaskSlab::<Ready<()>>::new(10);
        unsafe {
            assert!(!TaskSlab::is_occupied(&slab as *const _, 0));
        }
    }

    #[test]
    fn is_occupied_after_init() {
        let mut slab = TaskSlab::<Ready<()>>::new(10);
        unsafe {
            let idx = TaskSlab::insert_vacant(&mut slab).unwrap();
            let task = Task {
                ready: true,
                io: IoState::new(),
                arena: Arena::new(4096),
            };
            TaskSlab::init_task_unchecked(&mut slab, idx, task);
            TaskSlab::init_future_unchecked(&mut slab, idx, core::future::ready(()));
            TaskSlab::init_context_unchecked(
                &mut slab,
                idx,
                TaskContext::new(
                    idx,
                    TaskSlab::task_ptr_unchecked(&mut slab, idx),
                    &mut Vec::new(),
                ),
            );
            assert!(TaskSlab::is_occupied(&slab as *const _, idx));
        }
    }

    #[test]
    fn remove_unchecked_returns_values_and_frees_slot() {
        let mut slab = TaskSlab::<Ready<()>>::new(10);
        unsafe {
            let idx = TaskSlab::insert_vacant(&mut slab).unwrap();
            let task = Task {
                ready: true,
                io: IoState::new(),
                arena: Arena::new(4096),
            };
            TaskSlab::init_task_unchecked(&mut slab, idx, task);
            TaskSlab::init_future_unchecked(&mut slab, idx, core::future::ready(()));
            let mut wakeups = Vec::new();
            TaskSlab::init_context_unchecked(
                &mut slab,
                idx,
                TaskContext::new(
                    idx,
                    TaskSlab::task_ptr_unchecked(&mut slab, idx),
                    &mut wakeups,
                ),
            );

            let (t, _f) = TaskSlab::remove_unchecked(&mut slab, idx);
            assert!(t.ready);
            // future is Ready<()>, dropping it after taking is fine

            // Slot should now be free again
            assert!(!TaskSlab::is_occupied(&slab as *const _, idx));
            let next_idx = TaskSlab::insert_vacant(&mut slab).unwrap();
            assert_eq!(next_idx, idx); // reused
        }
    }

    // ── TaskContext ───────────────────────────────────────────────────

    #[test]
    fn task_context_with_task_reads_correct_task() {
        let mut task = Task {
            ready: false,
            io: IoState::new(),
            arena: Arena::new(4096),
        };
        let mut wakeups = Vec::new();
        let ctx = TaskContext::new(5, &mut task, &mut wakeups);

        let ready = ctx.with_task(|t| t.ready);
        assert!(!ready);
    }

    #[test]
    fn task_context_with_task_modifies() {
        let mut task = Task {
            ready: false,
            io: IoState::new(),
            arena: Arena::new(4096),
        };
        let mut wakeups = Vec::new();
        let ctx = TaskContext::new(5, &mut task, &mut wakeups);

        ctx.with_task(|t| t.ready = true);
        assert!(task.ready);
    }

    #[test]
    fn task_context_wake_pushes_index() {
        let mut task = Task {
            ready: false,
            io: IoState::new(),
            arena: Arena::new(4096),
        };
        let mut wakeups = Vec::new();
        let ctx = TaskContext::new(3, &mut task, &mut wakeups);

        ctx.wake();
        assert_eq!(wakeups, vec![3]);

        ctx.wake();
        assert_eq!(wakeups, vec![3, 3]);
    }
}
