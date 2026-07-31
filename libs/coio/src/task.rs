use core::mem::MaybeUninit;

use crate::arena::Arena;
use crate::runtime;
use crate::runtime::IoState;

pub struct Task {
    pub ready: bool,
    pub io: IoState,
    pub arena: Arena,
}

#[derive(Debug, Clone, Copy)]
pub struct TaskContext {
    generation: u64,
    task_index: u32,
    task: *mut Task,
    wakeups: *mut Vec<u32>,
}

impl TaskContext {
    pub fn new(generation: u64, task_index: u32, task: *mut Task, wakeups: *mut Vec<u32>) -> Self {
        Self {
            generation,
            task_index,
            task,
            wakeups,
        }
    }

    fn check_active(&self) {
        assert!(
            runtime::active_gen_matches(self.generation),
            "TaskContext used outside the runtime it belongs to"
        );
    }

    pub fn with_task<R>(&self, f: impl FnOnce(&mut Task) -> R) -> R {
        self.check_active();
        unsafe { f(&mut *self.task) }
    }

    pub fn wake(&self) {
        self.check_active();
        unsafe { (*self.wakeups).push(self.task_index) };
    }
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

    pub fn is_occupied(&self, index: u32) -> bool {
        let word = index as usize / 64;
        let bit = index as usize % 64;
        word < self.free.len() && self.free[word] & (1 << bit) == 0
    }

    pub fn has_io_in_flight(&self) -> bool {
        for (word_idx, &word) in self.free.iter().enumerate() {
            let base = word_idx as u32 * 64;
            let cap = self.tasks.len() as u32;
            if base >= cap {
                break;
            }
            let max = (cap - base).min(64);
            let occupied = !word & ((1u64 << max) - 1);
            for slot in 0..max {
                if occupied & (1 << slot) != 0 {
                    let idx = base + slot;
                    let task = unsafe {
                        self.tasks[idx as usize]
                            .assume_init_ref()
                            .io
                            .has_io_in_flight()
                    };
                    if task {
                        return true;
                    }
                }
            }
        }
        false
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

    /// # Safety
    /// `index` must be an in-bounds, initialized slot.
    pub unsafe fn task_unchecked(&self, index: u32) -> &Task {
        unsafe { self.tasks[index as usize].assume_init_ref() }
    }

    /// # Safety
    /// `index` must be an in-bounds, initialized slot.
    pub unsafe fn task_mut_unchecked(&mut self, index: u32) -> &mut Task {
        unsafe { self.tasks[index as usize].assume_init_mut() }
    }

    /// # Safety
    /// `index` must be an in-bounds, initialized slot.
    pub unsafe fn future_mut_unchecked(&mut self, index: u32) -> &mut F {
        unsafe { self.futures[index as usize].assume_init_mut() }
    }

    /// # Safety
    /// `index` must be an in-bounds slot that has not already been initialized.
    pub unsafe fn init_task_unchecked(&mut self, index: u32, task: Task) {
        self.tasks[index as usize] = MaybeUninit::new(task);
    }

    /// # Safety
    /// `index` must be an in-bounds slot that has not already been initialized.
    pub unsafe fn init_future_unchecked(&mut self, index: u32, future: F) {
        self.futures[index as usize] = MaybeUninit::new(future);
    }

    pub fn task_ptr_unchecked(&mut self, index: u32) -> *mut Task {
        self.tasks[index as usize].as_mut_ptr()
    }

    pub fn future_ptr_unchecked(&mut self, index: u32) -> *mut F {
        self.futures[index as usize].as_mut_ptr()
    }

    /// # Safety
    /// `index` must be an in-bounds, initialized slot that has not already
    /// been removed.
    pub unsafe fn remove_unchecked(&mut self, index: u32) -> (Task, F) {
        let word = index as usize / 64;
        let bit = index as usize % 64;
        self.free[word] |= 1 << bit;
        unsafe {
            let task = self.tasks[index as usize].assume_init_read();
            let future = self.futures[index as usize].assume_init_read();
            (task, future)
        }
    }
}

#[cfg(test)]
mod tests {
    use core::future::Ready;

    use super::*;
    use crate::arena::Arena;
    use crate::runtime;
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
        assert_eq!(slab.insert_vacant(), Some(0));
        assert_eq!(slab.insert_vacant(), Some(1));
        assert_eq!(slab.insert_vacant(), Some(2));
    }

    #[test]
    fn insert_vacant_exhaustion() {
        let mut slab = TaskSlab::<Ready<()>>::new(3);
        assert_eq!(slab.insert_vacant(), Some(0));
        assert_eq!(slab.insert_vacant(), Some(1));
        assert_eq!(slab.insert_vacant(), Some(2));
        assert_eq!(slab.insert_vacant(), None);
    }

    #[test]
    fn init_and_retrieve_task() {
        let mut slab = TaskSlab::<Ready<()>>::new(10);
        unsafe {
            let idx = slab.insert_vacant().unwrap();
            let task = Task {
                ready: true,
                io: IoState::new(),
                arena: Arena::new(4096),
            };
            slab.init_task_unchecked(idx, task);

            let ptr = slab.task_ptr_unchecked(idx);
            assert!((*ptr).ready);
        }
    }

    #[test]
    fn is_occupied_initially_false() {
        let slab = TaskSlab::<Ready<()>>::new(10);
        assert!(!slab.is_occupied(0));
    }

    #[test]
    fn is_occupied_after_init() {
        let mut slab = TaskSlab::<Ready<()>>::new(10);
        unsafe {
            let idx = slab.insert_vacant().unwrap();
            let task = Task {
                ready: true,
                io: IoState::new(),
                arena: Arena::new(4096),
            };
            slab.init_task_unchecked(idx, task);
            slab.init_future_unchecked(idx, core::future::ready(()));
            assert!(slab.is_occupied(idx));
        }
    }

    #[test]
    fn remove_unchecked_returns_values_and_frees_slot() {
        let mut slab = TaskSlab::<Ready<()>>::new(10);
        unsafe {
            let idx = slab.insert_vacant().unwrap();
            let task = Task {
                ready: true,
                io: IoState::new(),
                arena: Arena::new(4096),
            };
            slab.init_task_unchecked(idx, task);
            slab.init_future_unchecked(idx, core::future::ready(()));

            let (t, _f) = slab.remove_unchecked(idx);
            assert!(t.ready);
            // future is Ready<()>, dropping it after taking is fine

            // Slot should now be free again
            assert!(!slab.is_occupied(idx));
            let next_idx = slab.insert_vacant().unwrap();
            assert_eq!(next_idx, idx); // reused
        }
    }

    // ── has_io_in_flight ────────────────────────────────────────────

    #[test]
    fn has_io_in_flight_empty_slab() {
        let slab = TaskSlab::<Ready<()>>::new(10);
        assert!(!slab.has_io_in_flight());
    }

    #[test]
    fn has_io_in_flight_occupied_no_io() {
        let mut slab = TaskSlab::<Ready<()>>::new(10);
        unsafe {
            let idx = slab.insert_vacant().unwrap();
            let task = Task {
                ready: true,
                io: IoState::new(),
                arena: Arena::new(4096),
            };
            slab.init_task_unchecked(idx, task);
            assert!(!slab.has_io_in_flight());
        }
    }

    #[test]
    fn has_io_in_flight_with_pending_io() {
        let mut slab = TaskSlab::<Ready<()>>::new(10);
        unsafe {
            let idx = slab.insert_vacant().unwrap();
            let mut io = IoState::new();
            io.set_submitted(0, true); // submitted but not ready → in flight
            let task = Task {
                ready: true,
                io,
                arena: Arena::new(4096),
            };
            slab.init_task_unchecked(idx, task);
            assert!(slab.has_io_in_flight());
        }
    }

    #[test]
    fn has_io_in_flight_completed_io() {
        let mut slab = TaskSlab::<Ready<()>>::new(10);
        unsafe {
            let idx = slab.insert_vacant().unwrap();
            let mut io = IoState::new();
            io.set_submitted(0, true);
            io.set_ready(0, true); // completed → not in flight
            let task = Task {
                ready: true,
                io,
                arena: Arena::new(4096),
            };
            slab.init_task_unchecked(idx, task);
            assert!(!slab.has_io_in_flight());
        }
    }

    #[test]
    fn has_io_in_flight_multi_task() {
        let mut slab = TaskSlab::<Ready<()>>::new(10);
        unsafe {
            let idx0 = slab.insert_vacant().unwrap();
            let task0 = Task {
                ready: true,
                io: IoState::new(),
                arena: Arena::new(4096),
            };
            slab.init_task_unchecked(idx0, task0);

            let idx1 = slab.insert_vacant().unwrap();
            let mut io1 = IoState::new();
            io1.set_submitted(5, true);
            let task1 = Task {
                ready: true,
                io: io1,
                arena: Arena::new(4096),
            };
            slab.init_task_unchecked(idx1, task1);

            // Task 1 has in-flight IO → overall true
            assert!(slab.has_io_in_flight());
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
        let ctx = TaskContext::new(1, 5, &mut task, &mut wakeups);

        let ready = {
            let _g = runtime::enter_active_gen(1);
            ctx.with_task(|t| t.ready)
        };
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
        let ctx = TaskContext::new(1, 5, &mut task, &mut wakeups);

        let _g = runtime::enter_active_gen(1);
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
        let ctx = TaskContext::new(1, 3, &mut task, &mut wakeups);

        let _g = runtime::enter_active_gen(1);
        ctx.wake();
        assert_eq!(wakeups, vec![3]);

        ctx.wake();
        assert_eq!(wakeups, vec![3, 3]);
    }
}
