use core::any::TypeId;
use core::cell::Cell;
use core::fmt;
use core::mem::MaybeUninit;
use core::ptr::NonNull;
use std::alloc::Layout;
use std::alloc::alloc;
use std::alloc::dealloc;
use std::io;

use io_uring::squeue;

use crate::arena::Arena;
use crate::runtime;
use crate::runtime::IoState;
use crate::runtime::IoUserData;
use crate::runtime::Runtime;

thread_local! {
    static NEXT_TASK_ID: Cell<u64> = const { Cell::new(1) };
}

pub struct Task {
    pub ready: bool,
    pub io: IoState,
    pub arena: Arena,
    pub(crate) id: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskContextError {
    InactiveRuntime,
    SlotOutOfBounds,
    TaskRemoved,
    TaskReused,
}

impl fmt::Display for TaskContextError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let msg = match self {
            Self::InactiveRuntime => "TaskContext used outside the runtime it belongs to",
            Self::SlotOutOfBounds => "TaskContext refers to an out-of-bounds slot",
            Self::TaskRemoved => "TaskContext refers to a removed task",
            Self::TaskReused => "TaskContext refers to a slot reused by a different task",
        };
        f.write_str(msg)
    }
}

impl std::error::Error for TaskContextError {}

#[derive(Debug, Clone, Copy)]
pub struct TaskContext {
    task_index: u32,
    task_id: u64,
}

impl TaskContext {
    pub(crate) fn new(task_index: u32, task_id: u64) -> Self {
        Self {
            task_index,
            task_id,
        }
    }

    pub fn validate(&self) -> Result<(), TaskContextError> {
        if !runtime::is_running() {
            return Err(TaskContextError::InactiveRuntime);
        }

        runtime::with_runtime(|rt| {
            let word = self.task_index as usize / 64;
            let bit = self.task_index as usize % 64;
            if word >= rt.tasks.free.len() {
                return Err(TaskContextError::SlotOutOfBounds);
            }
            if rt.tasks.free[word] & (1 << bit) != 0 {
                return Err(TaskContextError::TaskRemoved);
            }

            unsafe {
                if rt.tasks.task_unchecked(self.task_index).id != self.task_id {
                    return Err(TaskContextError::TaskReused);
                }
            }
            Ok(())
        })
    }

    pub fn with_task<R>(&self, f: impl FnOnce(&mut Task) -> R) -> R {
        if let Err(e) = self.validate() {
            panic!("invalid TaskContext: {e}");
        }
        runtime::with_runtime(|rt| unsafe { f(rt.tasks.task_mut_unchecked(self.task_index)) })
    }

    pub(crate) fn with_runtime<R>(&self, f: impl FnOnce(&mut Runtime) -> R) -> R {
        if let Err(e) = self.validate() {
            panic!("invalid TaskContext: {e}");
        }
        runtime::with_runtime(f)
    }

    pub fn wake(&self) {
        if let Err(e) = self.validate() {
            panic!("invalid TaskContext: {e}");
        }
        runtime::with_runtime(|rt| rt.wakeups.push(self.task_index));
    }

    pub fn push_io(&self, entry: squeue::Entry, io_slot: u32) -> io::Result<()> {
        runtime::with_runtime(|rt| {
            let mut sq = rt.ring.submission();
            let user_data = IoUserData {
                index: self.task_index,
                io_slot,
            };
            let entry = entry.user_data(user_data.into());
            unsafe { sq.push(&entry) }.map_err(|_| io::Error::from_raw_os_error(libc::EAGAIN))
        })
    }
}

pub struct TaskSlab {
    tasks: Box<[MaybeUninit<Task>]>,
    futures: NonNull<u8>,
    future_type_id: TypeId,
    free: Box<[u64]>,
}

impl TaskSlab {
    pub fn new<F: 'static>(capacity: u32) -> Self {
        let cap = capacity as usize;
        let mut tasks = Vec::with_capacity(cap);
        tasks.resize_with(cap, MaybeUninit::uninit);
        let words = cap.div_ceil(64);
        let free = vec![u64::MAX; words].into_boxed_slice();

        let layout = Layout::array::<F>(cap).unwrap();
        let futures = unsafe { alloc(layout) };
        let futures = NonNull::new(futures).expect("future allocation failed");

        Self {
            tasks: tasks.into_boxed_slice(),
            futures,
            future_type_id: TypeId::of::<F>(),
            free,
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

    /// Returns a pointer to the byte slot backing future `index`. The slot is
    /// only valid for the `F` the slab was created for.
    fn future_slot<F>(&self, index: u32) -> *mut MaybeUninit<u8> {
        let base = self.futures.as_ptr().cast::<F>();
        unsafe { base.add(index as usize) as _ }
    }

    /// # Safety
    /// `index` must be an in-bounds, initialized slot.
    pub unsafe fn future_mut_unchecked<F: 'static>(&mut self, index: u32) -> &mut F {
        assert_eq!(TypeId::of::<F>(), self.future_type_id);
        unsafe { &mut *(self.future_slot::<F>(index) as *mut F) }
    }

    /// # Safety
    /// `index` must be an in-bounds slot that has not already been initialized.
    pub unsafe fn init_task_unchecked(&mut self, index: u32, mut task: Task) {
        task.id = NEXT_TASK_ID.with(|c| {
            let id = c.get();
            c.set(id.wrapping_add(1));
            id
        });
        self.tasks[index as usize] = MaybeUninit::new(task);
    }

    /// # Safety
    /// `index` must be an in-bounds slot that has not already been initialized.
    pub unsafe fn init_future_unchecked<F: 'static>(&mut self, index: u32, future: F) {
        assert_eq!(TypeId::of::<F>(), self.future_type_id);
        unsafe { (self.future_slot::<F>(index) as *mut F).write(future) };
    }

    pub fn task_ptr_unchecked(&mut self, index: u32) -> *mut Task {
        self.tasks[index as usize].as_mut_ptr()
    }

    pub fn future_ptr_unchecked<F: 'static>(&mut self, index: u32) -> *mut F {
        assert_eq!(TypeId::of::<F>(), self.future_type_id);
        self.future_slot::<F>(index) as *mut F
    }

    /// # Safety
    /// `index` must be an in-bounds, initialized slot that has not already
    /// been removed.
    pub unsafe fn remove_unchecked<F: 'static>(&mut self, index: u32) -> Task {
        assert_eq!(TypeId::of::<F>(), self.future_type_id);
        let word = index as usize / 64;
        let bit = index as usize % 64;
        self.free[word] |= 1 << bit;
        unsafe {
            (self.future_slot::<F>(index) as *mut F).drop_in_place();
            self.tasks[index as usize].assume_init_read()
        }
    }

    /// # Safety
    /// `slab` must point to a live slab that is never used again. `F` must
    /// match the type the slab was created for.
    pub unsafe fn drop_futures_raw<F: 'static>(slab: *mut TaskSlab) {
        let this = unsafe { &*slab };
        assert_eq!(TypeId::of::<F>(), this.future_type_id);
        let layout = Layout::array::<F>(this.tasks.len()).unwrap();
        for index in 0..this.tasks.len() as u32 {
            if this.is_occupied(index) {
                unsafe { core::ptr::drop_in_place(this.future_slot::<F>(index).cast::<F>()) };
            }
        }
        if layout.size() > 0 {
            unsafe { dealloc(this.futures.as_ptr(), layout) };
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
        let slab = TaskSlab::new::<Ready<()>>(128);
        assert_eq!(slab.tasks.len(), 128);
        assert_eq!(slab.free.len(), 2); // 128 / 64 = 2 words
        assert_eq!(slab.free[0], u64::MAX);
        assert_eq!(slab.free[1], u64::MAX);
    }

    #[test]
    fn insert_vacant_sequential() {
        let mut slab = TaskSlab::new::<Ready<()>>(10);
        assert_eq!(slab.insert_vacant(), Some(0));
        assert_eq!(slab.insert_vacant(), Some(1));
        assert_eq!(slab.insert_vacant(), Some(2));
    }

    #[test]
    fn insert_vacant_exhaustion() {
        let mut slab = TaskSlab::new::<Ready<()>>(3);
        assert_eq!(slab.insert_vacant(), Some(0));
        assert_eq!(slab.insert_vacant(), Some(1));
        assert_eq!(slab.insert_vacant(), Some(2));
        assert_eq!(slab.insert_vacant(), None);
    }

    #[test]
    fn init_and_retrieve_task() {
        let mut slab = TaskSlab::new::<Ready<()>>(10);
        unsafe {
            let idx = slab.insert_vacant().unwrap();
            let task = Task {
                ready: true,
                io: IoState::new(),
                arena: Arena::new(4096),
                id: 0,
            };
            slab.init_task_unchecked(idx, task);

            let ptr = slab.task_ptr_unchecked(idx);
            assert!((*ptr).ready);
        }
    }

    #[test]
    fn is_occupied_initially_false() {
        let slab = TaskSlab::new::<Ready<()>>(10);
        assert!(!slab.is_occupied(0));
    }

    #[test]
    fn is_occupied_after_init() {
        let mut slab = TaskSlab::new::<Ready<()>>(10);
        unsafe {
            let idx = slab.insert_vacant().unwrap();
            let task = Task {
                ready: true,
                io: IoState::new(),
                arena: Arena::new(4096),
                id: 0,
            };
            slab.init_task_unchecked(idx, task);
            slab.init_future_unchecked(idx, core::future::ready(()));
            assert!(slab.is_occupied(idx));
        }
    }

    #[test]
    fn remove_unchecked_returns_values_and_frees_slot() {
        let mut slab = TaskSlab::new::<Ready<()>>(10);
        unsafe {
            let idx = slab.insert_vacant().unwrap();
            let task = Task {
                ready: true,
                io: IoState::new(),
                arena: Arena::new(4096),
                id: 0,
            };
            slab.init_task_unchecked(idx, task);
            slab.init_future_unchecked(idx, core::future::ready(()));

            let t = slab.remove_unchecked::<Ready<()>>(idx);
            assert!(t.ready);
            // future is Ready<()>, dropping it after taking is fine

            // Slot should now be free again
            assert!(!slab.is_occupied(idx));
            let next_idx = slab.insert_vacant().unwrap();
            assert_eq!(next_idx, idx); // reused
        }
    }

    #[test]
    fn future_type_mismatch_panics() {
        let mut slab = TaskSlab::new::<Ready<()>>(10);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = slab.future_ptr_unchecked::<Ready<u8>>(0);
        }));
        assert!(result.is_err());
    }

    // ── has_io_in_flight ────────────────────────────────────────────

    #[test]
    fn has_io_in_flight_empty_slab() {
        let slab = TaskSlab::new::<Ready<()>>(10);
        assert!(!slab.has_io_in_flight());
    }

    #[test]
    fn has_io_in_flight_occupied_no_io() {
        let mut slab = TaskSlab::new::<Ready<()>>(10);
        unsafe {
            let idx = slab.insert_vacant().unwrap();
            let task = Task {
                ready: true,
                io: IoState::new(),
                arena: Arena::new(4096),
                id: 0,
            };
            slab.init_task_unchecked(idx, task);
            assert!(!slab.has_io_in_flight());
        }
    }

    #[test]
    fn has_io_in_flight_with_pending_io() {
        let mut slab = TaskSlab::new::<Ready<()>>(10);
        unsafe {
            let idx = slab.insert_vacant().unwrap();
            let mut io = IoState::new();
            io.set_submitted(0, true); // submitted but not ready → in flight
            let task = Task {
                ready: true,
                io,
                arena: Arena::new(4096),
                id: 0,
            };
            slab.init_task_unchecked(idx, task);
            assert!(slab.has_io_in_flight());
        }
    }

    #[test]
    fn has_io_in_flight_completed_io() {
        let mut slab = TaskSlab::new::<Ready<()>>(10);
        unsafe {
            let idx = slab.insert_vacant().unwrap();
            let mut io = IoState::new();
            io.set_submitted(0, true);
            io.set_ready(0, true); // completed → not in flight
            let task = Task {
                ready: true,
                io,
                arena: Arena::new(4096),
                id: 0,
            };
            slab.init_task_unchecked(idx, task);
            assert!(!slab.has_io_in_flight());
        }
    }

    #[test]
    fn has_io_in_flight_multi_task() {
        let mut slab = TaskSlab::new::<Ready<()>>(10);
        unsafe {
            let idx0 = slab.insert_vacant().unwrap();
            let task0 = Task {
                ready: true,
                io: IoState::new(),
                arena: Arena::new(4096),
                id: 0,
            };
            slab.init_task_unchecked(idx0, task0);

            let idx1 = slab.insert_vacant().unwrap();
            let mut io1 = IoState::new();
            io1.set_submitted(5, true);
            let task1 = Task {
                ready: true,
                io: io1,
                arena: Arena::new(4096),
                id: 0,
            };
            slab.init_task_unchecked(idx1, task1);

            // Task 1 has in-flight IO → overall true
            assert!(slab.has_io_in_flight());
        }
    }
}
