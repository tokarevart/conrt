use crate::task::Task;

pub trait IoReadBuffer: Sized {
    fn prepare_read(self, task: &mut Task) -> Option<u32>;
}

pub trait IoWriteBuffer: Sized {
    fn prepare_write(self, task: &mut Task) -> Option<u32>;
}

/// # Safety
/// Slot must correspond to a completed IO operation (ready bit set).
/// Caller must guarantee the buffer's underlying memory is still valid.
pub unsafe fn complete_read<B: Sized>(task: &mut Task, slot: u32) -> Option<B> {
    let alloc = task.io.take_alloc(slot);
    let value = task.arena.read::<B>(&alloc);
    unsafe {
        task.arena.dealloc(alloc);
    }
    task.io.set_submitted(slot, false);
    task.io.set_ready(slot, false);
    Some(value)
}

/// # Safety
/// Slot must correspond to a completed IO operation (ready bit set).
/// Caller must guarantee the buffer's underlying memory is still valid.
pub unsafe fn complete_write<B: Sized>(task: &mut Task, slot: u32) -> Option<B> {
    let alloc = task.io.take_alloc(slot);
    let value = task.arena.read::<B>(&alloc);
    unsafe {
        task.arena.dealloc(alloc);
    }
    task.io.set_submitted(slot, false);
    task.io.set_ready(slot, false);
    Some(value)
}

impl IoReadBuffer for Vec<u8> {
    fn prepare_read(self, task: &mut Task) -> Option<u32> {
        let slot = task.io.free_slot()?;
        task.io.set_submitted(slot, true);
        let alloc = match task.arena.alloc_write(self) {
            Some(a) => a,
            None => {
                task.io.set_submitted(slot, false);
                return None;
            }
        };
        task.io.set_alloc(slot, alloc);
        Some(slot)
    }
}

impl IoWriteBuffer for Vec<u8> {
    fn prepare_write(self, task: &mut Task) -> Option<u32> {
        let slot = task.io.free_slot()?;
        task.io.set_submitted(slot, true);
        let alloc = match task.arena.alloc_write(self) {
            Some(a) => a,
            None => {
                task.io.set_submitted(slot, false);
                return None;
            }
        };
        task.io.set_alloc(slot, alloc);
        Some(slot)
    }
}

impl IoWriteBuffer for &[u8] {
    fn prepare_write(self, task: &mut Task) -> Option<u32> {
        let slot = task.io.free_slot()?;
        task.io.set_submitted(slot, true);
        let alloc = match task.arena.alloc_write(self) {
            Some(a) => a,
            None => {
                task.io.set_submitted(slot, false);
                return None;
            }
        };
        task.io.set_alloc(slot, alloc);
        Some(slot)
    }
}

impl IoReadBuffer for &mut [u8] {
    fn prepare_read(self, task: &mut Task) -> Option<u32> {
        let slot = task.io.free_slot()?;
        task.io.set_submitted(slot, true);
        let alloc = match task.arena.alloc_write(self) {
            Some(a) => a,
            None => {
                task.io.set_submitted(slot, false);
                return None;
            }
        };
        task.io.set_alloc(slot, alloc);
        Some(slot)
    }
}

impl IoWriteBuffer for &mut [u8] {
    fn prepare_write(self, task: &mut Task) -> Option<u32> {
        let slot = task.io.free_slot()?;
        task.io.set_submitted(slot, true);
        let alloc = match task.arena.alloc_write(self) {
            Some(a) => a,
            None => {
                task.io.set_submitted(slot, false);
                return None;
            }
        };
        task.io.set_alloc(slot, alloc);
        Some(slot)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arena::Arena;
    use crate::runtime::IoState;
    use crate::task::Task;

    // ── Vec<u8> prepare_read ──────────────────────────────────────────

    #[test]
    fn vec_prepare_read_allocates_slot() {
        let mut task = Task {
            ready: false,
            io: IoState::new(),
            arena: Arena::new(4096),
            id: 0,
        };
        let data = vec![1u8, 2, 3];
        let slot = data.prepare_read(&mut task).expect("prepare_read failed");
        assert!(task.io.is_submitted(slot));
        assert!(slot < 64);
    }

    #[test]
    fn vec_prepare_read_no_free_slot() {
        let mut task = Task {
            ready: false,
            io: IoState::new(),
            arena: Arena::new(4096),
            id: 0,
        };
        // fill all 64 slots
        for i in 0..64 {
            task.io.set_submitted(i, true);
        }
        let data = vec![1u8, 2, 3];
        assert!(data.prepare_read(&mut task).is_none());
    }

    // ── &[u8] prepare_write ───────────────────────────────────────────

    #[test]
    fn slice_ref_prepare_write() {
        let mut task = Task {
            ready: false,
            io: IoState::new(),
            arena: Arena::new(4096),
            id: 0,
        };
        let data: &[u8] = &[10, 20, 30];
        let slot = data
            .prepare_write(&mut task)
            .expect("&[u8] prepare_write failed");
        assert!(task.io.is_submitted(slot));
    }

    // ── &mut [u8] prepare_read ────────────────────────────────────────

    #[test]
    fn mut_slice_prepare_read() {
        let mut task = Task {
            ready: false,
            io: IoState::new(),
            arena: Arena::new(4096),
            id: 0,
        };
        let mut buf = [0u8; 8];
        let data: &mut [u8] = &mut buf;
        let slot = data
            .prepare_read(&mut task)
            .expect("&mut [u8] prepare_read failed");
        assert!(task.io.is_submitted(slot));
    }

    // ── complete_read ─────────────────────────────────────────────────

    #[test]
    fn complete_read_returns_data_and_cleans_state() {
        let mut task = Task {
            ready: false,
            io: IoState::new(),
            arena: Arena::new(4096),
            id: 0,
        };
        // Simulate a completed read: write a u64 to arena via an IO slot
        let slot = task.io.free_slot().unwrap();
        task.io.set_submitted(slot, true);
        let alloc = task.arena.alloc_write(42u64).unwrap();
        task.io.set_alloc(slot, alloc);
        task.io.set_result(slot, 4); // 4 bytes read
        task.io.set_ready(slot, true);

        let result: u64 = unsafe { complete_read(&mut task, slot).unwrap() };
        assert_eq!(result, 42);
        assert!(!task.io.is_submitted(slot));
        assert!(!task.io.is_ready(slot));
    }

    // ── complete_write ────────────────────────────────────────────────

    // ── full pipeline: prepare → complete (with Vec<u8>) ──

    #[test]
    fn vec_prepare_and_complete_read_pipeline() {
        let mut task = Task {
            ready: false,
            io: IoState::new(),
            arena: Arena::new(4096),
            id: 0,
        };
        let data = vec![1u8, 2, 3, 4];
        let slot = data.prepare_read(&mut task).expect("prepare_read");
        // Simulate IO completion by setting ready + result
        task.io.set_ready(slot, true);
        task.io.set_result(slot, 4);

        let result: Vec<u8> = unsafe { complete_read(&mut task, slot).unwrap() };
        assert_eq!(result, vec![1, 2, 3, 4]);
        // result will be dropped here, freeing the heap allocation.
        // The arena memory for the Vec's bytes is no longer needed.
    }
}
