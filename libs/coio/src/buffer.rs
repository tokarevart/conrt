use crate::task::Task;

pub trait IoWriteBuffer: Sized {
    fn prepare_write(self, task: &mut Task) -> Option<u32>;
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
    task.io.reset_slot(slot);
    Some(value)
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

    // ── complete_write ────────────────────────────────────────────────

    #[test]
    fn complete_write_returns_data_and_cleans_state() {
        let mut task = Task {
            ready: false,
            io: IoState::new(),
            arena: Arena::new(4096),
            id: 0,
        };
        // Simulate a completed write: write a u64 to arena via an IO slot
        let slot = task.io.free_slot().unwrap();
        task.io.set_submitted(slot, true);
        let alloc = task.arena.alloc_write(42u64).unwrap();
        task.io.set_alloc(slot, alloc);
        task.io.set_result(slot, 4); // 4 bytes written
        task.io.set_ready(slot, true);

        let result: u64 = unsafe { complete_write(&mut task, slot).unwrap() };
        assert_eq!(result, 42);
        assert!(!task.io.is_submitted(slot));
        assert!(!task.io.is_ready(slot));
    }

    // ── full pipeline: prepare → complete (with Vec<u8>) ──

    #[test]
    fn vec_prepare_and_complete_write_pipeline() {
        let mut task = Task {
            ready: false,
            io: IoState::new(),
            arena: Arena::new(4096),
            id: 0,
        };
        let data = vec![1u8, 2, 3, 4];
        let slot = data.prepare_write(&mut task).expect("prepare_write");
        // Simulate IO completion by setting ready + result
        task.io.set_ready(slot, true);
        task.io.set_result(slot, 4);

        let result: Vec<u8> = unsafe { complete_write(&mut task, slot).unwrap() };
        assert_eq!(result, vec![1, 2, 3, 4]);
    }
}
