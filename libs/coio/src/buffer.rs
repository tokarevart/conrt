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
        let alloc = task.arena.alloc_write(self)?;
        task.io.set_alloc(slot, alloc);
        Some(slot)
    }
}

impl IoWriteBuffer for Vec<u8> {
    fn prepare_write(self, task: &mut Task) -> Option<u32> {
        let slot = task.io.free_slot()?;
        task.io.set_submitted(slot, true);
        let alloc = task.arena.alloc_write(self)?;
        task.io.set_alloc(slot, alloc);
        Some(slot)
    }
}

impl IoWriteBuffer for &[u8] {
    fn prepare_write(self, task: &mut Task) -> Option<u32> {
        let slot = task.io.free_slot()?;
        task.io.set_submitted(slot, true);
        let alloc = task.arena.alloc_write(self)?;
        task.io.set_alloc(slot, alloc);
        Some(slot)
    }
}

impl IoReadBuffer for &mut [u8] {
    fn prepare_read(self, task: &mut Task) -> Option<u32> {
        let slot = task.io.free_slot()?;
        task.io.set_submitted(slot, true);
        let alloc = task.arena.alloc_write(self)?;
        task.io.set_alloc(slot, alloc);
        Some(slot)
    }
}

impl IoWriteBuffer for &mut [u8] {
    fn prepare_write(self, task: &mut Task) -> Option<u32> {
        let slot = task.io.free_slot()?;
        task.io.set_submitted(slot, true);
        let alloc = task.arena.alloc_write(self)?;
        task.io.set_alloc(slot, alloc);
        Some(slot)
    }
}
