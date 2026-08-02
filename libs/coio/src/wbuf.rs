//! Fixed-buffer write slab (`IORING_REGISTER_BUFFERS`) support.
//!
//! A [`WriteBufferPool`] owns a single contiguous slab split into fixed-size
//! slots. The caller must [`WriteBufferPool::register`] it with an `io_uring`
//! instance before use and [`WriteBufferPool::unregister`] it before the ring
//! is closed. Writes submitted with `IORING_OP_WRITE_FIXED` reference a slot
//! directly; a [`WriteBuffer`] owns a slot and releases it back to the pool on
//! drop.

#![allow(dead_code)]

use std::io;
use std::ptr::NonNull;

use io_uring::IoUring;

use crate::runtime::active_gen_matches;
use crate::runtime::with_runtime;

pub struct WriteBufferPool {
    slot_count: u16,
    slot_size: u32,
    slab: Vec<u8>,
    free: Vec<u16>,
}

impl WriteBufferPool {
    /// Allocates a slab of `slot_count` slots of `slot_size` bytes each,
    /// without registering it with any `io_uring`. Call
    /// [`WriteBufferPool::register`] to publish the slab to a ring.
    pub fn new(slot_count: u16, slot_size: u32) -> Self {
        assert!(slot_count > 0);
        assert!(slot_size > 0);

        let slab = vec![0u8; slot_count as usize * slot_size as usize];
        // Seed the free stack so the first acquire returns slot 0.
        let mut free = Vec::with_capacity(slot_count as usize);
        free.extend((0..slot_count).rev());

        Self {
            slot_count,
            slot_size,
            slab,
            free,
        }
    }

    /// Registers the whole slab with `ring` as fixed buffer index 0. Must be
    /// called before `WriteFixed` submissions can reference the slab.
    pub fn register(&self, ring: &IoUring) -> io::Result<()> {
        let iovec = libc::iovec {
            iov_base: self.slab.as_ptr().cast_mut().cast(),
            iov_len: self.slab.len(),
        };
        unsafe { ring.submitter().register_buffers(&[iovec]) }
    }

    /// Unregisters the slab from `ring`. Must be called before the pool is
    /// dropped and the ring is closed.
    pub fn unregister(&self, ring: &IoUring) -> io::Result<()> {
        ring.submitter().unregister_buffers()
    }

    pub fn slot_count(&self) -> u16 {
        self.slot_count
    }

    pub fn slot_size(&self) -> u32 {
        self.slot_size
    }

    pub fn acquire(&mut self) -> Option<u16> {
        self.free.pop()
    }

    pub fn release(&mut self, id: u16) {
        self.free.push(id);
    }

    pub fn get_slice_mut(&mut self, id: u16) -> &mut [u8] {
        let start = id as usize * self.slot_size as usize;
        &mut self.slab[start..start + self.slot_size as usize]
    }

    /// Returns a raw pointer to the start of slot `id`'s slab memory.
    pub fn slot_ptr(&self, id: u16) -> NonNull<u8> {
        unsafe {
            NonNull::new_unchecked(
                self.slab
                    .as_ptr()
                    .add(id as usize * self.slot_size as usize) as *mut u8,
            )
        }
    }

    #[cfg(test)]
    pub(crate) fn free_count(&self) -> usize {
        self.free.len()
    }
}

/// A zero-copy buffer that borrows a slot from the runtime's fixed write
/// buffer slab.
///
/// Created by [`crate::runtime::write_buffer`]; the caller fills the slot
/// through [`WriteBuffer::as_mut`], records the filled length with
/// [`WriteBuffer::set_len`], then hands the buffer to
/// [`crate::runtime::write`], which submits it without copying. The slot is
/// recycled when the buffer is dropped. Like [`crate::ReadBuffer`], the pool
/// is reached through the thread-local runtime pointer, guarded by the
/// generation the buffer was created in: a `WriteBuffer` dropped after its
/// runtime has shut down skips recycling instead of touching freed memory.
pub struct WriteBuffer {
    ptr: NonNull<u8>,
    len: u32,
    bid: u16,
    generation: u64,
}

impl WriteBuffer {
    pub(crate) fn new(ptr: NonNull<u8>, bid: u16, generation: u64) -> Self {
        Self {
            ptr,
            len: 0,
            bid,
            generation,
        }
    }

    pub(crate) fn ptr(&self) -> NonNull<u8> {
        self.ptr
    }

    /// Returns the slot size; the maximum number of bytes that can be written.
    pub fn capacity(&self) -> u32 {
        assert!(
            active_gen_matches(self.generation),
            "capacity() called outside the runtime that owns this buffer"
        );
        with_runtime(|r| r.write_pool.slot_size())
    }

    /// Returns the number of bytes marked as filled via
    /// [`WriteBuffer::set_len`].
    pub fn len(&self) -> usize {
        self.len as _
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the whole writable slot, so the caller can use its full
    /// capacity. Call [`WriteBuffer::set_len`] with the number of bytes
    /// actually written before submitting.
    #[allow(clippy::should_implement_trait)]
    pub fn as_mut(&mut self) -> &mut [u8] {
        unsafe { core::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.capacity() as _) }
    }

    /// Returns the filled region `[0, len)`.
    #[allow(clippy::should_implement_trait)]
    pub fn as_ref(&self) -> &[u8] {
        unsafe { core::slice::from_raw_parts(self.ptr.as_ptr(), self.len as _) }
    }

    /// Sets the number of filled bytes. Panics if `new_len` exceeds
    /// [`WriteBuffer::capacity`].
    pub fn set_len(&mut self, new_len: u32) {
        let capacity = self.capacity();
        assert!(
            new_len <= capacity,
            "set_len({new_len}) exceeds capacity {capacity}"
        );
        self.len = new_len;
    }

    /// Resets the filled length to zero, keeping the slot acquired.
    pub fn clear(&mut self) {
        self.len = 0;
    }

    fn recycle(&mut self) {
        if active_gen_matches(self.generation) {
            with_runtime(|r| r.write_pool.release(self.bid));
        }
    }
}

impl Drop for WriteBuffer {
    fn drop(&mut self) {
        self.recycle();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acquire_release_roundtrip() {
        let mut pool = WriteBufferPool::new(4, 16);
        let a = pool.acquire().unwrap();
        let b = pool.acquire().unwrap();
        assert_ne!(a, b);
        pool.release(a);
        assert_eq!(pool.acquire().unwrap(), a);
    }

    #[test]
    fn acquire_exhaustion() {
        let mut pool = WriteBufferPool::new(4, 16);
        for _ in 0..4 {
            assert!(pool.acquire().is_some());
        }
        assert_eq!(pool.acquire(), None);
        pool.release(2);
        assert_eq!(pool.acquire(), Some(2));
    }

    #[test]
    fn get_slice_mut_writes_within_slot() {
        let mut pool = WriteBufferPool::new(2, 8);
        let slot = pool.acquire().unwrap();
        pool.get_slice_mut(slot)[..5].copy_from_slice(b"hello");
        let back = pool.get_slice_mut(slot);
        assert_eq!(&back[..5], b"hello");
        assert!(back.len() >= 8);
    }

    #[test]
    fn slots_are_isolated() {
        let mut pool = WriteBufferPool::new(2, 8);
        let a = pool.acquire().unwrap();
        let b = pool.acquire().unwrap();
        pool.get_slice_mut(a)[0] = 1;
        pool.get_slice_mut(b)[0] = 2;
        assert_eq!(pool.get_slice_mut(a)[0], 1);
        assert_eq!(pool.get_slice_mut(b)[0], 2);
    }
}
