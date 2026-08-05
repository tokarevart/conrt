//! Fixed-buffer write support (`IORING_REGISTER_BUFFERS`).
//!
//! The runtime registers its whole shared buffer slab with the `io_uring` as
//! fixed buffer index 0. A [`WriteBufferPool`] describes one level of that
//! slab: a free stack of local slot ids for the level's equal-sized slots. A
//! [`WriteBuffer`] owns a slot and releases it back to the pool on drop;
//! writes submitted with `IORING_OP_WRITE_FIXED` reference a slot's address
//! directly.

#![allow(dead_code)]

use core::num::NonZeroU32;
use std::ptr::NonNull;

use crate::levels::bid_level;
use crate::levels::bid_local;
use crate::runtime::active_gen_matches;
use crate::runtime::with_runtime;

pub struct WriteBufferPool {
    size: u32,
    /// Byte offset of this level's first slot within the shared slab.
    base_offset: u32,
    /// Start of the shared slab this level's slots live in.
    slab_base: NonNull<u8>,
    free: Vec<u32>,
}

impl WriteBufferPool {
    /// Creates a free stack of `count` slots of `size` bytes each, backed by
    /// the slot range `[base_offset, base_offset + count*size)` of the
    /// caller-owned slab at `slab_base`. The slab is registered with the ring
    /// once, by the runtime.
    pub(crate) fn new(slab_base: NonNull<u8>, base_offset: usize, size: u32, count: u32) -> Self {
        assert!(size > 0);
        assert!(count > 0);

        // Seed the free stack so the first acquire returns slot 0.
        let mut free = Vec::with_capacity(count as usize);
        free.extend((0..count).rev());

        Self {
            size,
            base_offset: base_offset as u32,
            slab_base,
            free,
        }
    }

    pub fn slot_size(&self) -> u32 {
        self.size
    }

    /// The byte offset of slot `local` within the shared slab.
    pub(crate) fn slot_offset(&self, local: u32) -> u32 {
        self.base_offset + local * self.size
    }

    pub fn acquire(&mut self) -> Option<u32> {
        self.free.pop()
    }

    pub fn release(&mut self, local: u32) {
        self.free.push(local);
    }

    pub fn get_slice_mut(&mut self, local: u32) -> &mut [u8] {
        let start = self.slot_offset(local) as usize;
        unsafe {
            core::slice::from_raw_parts_mut(self.slab_base.as_ptr().add(start), self.size as usize)
        }
    }

    /// Returns a raw pointer to the start of slot `local`'s slab memory.
    pub fn slot_ptr(&self, local: u32) -> NonNull<u8> {
        unsafe {
            NonNull::new_unchecked(
                self.slab_base
                    .as_ptr()
                    .add(self.slot_offset(local) as usize),
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
/// Created by [`crate::io::write_buffer`]; the caller fills the slot
/// through [`WriteBuffer::as_mut`], records the filled length with
/// [`WriteBuffer::set_len`], then hands the buffer to
/// [`crate::io::write`], which submits it without copying. The slot is
/// recycled when the buffer is dropped. Like [`crate::ReadBuffer`], the slab
/// is reached through the thread-local runtime pointer, guarded by the
/// generation the buffer was created in: a `WriteBuffer` dropped after its
/// runtime has shut down skips recycling instead of touching freed memory.
pub struct WriteBuffer {
    offset: u32,
    bid: u32,
    len: u32,
    generation: NonZeroU32,
}

impl WriteBuffer {
    pub(crate) fn new(offset: u32, bid: u32, generation: NonZeroU32) -> Self {
        Self {
            offset,
            bid,
            len: 0,
            generation,
        }
    }

    pub(crate) fn offset(&self) -> u32 {
        self.offset
    }

    /// Returns the slot size; the maximum number of bytes that can be written.
    pub fn capacity(&self) -> u32 {
        assert!(
            active_gen_matches(self.generation),
            "capacity() called outside the runtime that owns this buffer"
        );
        with_runtime(|r| {
            let level = bid_level(self.bid) as usize;
            r.write_pools[level].slot_size()
        })
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
        assert!(
            active_gen_matches(self.generation),
            "as_mut() called outside the runtime that owns this buffer"
        );
        with_runtime(|r| {
            let level = bid_level(self.bid) as usize;
            let capacity = r.write_pools[level].slot_size() as usize;
            let ptr = unsafe { r.slab.as_ptr().cast_mut().add(self.offset as usize) };
            unsafe { core::slice::from_raw_parts_mut(ptr, capacity) }
        })
    }

    /// Returns the filled region `[0, len)`.
    #[allow(clippy::should_implement_trait)]
    pub fn as_ref(&self) -> &[u8] {
        assert!(
            active_gen_matches(self.generation),
            "as_ref() called outside the runtime that owns this buffer"
        );
        with_runtime(|r| unsafe {
            core::slice::from_raw_parts(
                r.slab.as_ptr().add(self.offset as usize),
                self.len as usize,
            )
        })
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
            let level = bid_level(self.bid) as usize;
            let local = bid_local(self.bid);
            with_runtime(|r| r.write_pools[level].release(local));
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
    fn write_buffer_is_16_bytes_with_niche() {
        assert_eq!(size_of::<WriteBuffer>(), 16);
        assert_eq!(size_of::<Option<WriteBuffer>>(), 16);
    }

    fn make_pool(count: u32, size: u32) -> (WriteBufferPool, Vec<u8>) {
        let mut slab = vec![0u8; count as usize * size as usize];
        let base = unsafe { NonNull::new_unchecked(slab.as_mut_ptr()) };
        (WriteBufferPool::new(base, 0, size, count), slab)
    }

    #[test]
    fn acquire_release_roundtrip() {
        let (mut pool, _slab) = make_pool(4, 16);
        let a = pool.acquire().unwrap();
        let b = pool.acquire().unwrap();
        assert_ne!(a, b);
        pool.release(a);
        assert_eq!(pool.acquire().unwrap(), a);
    }

    #[test]
    fn acquire_exhaustion() {
        let (mut pool, _slab) = make_pool(4, 16);
        for _ in 0..4 {
            assert!(pool.acquire().is_some());
        }
        assert_eq!(pool.acquire(), None);
        pool.release(2);
        assert_eq!(pool.acquire(), Some(2));
    }

    #[test]
    fn get_slice_mut_writes_within_slot() {
        let (mut pool, _slab) = make_pool(2, 8);
        let slot = pool.acquire().unwrap();
        pool.get_slice_mut(slot)[..5].copy_from_slice(b"hello");
        let back = pool.get_slice_mut(slot);
        assert_eq!(&back[..5], b"hello");
        assert!(back.len() >= 8);
    }

    #[test]
    fn slots_are_isolated() {
        let (mut pool, _slab) = make_pool(2, 8);
        let a = pool.acquire().unwrap();
        let b = pool.acquire().unwrap();
        pool.get_slice_mut(a)[0] = 1;
        pool.get_slice_mut(b)[0] = 2;
        assert_eq!(pool.get_slice_mut(a)[0], 1);
        assert_eq!(pool.get_slice_mut(b)[0], 2);
    }
}
