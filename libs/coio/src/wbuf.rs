//! Fixed-buffer write slab (`IORING_REGISTER_BUFFERS`) support.
//!
//! A [`WriteBufferPool`] owns a single contiguous slab split into fixed-size
//! slots. The caller must [`WriteBufferPool::register`] it with an `io_uring`
//! instance before use and [`WriteBufferPool::unregister`] it before the ring
//! is closed. Writes submitted with `IORING_OP_WRITE_FIXED` reference a slot
//! directly; the slot id is stored in the task's io-slot `bids` field and
//! released back to the pool on completion.

#![allow(dead_code)]

use std::io;

use io_uring::IoUring;

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
