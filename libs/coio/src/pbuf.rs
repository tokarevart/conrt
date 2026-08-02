//! Provided buffer ring (`IORING_REGISTER_PBUF_RING`) support.
//!
//! A [`ProvidedBufferPool`] owns a slab of fixed-size data buffers and a
//! descriptor ring. The caller must [`ProvidedBufferPool::register`] it with an
//! `io_uring` instance before use and [`ProvidedBufferPool::unregister`] it
//! before the ring is closed. Reads submitted with `IOSQE_BUFFER_SELECT` let
//! the kernel pick a buffer from the pool and report which one via
//! `cqueue::buffer_select`; the caller must then recycle the buffer back into
//! the pool.

#![allow(dead_code)]

use core::ops::Deref;
use std::alloc::Layout;
use std::alloc::alloc_zeroed;
use std::alloc::dealloc;
use std::io;
use std::ptr::NonNull;
use std::sync::atomic::AtomicU16;
use std::sync::atomic::Ordering;

use io_uring::IoUring;

use crate::runtime::active_gen_matches;
use crate::runtime::with_runtime;

const PAGE_SIZE: usize = 4096;

fn page_align(n: usize) -> usize {
    (n + PAGE_SIZE - 1) & !(PAGE_SIZE - 1)
}

/// `struct io_uring_buf` descriptor, matching the kernel layout.
#[repr(C)]
#[derive(Clone, Copy)]
struct IoUringBuf {
    addr: u64,
    len: u32,
    bid: u16,
    resv: u16,
}

/// `struct io_uring_buf_ring`. `tail` is shared with the kernel and lives at
/// byte offset 14; the descriptor array is overlaid on the header via a union,
/// so `bufs[0]` starts at byte offset 0 and `bufs[0].resv` overlaps `tail`.
/// The tail is stored atomically with release ordering so the kernel observes
/// the descriptors before the tail that publishes them.
#[repr(C)]
struct IoUringBufRingHeader {
    resv1: u64,
    resv2: u32,
    resv3: u16,
    tail: AtomicU16,
}

const BGID: u16 = 0;

pub struct ProvidedBufferPool {
    buf_count: u16,
    buf_size: u32,
    slab: Vec<u8>,
    ring_ptr: NonNull<u8>,
    ring_size: usize,
    tail: u16,
}

impl ProvidedBufferPool {
    /// Allocates a slab of `buf_count` buffers of `buf_size` bytes each and
    /// fills the descriptor ring, without registering it with any `io_uring`.
    /// Call [`ProvidedBufferPool::register`] to publish the buffers to a ring.
    /// `buf_count` must be a power of two.
    pub fn new(buf_count: u16, buf_size: u32) -> Self {
        assert!(buf_count.is_power_of_two());
        assert!(buf_count > 0 && buf_count <= 32768);
        assert!(buf_size > 0);

        let slab = vec![0u8; buf_count as usize * buf_size as usize];

        let ring_size = page_align(buf_count as usize * size_of::<IoUringBuf>());
        let ring_layout = Layout::from_size_align(ring_size, PAGE_SIZE).unwrap();
        let ring_raw = unsafe { alloc_zeroed(ring_layout) as *mut IoUringBufRingHeader };
        if ring_raw.is_null() {
            std::alloc::handle_alloc_error(ring_layout);
        }

        let bufs_ptr = ring_raw as *mut IoUringBuf;
        let bufs = unsafe { std::slice::from_raw_parts_mut(bufs_ptr, buf_count as usize) };
        for (bid, slot) in bufs.iter_mut().enumerate() {
            let addr = unsafe { slab.as_ptr().add(bid * buf_size as usize) as u64 };
            *slot = IoUringBuf {
                addr,
                len: buf_size,
                bid: bid as u16,
                resv: 0,
            };
        }

        // Publish the descriptors before advertising the tail to the kernel.
        unsafe { (*ring_raw).tail.store(buf_count, Ordering::Release) };

        Self {
            buf_count,
            buf_size,
            slab,
            ring_ptr: unsafe { NonNull::new_unchecked(ring_raw as *mut u8) },
            ring_size,
            tail: buf_count,
        }
    }

    /// Registers the pool with `ring`, publishing all buffers to the kernel
    /// under the pool's fixed buffer group. Must be called before reads with
    /// `IOSQE_BUFFER_SELECT` can use the pool.
    pub fn register(&self, ring: &IoUring) -> io::Result<()> {
        unsafe {
            ring.submitter().register_buf_ring_with_flags(
                self.ring_ptr.as_ptr() as u64,
                self.buf_count,
                BGID,
                0,
            )
        }
    }

    /// Unregisters the pool from `ring`. Must be called before the pool is
    /// dropped and the ring is closed.
    pub fn unregister(&self, ring: &IoUring) -> io::Result<()> {
        ring.submitter().unregister_buf_ring(BGID)
    }

    pub fn bgid(&self) -> u16 {
        BGID
    }

    /// Recycles buffer `bid` back to the kernel so it can be selected again.
    /// Safe to call out of order, as results are consumed.
    pub fn recycle_buffer(&mut self, bid: u16) {
        let mask = self.buf_count - 1;
        let ring_idx = (self.tail & mask) as usize;
        let buf_addr = unsafe {
            self.slab
                .as_ptr()
                .add(bid as usize * self.buf_size as usize) as u64
        };

        // `resv` is left untouched: for ring index 0 it overlaps the `tail`
        // the kernel reads, so writing it would transiently clobber the tail.
        unsafe {
            let bufs_ptr = self.ring_ptr.as_ptr() as *mut IoUringBuf;
            let slot = bufs_ptr.add(ring_idx);
            (*slot).addr = buf_addr;
            (*slot).len = self.buf_size;
            (*slot).bid = bid;
        }

        self.tail = self.tail.wrapping_add(1);
        let new_tail = self.tail;
        unsafe {
            let header = &mut *(self.ring_ptr.as_ptr() as *mut IoUringBufRingHeader);
            header.tail.store(new_tail, Ordering::Release);
        }
    }

    /// Returns the bytes of buffer `bid`. `len` must not exceed `buf_size`.
    pub fn get_slice(&self, bid: u16, len: usize) -> &[u8] {
        let start = bid as usize * self.buf_size as usize;
        &self.slab[start..start + len]
    }

    /// Returns a raw pointer to the start of buffer `bid`'s slab memory.
    pub fn slot_ptr(&self, bid: u16) -> NonNull<u8> {
        unsafe {
            NonNull::new_unchecked(
                self.slab
                    .as_ptr()
                    .add(bid as usize * self.buf_size as usize) as *mut u8,
            )
        }
    }

    #[cfg(test)]
    pub(crate) fn ring_tail(&self) -> u16 {
        self.tail
    }
}

impl Drop for ProvidedBufferPool {
    fn drop(&mut self) {
        unsafe {
            dealloc(
                self.ring_ptr.as_ptr(),
                Layout::from_size_align(self.ring_size, PAGE_SIZE).unwrap(),
            )
        };
    }
}

/// A zero-copy view of a buffer selected from the runtime's provided buffer
/// pool by [`crate::runtime::read`].
///
/// The caller owns the buffer until this value is dropped, at which point the
/// pool slot is recycled. The pool is reached through the thread-local runtime
/// pointer, guarded by the generation the buffer was created in: a
/// `ReadBuffer` dropped after its runtime has shut down skips recycling
/// instead of touching freed memory. The data itself must not be read after
/// the runtime is gone.
pub struct ReadBuffer {
    ptr: NonNull<u8>,
    len: u32,
    bid: u16,
    generation: u64,
}

impl ReadBuffer {
    pub(crate) fn new(ptr: NonNull<u8>, len: u32, bid: u16, generation: u64) -> Self {
        Self {
            ptr,
            len,
            bid,
            generation,
        }
    }

    /// Copies the buffer into an owned `Vec` and recycles the pool slot.
    pub fn into_vec(mut self) -> Vec<u8> {
        let data =
            unsafe { core::slice::from_raw_parts(self.ptr.as_ptr(), self.len as usize) }.to_vec();
        self.recycle();
        core::mem::forget(self);
        data
    }

    fn recycle(&mut self) {
        if active_gen_matches(self.generation) {
            with_runtime(|r| r.buffer_pool.recycle_buffer(self.bid));
        }
    }
}

impl Drop for ReadBuffer {
    fn drop(&mut self) {
        self.recycle();
    }
}

impl Deref for ReadBuffer {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        unsafe { core::slice::from_raw_parts(self.ptr.as_ptr(), self.len as usize) }
    }
}

impl AsRef<[u8]> for ReadBuffer {
    fn as_ref(&self) -> &[u8] {
        self.deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmpfile() -> i32 {
        let path = b"/tmp/conrt-pbuf-test.dat\0";
        let fd = unsafe {
            libc::open(
                path.as_ptr().cast(),
                libc::O_CREAT | libc::O_TRUNC | libc::O_RDWR,
                0o600,
            )
        };
        assert!(fd >= 0, "open failed");
        fd
    }

    #[test]
    fn pbuf_ring_select_and_recycle() {
        const BUF_SIZE: u32 = 16;
        const BUF_COUNT: u16 = 4;

        let fd = tmpfile();
        let written = unsafe { libc::write(fd, b"hello world".as_ptr().cast(), 11) };
        assert_eq!(written, 11);
        assert_eq!(unsafe { libc::lseek(fd, 0, libc::SEEK_SET) }, 0);

        let mut ring = io_uring::IoUring::new(8).unwrap();
        let mut pool = ProvidedBufferPool::new(BUF_COUNT, BUF_SIZE);
        pool.register(&ring).unwrap();

        let read_entry = || {
            io_uring::opcode::Read::new(io_uring::types::Fd(fd), std::ptr::null_mut(), BUF_SIZE)
                .buf_group(BGID)
                .build()
                .flags(io_uring::squeue::Flags::BUFFER_SELECT)
        };

        // More reads than the ring holds, to exercise the tail wrap.
        for _ in 0..8 {
            unsafe { ring.submission().push(&read_entry()).unwrap() };
            ring.submit_and_wait(1).unwrap();

            let cqe = ring.completion().next().unwrap();
            assert_eq!(cqe.result(), 11);
            let bid = io_uring::cqueue::buffer_select(cqe.flags()).expect("no buffer selected");
            assert_eq!(pool.get_slice(bid, 11), b"hello world");
            pool.recycle_buffer(bid);
        }

        unsafe { libc::close(fd) };

        pool.unregister(&ring).unwrap();
    }
}
