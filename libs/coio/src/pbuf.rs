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

use std::alloc::Layout;
use std::alloc::alloc_zeroed;
use std::alloc::dealloc;
use std::io;
use std::ptr::NonNull;
use std::sync::atomic::Ordering;

use io_uring::IoUring;

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
/// so `bufs[0]` starts at byte offset 0.
#[repr(C)]
struct IoUringBufRingHeader {
    resv1: u64,
    resv2: u32,
    resv3: u16,
    tail: u16,
}

const BGID: u16 = 0;

pub struct ProvidedBufferPool {
    buf_count: u16,
    buf_size: u32,
    slab: Vec<u8>,
    ring_ptr: NonNull<u8>,
    ring_layout: Layout,
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

        let ring_layout = Layout::from_size_align(
            page_align(buf_count as usize * size_of::<IoUringBuf>()),
            PAGE_SIZE,
        )
        .unwrap();
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
        std::sync::atomic::fence(Ordering::Release);
        unsafe { (*ring_raw).tail = buf_count };

        Self {
            buf_count,
            buf_size,
            slab,
            ring_ptr: unsafe { NonNull::new_unchecked(ring_raw as *mut u8) },
            ring_layout,
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

    pub(crate) fn bgid(&self) -> u16 {
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

        unsafe {
            let bufs_ptr = self.ring_ptr.as_ptr() as *mut IoUringBuf;
            let slot = bufs_ptr.add(ring_idx);
            (*slot).addr = buf_addr;
            (*slot).len = self.buf_size;
            (*slot).bid = bid;
            (*slot).resv = 0;
        }

        // Ensure the descriptor is visible before the tail that publishes it.
        std::sync::atomic::fence(Ordering::Release);
        self.tail = self.tail.wrapping_add(1);
        let new_tail = self.tail;
        unsafe {
            let header = &mut *(self.ring_ptr.as_ptr() as *mut IoUringBufRingHeader);
            header.tail = new_tail;
        }
    }

    /// Returns the bytes of buffer `bid`. `len` must not exceed `buf_size`.
    pub fn get_slice(&self, bid: u16, len: usize) -> &[u8] {
        let start = bid as usize * self.buf_size as usize;
        &self.slab[start..start + len]
    }
}

impl Drop for ProvidedBufferPool {
    fn drop(&mut self) {
        unsafe { dealloc(self.ring_ptr.as_ptr(), self.ring_layout) };
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
            io_uring::opcode::Read::new(
                io_uring::types::Fd(fd),
                std::ptr::null_mut(),
                BUF_SIZE as u32,
            )
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
