//! Provided buffer ring (`IORING_REGISTER_PBUF_RING`) support.
//!
//! A [`ProvidedBufferPool`] describes one size class of buffers: a descriptor
//! ring for `count` fixed-size slots that live inside the runtime's shared
//! buffer slab. The pool does not own the slab. The caller must
//! [`ProvidedBufferPool::register`] it with an `io_uring` instance before use
//! and [`ProvidedBufferPool::unregister`] it before the ring is closed. Reads
//! submitted with `IOSQE_BUFFER_SELECT` let the kernel pick a buffer from the
//! pool and report which one via `cqueue::buffer_select`; the caller must then
//! recycle the buffer back into the pool.
//!
//! Borrow tracking: the kernel consumes a buffer when it selects one, and the
//! pool hands it out as a view (the `Bytes` returned by
//! [`crate::io::read`]). The shared [`BorrowTracker`] records the hand-off
//! (`0 → +1`); the count is positive while shared views hold the buffer and
//! negative while exclusive views do. Each drop of a view releases one
//! borrower, and the buffer returns to the ring when the count hits zero.

use std::alloc::Layout;
use std::alloc::alloc_zeroed;
use std::alloc::dealloc;
use std::io;
use std::ptr::NonNull;
use std::sync::atomic::AtomicU16;
use std::sync::atomic::Ordering;

use io_uring::IoUring;

use crate::buf::Bytes;
use crate::buf::BytesMut;
use crate::buf::Ref;
use crate::buf::RefMut;
use crate::buf::Slice;
use crate::buf::SliceMut;
use crate::classes::pack_bid;
use crate::pool::BorrowTracker;
use crate::runtime::active_gen;

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

pub struct ProvidedBufferPool {
    buf_count: u16,
    size: u32,
    /// The start of this class's slot 0 within the shared slab, aligned to
    /// `min(size, BUFFER_MAX_ALIGN)`; slot `local` lives `local * size` bytes
    /// in.
    slab_base: NonNull<u8>,
    class: u8,
    ring_ptr: NonNull<u8>,
    ring_size: usize,
    tail: u16,
    /// Per-slot borrow counts shared with the fixed pools.
    pub(crate) tracker: BorrowTracker,
}

impl ProvidedBufferPool {
    /// Creates a provided-buffer ring for `count` buffers of `size` bytes,
    /// backed by the `count * size` bytes of the caller-owned slab starting at
    /// `slab_base` (which must point at the class's slot 0 and be aligned to
    /// `min(size, BUFFER_MAX_ALIGN)`). Does not register the ring with any
    /// `io_uring`; call [`ProvidedBufferPool::register`] to publish the
    /// buffers under `bgid`. `count` must be a power of two.
    pub fn new(slab_base: NonNull<u8>, count: u16, size: u32, class: u8) -> Self {
        assert!(count.is_power_of_two());
        assert!(count > 0 && count <= 32768);
        assert!(size > 0);
        assert!(class < 128);

        let ring_size = page_align(count as usize * size_of::<IoUringBuf>());
        let ring_layout = Layout::from_size_align(ring_size, PAGE_SIZE).unwrap();
        let ring_raw = unsafe { alloc_zeroed(ring_layout) as *mut IoUringBufRingHeader };
        if ring_raw.is_null() {
            std::alloc::handle_alloc_error(ring_layout);
        }

        let bufs_ptr = ring_raw as *mut IoUringBuf;
        let bufs = unsafe { std::slice::from_raw_parts_mut(bufs_ptr, count as usize) };
        for (local, slot) in bufs.iter_mut().enumerate() {
            let addr = unsafe { slab_base.as_ptr().add(local * size as usize) as u64 };
            *slot = IoUringBuf {
                addr,
                len: size,
                bid: local as u16,
                resv: 0,
            };
        }

        // Publish the descriptors before advertising the tail to the kernel.
        unsafe { (*ring_raw).tail.store(count, Ordering::Release) };

        Self {
            buf_count: count,
            size,
            slab_base,
            class,
            ring_ptr: unsafe { NonNull::new_unchecked(ring_raw as *mut u8) },
            ring_size,
            tail: count,
            tracker: BorrowTracker::new(count as usize),
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
                self.class as _,
                0,
            )
        }
    }

    /// Unregisters the pool from `ring`. Must be called before the pool is
    /// dropped and the ring is closed.
    pub fn unregister(&self, ring: &IoUring) -> io::Result<()> {
        ring.submitter().unregister_buf_ring(self.class as _)
    }

    #[cfg(test)]
    pub fn bgid(&self) -> u8 {
        self.class
    }

    /// The slot size of this class, in bytes.
    pub(crate) fn slot_size(&self) -> u32 {
        self.size
    }

    /// Recycles buffer `local` back to the kernel so it can be selected again.
    /// Safe to call out of order, as results are consumed.
    pub fn recycle_buffer(&mut self, local: u16) {
        let mask = self.buf_count - 1;
        let ring_idx = (self.tail & mask) as usize;
        let buf_addr = self.slot_ptr(local).as_ptr() as u64;

        // `resv` is left untouched: for ring index 0 it overlaps the `tail`
        // the kernel reads, so writing it would transiently clobber the tail.
        unsafe {
            let bufs_ptr = self.ring_ptr.as_ptr() as *mut IoUringBuf;
            let slot = bufs_ptr.add(ring_idx);
            (*slot).addr = buf_addr;
            (*slot).len = self.size;
            (*slot).bid = local;
        }

        self.tail = self.tail.wrapping_add(1);
        let new_tail = self.tail;
        unsafe {
            let header = &mut *(self.ring_ptr.as_ptr() as *mut IoUringBufRingHeader);
            header.tail.store(new_tail, Ordering::Release);
        }
    }

    /// Hands the kernel-selected buffer `local` out as a shared [`Bytes`]
    /// view covering `len` bytes, capturing the live generation. `local` is
    /// the buffer id the kernel reported on a `BUFFER_SELECT` read result and
    /// `len` is the op's byte count. The view borrows the slot: it stays out
    /// of the ring until the last view drops.
    ///
    /// # Panics
    ///
    /// - If `len` exceeds the slot size.
    /// - On a double selection: `local` is already out (the kernel cannot hand
    ///   a buffer out twice).
    /// - Outside an active runtime.
    pub fn select(&mut self, local: u16, len: u32) -> Bytes {
        let generation = active_gen().expect("select outside an active runtime");
        assert!(len <= self.size, "select: len exceeds the slot size");
        self.tracker.take_shared(local as _);
        // SAFETY: take_shared registered a shared borrow on the slot, the
        // generation is the live one, and `len` is asserted within the slot
        // size above.
        unsafe {
            Slice::new(
                Ref::new(pack_bid(true, self.class, local as _), generation, 0),
                len,
            )
        }
    }

    /// Hands the kernel-selected buffer `local` out as an exclusive
    /// [`BytesMut`] view covering `len` bytes, capturing the live generation.
    /// `local` is the buffer id the kernel reported on a `BUFFER_SELECT` read
    /// result and `len` is the op's byte count. The view borrows the slot
    /// exclusively: it stays out of the ring until the view drops.
    ///
    /// # Panics
    ///
    /// - If `len` exceeds the slot size.
    /// - On a double selection: `local` is already out (the kernel cannot hand
    ///   a buffer out twice).
    /// - Outside an active runtime.
    ///
    /// No caller yet: the exclusive variant for future receive paths that
    /// write in place.
    #[allow(dead_code)]
    pub fn select_mut(&mut self, local: u16, len: u32) -> BytesMut {
        let generation = active_gen().expect("select_mut outside an active runtime");
        assert!(len <= self.size, "select_mut: len exceeds the slot size");
        self.tracker.take_shared(local as _);
        self.tracker.upgrade(local as _);
        // SAFETY: take_shared and upgrade above left the slot exclusively
        // borrowed, the generation is the live one, and `len` is asserted
        // within the slot size above.
        unsafe {
            SliceMut::new(
                RefMut::new(pack_bid(true, self.class, local as _), generation, 0),
                len,
            )
        }
    }

    /// Releases one borrower of `local`, recycling the buffer back to the
    /// ring when the last view drops. `exclusive` selects the exclusive
    /// (`RefMut`/`SliceMut`) vs. shared (`Ref`/`Slice`) release. Panics on a
    /// count mismatch (an over-release, a double-drop, or releasing the wrong
    /// kind of borrow).
    pub fn drop_view(&mut self, exclusive: bool, local: u16) {
        if self.tracker.drop_view(exclusive, local as _) {
            self.recycle_buffer(local);
        }
    }

    /// Returns the bytes of buffer `local`. `len` must not exceed `size`.
    #[cfg(test)]
    pub fn get_slice(&self, local: u16, len: usize) -> &[u8] {
        unsafe { core::slice::from_raw_parts(self.slot_ptr(local).as_ptr(), len) }
    }

    /// Returns a raw pointer to the start of slot `local`'s slab memory.
    pub fn slot_ptr(&self, local: u16) -> NonNull<u8> {
        unsafe {
            NonNull::new_unchecked(
                self.slab_base
                    .as_ptr()
                    .add(local as usize * self.size as usize),
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
        const BGID: u8 = 0;

        let fd = tmpfile();
        let written = unsafe { libc::write(fd, b"hello world".as_ptr().cast(), 11) };
        assert_eq!(written, 11);
        assert_eq!(unsafe { libc::lseek(fd, 0, libc::SEEK_SET) }, 0);

        let mut ring = io_uring::IoUring::new(8).unwrap();
        let mut slab = vec![0u8; BUF_COUNT as usize * BUF_SIZE as usize];
        let base = unsafe { NonNull::new_unchecked(slab.as_mut_ptr()) };
        let mut pool = ProvidedBufferPool::new(base, BUF_COUNT, BUF_SIZE, BGID);
        pool.register(&ring).unwrap();

        let read_entry = || {
            io_uring::opcode::Read::new(io_uring::types::Fd(fd), std::ptr::null_mut(), BUF_SIZE)
                .buf_group(BGID as _)
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

    #[test]
    fn pbuf_borrow_tracks_selection_and_release() {
        let mut slab = vec![0u8; 4 * 16];
        let base = unsafe { NonNull::new_unchecked(slab.as_mut_ptr()) };
        let mut pool = ProvidedBufferPool::new(base, 4, 16, 0);

        assert_eq!(pool.tracker.borrows(1), 0);
        pool.tracker.take_shared(1);
        assert_eq!(pool.tracker.borrows(1), 1);
        assert_eq!(pool.ring_tail(), 4);
        // A cloned view holds a second borrower; the buffer stays out of the
        // ring until both drop.
        pool.tracker.clone_shared(1);
        assert_eq!(pool.tracker.borrows(1), 2);
        pool.drop_view(false, 1);
        assert_eq!(pool.tracker.borrows(1), 1);
        assert_eq!(pool.ring_tail(), 4);
        pool.drop_view(false, 1);
        assert_eq!(pool.tracker.borrows(1), 0);
        assert_eq!(pool.ring_tail(), 5);
    }

    #[test]
    fn pbuf_upgrade_downgrade_roundtrip() {
        let mut slab = vec![0u8; 4 * 16];
        let base = unsafe { NonNull::new_unchecked(slab.as_mut_ptr()) };
        let mut pool = ProvidedBufferPool::new(base, 4, 16, 0);

        pool.tracker.take_shared(1);
        pool.tracker.upgrade(1);
        assert_eq!(pool.tracker.borrows(1), -1);
        // The buffer stays out of the ring through the exclusive phase.
        assert_eq!(pool.ring_tail(), 4);
        pool.tracker.downgrade(1);
        assert_eq!(pool.tracker.borrows(1), 1);
        pool.drop_view(false, 1);
        assert_eq!(pool.tracker.borrows(1), 0);
        assert_eq!(pool.ring_tail(), 5);
    }

    #[test]
    fn pbuf_exclusive_split_and_release() {
        let mut slab = vec![0u8; 4 * 16];
        let base = unsafe { NonNull::new_unchecked(slab.as_mut_ptr()) };
        let mut pool = ProvidedBufferPool::new(base, 4, 16, 0);

        pool.tracker.take_shared(1);
        pool.tracker.upgrade(1);
        pool.tracker.split_exclusive(1);
        assert_eq!(pool.tracker.borrows(1), -2);
        // An exclusive release of one split half still leaves a holder.
        pool.drop_view(true, 1);
        assert_eq!(pool.tracker.borrows(1), -1);
        assert_eq!(pool.ring_tail(), 4);
        pool.drop_view(true, 1);
        assert_eq!(pool.tracker.borrows(1), 0);
        assert_eq!(pool.ring_tail(), 5);
    }

    #[test]
    fn pbuf_double_selection_panics() {
        let mut slab = vec![0u8; 4 * 16];
        let base = unsafe { NonNull::new_unchecked(slab.as_mut_ptr()) };
        let mut pool = ProvidedBufferPool::new(base, 4, 16, 0);
        pool.tracker.take_shared(0);
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                pool.tracker.take_shared(0);
            }))
            .is_err()
        );
    }

    #[test]
    fn pbuf_select_outside_runtime_panics() {
        let mut slab = vec![0u8; 4 * 16];
        let base = unsafe { NonNull::new_unchecked(slab.as_mut_ptr()) };
        let mut pool = ProvidedBufferPool::new(base, 4, 16, 0);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            pool.select(1, 5);
        }));
        assert!(result.is_err());
        // The panic fired before any borrow was registered.
        assert_eq!(pool.tracker.borrows(1), 0);
    }
}
