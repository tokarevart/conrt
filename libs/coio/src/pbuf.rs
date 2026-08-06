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
//! [`crate::io::read`]). [`ProvidedBufferPool::mark_selected`] records the
//! hand-off (`0 → +1`); the count is positive while shared views hold the
//! buffer and negative while exclusive views do. Each drop of a view releases
//! one borrower, and the buffer returns to the ring when the count hits zero.

use std::alloc::Layout;
use std::alloc::alloc_zeroed;
use std::alloc::dealloc;
use std::io;
use std::ptr::NonNull;
use std::sync::atomic::AtomicU16;
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
    buf_size: u32,
    /// Byte offset of this class's first slot within the shared slab.
    base_offset: u32,
    /// Start of the shared slab this class's slots live in.
    slab_base: NonNull<u8>,
    bgid: u16,
    ring_ptr: NonNull<u8>,
    ring_size: usize,
    tail: u16,
    /// Per-slot borrow count: `0` means free (in the ring), positive values
    /// count shared views holding the buffer out of the ring, negative values
    /// count exclusive views.
    borrows: Vec<i32>,
}

impl ProvidedBufferPool {
    /// Creates a provided-buffer ring for `count` buffers of `size` bytes,
    /// backed by the slot range `[base_offset, base_offset + count*size)` of
    /// the caller-owned slab at `slab_base`. Does not register the ring with
    /// any `io_uring`; call [`ProvidedBufferPool::register`] to publish the
    /// buffers under `bgid`. `count` must be a power of two.
    pub fn new(
        slab_base: NonNull<u8>,
        base_offset: usize,
        count: u16,
        size: u32,
        bgid: u16,
    ) -> Self {
        assert!(count.is_power_of_two());
        assert!(count > 0 && count <= 32768);
        assert!(size > 0);
        assert!(base_offset as u64 <= u64::from(u32::MAX));

        let base_offset = base_offset as u32;

        let ring_size = page_align(count as usize * size_of::<IoUringBuf>());
        let ring_layout = Layout::from_size_align(ring_size, PAGE_SIZE).unwrap();
        let ring_raw = unsafe { alloc_zeroed(ring_layout) as *mut IoUringBufRingHeader };
        if ring_raw.is_null() {
            std::alloc::handle_alloc_error(ring_layout);
        }

        let bufs_ptr = ring_raw as *mut IoUringBuf;
        let bufs = unsafe { std::slice::from_raw_parts_mut(bufs_ptr, count as usize) };
        for (local, slot) in bufs.iter_mut().enumerate() {
            let addr = unsafe {
                slab_base
                    .as_ptr()
                    .add(base_offset as usize + local * size as usize) as u64
            };
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
            buf_size: size,
            base_offset,
            slab_base,
            bgid,
            ring_ptr: unsafe { NonNull::new_unchecked(ring_raw as *mut u8) },
            ring_size,
            tail: count,
            borrows: vec![0; count as usize],
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
                self.bgid,
                0,
            )
        }
    }

    /// Unregisters the pool from `ring`. Must be called before the pool is
    /// dropped and the ring is closed.
    pub fn unregister(&self, ring: &IoUring) -> io::Result<()> {
        ring.submitter().unregister_buf_ring(self.bgid)
    }

    #[cfg(test)]
    pub fn bgid(&self) -> u16 {
        self.bgid
    }

    /// The byte offset of slot `local` within the shared slab.
    pub(crate) fn slot_offset(&self, local: u16) -> u32 {
        self.base_offset + u32::from(local) * self.buf_size
    }

    /// The slot size of this class, in bytes.
    pub(crate) fn slot_size(&self) -> u32 {
        self.buf_size
    }

    /// Recycles buffer `local` back to the kernel so it can be selected again.
    /// Safe to call out of order, as results are consumed.
    pub fn recycle_buffer(&mut self, local: u16) {
        let mask = self.buf_count - 1;
        let ring_idx = (self.tail & mask) as usize;
        let buf_addr = unsafe {
            self.slab_base
                .as_ptr()
                .add(self.slot_offset(local) as usize) as u64
        };

        // `resv` is left untouched: for ring index 0 it overlaps the `tail`
        // the kernel reads, so writing it would transiently clobber the tail.
        unsafe {
            let bufs_ptr = self.ring_ptr.as_ptr() as *mut IoUringBuf;
            let slot = bufs_ptr.add(ring_idx);
            (*slot).addr = buf_addr;
            (*slot).len = self.buf_size;
            (*slot).bid = local;
        }

        self.tail = self.tail.wrapping_add(1);
        let new_tail = self.tail;
        unsafe {
            let header = &mut *(self.ring_ptr.as_ptr() as *mut IoUringBufRingHeader);
            header.tail.store(new_tail, Ordering::Release);
        }
    }

    /// Records that the kernel handed slot `local` to a read result: the
    /// buffer leaves the ring until its last view drops. Panics if the slot is
    /// already borrowed (a double selection).
    pub fn mark_selected(&mut self, local: u16) {
        let borrow = &mut self.borrows[local as usize];
        assert_eq!(
            *borrow, 0,
            "mark_selected: buffer {local} is already borrowed"
        );
        *borrow = 1;
    }

    /// Releases one borrower of `local`, recycling the buffer back to the
    /// ring when the last view drops. `exclusive` selects the exclusive
    /// (`RefMut`/`SliceMut`) vs. shared (`Ref`/`Slice`) release. Panics on a
    /// count mismatch (an over-release, a double-drop, or releasing the wrong
    /// kind of borrow).
    pub fn drop_view(&mut self, exclusive: bool, local: u16) {
        let borrow = &mut self.borrows[local as usize];
        assert!(*borrow != 0, "drop_view: buffer {local} is not borrowed");
        if exclusive {
            assert!(
                *borrow < 0,
                "drop_view: exclusive release of a shared borrow on buffer {local}"
            );
            *borrow += 1;
        } else {
            assert!(
                *borrow > 0,
                "drop_view: shared release of an exclusive borrow on buffer {local}"
            );
            *borrow -= 1;
        }
        if *borrow == 0 {
            self.recycle_buffer(local);
        }
    }

    /// Flips a sole shared borrower into an exclusive one, so a `Bytes` that
    /// is the only view of its slot can be upgraded to a `BytesMut`. Panics
    /// unless this buffer has exactly one shared holder.
    pub fn upgrade(&mut self, local: u16) {
        let borrow = &mut self.borrows[local as usize];
        assert_eq!(
            *borrow, 1,
            "upgrade: buffer {local} must have exactly one shared holder"
        );
        *borrow = -1;
    }

    /// Flips a sole exclusive borrower into a shared one, so a `BytesMut`
    /// that is the only view of its slot can be downgraded to a `Bytes`.
    /// Panics unless this buffer has exactly one exclusive holder.
    pub fn downgrade(&mut self, local: u16) {
        let borrow = &mut self.borrows[local as usize];
        assert_eq!(
            *borrow, -1,
            "downgrade: buffer {local} must have exactly one exclusive holder"
        );
        *borrow = 1;
    }

    /// Registers one more exclusive borrower (a split of an exclusive view).
    /// Panics if the buffer is not currently exclusively borrowed.
    pub fn split_exclusive(&mut self, local: u16) {
        let borrow = &mut self.borrows[local as usize];
        assert!(
            *borrow < 0,
            "split_exclusive: buffer {local} is not exclusively borrowed"
        );
        *borrow -= 1;
    }

    /// Registers one more shared borrower (a cloned view). Panics if the slot
    /// is not currently shared.
    pub fn clone_shared(&mut self, local: u16) {
        let borrow = &mut self.borrows[local as usize];
        assert!(*borrow > 0, "clone_shared: buffer {local} is not shared");
        *borrow += 1;
    }

    /// Returns the bytes of buffer `local`. `len` must not exceed `buf_size`.
    #[cfg(test)]
    pub fn get_slice(&self, local: u16, len: usize) -> &[u8] {
        let start = self.slot_offset(local) as usize;
        unsafe { core::slice::from_raw_parts(self.slab_base.as_ptr().add(start), len) }
    }

    /// Returns a raw pointer to the start of slot `local`'s slab memory.
    pub fn slot_ptr(&self, local: u16) -> NonNull<u8> {
        unsafe {
            NonNull::new_unchecked(
                self.slab_base
                    .as_ptr()
                    .add(self.slot_offset(local) as usize),
            )
        }
    }

    #[cfg(test)]
    pub(crate) fn ring_tail(&self) -> u16 {
        self.tail
    }

    #[cfg(test)]
    pub(crate) fn borrows(&self, local: u16) -> i32 {
        self.borrows[local as usize]
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
        const BGID: u16 = 0;

        let fd = tmpfile();
        let written = unsafe { libc::write(fd, b"hello world".as_ptr().cast(), 11) };
        assert_eq!(written, 11);
        assert_eq!(unsafe { libc::lseek(fd, 0, libc::SEEK_SET) }, 0);

        let mut ring = io_uring::IoUring::new(8).unwrap();
        let mut slab = vec![0u8; BUF_COUNT as usize * BUF_SIZE as usize];
        let base = unsafe { NonNull::new_unchecked(slab.as_mut_ptr()) };
        let mut pool = ProvidedBufferPool::new(base, 0, BUF_COUNT, BUF_SIZE, BGID);
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

    #[test]
    fn pbuf_borrow_tracks_selection_and_release() {
        let mut slab = vec![0u8; 4 * 16];
        let base = unsafe { NonNull::new_unchecked(slab.as_mut_ptr()) };
        let mut pool = ProvidedBufferPool::new(base, 0, 4, 16, 0);

        assert_eq!(pool.borrows(1), 0);
        pool.mark_selected(1);
        assert_eq!(pool.borrows(1), 1);
        assert_eq!(pool.ring_tail(), 4);
        // A cloned view holds a second borrower; the buffer stays out of the
        // ring until both drop.
        pool.clone_shared(1);
        assert_eq!(pool.borrows(1), 2);
        pool.drop_view(false, 1);
        assert_eq!(pool.borrows(1), 1);
        assert_eq!(pool.ring_tail(), 4);
        pool.drop_view(false, 1);
        assert_eq!(pool.borrows(1), 0);
        assert_eq!(pool.ring_tail(), 5);
    }

    #[test]
    fn pbuf_upgrade_downgrade_roundtrip() {
        let mut slab = vec![0u8; 4 * 16];
        let base = unsafe { NonNull::new_unchecked(slab.as_mut_ptr()) };
        let mut pool = ProvidedBufferPool::new(base, 0, 4, 16, 0);

        pool.mark_selected(1);
        pool.upgrade(1);
        assert_eq!(pool.borrows(1), -1);
        // The buffer stays out of the ring through the exclusive phase.
        assert_eq!(pool.ring_tail(), 4);
        pool.downgrade(1);
        assert_eq!(pool.borrows(1), 1);
        pool.drop_view(false, 1);
        assert_eq!(pool.borrows(1), 0);
        assert_eq!(pool.ring_tail(), 5);
    }

    #[test]
    fn pbuf_exclusive_split_and_release() {
        let mut slab = vec![0u8; 4 * 16];
        let base = unsafe { NonNull::new_unchecked(slab.as_mut_ptr()) };
        let mut pool = ProvidedBufferPool::new(base, 0, 4, 16, 0);

        pool.mark_selected(1);
        pool.upgrade(1);
        pool.split_exclusive(1);
        assert_eq!(pool.borrows(1), -2);
        // An exclusive release of one split half still leaves a holder.
        pool.drop_view(true, 1);
        assert_eq!(pool.borrows(1), -1);
        assert_eq!(pool.ring_tail(), 4);
        pool.drop_view(true, 1);
        assert_eq!(pool.borrows(1), 0);
        assert_eq!(pool.ring_tail(), 5);
    }

    #[test]
    fn pbuf_double_selection_panics() {
        let mut slab = vec![0u8; 4 * 16];
        let base = unsafe { NonNull::new_unchecked(slab.as_mut_ptr()) };
        let mut pool = ProvidedBufferPool::new(base, 0, 4, 16, 0);
        pool.mark_selected(0);
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                pool.mark_selected(0);
            }))
            .is_err()
        );
    }
}
