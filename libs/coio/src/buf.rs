//! Fixed-buffer support (`IORING_REGISTER_BUFFERS`) pooled memory.
//!
//! The runtime registers its whole shared buffer slab with the `io_uring` as
//! fixed buffer index 0. A [`BufferPool`] describes one size class of that
//! slab: a free stack of local slot ids for the class's equal-sized slots. A
//! [`BufferBytes`] owns a slot and releases it back to the pool on drop;
//! writes submitted with `IORING_OP_WRITE_FIXED` reference a slot's address
//! directly. A [`Buffer`] is the same pool's typed handle, used to pin
//! io_uring argument objects (`msghdr`, iovec arrays, control buffers) in
//! stable memory.

#![allow(dead_code)]

use core::marker::PhantomData;
use core::mem::MaybeUninit;
use core::num::NonZeroU32;
use std::ptr::NonNull;

use crate::classes::bid_class;
use crate::classes::bid_local;
use crate::runtime::active_gen_matches;
use crate::runtime::with_runtime;

pub struct BufferPool {
    size: u32,
    /// Byte offset of this class's first slot within the shared slab.
    base_offset: u32,
    /// Start of the shared slab this class's slots live in.
    slab_base: NonNull<u8>,
    free: Vec<u32>,
}

impl BufferPool {
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

/// A handle to a slot of the runtime's pooled, pinned memory that stays stable
/// until this value is dropped.
///
/// The slot is identified by its packed `bid` (size-class index and slot id)
/// rather than a raw pointer, keeping this value at exactly 8 bytes with a
/// `NonZeroU32` niche (so `Option<Buffer<T>>` costs nothing extra). The
/// address is resolved against the owning runtime each time, guarded by the
/// generation captured at allocation: a `Buffer` used or dropped after its
/// runtime has shut down panics on resolve or leaks the slot on drop instead
/// of touching freed memory.
///
/// `Buffer<T>` is invariant over `T` and neither `Send` nor `Sync`, because it
/// resolves through a thread-local runtime pointer.
pub struct Buffer<T> {
    bid: u32,
    generation: NonZeroU32,
    _t: PhantomData<*const T>,
}

const _: () = assert!(size_of::<Buffer<u8>>() == 8);
const _: () = assert!(size_of::<Option<Buffer<u8>>>() == 8);

impl<T> Buffer<T> {
    pub(crate) fn new(bid: u32, generation: NonZeroU32) -> Self {
        Self {
            bid,
            generation,
            _t: PhantomData,
        }
    }

    /// Resolves the guarded slot's memory to a raw pointer. The pointer is
    /// only valid while this value is alive and the runtime that owns it is
    /// still running. Panics if used outside the owning runtime.
    pub fn as_ptr(&self) -> *mut T {
        assert!(
            active_gen_matches(self.generation),
            "Buffer used outside the runtime that owns it"
        );
        with_runtime(|r| {
            let class = bid_class(self.bid) as usize;
            let local = bid_local(self.bid);
            r.fixed_pools[class].slot_ptr(local).cast().as_ptr()
        })
    }

    /// Reinterprets the guarded slot's memory as `U`. The class, slot id and
    /// generation are preserved, so the cast names the same pooled slot.
    ///
    /// # Safety
    ///
    /// `U` must have the same size as `T` and an alignment no greater than
    /// `T`'s (both are also enforced at compile time below). Reinterpreting
    /// the slot's contents as `U` must be semantically valid: in particular,
    /// a `U` that is not wrapped in [`MaybeUninit`] claims the contents are
    /// initialized, so the caller must ensure that is true before reading.
    pub unsafe fn cast<U>(self) -> Buffer<U> {
        const { assert!(size_of::<T>() == size_of::<U>()) };
        const { assert!(align_of::<U>() <= align_of::<T>()) };
        // Read the fields, then forget the consumed value: running `self`'s
        // `Drop` would recycle the slot while the returned buffer still
        // claims it, letting the pool hand the same slot out twice.
        let result = Buffer {
            bid: self.bid,
            generation: self.generation,
            _t: PhantomData,
        };
        core::mem::forget(self);
        result
    }
}

impl<T> Drop for Buffer<T> {
    fn drop(&mut self) {
        // Leak the slot when the owning runtime is gone: recycling it would
        // touch freed memory. Same policy as BufferBytes/ProvidedBuffer.
        if active_gen_matches(self.generation) {
            let class = bid_class(self.bid) as usize;
            let local = bid_local(self.bid);
            with_runtime(|r| r.fixed_pools[class].release(local));
        }
    }
}

impl<T> Buffer<MaybeUninit<T>> {
    /// A shared reference to the guarded slot's memory. The memory is not
    /// assumed initialized; initialize it (for example with
    /// `MaybeUninit::zeroed`) before reading a `T` out of it.
    #[allow(clippy::should_implement_trait)]
    pub fn as_ref(&self) -> &MaybeUninit<T> {
        unsafe { &*self.as_ptr() }
    }

    /// A mutable reference to the guarded slot's memory. The memory is not
    /// assumed initialized.
    #[allow(clippy::should_implement_trait)]
    pub fn as_mut(&mut self) -> &mut MaybeUninit<T> {
        unsafe { &mut *self.as_ptr() }
    }
}

/// A zero-copy buffer that borrows a slot from the runtime's fixed buffer
/// slab.
///
/// Created by [`crate::task::TaskContext::alloc_bytes`]; the caller fills the
/// slot through [`BufferBytes::as_mut`], records the filled length with
/// [`BufferBytes::set_len`], then hands the buffer to [`crate::io::write`],
/// which submits it without copying. The slot is recycled when the buffer is
/// dropped. The slab is reached through the thread-local runtime pointer,
/// guarded by the generation the buffer was created in: a `BufferBytes`
/// dropped after its runtime has shut down skips recycling instead of
/// touching freed memory.
pub struct BufferBytes {
    offset: u32,
    bid: u32,
    len: u32,
    generation: NonZeroU32,
}

impl BufferBytes {
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
            let class = bid_class(self.bid) as usize;
            r.fixed_pools[class].slot_size()
        })
    }

    /// Returns the number of bytes marked as filled via
    /// [`BufferBytes::set_len`].
    pub fn len(&self) -> usize {
        self.len as _
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the whole writable slot, so the caller can use its full
    /// capacity. Call [`BufferBytes::set_len`] with the number of bytes
    /// actually written before submitting.
    #[allow(clippy::should_implement_trait)]
    pub fn as_mut(&mut self) -> &mut [u8] {
        assert!(
            active_gen_matches(self.generation),
            "as_mut() called outside the runtime that owns this buffer"
        );
        with_runtime(|r| {
            let class = bid_class(self.bid) as usize;
            let capacity = r.fixed_pools[class].slot_size() as usize;
            let ptr = unsafe { r.slab.as_ptr().add(self.offset as usize) };
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
    /// [`BufferBytes::capacity`].
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
            let class = bid_class(self.bid) as usize;
            let local = bid_local(self.bid);
            with_runtime(|r| r.fixed_pools[class].release(local));
        }
    }
}

impl Drop for BufferBytes {
    fn drop(&mut self) {
        self.recycle();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buffer_is_8_bytes_with_niche() {
        assert_eq!(size_of::<Buffer<u8>>(), 8);
        assert_eq!(size_of::<Option<Buffer<u8>>>(), 8);
        assert_eq!(size_of::<Buffer<[u8; 64]>>(), 8);
    }

    #[test]
    fn buffer_bytes_is_16_bytes_with_niche() {
        assert_eq!(size_of::<BufferBytes>(), 16);
        assert_eq!(size_of::<Option<BufferBytes>>(), 16);
    }

    fn make_pool(count: u32, size: u32) -> (BufferPool, Vec<u8>) {
        let mut slab = vec![0u8; count as usize * size as usize];
        let base = unsafe { NonNull::new_unchecked(slab.as_mut_ptr()) };
        (BufferPool::new(base, 0, size, count), slab)
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

    #[test]
    fn slot_ptr_resolves_stable_addresses() {
        let (pool, _slab) = make_pool(4, 16);
        let a = pool.slot_ptr(0);
        let b = pool.slot_ptr(1);
        assert_eq!(b.as_ptr() as usize - a.as_ptr() as usize, 16);
    }
}
