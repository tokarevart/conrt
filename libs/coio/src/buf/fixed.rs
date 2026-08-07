//! Fixed-buffer slab: one size class of the runtime's registered buffer pool
//! (`IORING_REGISTER_BUFFERS`).
//!
//! A [`FixedSlab`] is one size class's run of equal-sized slots carved out of
//! the runtime's shared buffer slab: a free stack of local slot ids plus the
//! per-slot borrow tracking it shares with the provided slabs via
//! [`crate::buf::tracker::BorrowTracker`]. Views into the slab — [`Ref`]/
//! [`RefMut`] for typed slots and [`Slice`]/[`SliceMut`] for byte ranges —
//! hold a slot's borrow; the slot returns to the slab only when the last view
//! is dropped.

use std::ptr::NonNull;

use crate::buf::Bytes;
use crate::buf::BytesMut;
use crate::buf::Ref;
use crate::buf::RefMut;
use crate::buf::Slice;
use crate::buf::SliceMut;
use crate::buf::pool::Slab;
use crate::buf::tracker::BorrowTracker;
use crate::classes::pack_bid;
use crate::runtime::active_gen;

pub struct FixedSlab {
    size: u32,
    /// This class's index in the runtime's fixed-pool table, packed into the
    /// high bits of every view this slab hands out.
    class: u8,
    /// The start of this class's slot 0 within the shared slab, aligned to
    /// `min(size, BUFFER_MAX_ALIGN)`; slot `local` lives `local * size` bytes
    /// in.
    slab_base: NonNull<u8>,
    free: Vec<u32>,
    /// Per-slot borrow counts shared with the provided slabs.
    pub(crate) tracker: BorrowTracker,
}

impl FixedSlab {
    /// Creates a free stack of `count` slots of `size` bytes each, backed by
    /// the `count * size` bytes of the caller-owned slab starting at
    /// `slab_base` (which must point at the class's slot 0 and be aligned to
    /// `min(size, BUFFER_MAX_ALIGN)`). The slab is registered with the ring
    /// once, by the runtime.
    pub(crate) fn new(slab_base: NonNull<u8>, size: u32, count: u32, class: u8) -> Self {
        assert!(size > 0);
        assert!(count > 0);

        // Seed the free stack so the first acquire returns slot 0.
        let mut free = Vec::with_capacity(count as usize);
        free.extend((0..count).rev());

        Self {
            size,
            class,
            slab_base,
            free,
            tracker: BorrowTracker::new(count as usize),
        }
    }

    #[cfg(test)]
    pub fn get_slice_mut(&mut self, local: u32) -> &mut [u8] {
        unsafe {
            core::slice::from_raw_parts_mut(self.slot_ptr(local).as_ptr(), self.size as usize)
        }
    }

    /// Pops a free slot and marks it exclusively borrowed (`−1`). Returns
    /// `None` when the class is exhausted. This is the root of an exclusive
    /// view, e.g. a `BytesMut` handed to
    /// [`crate::task::TaskContext::alloc_bytes`].
    pub fn acquire_slot(&mut self) -> Option<u32> {
        let local = self.free.pop()?;
        self.tracker.take_exclusive(local);
        Some(local)
    }

    /// Pops a free slot and marks it shared (`+1`). This is the root of a
    /// shared view, e.g. the `Ref` handed to
    /// [`crate::task::TaskContext::alloc`] for pooled op arguments.
    pub fn acquire_slot_shared(&mut self) -> Option<u32> {
        let local = self.free.pop()?;
        self.tracker.take_shared(local);
        Some(local)
    }

    /// Pops a free slot and builds a shared [`Ref<T>`] over it, capturing the
    /// live generation. This slab's slots hold at least `size_of::<T>()` bytes
    /// (the caller selects this slab with [`crate::classes::class_for`]).
    /// `None` when the class is exhausted. Panics if `T` is zero-sized.
    pub fn acquire_ref<T>(&mut self) -> Option<Ref<T>> {
        assert!(
            core::mem::size_of::<T>() > 0,
            "acquire_ref: zero-sized types cannot be pooled"
        );
        let local = self.acquire_slot_shared()?;
        let generation = active_gen().expect("acquire_ref outside an active runtime");
        // SAFETY: acquire_shared registered a shared borrow on the slot and the
        // generation is the live one.
        Some(unsafe { Ref::new(pack_bid(false, self.class, local), generation, 0) })
    }

    /// Pops a free slot and builds an exclusive [`RefMut<T>`] over it,
    /// capturing the live generation. This slab's slots hold at least
    /// `size_of::<T>()` bytes (the caller selects this slab with
    /// [`crate::classes::class_for`]). `None` when the class is exhausted.
    /// Panics if `T` is zero-sized.
    pub fn acquire_mut<T>(&mut self) -> Option<RefMut<T>> {
        assert!(
            core::mem::size_of::<T>() > 0,
            "acquire_mut: zero-sized types cannot be pooled"
        );
        let local = self.acquire_slot()?;
        let generation = active_gen().expect("acquire_mut outside an active runtime");
        // SAFETY: acquire registered an exclusive borrow on the slot and the
        // generation is the live one.
        Some(unsafe { RefMut::new(pack_bid(false, self.class, local), generation, 0) })
    }

    /// Pops a free slot and builds a shared [`Slice<T>`] covering the whole
    /// slot as `T` elements, capturing the live generation. `None` when the
    /// class is exhausted.
    ///
    /// No caller yet: the shared typed-slice root for future receive paths.
    #[allow(dead_code)]
    pub fn acquire_slice<T>(&mut self) -> Option<Slice<T>> {
        let local = self.acquire_slot_shared()?;
        let generation = active_gen().expect("acquire_slice outside an active runtime");
        let elements = self.size / core::mem::size_of::<T>() as u32;
        // SAFETY: acquire_shared registered a shared borrow on the slot and the
        // generation is the live one; `elements` covers exactly the slot.
        Some(unsafe {
            Slice::new(
                Ref::new(pack_bid(false, self.class, local), generation, 0),
                elements,
            )
        })
    }

    /// Pops a free slot and builds an exclusive [`SliceMut<T>`] covering the
    /// whole slot as `T` elements, capturing the live generation. `None` when
    /// the class is exhausted.
    pub fn acquire_slice_mut<T>(&mut self) -> Option<SliceMut<T>> {
        let local = self.acquire_slot()?;
        let generation = active_gen().expect("acquire_slice_mut outside an active runtime");
        let elements = self.size / core::mem::size_of::<T>() as u32;
        // SAFETY: acquire registered an exclusive borrow on the slot and the
        // generation is the live one; `elements` covers exactly the slot.
        Some(unsafe {
            SliceMut::new(
                RefMut::new(pack_bid(false, self.class, local), generation, 0),
                elements,
            )
        })
    }

    /// Pops a free slot and builds a shared [`Bytes`] covering the whole
    /// slot, capturing the live generation. `None` when the class is
    /// exhausted.
    ///
    /// No caller yet: the shared bytes root for future receive paths.
    #[allow(dead_code)]
    pub fn acquire_bytes(&mut self) -> Option<Bytes> {
        self.acquire_slice::<u8>()
    }

    /// Pops a free slot and builds an exclusive [`BytesMut`] covering the
    /// whole slot, capturing the live generation. `None` when the class is
    /// exhausted.
    pub fn acquire_bytes_mut(&mut self) -> Option<BytesMut> {
        self.acquire_slice_mut::<u8>()
    }

    #[cfg(test)]
    pub(crate) fn free_count(&self) -> usize {
        self.free.len()
    }
}

impl Slab for FixedSlab {
    /// The slot size of this class, in bytes.
    fn slot_size(&self) -> u32 {
        self.size
    }

    /// The start of this slab's slot 0, aligned to `min(slot_size,
    /// BUFFER_MAX_ALIGN)`.
    fn base(&self) -> NonNull<u8> {
        self.slab_base
    }

    /// Returns `local` to the pool by pushing it back onto the free stack.
    fn recycle(&mut self, local: u32) {
        self.free.push(local);
    }

    /// The slab's mutable per-slot borrow tracker.
    fn tracker_mut(&mut self) -> &mut BorrowTracker {
        &mut self.tracker
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_slab(count: u32, size: u32) -> (FixedSlab, Vec<u8>) {
        let mut slab = vec![0u8; count as usize * size as usize];
        let base = unsafe { NonNull::new_unchecked(slab.as_mut_ptr()) };
        (FixedSlab::new(base, size, count, 0), slab)
    }

    #[test]
    fn acquire_drop_roundtrip() {
        let (mut slab, _backing) = make_slab(4, 16);
        let a = slab.acquire_slot().unwrap();
        let b = slab.acquire_slot().unwrap();
        assert_ne!(a, b);
        slab.drop_view(true, a);
        assert_eq!(slab.acquire_slot().unwrap(), a);
    }

    #[test]
    fn acquire_exhaustion() {
        let (mut slab, _backing) = make_slab(4, 16);
        for _ in 0..4 {
            assert!(slab.acquire_slot().is_some());
        }
        assert_eq!(slab.acquire_slot(), None);
        slab.drop_view(true, 2);
        assert_eq!(slab.acquire_slot(), Some(2));
    }

    #[test]
    fn shared_borrow_returns_to_free_at_zero() {
        let (mut slab, _backing) = make_slab(2, 16);
        let slot = slab.acquire_slot_shared().unwrap();
        slab.tracker.clone_shared(slot);
        assert_eq!(slab.tracker.borrows(slot), 2);
        assert_eq!(slab.free_count(), 1);
        slab.drop_view(false, slot);
        assert_eq!(slab.tracker.borrows(slot), 1);
        assert_eq!(slab.free_count(), 1);
        slab.drop_view(false, slot);
        assert_eq!(slab.tracker.borrows(slot), 0);
        assert_eq!(slab.free_count(), 2);
    }

    #[test]
    fn exclusive_split_requires_both_drops() {
        let (mut slab, _backing) = make_slab(2, 16);
        let slot = slab.acquire_slot().unwrap();
        slab.tracker.split_exclusive(slot);
        assert_eq!(slab.tracker.borrows(slot), -2);
        assert_eq!(slab.free_count(), 1);
        slab.drop_view(true, slot);
        assert_eq!(slab.tracker.borrows(slot), -1);
        assert_eq!(slab.free_count(), 1);
        slab.drop_view(true, slot);
        assert_eq!(slab.tracker.borrows(slot), 0);
        assert_eq!(slab.free_count(), 2);
    }

    #[test]
    fn upgrade_downgrade_sole_holder() {
        let (mut slab, _backing) = make_slab(2, 16);
        let slot = slab.acquire_slot_shared().unwrap();
        slab.tracker.upgrade(slot);
        assert_eq!(slab.tracker.borrows(slot), -1);
        slab.tracker.downgrade(slot);
        assert_eq!(slab.tracker.borrows(slot), 1);
    }

    #[test]
    fn clone_shared_panics_on_exclusive() {
        let (mut slab, _backing) = make_slab(2, 16);
        let slot = slab.acquire_slot().unwrap();
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                slab.tracker.clone_shared(slot);
            }))
            .is_err()
        );
    }

    #[test]
    fn drop_view_over_release_panics() {
        let (mut slab, _backing) = make_slab(2, 16);
        let slot = slab.acquire_slot_shared().unwrap();
        slab.drop_view(false, slot);
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                slab.drop_view(false, slot);
            }))
            .is_err()
        );
    }

    #[test]
    fn get_slice_mut_writes_within_slot() {
        let (mut slab, _backing) = make_slab(2, 8);
        let slot = slab.acquire_slot().unwrap();
        slab.get_slice_mut(slot)[..5].copy_from_slice(b"hello");
        let back = slab.get_slice_mut(slot);
        assert_eq!(&back[..5], b"hello");
        assert!(back.len() >= 8);
    }

    #[test]
    fn slots_are_isolated() {
        let (mut slab, _backing) = make_slab(2, 8);
        let a = slab.acquire_slot().unwrap();
        let b = slab.acquire_slot().unwrap();
        slab.get_slice_mut(a)[0] = 1;
        slab.get_slice_mut(b)[0] = 2;
        assert_eq!(slab.get_slice_mut(a)[0], 1);
        assert_eq!(slab.get_slice_mut(b)[0], 2);
    }

    #[test]
    fn slot_ptr_resolves_stable_addresses() {
        let (slab, _backing) = make_slab(4, 16);
        let a = slab.slot_ptr(0);
        let b = slab.slot_ptr(1);
        assert_eq!(b.as_ptr() as usize - a.as_ptr() as usize, 16);
    }
}
