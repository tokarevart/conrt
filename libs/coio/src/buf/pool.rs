//! Buffer pools: a class table plus one slab per size class.
//!
//! A *pool* is the collection of slabs across the size classes of one
//! direction ([`FixedPool`] for the registered write buffers and
//! [`ProvidedPool`] for the provided-buffer rings). [`Pool`] stores the
//! sorted class table and one [`Slab`] per class; the class-indexed operations
//! forward to the slab of the chosen class, so callers never index a `Vec`
//! themselves. The pool delegates the per-slab work to the slab: view-building
//! ops ([`FixedSlab::acquire_ref`] etc., [`ProvidedSlab::select`]) exist on
//! both the slab and the pool.

use std::io;
use std::ptr::NonNull;

use io_uring::IoUring;

use crate::buf::Bytes;
use crate::buf::BytesMut;
use crate::buf::Ref;
use crate::buf::RefMut;
use crate::buf::Slice;
use crate::buf::SliceMut;
use crate::buf::fixed::FixedSlab;
use crate::buf::provided::ProvidedSlab;
use crate::buf::tracker::BorrowTracker;
use crate::classes::SizeClass;
use crate::classes::class_for;

/// The per-slab interface [`Pool`] operates on. Locals are `u32` here (as in
/// the packed `bid`); a slab that numbers its slots with a narrower type casts
/// internally.
pub(crate) trait Slab {
    /// The slot size of this slab's class, in bytes.
    fn slot_size(&self) -> u32;

    /// The start of this slab's slot 0, aligned to `min(slot_size,
    /// BUFFER_MAX_ALIGN)`.
    fn base(&self) -> NonNull<u8>;

    /// A raw pointer to the start of slot `local`'s memory.
    fn slot_ptr(&self, local: u32) -> NonNull<u8> {
        unsafe {
            NonNull::new_unchecked(
                self.base()
                    .as_ptr()
                    .add(local as usize * self.slot_size() as usize),
            )
        }
    }

    /// Returns `local` to the pool once its last view has dropped.
    fn recycle(&mut self, local: u32);

    /// Releases one borrower of `local`, recycling the slot when the last view
    /// drops. Panics on a count mismatch (over-release, a double-drop, or
    /// releasing the wrong kind of borrow).
    fn drop_view(&mut self, exclusive: bool, local: u32) {
        if self.tracker_mut().drop_view(exclusive, local) {
            self.recycle(local);
        }
    }

    /// The slab's mutable per-slot borrow tracker.
    fn tracker_mut(&mut self) -> &mut BorrowTracker;
}

/// A collection of slabs across the size classes of one direction.
pub(crate) struct Pool<S: Slab> {
    /// The size-class table, sorted by ascending size.
    classes: Vec<SizeClass>,
    /// One slab per class, aligned with `classes`.
    slabs: Vec<S>,
}

impl<S: Slab> Pool<S> {
    /// Builds a pool from a class table and a slab per class. The two lists
    /// must have the same length; class `i` of the table is backed by slab
    /// `i`.
    pub(crate) fn new(classes: Vec<SizeClass>, slabs: Vec<S>) -> Self {
        assert_eq!(
            classes.len(),
            slabs.len(),
            "the class table and the slab list must have the same length"
        );
        Self { classes, slabs }
    }

    /// The pool's size-class table, in ascending size order.
    #[cfg(test)]
    pub(crate) fn classes(&self) -> &[SizeClass] {
        &self.classes
    }

    /// The pool's slabs, one per class, aligned with [`Pool::classes`].
    #[cfg(test)]
    pub(crate) fn slabs(&self) -> &[S] {
        &self.slabs
    }

    /// The index of the smallest class whose slot size is at least `size`.
    pub(crate) fn class_for(&self, size: usize) -> Option<u8> {
        class_for(&self.classes, size)
    }

    /// The slab backing size class `class`.
    pub(crate) fn slab(&self, class: u8) -> &S {
        &self.slabs[class as usize]
    }

    /// The mutable slab backing size class `class`.
    pub(crate) fn slab_mut(&mut self, class: u8) -> &mut S {
        &mut self.slabs[class as usize]
    }

    /// The slot size of size class `class`, in bytes.
    pub(crate) fn slot_size(&self, class: u8) -> u32 {
        self.slab(class).slot_size()
    }

    /// A raw pointer to the start of slot `local` of size class `class`.
    pub(crate) fn slot_ptr(&self, class: u8, local: u32) -> NonNull<u8> {
        self.slab(class).slot_ptr(local)
    }

    /// The mutable per-slot borrow tracker of size class `class`.
    pub(crate) fn tracker_mut(&mut self, class: u8) -> &mut BorrowTracker {
        self.slab_mut(class).tracker_mut()
    }

    /// Releases one borrower of slot `local` of size class `class`, recycling
    /// the slot when the last view drops.
    pub(crate) fn drop_view(&mut self, class: u8, exclusive: bool, local: u32) {
        self.slab_mut(class).drop_view(exclusive, local);
    }
}

impl Pool<FixedSlab> {
    /// Pops a free slot of size class `class` and builds a shared [`Ref<T>`]
    /// over it, capturing the live generation. The class-indexed counterpart
    /// of [`FixedSlab::acquire_ref`].
    pub(crate) fn acquire_ref<T>(&mut self, class: u8) -> Option<Ref<T>> {
        self.slab_mut(class).acquire_ref()
    }

    /// Pops a free slot of size class `class` and builds an exclusive
    /// [`RefMut<T>`] over it, capturing the live generation. The class-indexed
    /// counterpart of [`FixedSlab::acquire_mut`].
    pub(crate) fn acquire_mut<T>(&mut self, class: u8) -> Option<RefMut<T>> {
        self.slab_mut(class).acquire_mut()
    }

    /// Pops a free slot of size class `class` and builds a shared [`Slice<T>`]
    /// covering the whole slot. The class-indexed counterpart of
    /// [`FixedSlab::acquire_slice`].
    ///
    /// No caller yet: the shared typed-slice root for future receive paths.
    #[allow(dead_code)]
    pub(crate) fn acquire_slice<T>(&mut self, class: u8) -> Option<Slice<T>> {
        self.slab_mut(class).acquire_slice()
    }

    /// Pops a free slot of size class `class` and builds an exclusive
    /// [`SliceMut<T>`] covering the whole slot. The class-indexed counterpart
    /// of [`FixedSlab::acquire_slice_mut`].
    #[allow(dead_code)]
    pub(crate) fn acquire_slice_mut<T>(&mut self, class: u8) -> Option<SliceMut<T>> {
        self.slab_mut(class).acquire_slice_mut()
    }

    /// Pops a free slot of size class `class` and builds a shared [`Bytes`]
    /// covering the whole slot. The class-indexed counterpart of
    /// [`FixedSlab::acquire_bytes`].
    ///
    /// No caller yet: the shared bytes root for future receive paths.
    #[allow(dead_code)]
    pub(crate) fn acquire_bytes(&mut self, class: u8) -> Option<Bytes> {
        self.slab_mut(class).acquire_bytes()
    }

    /// Pops a free slot of size class `class` and builds an exclusive
    /// [`BytesMut`] covering the whole slot. The class-indexed counterpart of
    /// [`FixedSlab::acquire_bytes_mut`].
    pub(crate) fn acquire_bytes_mut(&mut self, class: u8) -> Option<BytesMut> {
        self.slab_mut(class).acquire_bytes_mut()
    }
}

impl Pool<ProvidedSlab> {
    /// Hands the kernel-selected buffer `local` of size class `class` out as a
    /// shared [`Bytes`] view covering `len` bytes. The class-indexed
    /// counterpart of [`ProvidedSlab::select`].
    pub(crate) fn select(&mut self, class: u8, local: u16, len: u32) -> Bytes {
        self.slab_mut(class).select(local, len)
    }

    /// Hands the kernel-selected buffer `local` of size class `class` out as
    /// an exclusive [`BytesMut`] view covering `len` bytes. The class-indexed
    /// counterpart of [`ProvidedSlab::select_mut`].
    ///
    /// No caller yet: the exclusive variant for future receive paths that
    /// write in place.
    #[allow(dead_code)]
    pub(crate) fn select_mut(&mut self, class: u8, local: u16, len: u32) -> BytesMut {
        self.slab_mut(class).select_mut(local, len)
    }

    /// Registers every provided-buffer ring with `ring`, publishing the pool's
    /// buffers under each slab's buffer group. Must be called before reads
    /// with `IOSQE_BUFFER_SELECT` can use the pool.
    pub(crate) fn register_all(&self, ring: &IoUring) -> io::Result<()> {
        for slab in &self.slabs {
            slab.register(ring)?;
        }
        Ok(())
    }

    /// Unregisters every provided-buffer ring from `ring`. Must be called
    /// before the pool is dropped and the ring is closed.
    pub(crate) fn unregister_all(&self, ring: &IoUring) -> io::Result<()> {
        for slab in &self.slabs {
            slab.unregister(ring)?;
        }
        Ok(())
    }
}
