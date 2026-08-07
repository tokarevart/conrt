//! Buffers: the slabs that hold buffer memory, the pools that collect them,
//! and the views over the slots.
//!
//! A *slab* is one size class's run of equal-sized slots carved out of the
//! runtime's shared buffer slab: [`fixed::FixedSlab`] for the registered
//! write buffers (`IORING_REGISTER_BUFFERS`) and [`provided::ProvidedSlab`]
//! for the provided-buffer rings (`IORING_REGISTER_PBUF_RING`). A *pool* is a
//! collection of slabs across the size classes of one direction, stored by
//! [`pool::Pool`]: [`FixedPool`] and [`ProvidedPool`] are the runtime's two
//! pools. Pool operations are class-indexed and forward to the slab of the
//! chosen class.
//!
//! Slab slots are borrow-tracked by the shared [`tracker::BorrowTracker`]: a
//! slot is shared (count `+1`, `+2`, …) while [`Ref`]/[`Slice`] views borrow
//! it and exclusive (count `−1`, `−2`, …) while [`RefMut`]/[`SliceMut`] views
//! borrow it. Exclusive views only ever arise by splitting a sole holder, so
//! two exclusive views of the same slot never overlap; the counts only track
//! how many views are still alive. A slot returns to its slab only when the
//! last view is dropped.
//!
//! Views resolve their memory through the thread-local runtime pointer,
//! guarded by the generation captured at allocation: a view used or dropped
//! after its runtime has shut down panics on resolve or leaks the slot on drop
//! instead of touching freed memory.

pub(crate) mod fixed;
pub(crate) mod pool;
pub(crate) mod provided;
pub(crate) mod tracker;

pub(crate) type FixedPool = pool::Pool<fixed::FixedSlab>;
pub(crate) type ProvidedPool = pool::Pool<provided::ProvidedSlab>;

use core::marker::PhantomData;
use core::num::NonZeroU32;
use std::ops::Deref;
use std::ops::DerefMut;
use std::ptr::NonNull;

pub(crate) use fixed::FixedSlab;
pub(crate) use provided::ProvidedSlab;

use crate::runtime;
use crate::runtime::clone_view;
use crate::runtime::downgrade_view;
use crate::runtime::drop_view;
use crate::runtime::resolve_ptr;
use crate::runtime::upgrade_view;

/// A shared view of a slot of the runtime's pooled, pinned memory that stays
/// stable until this value is dropped.
///
/// The slot is identified by its packed `bid` (pool, size-class index and slot
/// id) plus a byte `offset` rather than a raw pointer, keeping this value at
/// exactly 12 bytes with a `NonZeroU32` niche (so `Option<Ref<T>>` costs
/// nothing extra). The address is resolved against the owning runtime each
/// time, guarded by the generation captured at allocation.
///
/// A `Ref` records a shared borrow on its slot: cloning it registers one more
/// shared borrower, and the slot returns to its slab when the last view is
/// dropped. Converting to a [`RefMut`] (sole shared holder only) flips the
/// borrow to exclusive.
///
/// `Ref<T>` is invariant over `T` and neither `Send` nor `Sync`, because it
/// resolves through a thread-local runtime pointer.
pub struct Ref<T> {
    bid: u32,
    generation: NonZeroU32,
    offset: u32,
    _t: PhantomData<*const T>,
}

const _: () = assert!(size_of::<Ref<u8>>() == 12);
const _: () = assert!(size_of::<Option<Ref<u8>>>() == 12);
const _: () = assert!(size_of::<Ref<[u8; 64]>>() == 12);

impl<T> Ref<T> {
    /// Creates a view over an already-borrowed slot, `offset` bytes into it.
    ///
    /// # Safety
    ///
    /// The caller must already hold the slot's shared borrow (or exclusively
    /// own it as the sole view): building a `Ref` without the borrow lets the
    /// slab recycle the slot while the view is alive and hand it out twice.
    /// `generation` must be the runtime generation the borrow was taken in,
    /// and `offset` must be within the slot's size.
    pub(crate) unsafe fn new(bid: u32, generation: NonZeroU32, offset: u32) -> Self {
        Self {
            bid,
            generation,
            offset,
            _t: PhantomData,
        }
    }

    /// Resolves the guarded slot's memory to a raw pointer. The pointer is
    /// only valid while this value is alive and the runtime that owns it is
    /// still running. Panics if used outside the owning runtime.
    pub fn as_ptr(&self) -> NonNull<T> {
        resolve_ptr(self.bid, self.generation, self.offset).cast::<T>()
    }

    /// Reinterprets the guarded slot's memory as `U`. The pool, slot id,
    /// generation and offset are preserved, so the cast names the same
    /// memory. The borrow is transferred: the returned view is the same
    /// borrower, so no count changes.
    ///
    /// # Safety
    ///
    /// `U` must have the same size as `T` and an alignment no greater than
    /// `T`'s (both are also enforced at compile time below). Reinterpreting
    /// the slot's contents as `U` must be semantically valid: in particular,
    /// a `U` that is not wrapped in [`core::mem::MaybeUninit`] claims the
    /// contents are initialized, so the caller must ensure that is true before
    /// reading.
    pub unsafe fn cast<U>(self) -> Ref<U> {
        const { assert!(size_of::<T>() == size_of::<U>()) };
        const { assert!(align_of::<U>() <= align_of::<T>()) };
        // Read the fields, then forget the consumed value: running `self`'s
        // `Drop` would release the borrow while the returned view still
        // claims the slot, letting the slab hand it out twice.
        let result = Ref {
            bid: self.bid,
            generation: self.generation,
            offset: self.offset,
            _t: PhantomData,
        };
        core::mem::forget(self);
        result
    }

    /// Converts this sole shared view into an exclusive [`RefMut`]. Panics
    /// unless this view is the only shared borrower of its slot.
    pub fn into_mut(self) -> RefMut<T> {
        upgrade_view(self.bid, self.generation);
        let result = RefMut {
            bid: self.bid,
            generation: self.generation,
            offset: self.offset,
            _t: PhantomData,
        };
        core::mem::forget(self);
        result
    }
}

impl<T> Clone for Ref<T> {
    /// Clones this shared view, registering one more shared borrower on the
    /// slot so it is not recycled while both views are alive.
    fn clone(&self) -> Ref<T> {
        clone_view(self.bid, self.generation);
        Ref {
            bid: self.bid,
            generation: self.generation,
            offset: self.offset,
            _t: PhantomData,
        }
    }
}

impl<T> Drop for Ref<T> {
    fn drop(&mut self) {
        // Release the borrow; a stale generation leaks the slot rather than
        // touching freed slab memory.
        drop_view(self.bid, self.generation, false);
    }
}

/// An exclusive view of a slot of the runtime's pooled, pinned memory that
/// stays stable until this value is dropped. The mutable counterpart of
/// [`Ref`]: records an exclusive borrow (count `−1`), cannot be cloned, and
/// converts to a shared [`Ref`] (sole exclusive holder only) with
/// [`into_ref`](Self::into_ref).
pub struct RefMut<T> {
    bid: u32,
    generation: NonZeroU32,
    offset: u32,
    _t: PhantomData<*mut T>,
}

const _: () = assert!(size_of::<RefMut<u8>>() == 12);
const _: () = assert!(size_of::<Option<RefMut<u8>>>() == 12);

impl<T> RefMut<T> {
    /// Creates an exclusive view over an already-borrowed slot, `offset`
    /// bytes into it.
    ///
    /// # Safety
    ///
    /// The caller must already hold the slot's exclusive borrow (or
    /// exclusively own it as the sole view): building a `RefMut` without the
    /// borrow lets the slab recycle the slot while the view is alive and hand
    /// it out twice. `generation` must be the runtime generation the borrow
    /// was taken in, and `offset` must be within the slot's size.
    pub(crate) unsafe fn new(bid: u32, generation: NonZeroU32, offset: u32) -> Self {
        Self {
            bid,
            generation,
            offset,
            _t: PhantomData,
        }
    }

    /// Resolves the guarded slot's memory to a raw pointer. The pointer is
    /// only valid while this value is alive and the runtime that owns it is
    /// still running. Panics if used outside the owning runtime.
    pub fn as_ptr(&self) -> NonNull<T> {
        resolve_ptr(self.bid, self.generation, self.offset).cast::<T>()
    }

    /// Reinterprets the guarded slot's memory as `U`, preserving the pool,
    /// slot id, generation and offset and transferring the exclusive borrow.
    ///
    /// # Safety
    ///
    /// `U` must have the same size as `T` and an alignment no greater than
    /// `T`'s (both are also enforced at compile time below). Reinterpreting
    /// the slot's contents as `U` must be semantically valid.
    pub unsafe fn cast<U>(self) -> RefMut<U> {
        const { assert!(size_of::<T>() == size_of::<U>()) };
        const { assert!(align_of::<U>() <= align_of::<T>()) };
        let result = RefMut {
            bid: self.bid,
            generation: self.generation,
            offset: self.offset,
            _t: PhantomData,
        };
        core::mem::forget(self);
        result
    }

    /// Converts this sole exclusive view into a shared [`Ref`]. Panics unless
    /// this view is the only exclusive holder of its slot.
    pub fn into_ref(self) -> Ref<T> {
        downgrade_view(self.bid, self.generation);
        let result = Ref {
            bid: self.bid,
            generation: self.generation,
            offset: self.offset,
            _t: PhantomData,
        };
        core::mem::forget(self);
        result
    }
}

impl<T> Drop for RefMut<T> {
    fn drop(&mut self) {
        drop_view(self.bid, self.generation, true);
    }
}

/// A shared, typed view over `len` elements of a pooled slot starting at the
/// base [`Ref`]'s offset. The `len` is the covered region: for a read buffer
/// it is the number of bytes the kernel wrote, for an op-argument array it is
/// the whole slot.
pub struct Slice<T> {
    base: Ref<T>,
    len: u32,
}

const _: () = assert!(size_of::<Slice<u8>>() == 16);
const _: () = assert!(size_of::<Option<Slice<u8>>>() == 16);

impl<T> Slice<T> {
    /// Creates a slice view over `len` elements of a slot, transferring the
    /// base view's borrow to the slice.
    ///
    /// # Safety
    ///
    /// `base` must be a live view whose borrow is transferred to the slice
    /// (the caller must not use `base` again — it is dropped without releasing
    /// the borrow), and `base.offset + len * size_of::<T>()` must not exceed
    /// the slot's size, or reads through the slice run past the slot.
    pub(crate) unsafe fn new(base: Ref<T>, len: u32) -> Self {
        Self { base, len }
    }

    pub fn len(&self) -> usize {
        self.len as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The address of this view's first element. Only valid while this value
    /// is alive and the owning runtime is running.
    pub fn as_ptr(&self) -> NonNull<T> {
        self.base.as_ptr()
    }

    /// Recovers the underlying [`Ref`], shrinking the borrow footprint to the
    /// base slot. The borrow is transferred: no count changes.
    pub fn into_ref(self) -> Ref<T> {
        let base = unsafe { core::ptr::read(&self.base) };
        core::mem::forget(self);
        base
    }

    /// Reinterprets this slice's bytes as `U`. The covered byte range is
    /// preserved; the element count is `len * size_of::<T>() / size_of::<U>()`,
    /// which must be exact. The borrow is transferred.
    ///
    /// # Safety
    ///
    /// `U` must have an alignment no greater than `T`'s (enforced at compile
    /// time below), and reading the bytes as `U` must be semantically valid.
    pub unsafe fn cast<U>(self) -> Slice<U> {
        const { assert!(align_of::<U>() <= align_of::<T>()) };
        let bytes = self.len as usize * size_of::<T>();
        assert!(
            bytes.is_multiple_of(size_of::<U>()),
            "Slice::cast: the byte length {bytes} is not a multiple of U's size"
        );
        let base = unsafe { core::ptr::read(&self.base) };
        core::mem::forget(self);
        // SAFETY: `base`'s borrow is transferred to the returned slice (the
        // byte length is at most the slot's, enforced at construction), so no
        // count changes and the covered range stays in bounds.
        unsafe {
            Slice::new(
                Ref::new(base.bid, base.generation, base.offset),
                (bytes / size_of::<U>()) as u32,
            )
        }
    }
}

impl<T> Drop for Slice<T> {
    fn drop(&mut self) {
        // The base `Ref`'s `Drop` releases the shared borrow.
    }
}

/// A `[u8]` slice view: pooled read buffers returned by
/// [`crate::io::read`] and pooled data handed to a send.
pub type Bytes = Slice<u8>;

impl Bytes {
    /// Returns a view over `[a, b)` bytes of this slice, registering one more
    /// shared borrower so both views can be alive at once. `None` when `a > b`
    /// or `b > len`. Both views must be dropped before the slot returns to its
    /// pool.
    pub fn sub(&self, a: usize, b: usize) -> Option<Bytes> {
        if a > b || b > self.len as usize {
            return None;
        }
        clone_view(self.base.bid, self.base.generation);
        // SAFETY: clone_view registered one more shared borrower, so the
        // borrow is held; the checked `a <= b <= len` keeps the sub-slice
        // within the slot.
        Some(unsafe {
            Slice::new(
                Ref::new(
                    self.base.bid,
                    self.base.generation,
                    self.base.offset + a as u32,
                ),
                (b - a) as u32,
            )
        })
    }

    /// Copies the covered bytes into an owned `Vec` and releases the slot.
    pub fn into_vec(self) -> Vec<u8> {
        let data = self.as_ref().to_vec();
        drop_view(self.base.bid, self.base.generation, false);
        core::mem::forget(self);
        data
    }

    /// Converts this sole shared read buffer into an exclusive [`BytesMut`],
    /// preserving the covered length, so a buffer read into a provided pool
    /// can be handed to a send or modified in place. Panics unless this view
    /// is the only shared borrower of its slot.
    pub fn into_mut(self) -> BytesMut {
        let len = self.len;
        // SAFETY: into_ref transfers the shared borrow held by `self` to the
        // returned `Ref`; Ref::into_mut upgrades that sole shared borrow to
        // exclusive (panicking otherwise), so the borrow is held and `len` is
        // within the slot.
        unsafe { SliceMut::new(self.into_ref().into_mut(), len) }
    }
}

impl Deref for Bytes {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.as_ref()
    }
}

impl AsRef<[u8]> for Bytes {
    fn as_ref(&self) -> &[u8] {
        unsafe { core::slice::from_raw_parts(self.as_ptr().as_ptr(), self.len as usize) }
    }
}

/// An exclusive, typed view over `len` elements of a pooled slot. The mutable
/// counterpart of [`Slice`].
pub struct SliceMut<T> {
    base: RefMut<T>,
    len: u32,
}

const _: () = assert!(size_of::<SliceMut<u8>>() == 16);
const _: () = assert!(size_of::<Option<SliceMut<u8>>>() == 16);

impl<T> SliceMut<T> {
    /// Creates an exclusive slice view over `len` elements of a slot,
    /// transferring the base view's borrow to the slice.
    ///
    /// # Safety
    ///
    /// `base` must be a live view whose borrow is transferred to the slice
    /// (the caller must not use `base` again — it is dropped without
    /// releasing the borrow), and `base.offset + len * size_of::<T>()` must
    /// not exceed the slot's size, or accesses through the slice run past the
    /// slot.
    pub(crate) unsafe fn new(base: RefMut<T>, len: u32) -> Self {
        Self { base, len }
    }

    pub fn len(&self) -> usize {
        self.len as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The address of this view's first element. Only valid while this value
    /// is alive and the owning runtime is running.
    pub fn as_ptr(&self) -> NonNull<T> {
        self.base.as_ptr()
    }

    /// Recovers the underlying [`RefMut`]. The borrow is transferred.
    pub fn into_ref(self) -> RefMut<T> {
        let base = unsafe { core::ptr::read(&self.base) };
        core::mem::forget(self);
        base
    }

    /// Reinterprets this slice's bytes as `U`. See [`Slice::cast`].
    ///
    /// # Safety
    ///
    /// `U` must have an alignment no greater than `T`'s (enforced at compile
    /// time below), and the byte length must be a multiple of `size_of::<U>()`.
    pub unsafe fn cast<U>(self) -> SliceMut<U> {
        const { assert!(align_of::<U>() <= align_of::<T>()) };
        let bytes = self.len as usize * size_of::<T>();
        assert!(
            bytes.is_multiple_of(size_of::<U>()),
            "SliceMut::cast: the byte length {bytes} is not a multiple of U's size"
        );
        let base = unsafe { core::ptr::read(&self.base) };
        core::mem::forget(self);
        // SAFETY: `base`'s exclusive borrow is transferred to the returned
        // slice (the byte length is at most the slot's, enforced at
        // construction), so no count changes and the covered range stays in
        // bounds.
        unsafe {
            SliceMut::new(
                RefMut::new(base.bid, base.generation, base.offset),
                (bytes / size_of::<U>()) as u32,
            )
        }
    }
}

impl<T> Drop for SliceMut<T> {
    fn drop(&mut self) {
        // The base `RefMut`'s `Drop` releases the exclusive borrow.
    }
}

/// A writable `[u8]` slice view: pooled write buffers from
/// [`crate::task::TaskContext::alloc_bytes`] and pooled receive buffers in a
/// [`crate::io::MsgMut`].
pub type BytesMut = SliceMut<u8>;

impl BytesMut {
    /// The maximum number of bytes this view can cover: the slot's size minus
    /// this view's offset. Panics outside the owning runtime.
    pub fn capacity(&self) -> u32 {
        let slot_size = with_runtime_capacity(self.base.bid, self.base.generation);
        slot_size - self.base.offset
    }

    /// Sets the covered length. Panics if `new_len` exceeds
    /// [`BytesMut::capacity`].
    pub fn set_len(&mut self, new_len: u32) {
        let capacity = self.capacity();
        assert!(
            new_len <= capacity,
            "set_len({new_len}) exceeds capacity {capacity}"
        );
        self.len = new_len;
    }

    /// Resets the covered length to zero, keeping the slot acquired.
    pub fn clear(&mut self) {
        self.len = 0;
    }

    /// Splits this exclusive buffer into two non-overlapping halves
    /// `[0, mid)` and `[mid, len)`, registering a second exclusive borrower.
    /// Both halves must be dropped before the slot returns to its slab.
    /// `None` when `mid > len`. Consumes `self`.
    pub fn split_at(self, mid: usize) -> Option<(BytesMut, BytesMut)> {
        let len = self.len as usize;
        if mid > len {
            return None;
        }
        split_exclusive(self.base.bid, self.base.generation);
        let base = unsafe { core::ptr::read(&self.base) };
        core::mem::forget(self);
        // SAFETY: split_exclusive registered a second exclusive borrower, and
        // both halves lie within the original covered range, so the slot's
        // borrow is held and the halves stay in bounds. The copied `base` is
        // a live local: forget it so its `Drop` does not release the borrow
        // while both halves still claim it.
        let head = unsafe {
            SliceMut::new(
                RefMut::new(base.bid, base.generation, base.offset),
                mid as u32,
            )
        };
        let tail = unsafe {
            SliceMut::new(
                RefMut::new(base.bid, base.generation, base.offset + mid as u32),
                (len - mid) as u32,
            )
        };
        core::mem::forget(base);
        Some((head, tail))
    }

    /// Downgrades this exclusive write buffer into a shared, read-only
    /// [`Bytes`] covering the same bytes, so it can be handed to
    /// [`crate::io::write`] while still holding the slot. The borrow is
    /// transferred: no count changes. Panics unless this view is the only
    /// exclusive holder of its slot.
    pub fn into_bytes(self) -> Bytes {
        let len = self.len;
        let base = unsafe { core::ptr::read(&self.base) };
        core::mem::forget(self);
        // SAFETY: into_ref flips this sole exclusive borrow to shared, so the
        // borrow is held; `self.len` is within the slot.
        unsafe { Slice::new(base.into_ref(), len) }
    }
}

impl Deref for BytesMut {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.as_ref()
    }
}

impl DerefMut for BytesMut {
    fn deref_mut(&mut self) -> &mut [u8] {
        self.as_mut()
    }
}

impl AsRef<[u8]> for BytesMut {
    fn as_ref(&self) -> &[u8] {
        unsafe { core::slice::from_raw_parts(self.as_ptr().as_ptr(), self.len as usize) }
    }
}

impl AsMut<[u8]> for BytesMut {
    fn as_mut(&mut self) -> &mut [u8] {
        unsafe { core::slice::from_raw_parts_mut(self.base.as_ptr().as_ptr(), self.len as usize) }
    }
}

fn with_runtime_capacity(bid: u32, generation: NonZeroU32) -> u32 {
    use crate::runtime::active_gen_matches;
    use crate::runtime::with_runtime;
    assert!(
        active_gen_matches(generation),
        "capacity() called outside the runtime that owns this buffer"
    );
    with_runtime(|r| r.with_slab(bid, |slab, _| slab.slot_size()))
}

fn split_exclusive(bid: u32, generation: NonZeroU32) {
    if !runtime::active_gen_matches(generation) {
        return;
    }

    runtime::with_runtime(|r| {
        r.with_slab(bid, |slab, local| slab.tracker_mut().split_exclusive(local))
    })
}
