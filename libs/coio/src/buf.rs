//! Fixed-buffer support (`IORING_REGISTER_BUFFERS`) pooled memory.
//!
//! The runtime registers its whole shared buffer slab with the `io_uring` as
//! fixed buffer index 0. A [`BufferPool`] describes one size class of that
//! slab: a free stack of local slot ids plus the per-slot borrow tracking that
//! it shares with the provided pools via [`BorrowTracker`]. Views into the slab
//! — [`Ref`]/[`RefMut`] for typed slots and [`Slice`]/[`SliceMut`] for byte
//! ranges — hold a slot's borrow; the slot returns to the pool only when the
//! last view is dropped.
//!
//! A slot is shared (count `+1`, `+2`, …) while [`Ref`]/[`Slice`] views borrow
//! it and exclusive (count `−1`, `−2`, …) while [`RefMut`]/[`SliceMut`] views
//! borrow it. Exclusive views only ever arise by splitting a sole holder, so
//! two exclusive views of the same slot never overlap; the counts only track
//! how many views are still alive. Views resolve their memory through the
//! thread-local runtime pointer, guarded by the generation captured at
//! allocation: a view used or dropped after its runtime has shut down panics
//! on resolve or leaks the slot on drop instead of touching freed memory.

use core::marker::PhantomData;
use core::num::NonZeroU32;
use std::ops::Deref;
use std::ops::DerefMut;
use std::ptr::NonNull;

use crate::classes::pack_bid;
use crate::pool::BorrowTracker;
use crate::runtime::active_gen;
use crate::runtime::clone_view;
use crate::runtime::downgrade_view;
use crate::runtime::drop_view;
use crate::runtime::resolve_ptr;
use crate::runtime::upgrade_view;

pub struct BufferPool {
    size: u32,
    /// This class's index in the runtime's fixed-pool table, packed into the
    /// high bits of every view this pool hands out.
    class: u8,
    /// The start of this class's slot 0 within the shared slab, aligned to
    /// `min(size, BUFFER_MAX_ALIGN)`; slot `local` lives `local * size` bytes
    /// in.
    slab_base: NonNull<u8>,
    free: Vec<u32>,
    /// Per-slot borrow counts shared with the provided pools.
    pub(crate) tracker: BorrowTracker,
}

impl BufferPool {
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

    pub fn slot_size(&self) -> u32 {
        self.size
    }

    /// Returns a raw pointer to the start of slot `local`'s slab memory.
    pub fn slot_ptr(&self, local: u32) -> NonNull<u8> {
        unsafe {
            NonNull::new_unchecked(
                self.slab_base
                    .as_ptr()
                    .add(local as usize * self.size as usize),
            )
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
    /// live generation. This pool's slots hold at least `size_of::<T>()` bytes
    /// (the caller selects this pool with [`crate::classes::class_for`]).
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
    /// capturing the live generation. This pool's slots hold at least
    /// `size_of::<T>()` bytes (the caller selects this pool with
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

    /// Releases one borrower of `local`, pushing the slot back onto the free
    /// stack when the last view is dropped. Panics on a count mismatch
    /// (over-release, a double-drop, or releasing the wrong kind of borrow).
    pub fn drop_view(&mut self, exclusive: bool, local: u32) {
        if self.tracker.drop_view(exclusive, local) {
            self.free.push(local);
        }
    }

    #[cfg(test)]
    pub(crate) fn free_count(&self) -> usize {
        self.free.len()
    }
}

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
/// shared borrower, and the slot returns to its pool when the last view is
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

impl<T> Ref<T> {
    /// Creates a view over an already-borrowed slot, `offset` bytes into it.
    ///
    /// # Safety
    ///
    /// The caller must already hold the slot's shared borrow (or exclusively
    /// own it as the sole view): building a `Ref` without the borrow lets the
    /// pool recycle the slot while the view is alive and hand it out twice.
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
    pub fn as_ptr(&self) -> *mut T {
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
        // claims the slot, letting the pool hand it out twice.
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
        // touching freed pool memory.
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
    /// borrow lets the pool recycle the slot while the view is alive and hand
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
    pub fn as_ptr(&self) -> *mut T {
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
    pub fn as_ptr(&self) -> *const T {
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
        unsafe { core::slice::from_raw_parts(self.as_ptr(), self.len as usize) }
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
    pub fn as_ptr(&self) -> *mut T {
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
    /// Both halves must be dropped before the slot returns to its pool.
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
        unsafe { core::slice::from_raw_parts(self.as_ptr(), self.len as usize) }
    }
}

impl AsMut<[u8]> for BytesMut {
    fn as_mut(&mut self) -> &mut [u8] {
        unsafe { core::slice::from_raw_parts_mut(self.base.as_ptr(), self.len as usize) }
    }
}

fn with_runtime_capacity(bid: u32, generation: NonZeroU32) -> u32 {
    use crate::classes::bid_class;
    use crate::classes::bid_provided;
    use crate::runtime::active_gen_matches;
    use crate::runtime::with_runtime;
    assert!(
        active_gen_matches(generation),
        "capacity() called outside the runtime that owns this buffer"
    );
    with_runtime(|r| {
        let class = usize::from(bid_class(bid));
        if bid_provided(bid) {
            r.provided_pools[class].slot_size()
        } else {
            r.fixed_pools[class].slot_size()
        }
    })
}

fn split_exclusive(bid: u32, generation: NonZeroU32) {
    use crate::classes::bid_class;
    use crate::classes::bid_local;
    use crate::classes::bid_provided;
    use crate::runtime::active_gen_matches;
    use crate::runtime::with_runtime;
    if !active_gen_matches(generation) {
        return;
    }
    with_runtime(|r| {
        let class = usize::from(bid_class(bid));
        let local = bid_local(bid);
        if bid_provided(bid) {
            r.provided_pools[class].tracker.split_exclusive(local as _);
        } else {
            r.fixed_pools[class].tracker.split_exclusive(local);
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ref_is_12_bytes_with_niche() {
        assert_eq!(size_of::<Ref<u8>>(), 12);
        assert_eq!(size_of::<Option<Ref<u8>>>(), 12);
        assert_eq!(size_of::<Ref<[u8; 64]>>(), 12);
    }

    #[test]
    fn bytes_views_are_16_bytes_with_niche() {
        assert_eq!(size_of::<Bytes>(), 16);
        assert_eq!(size_of::<Option<Bytes>>(), 16);
        assert_eq!(size_of::<BytesMut>(), 16);
        assert_eq!(size_of::<Option<BytesMut>>(), 16);
    }

    fn make_pool(count: u32, size: u32) -> (BufferPool, Vec<u8>) {
        let mut slab = vec![0u8; count as usize * size as usize];
        let base = unsafe { NonNull::new_unchecked(slab.as_mut_ptr()) };
        (BufferPool::new(base, size, count, 0), slab)
    }

    #[test]
    fn acquire_drop_roundtrip() {
        let (mut pool, _slab) = make_pool(4, 16);
        let a = pool.acquire_slot().unwrap();
        let b = pool.acquire_slot().unwrap();
        assert_ne!(a, b);
        pool.drop_view(true, a);
        assert_eq!(pool.acquire_slot().unwrap(), a);
    }

    #[test]
    fn acquire_exhaustion() {
        let (mut pool, _slab) = make_pool(4, 16);
        for _ in 0..4 {
            assert!(pool.acquire_slot().is_some());
        }
        assert_eq!(pool.acquire_slot(), None);
        pool.drop_view(true, 2);
        assert_eq!(pool.acquire_slot(), Some(2));
    }

    #[test]
    fn shared_borrow_returns_to_free_at_zero() {
        let (mut pool, _slab) = make_pool(2, 16);
        let slot = pool.acquire_slot_shared().unwrap();
        pool.tracker.clone_shared(slot);
        assert_eq!(pool.tracker.borrows(slot), 2);
        assert_eq!(pool.free_count(), 1);
        pool.drop_view(false, slot);
        assert_eq!(pool.tracker.borrows(slot), 1);
        assert_eq!(pool.free_count(), 1);
        pool.drop_view(false, slot);
        assert_eq!(pool.tracker.borrows(slot), 0);
        assert_eq!(pool.free_count(), 2);
    }

    #[test]
    fn exclusive_split_requires_both_drops() {
        let (mut pool, _slab) = make_pool(2, 16);
        let slot = pool.acquire_slot().unwrap();
        pool.tracker.split_exclusive(slot);
        assert_eq!(pool.tracker.borrows(slot), -2);
        assert_eq!(pool.free_count(), 1);
        pool.drop_view(true, slot);
        assert_eq!(pool.tracker.borrows(slot), -1);
        assert_eq!(pool.free_count(), 1);
        pool.drop_view(true, slot);
        assert_eq!(pool.tracker.borrows(slot), 0);
        assert_eq!(pool.free_count(), 2);
    }

    #[test]
    fn upgrade_downgrade_sole_holder() {
        let (mut pool, _slab) = make_pool(2, 16);
        let slot = pool.acquire_slot_shared().unwrap();
        pool.tracker.upgrade(slot);
        assert_eq!(pool.tracker.borrows(slot), -1);
        pool.tracker.downgrade(slot);
        assert_eq!(pool.tracker.borrows(slot), 1);
    }

    #[test]
    fn clone_shared_panics_on_exclusive() {
        let (mut pool, _slab) = make_pool(2, 16);
        let slot = pool.acquire_slot().unwrap();
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                pool.tracker.clone_shared(slot);
            }))
            .is_err()
        );
    }

    #[test]
    fn drop_view_over_release_panics() {
        let (mut pool, _slab) = make_pool(2, 16);
        let slot = pool.acquire_slot_shared().unwrap();
        pool.drop_view(false, slot);
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                pool.drop_view(false, slot);
            }))
            .is_err()
        );
    }

    #[test]
    fn get_slice_mut_writes_within_slot() {
        let (mut pool, _slab) = make_pool(2, 8);
        let slot = pool.acquire_slot().unwrap();
        pool.get_slice_mut(slot)[..5].copy_from_slice(b"hello");
        let back = pool.get_slice_mut(slot);
        assert_eq!(&back[..5], b"hello");
        assert!(back.len() >= 8);
    }

    #[test]
    fn slots_are_isolated() {
        let (mut pool, _slab) = make_pool(2, 8);
        let a = pool.acquire_slot().unwrap();
        let b = pool.acquire_slot().unwrap();
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
