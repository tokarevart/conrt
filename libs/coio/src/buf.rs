//! Fixed-buffer support (`IORING_REGISTER_BUFFERS`) pooled memory.
//!
//! The runtime registers its whole shared buffer slab with the `io_uring` as
//! fixed buffer index 0. A [`BufferPool`] describes one size class of that
//! slab: a free stack of local slot ids plus a per-slot borrow count. Views
//! into the slab — [`Ref`]/[`RefMut`] for typed slots and
//! [`Slice`]/[`SliceMut`] for byte ranges — hold a slot's borrow; the slot
//! returns to the pool only when the last view is dropped.
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

use crate::runtime::clone_view;
use crate::runtime::downgrade_view;
use crate::runtime::drop_view;
use crate::runtime::resolve_ptr;
use crate::runtime::upgrade_view;

pub struct BufferPool {
    size: u32,
    /// Byte offset of this class's first slot within the shared slab.
    base_offset: u32,
    /// Start of the shared slab this class's slots live in.
    slab_base: NonNull<u8>,
    free: Vec<u32>,
    /// Per-slot borrow count: `0` means free (on the stack), positive values
    /// count shared views, negative values count exclusive views.
    borrows: Vec<i32>,
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
        let borrows = vec![0; count as usize];

        Self {
            size,
            base_offset: base_offset as u32,
            slab_base,
            free,
            borrows,
        }
    }

    pub fn slot_size(&self) -> u32 {
        self.size
    }

    /// The byte offset of slot `local` within the shared slab.
    pub(crate) fn slot_offset(&self, local: u32) -> u32 {
        self.base_offset + local * self.size
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
    pub fn get_slice_mut(&mut self, local: u32) -> &mut [u8] {
        let start = self.slot_offset(local) as usize;
        unsafe {
            core::slice::from_raw_parts_mut(self.slab_base.as_ptr().add(start), self.size as usize)
        }
    }

    /// Pops a free slot and marks it exclusively borrowed (`−1`). Returns
    /// `None` when the class is exhausted. This is the root of an exclusive
    /// view, e.g. a `BytesMut` handed to
    /// [`crate::task::TaskContext::alloc_bytes`].
    pub fn acquire(&mut self) -> Option<u32> {
        let local = self.free.pop()?;
        self.borrows[local as usize] = -1;
        Some(local)
    }

    /// Pops a free slot and marks it shared (`+1`). This is the root of a
    /// shared view, e.g. the `Ref` handed to
    /// [`crate::task::TaskContext::alloc`] for pooled op arguments.
    pub fn acquire_shared(&mut self) -> Option<u32> {
        let local = self.free.pop()?;
        self.borrows[local as usize] = 1;
        Some(local)
    }

    /// Registers one more shared borrower (a cloned [`Ref`]). Panics if the
    /// slot is not currently shared.
    pub fn clone_shared(&mut self, local: u32) {
        let borrow = &mut self.borrows[local as usize];
        assert!(*borrow > 0, "clone_shared: slot {local} is not shared");
        *borrow += 1;
    }

    /// Registers one more exclusive borrower (a split of an exclusive view).
    /// Panics if the slot is not currently exclusive.
    pub fn split_exclusive(&mut self, local: u32) {
        let borrow = &mut self.borrows[local as usize];
        assert!(
            *borrow < 0,
            "split_exclusive: slot {local} is not exclusively borrowed"
        );
        *borrow -= 1;
    }

    /// Releases one borrower of `local`, pushing the slot back onto the free
    /// stack when the last view is dropped. Panics on a count mismatch
    /// (over-release, a double-drop, or releasing the wrong kind of borrow).
    pub fn drop_view(&mut self, exclusive: bool, local: u32) {
        let borrow = &mut self.borrows[local as usize];
        assert!(*borrow != 0, "drop_view: slot {local} is not borrowed");
        if exclusive {
            assert!(
                *borrow < 0,
                "drop_view: exclusive release of a shared borrow on slot {local}"
            );
            *borrow += 1;
            if *borrow == 0 {
                self.free.push(local);
            }
        } else {
            assert!(
                *borrow > 0,
                "drop_view: shared release of an exclusive borrow on slot {local}"
            );
            *borrow -= 1;
            if *borrow == 0 {
                self.free.push(local);
            }
        }
    }

    /// Exclusive `−1` → shared `+1`. Panics unless this slot has exactly one
    /// exclusive holder.
    pub fn downgrade(&mut self, local: u32) {
        let borrow = &mut self.borrows[local as usize];
        assert_eq!(
            *borrow, -1,
            "downgrade: slot {local} must have exactly one exclusive holder"
        );
        *borrow = 1;
    }

    /// Shared `+1` → exclusive `−1`. Panics unless this slot has exactly one
    /// shared holder.
    pub fn upgrade(&mut self, local: u32) {
        let borrow = &mut self.borrows[local as usize];
        assert_eq!(
            *borrow, 1,
            "upgrade: slot {local} must have exactly one shared holder"
        );
        *borrow = -1;
    }

    #[cfg(test)]
    pub(crate) fn free_count(&self) -> usize {
        self.free.len()
    }

    #[cfg(test)]
    pub(crate) fn borrows(&self, local: u32) -> i32 {
        self.borrows[local as usize]
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
    pub(crate) fn new(bid: u32, generation: NonZeroU32, offset: u32) -> Self {
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
    pub(crate) fn new(bid: u32, generation: NonZeroU32, offset: u32) -> Self {
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
    pub(crate) fn new(base: Ref<T>, len: u32) -> Self {
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
        Slice::new(
            Ref::new(base.bid, base.generation, base.offset),
            (bytes / size_of::<U>()) as u32,
        )
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
        Some(Slice::new(
            Ref::new(
                self.base.bid,
                self.base.generation,
                self.base.offset + a as u32,
            ),
            (b - a) as u32,
        ))
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
        upgrade_view(self.base.bid, self.base.generation);
        let result = SliceMut::new(
            RefMut::new(self.base.bid, self.base.generation, self.base.offset),
            self.len,
        );
        core::mem::forget(self);
        result
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
    pub(crate) fn new(base: RefMut<T>, len: u32) -> Self {
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
        SliceMut::new(
            RefMut::new(base.bid, base.generation, base.offset),
            (bytes / size_of::<U>()) as u32,
        )
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
        let head = SliceMut::new(
            RefMut::new(base.bid, base.generation, base.offset),
            mid as u32,
        );
        let tail = SliceMut::new(
            RefMut::new(base.bid, base.generation, base.offset + mid as u32),
            (len - mid) as u32,
        );
        // The copied `base` is a live local: forget it so its `Drop` does not
        // release the exclusive borrow while both halves still claim it.
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
        Slice::new(base.into_ref(), len)
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
            r.provided_pools[class].split_exclusive(local as u16);
        } else {
            r.fixed_pools[class].split_exclusive(local);
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
        (BufferPool::new(base, 0, size, count), slab)
    }

    #[test]
    fn acquire_drop_roundtrip() {
        let (mut pool, _slab) = make_pool(4, 16);
        let a = pool.acquire().unwrap();
        let b = pool.acquire().unwrap();
        assert_ne!(a, b);
        pool.drop_view(true, a);
        assert_eq!(pool.acquire().unwrap(), a);
    }

    #[test]
    fn acquire_exhaustion() {
        let (mut pool, _slab) = make_pool(4, 16);
        for _ in 0..4 {
            assert!(pool.acquire().is_some());
        }
        assert_eq!(pool.acquire(), None);
        pool.drop_view(true, 2);
        assert_eq!(pool.acquire(), Some(2));
    }

    #[test]
    fn shared_borrow_returns_to_free_at_zero() {
        let (mut pool, _slab) = make_pool(2, 16);
        let slot = pool.acquire_shared().unwrap();
        pool.clone_shared(slot);
        assert_eq!(pool.borrows(slot), 2);
        assert_eq!(pool.free_count(), 1);
        pool.drop_view(false, slot);
        assert_eq!(pool.borrows(slot), 1);
        assert_eq!(pool.free_count(), 1);
        pool.drop_view(false, slot);
        assert_eq!(pool.borrows(slot), 0);
        assert_eq!(pool.free_count(), 2);
    }

    #[test]
    fn exclusive_split_requires_both_drops() {
        let (mut pool, _slab) = make_pool(2, 16);
        let slot = pool.acquire().unwrap();
        pool.split_exclusive(slot);
        assert_eq!(pool.borrows(slot), -2);
        assert_eq!(pool.free_count(), 1);
        pool.drop_view(true, slot);
        assert_eq!(pool.borrows(slot), -1);
        assert_eq!(pool.free_count(), 1);
        pool.drop_view(true, slot);
        assert_eq!(pool.borrows(slot), 0);
        assert_eq!(pool.free_count(), 2);
    }

    #[test]
    fn upgrade_downgrade_sole_holder() {
        let (mut pool, _slab) = make_pool(2, 16);
        let slot = pool.acquire_shared().unwrap();
        pool.upgrade(slot);
        assert_eq!(pool.borrows(slot), -1);
        pool.downgrade(slot);
        assert_eq!(pool.borrows(slot), 1);
    }

    #[test]
    fn clone_shared_panics_on_exclusive() {
        let (mut pool, _slab) = make_pool(2, 16);
        let slot = pool.acquire().unwrap();
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                pool.clone_shared(slot);
            }))
            .is_err()
        );
    }

    #[test]
    fn drop_view_over_release_panics() {
        let (mut pool, _slab) = make_pool(2, 16);
        let slot = pool.acquire_shared().unwrap();
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
