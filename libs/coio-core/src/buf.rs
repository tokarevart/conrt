//! Caller-owned buffers for the portable read and write operations.
//!
//! A portable op takes its storage from the call site rather than from the
//! runtime's pools, and hands it back on completion so the caller can reuse it.
//! To make that survive the completion shim's erasure, the buffer is converted
//! to [`IoBufErased`] on the way in and rebuilt on the way out.
//!
//! The erasure is sixteen opaque bytes, so an implementation packs whatever it
//! needs: `(ptr, capacity)` for a `Vec<u8>` destination, `(ptr, len)` for a
//! slice, and `(bid, generation, offset, len)` for `coio-uring`'s pooled
//! [`Bytes`](https://docs.rs/coio-uring) — which holds no pointer at all and
//! could not fit a pointer-shaped erasure.
//!
//! These traits are backend-generic. The pooled provided buffers have their own
//! operations (`read_provided` and friends) because the *kernel*, not the
//! caller, chooses their memory.

/// A caller buffer as submitted: where it points, and how many bytes are
/// readable (source) or writable (destination).
///
/// Sixteen bytes of opaque payload, chosen by the implementation. Whatever
/// `into_erased` puts here, `from_erased` must be able to read back — including
/// the submitted byte count, which is otherwise unrecoverable at completion.
///
/// [`NONE`](Self::NONE) is the all-zero value used by ops that carry no caller
/// buffer. It is a placeholder rather than a discriminant: a shim knows
/// statically whether its op kind has a buffer, so it never has to test for it.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IoBufErased {
    /// First word of the implementation's payload.
    pub a: u64,
    /// Second word of the implementation's payload.
    pub b: u64,
}

impl IoBufErased {
    /// The placeholder for an op with no caller buffer.
    pub const NONE: Self = Self { a: 0, b: 0 };

    /// Whether this is the no-buffer placeholder.
    ///
    /// Used by the runtime to tell an unclaimed buffer field from a claimed
    /// one; shims never need it.
    pub fn is_none(&self) -> bool {
        self == &Self::NONE
    }
}

/// Converts a read-only buffer to and from raw parts so it can be submitted and
/// then handed back to the caller once the write finishes.
///
/// See [`IoBuf`] for the safety contract the conversion itself carries.
///
/// # Safety
///
/// Implementing [`IoBuf`] correctly is the whole contract; see it there.
pub unsafe trait IoBufRef: IoBuf {
    /// The pointer and byte count to submit.
    fn readable(&self) -> (*const u8, u32);
}

/// Converts a writable buffer to and from raw parts so it can be submitted and
/// then handed back to the caller once the read finishes.
///
/// See [`IoBuf`] for the safety contract. A destination is rebuilt with
/// `written` as its length and the submitted count as its capacity, so a
/// `Vec<u8>` needs `Vec::from_raw_parts(ptr, written, submitted)`.
///
/// # Safety
///
/// Implementing [`IoBuf`] correctly is the whole contract; see it there.
pub unsafe trait IoBufMut: IoBuf {
    /// The pointer and writable byte count to submit.
    fn writable(&mut self) -> (*mut u8, u32);
}

/// The erasure half of a buffer, shared by both directions.
///
/// # Safety
///
/// `from_erased` must accept exactly what `into_erased` produced, with
/// `written` substituted for the operation's result. For a type that
/// deallocates by a length or capacity, that field must be the *true*
/// allocation size — a `Vec<u8>` rebuilds through `Vec::from_raw_parts`, so
/// recording the visible capacity instead of the allocated one deallocates with
/// the wrong layout.
pub unsafe trait IoBuf: Sized {
    /// Converts the buffer into its erased form, giving up ownership.
    fn into_erased(self) -> IoBufErased;

    /// Rebuilds the buffer from the erased form after `written` bytes moved.
    ///
    /// # Safety
    ///
    /// `e` must have come from `Self::into_erased`, and `written` must not
    /// exceed the count the direction's submit method reported. A source is
    /// returned with its original length — `written` is reported separately in
    /// the completion so the caller can resume a partial send — while a
    /// destination is returned with `written` as its new length.
    unsafe fn from_erased(e: IoBufErased, written: u32) -> Self;
}

/// Reconstructs `Buf` from an erased buffer the runtime took from one.
///
/// # Safety
///
/// `e` must have come from `Buf::into_erased`, and `written` must be the byte
/// count the operation actually moved.
#[inline]
pub unsafe fn recover<Buf: IoBuf>(e: IoBufErased, written: u32) -> Buf {
    // SAFETY: forwarded from this function's own contract.
    unsafe { Buf::from_erased(e, written) }
}
// ── Implementations for the standard buffer types ─────────────────────

/// A `Vec<u8>` can be both a write source and a read destination.
///
/// Reads are limited to the vector's current length rather than its capacity:
/// `writable` hands out a `*mut u8`, so the kernel may only write over elements
/// that are already initialized. A caller that wants a read to fill the whole
/// allocation resizes the vector first. The *capacity* is what gets recorded in
/// the erased form, because that is what `Vec::from_raw_parts` must be given
/// back to deallocate correctly.
unsafe impl IoBuf for Vec<u8> {
    fn into_erased(self) -> IoBufErased {
        let mut v = core::mem::ManuallyDrop::new(self);
        IoBufErased {
            a: v.as_mut_ptr() as u64,
            b: v.capacity() as u64,
        }
    }

    unsafe fn from_erased(e: IoBufErased, written: u32) -> Self {
        // SAFETY: `e` came from `Vec::into_erased`, which recorded the true
        // allocation capacity, and `written` is bounded by the count
        // `writable` reported — which is this vector's old length, and its
        // capacity is never smaller.
        unsafe { Vec::from_raw_parts(e.a as *mut u8, written as usize, e.b as usize) }
    }
}

unsafe impl IoBufRef for Vec<u8> {
    fn readable(&self) -> (*const u8, u32) {
        (self.as_ptr(), self.len() as u32)
    }
}

unsafe impl IoBufMut for Vec<u8> {
    fn writable(&mut self) -> (*mut u8, u32) {
        (self.as_mut_ptr(), self.len() as u32)
    }
}

/// A borrowed slice can be a write source.
///
/// Only `'static` slices implement it: the buffer is handed back to the caller
/// through an erased pointer, which carries no lifetime, so a shorter-lived
/// borrow could not be handed back soundly.
unsafe impl IoBuf for &'static [u8] {
    fn into_erased(self) -> IoBufErased {
        IoBufErased {
            a: self.as_ptr() as u64,
            b: self.len() as u64,
        }
    }

    unsafe fn from_erased(e: IoBufErased, _written: u32) -> Self {
        // SAFETY: `e` came from a `&'static [u8]`, so the region is live for
        // `'static` and is exactly `b` bytes long.
        unsafe { core::slice::from_raw_parts(e.a as *const u8, e.b as usize) }
    }
}

unsafe impl IoBufRef for &'static [u8] {
    fn readable(&self) -> (*const u8, u32) {
        (self.as_ptr(), self.len() as u32)
    }
}
