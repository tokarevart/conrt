//! The portable operations, as methods on [`Runtime`].
//!
//! An operation that any backend can provide — a read from a caller-owned
//! buffer, a write to one, an accept — is declared here as a trait and turned
//! into an inherent method on [`Runtime`]. Two things follow from that split:
//!
//! * The method is *inherent*, so a caller writes `rt.read(…)` without
//!   importing anything. Were it an extension trait in the backend crate, every
//!   file that dispatched an op would need that trait in scope — including
//!   files that only ever want the portable API.
//! * The method only exists when the backend actually implements the operation,
//!   so a backend missing one has no such method rather than a runtime that
//!   fails at run time.
//!
//! A backend cannot add inherent methods to `Runtime` itself, because
//! `Runtime` is defined here and Rust forbids inherent impls on foreign types.
//! The traits are per-operation rather than one catch-all so that adding an
//! operation means adding a trait, not editing a dispatch.

use std::io;
use std::os::fd::RawFd;

use crate::backend::Backend;
use crate::buf::IoBuf;
use crate::buf::IoBufMut;
use crate::buf::IoBufRef;
use crate::runtime::Op;
use crate::runtime::OpOutcome;
use crate::runtime::OpShimArgs;
use crate::runtime::Runtime;

/// The callback a portable operation reports through when it moves bytes.
///
/// The buffer comes back with it, so one allocation can serve every read or
/// write in a loop.
pub type ByteDone<G, L, B, Buf> = fn(&mut Runtime<G, L, B>, L, Buf, io::Result<usize>);

/// The callback [`AcceptOp`] reports through, which has no buffer to return.
pub type AcceptDone<G, L, B> = fn(&mut Runtime<G, L, B>, L, io::Result<RawFd>);

/// Reads into a caller-supplied buffer.
pub trait ReadOp<G, L>: Backend {
    /// Submits one read of `buf`'s writable region into `fd`.
    ///
    /// `done` receives the buffer back with the bytes the backend wrote
    /// committed to it.
    fn read<Buf: IoBufMut>(
        rt: &mut Runtime<G, L, Self>,
        fd: RawFd,
        buf: Buf,
        local: L,
        done: ByteDone<G, L, Self, Buf>,
    ) -> io::Result<Op>;
}

/// Writes the whole of a caller-supplied buffer.
///
/// One request, not a loop: a short write is reported as a byte count, and the
/// caller re-arms with the unwritten tail. Looping here would mean holding the
/// buffer across attempts, which is exactly what handing it to `done` avoids.
pub trait WriteOp<G, L>: Backend {
    /// Submits one write of `buf` to `fd`.
    fn write<Buf: IoBufRef>(
        rt: &mut Runtime<G, L, Self>,
        fd: RawFd,
        buf: Buf,
        local: L,
        done: ByteDone<G, L, Self, Buf>,
    ) -> io::Result<Op>;
}

/// Accepts a connection on a listening socket.
pub trait AcceptOp<G, L>: Backend {
    /// Submits one accept on `listener_fd`.
    fn accept(
        rt: &mut Runtime<G, L, Self>,
        listener_fd: RawFd,
        local: L,
        done: AcceptDone<G, L, Self>,
    ) -> io::Result<Op>;
}

impl<G, L, B: ReadOp<G, L>> Runtime<G, L, B> {
    /// Reads into `buf` from `fd`.
    ///
    /// The caller supplies the memory and gets it back in `done` with the bytes
    /// the backend wrote committed to it, so one allocation can serve every
    /// read in a loop. A short read is reported as a byte count.
    pub fn read<Buf: IoBufMut>(
        &mut self,
        fd: RawFd,
        buf: Buf,
        local: L,
        done: ByteDone<G, L, B, Buf>,
    ) -> io::Result<Op> {
        B::read(self, fd, buf, local, done)
    }
}

impl<G, L, B: WriteOp<G, L>> Runtime<G, L, B> {
    /// Writes the whole of `buf` to `fd`.
    ///
    /// The buffer is submitted by address and handed back in `done`, so the
    /// caller reuses one allocation across as many writes as it likes. A short
    /// write is reported as a byte count; re-arm with the unwritten tail to
    /// send everything.
    pub fn write<Buf: IoBufRef>(
        &mut self,
        fd: RawFd,
        buf: Buf,
        local: L,
        done: ByteDone<G, L, B, Buf>,
    ) -> io::Result<Op> {
        B::write(self, fd, buf, local, done)
    }
}

impl<G, L, B: AcceptOp<G, L>> Runtime<G, L, B> {
    /// Accepts a connection on `listener_fd`, yielding the new socket's fd to
    /// `done`.
    pub fn accept(
        &mut self,
        listener_fd: RawFd,
        local: L,
        done: AcceptDone<G, L, B>,
    ) -> io::Result<Op> {
        B::accept(self, listener_fd, local, done)
    }
}

// ── Completion helpers ───────────────────────────────────────────────
//
// The tail of every completion shim is the same: turn the outcome into what the
// caller's callback reports, rebuild the caller's buffer if there was one, and
// either invoke the callback or — when the runtime is tearing the op down
// without completing it — drop everything silently. Only the shim knows the
// concrete buffer and callback types, so the shim still transmutates those two
// things; everything after that is here, so a backend gets the teardown path
// exactly right by construction rather than by copying it.

/// The error for a negative backend result, if it is one.
pub fn errno(result: i32) -> Option<io::Error> {
    (result < 0).then(|| io::Error::from_raw_os_error(-result))
}

/// What a cancelled operation reports to its callback.
///
/// `Interrupted` says "resume or clean up", which is the right reading for a
/// cancellation the caller asked for, and is distinguishable from the errno a
/// genuine failure carries.
pub fn cancelled() -> io::Error {
    io::Error::new(io::ErrorKind::Interrupted, "operation cancelled")
}

/// The byte count a byte-moving operation reports, or the failure.
pub fn transferred(outcome: OpOutcome) -> io::Result<u32> {
    match outcome {
        OpOutcome::Completed { result, .. } => match errno(result) {
            Some(e) => Err(e),
            None => Ok(result as u32),
        },
        OpOutcome::Cancelled => Err(cancelled()),
    }
}

/// Whether the runtime is tearing an op down without completing it.
///
/// A null `done` is that sentinel: the caller-supplied buffers have been
/// rebuilt and must be dropped, but the operation produced no result and no
/// user code may run.
pub fn is_teardown(done: *const ()) -> bool {
    done.is_null()
}

/// Hands a completed operation's buffer to `done`, or drops everything if the
/// runtime is tearing the op down.
///
/// # Safety
///
/// `done` must have come from an op whose `done` parameter has exactly the type
/// of the third argument, which is what pairs each op's signature with its
/// shim. `args.bufs` must be the single buffer that op erased on the way in.
pub unsafe fn finish_buf_op<Buf: IoBuf, G, L, B: Backend>(
    rt: &mut Runtime<G, L, B>,
    args: OpShimArgs<'_, L>,
    done: ByteDone<G, L, B, Buf>,
) {
    assert_eq!(
        args.bufs.len(),
        1,
        "shim expects 1 caller buffer but the op was given {}",
        args.bufs.len(),
    );
    let OpShimArgs {
        local,
        bufs,
        outcome,
        ..
    } = args;
    let res = transferred(outcome);
    // SAFETY: forwarded from this function's contract — the buffer was erased
    // by this op on the way in and only the operation has touched it since.
    let buf: Buf = unsafe { Buf::from_erased(bufs[0], res.as_ref().map_or(0, |n| *n)) };

    if is_teardown(args.done) {
        drop((buf, local));
        return;
    }
    done(rt, local, buf, res.map(|n| n as usize));
}

/// Hands a completed operation's result to `done`, or drops the local if the
/// runtime is tearing the op down.
///
/// `map` turns a successful backend result into the value the callback reports,
/// which differs by operation — a byte count for a transfer, the fd itself for
/// an accept.
///
/// # Safety
///
/// `done` must have come from an op whose `done` parameter has exactly the type
/// of the third argument. `args.bufs` must be empty, which is the case for
/// every operation that supplies no caller buffer.
pub fn finish_plain_op<T, F, G, L, B: Backend>(
    rt: &mut Runtime<G, L, B>,
    args: OpShimArgs<'_, L>,
    done: fn(&mut Runtime<G, L, B>, L, io::Result<T>),
    map: F,
) where
    F: FnOnce(i32) -> T,
{
    assert!(
        args.bufs.is_empty(),
        "this operation supplies no caller buffer, but was given {}",
        args.bufs.len(),
    );
    let OpShimArgs { local, outcome, .. } = args;
    let res = match outcome {
        OpOutcome::Completed { result, .. } => errno(result).map_or_else(|| Ok(map(result)), Err),
        OpOutcome::Cancelled => Err(cancelled()),
    };

    if is_teardown(args.done) {
        drop(local);
        return;
    }
    done(rt, local, res);
}
