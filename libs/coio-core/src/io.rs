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
use crate::buf::IoBufMut;
use crate::buf::IoBufRef;
use crate::runtime::Op;
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
