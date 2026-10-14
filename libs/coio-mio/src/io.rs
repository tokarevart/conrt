//! The portable operations, as operations on `MioRuntime`.
//!
//! These are the same three that `coio-core` declares for every backend —
//! `read` into a caller-supplied buffer, a write from one, and an accept — so
//! they reach callers as inherent methods on the runtime with nothing to
//! import. The bodies are thin because they have to be: the syscall is issued
//! by the backend when the descriptor becomes ready, not here.

use std::io;
use std::os::fd::RawFd;

use coio_core::AcceptDone;
use coio_core::AcceptOp;
use coio_core::ByteDone;
use coio_core::IoBufMut;
use coio_core::IoBufRef;
use coio_core::Op;
use coio_core::OpShimArgs;
use coio_core::ReadOp;
use coio_core::Runtime;
use coio_core::WriteOp;
use coio_core::finish_buf_op;
use coio_core::finish_plain_op;

use crate::mio::Mio;
use crate::mio::Req;

/// A runtime on this crate's backend.
type Rt<G, L> = Runtime<G, L, Mio>;

impl<G, L> ReadOp<G, L> for Mio {
    fn read<Buf: IoBufMut>(
        rt: &mut Runtime<G, L, Self>,
        fd: RawFd,
        mut buf: Buf,
        local: L,
        done: ByteDone<G, L, Self, Buf>,
    ) -> io::Result<Op> {
        fn shim<Buf: IoBufMut, G, L>(rt: &mut Rt<G, L>, args: OpShimArgs<'_, L>) {
            // SAFETY: `read` registered `done` with this exact type.
            let done: ByteDone<G, L, Mio, Buf> = unsafe { std::mem::transmute(args.done) };
            // SAFETY: forwarded from the op's own signature.
            unsafe { finish_buf_op::<Buf, G, L, Mio>(rt, args, done) };
        }

        // The address and length travel in the request rather than in the
        // erased buffer, because a readiness backend has no submission to
        // capture them: the syscall happens later, when the descriptor is ready,
        // and it is this backend's own storage that has to still describe it.
        let (addr, len) = buf.writable();
        let req = Req::Read {
            fd,
            addr,
            len: len as usize,
        };
        let erased = [buf.into_erased()];
        rt.register_op(shim::<Buf, G, L>, done as *const (), local, 0, &erased, req)
    }
}

impl<G, L> WriteOp<G, L> for Mio {
    fn write<Buf: IoBufRef>(
        rt: &mut Runtime<G, L, Self>,
        fd: RawFd,
        buf: Buf,
        local: L,
        done: ByteDone<G, L, Self, Buf>,
    ) -> io::Result<Op> {
        fn shim<Buf: IoBufRef, G, L>(rt: &mut Rt<G, L>, args: OpShimArgs<'_, L>) {
            // SAFETY: `write` registered `done` with this exact type.
            let done: ByteDone<G, L, Mio, Buf> = unsafe { std::mem::transmute(args.done) };
            // SAFETY: forwarded from the op's own signature.
            unsafe { finish_buf_op::<Buf, G, L, Mio>(rt, args, done) };
        }

        let (addr, len) = buf.readable();
        let req = Req::Write {
            fd,
            addr,
            len: len as usize,
        };
        let erased = [buf.into_erased()];
        rt.register_op(shim::<Buf, G, L>, done as *const (), local, 0, &erased, req)
    }
}

impl<G, L> AcceptOp<G, L> for Mio {
    fn accept(
        rt: &mut Runtime<G, L, Self>,
        listener_fd: RawFd,
        local: L,
        done: AcceptDone<G, L, Self>,
    ) -> io::Result<Op> {
        fn shim<G, L>(rt: &mut Rt<G, L>, args: OpShimArgs<'_, L>) {
            let done: AcceptDone<G, L, Mio> = unsafe { std::mem::transmute(args.done) };
            finish_plain_op(rt, args, done, |fd| fd);
        }

        rt.register_op(
            shim::<G, L>,
            done as *const (),
            local,
            0,
            &[],
            Req::Accept { listener_fd },
        )
    }
}
