//! Callback io operations driven by the runtime.
//!
//! Each function submits a single `io_uring` operation, moving the caller's
//! per-op local state `L` into the op. When the op completes, the runtime
//! hands that `L` back to a monomorphized shim together with the raw CQE; the
//! shim decodes the result and invokes the caller's `done` callback.
//!
//! These are the `io_uring` operations, so they pin the backend to [`Uring`]
//! rather than going through [`Backend`](coio_core::Backend): the SQE each
//! one builds is `Uring`'s own `Request` type. Operations that are not
//! `io_uring`-specific live behind the backend trait instead, so a second
//! backend can offer its own.

use core::mem::MaybeUninit;
use std::io;
use std::os::fd::RawFd;

use coio_core::AcceptDone;
use coio_core::AcceptOp;
use coio_core::ByteDone;
use coio_core::IoBufMut;
use coio_core::IoBufRef;
use coio_core::Op;
use coio_core::OpOutcome;
use coio_core::OpShimArgs;
use coio_core::ReadOp;
use coio_core::Runtime;
use coio_core::WriteOp;
use coio_core::recover;

use crate::buf::Bytes;
use crate::buf::RefMut;
use crate::buf::alloc_mut;
use crate::buf::provided_bytes;
use crate::uring::Uring;
use crate::uring::UringExt;

/// The runtime these operations drive. `Runtime<G, L>` already defaults to
/// this backend; the alias is here so the signatures below say so explicitly.
type Rt<G, L> = Runtime<G, L, Uring>;

fn errno(result: i32) -> Option<io::Error> {
    (result < 0).then(|| io::Error::from_raw_os_error(-result))
}

/// Reported to a callback whose operation was cancelled.
///
/// `Interrupted` says "resume or clean up", which is the right reading for a
/// cancellation the caller asked for, and is distinguishable from the errno a
/// genuine failure would carry.
fn cancelled() -> io::Error {
    io::Error::new(io::ErrorKind::Interrupted, "operation cancelled")
}

/// Maps a completion to the byte count its callback reports.
fn transferred(outcome: OpOutcome) -> io::Result<u32> {
    match outcome {
        OpOutcome::Completed { result, .. } => match errno(result) {
            Some(e) => Err(e),
            None => Ok(result as u32),
        },
        OpOutcome::Cancelled => Err(cancelled()),
    }
}

/// Asserts a shim was handed the number of buffers its op kind submits.
fn expect_bufs(bufs: &[coio_core::IoBufErased], n: usize) {
    assert_eq!(
        bufs.len(),
        n,
        "shim expects {n} caller buffer(s) but the op was given {}",
        bufs.len(),
    );
}

/// Whether the runtime is tearing an op down without completing it.
///
/// `done` is null on that path: the caller-supplied buffers have been rebuilt
/// and must be dropped, but the operation produced no result and no user code
/// may run. Every shim checks this before invoking its callback.
fn is_teardown(done: *const ()) -> bool {
    done.is_null()
}

/// Reads up to `max_len` bytes from `fd` into a buffer selected from the
/// runtime's provided buffer pools. The buffer is drawn from the smallest
/// size class whose slot size is at least `max_len`; `max_len` larger than
/// the largest class's slot size fails with `EFBIG`.
///
/// `done` receives the caller's `local` back along with the selected [`Bytes`]
/// (borrowed from the pool; recycled when the last view drops).
///
/// For a read into caller-supplied memory, see
/// [`Runtime::read`](coio_core::Runtime::read).
pub fn read_provided<G, L>(
    rt: &mut Rt<G, L>,
    fd: RawFd,
    max_len: usize,
    local: L,
    done: fn(&mut Rt<G, L>, L, io::Result<Bytes>),
) -> io::Result<Op> {
    fn shim<G, L>(rt: &mut Rt<G, L>, args: OpShimArgs<'_, L>) {
        let OpShimArgs {
            local,
            bufs,
            outcome,
            done,
            class,
        } = args;
        // The kernel chose the buffer, so there is no caller buffer to rebuild.
        expect_bufs(bufs, 0);

        let res = match outcome {
            OpOutcome::Completed { result, flags } => match errno(result) {
                Some(e) => Err(e),
                None => match io_uring::cqueue::buffer_select(flags) {
                    Some(local_buf) => Ok(provided_bytes(class, local_buf, result as u32)),
                    None => Err(io::Error::from_raw_os_error(libc::ENOBUFS)),
                },
            },
            OpOutcome::Cancelled => Err(cancelled()),
        };

        if is_teardown(done) {
            drop(local);
            return;
        }
        // SAFETY: `read_provided` registered `done` with this exact type.
        let done: fn(&mut Rt<G, L>, L, io::Result<Bytes>) =
            // SAFETY: the op this shim belongs to registered `done` with
            // exactly this type.
            unsafe { core::mem::transmute::<*const (), _>(done) };
        done(rt, local, res);
    }

    let class = rt
        .pools()
        .provided_pool
        .class_for(max_len)
        .ok_or_else(|| io::Error::from_raw_os_error(libc::EFBIG))?;
    let entry = io_uring::opcode::Read::new(
        io_uring::types::Fd(fd),
        std::ptr::null_mut(),
        max_len as u32,
    )
    .buf_group(u16::from(class))
    .build()
    .flags(io_uring::squeue::Flags::BUFFER_SELECT);
    rt.register_op(shim::<G, L>, done as *const (), local, class, &[], entry)
}

/// Writes `len` bytes from `addr` to `fd` via `IORING_OP_WRITE_FIXED`.
///
/// `addr` must point into the runtime's buffer slab, since `WRITE_FIXED`
/// addresses the registered slab by absolute offset. The whole slab is
/// registered as fixed buffer index 0 (see `build_pools`), so the address may
/// come from either pool:
///
/// * a [`Bytes`](crate::buf::Bytes) from [`alloc_bytes`](crate::alloc_bytes),
///   or
/// * a [`Bytes`](crate::buf::Bytes) returned by
///   [`read_provided`](crate::io::read_provided), i.e. straight out of a
///   provided buffer — so a read can be relayed to another fd with no copy into
///   the fixed region.
///
/// The caller must keep the buffer alive — e.g. by holding its `Bytes` inside
/// `local` — until `done` runs; coio does not own it. Holding it also keeps a
/// provided slot out of the pool for the op's duration, so the kernel cannot
/// hand the same slot to a later read. A partial write is reported as the byte
/// count; loop (re-arming `write` with the unwritten tail) to write
/// everything.
pub fn write_fixed<G, L>(
    rt: &mut Rt<G, L>,
    fd: RawFd,
    addr: *const u8,
    len: usize,
    local: L,
    done: fn(&mut Rt<G, L>, L, io::Result<usize>),
) -> io::Result<Op> {
    fn shim<G, L>(rt: &mut Rt<G, L>, args: OpShimArgs<'_, L>) {
        let OpShimArgs {
            local,
            bufs,
            outcome,
            done,
            ..
        } = args;
        expect_bufs(bufs, 0);
        let res = transferred(outcome).map(|n| n as usize);

        if is_teardown(done) {
            drop(local);
            return;
        }
        // SAFETY: `write_fixed` registered `done` with this exact type.
        let done: fn(&mut Rt<G, L>, L, io::Result<usize>) =
            // SAFETY: the op this shim belongs to registered `done` with
            // exactly this type.
            unsafe { core::mem::transmute::<*const (), _>(done) };
        done(rt, local, res);
    }

    let entry =
        io_uring::opcode::WriteFixed::new(io_uring::types::Fd(fd), addr, len as u32, 0).build();
    rt.register_op(shim::<G, L>, done as *const (), local, 0, &[], entry)
}

/// The maximum number of `iovec`s the kernel accepts for one message: Linux
/// `UIO_MAXIOV`/`IOV_MAX` (`linux/include/uapi/linux/uio.h`). A larger
/// `msg_iovlen` is rejected with `EINVAL` by `import_iovec`.
pub const MAX_IOV_CAP: usize = 1024;

/// The maximum number of file descriptors one `SCM_RIGHTS` message can carry:
/// Linux `SCM_MAX_FD` (`net/core/scm.c`). Each fd is a `c_int` of payload.
const SCM_MAX_FD: usize = 253;

/// The largest control buffer (in bytes) the kernel accepts for one message:
/// `CMSG_SPACE` of `SCM_MAX_FD` fds, i.e. the most control data a single
/// `SCM_RIGHTS` message can carry. Larger `msg_controllen` values are not an
/// error per se, but no control message type supports more.
pub const MAX_CTRL_CAP: usize = cmsg_space(SCM_MAX_FD * size_of::<libc::c_int>());

/// A reusable set of pooled, pinned argument objects for one
/// [`sendmsg`] op.
///
/// [`Msg::new`] allocates a `msghdr` plus an array of
/// [`MAX_IOV_CAP`] `iovec`s and a [`MAX_CTRL_CAP`]-byte control buffer from
/// the runtime's fixed buffer pool, zeroes the `msghdr`, and wires its
/// `msg_iov`/`msg_control` pointers to the pooled memory. The arguments
/// therefore never move while the views are alive, so a slot can be reused
/// across ops. Pass it borrowed to [`sendmsg`], which only needs its address
/// to build the submission.
///
/// The arrays behave like static-sized vecs: the capacity is fixed
/// ([`MAX_IOV_CAP`]/[`MAX_CTRL_CAP`]), the current length lives in the
/// `msghdr` (`msg_iovlen` and `msg_controllen`), and entries are appended with
/// [`Msg::push_iov`], [`Msg::push_cmsg`] and [`Msg::push_scm_rights`]. The
/// lengths therefore always reflect exactly what was pushed, so a send never
/// hands the kernel uninitialized entries. To start a fresh message, call
/// [`Msg::clear`], which resets both lengths to zero.
///
/// `Msg::new` returns `None` when the pool cannot serve a requested
/// allocation (a size class is exhausted). The pool pins the *arguments*; the
/// data buffers the `iovec`s point at are still the caller's to fill and keep
/// alive for the duration of the op. The receive counterpart is
/// [`MsgMut`], which the kernel fills and whose results are read back through
/// [`MsgMut::msg`], [`MsgMut::iov`] and [`MsgMut::ctrl`].
pub struct Msg {
    msg: RefMut<libc::msghdr>,
    iov: RefMut<[MaybeUninit<libc::iovec>; MAX_IOV_CAP]>,
    ctrl: RefMut<[u8; MAX_CTRL_CAP]>,
}

const _: () = assert!(size_of::<Msg>() == 36);

impl Msg {
    /// Allocates the slot's pooled arguments from the runtime's fixed buffer
    /// pool and wires the zeroed `msghdr` to them. Returns `None` when the
    /// pool cannot serve a requested allocation (a size class is exhausted).
    /// The lengths start at zero: data is declared by pushing with
    /// [`Msg::push_iov`], [`Msg::push_cmsg`] and [`Msg::push_scm_rights`].
    pub fn new() -> Option<Self> {
        let msg: RefMut<libc::msghdr> = unsafe { alloc_mut::<libc::msghdr>()?.cast() };
        let iov = unsafe {
            alloc_mut::<[MaybeUninit<libc::iovec>; MAX_IOV_CAP]>()?
                .cast::<[MaybeUninit<libc::iovec>; MAX_IOV_CAP]>()
        };
        let ctrl = unsafe { alloc_mut::<[u8; MAX_CTRL_CAP]>()?.cast::<[u8; MAX_CTRL_CAP]>() };

        unsafe {
            let msg = msg.as_ptr().as_mut();
            *msg = core::mem::zeroed();
            msg.msg_iov = iov.as_ptr().as_ptr().cast();
            msg.msg_control = ctrl.as_ptr().as_ptr().cast();
        }

        Some(Self { msg, iov, ctrl })
    }

    /// Appends one `iovec` to the message, updating `msg_iovlen`. Returns
    /// `false` when the vec is full; the iovec is then not pushed.
    pub fn push_iov(&mut self, iov: libc::iovec) -> bool {
        unsafe {
            let msg_ptr = self.msg_ptr();
            let len = (*msg_ptr).msg_iovlen;
            if len >= MAX_IOV_CAP {
                return false;
            }
            let base = self.iov.as_ptr().cast::<MaybeUninit<libc::iovec>>();
            base.add(len).write(MaybeUninit::new(iov));
            (*msg_ptr).msg_iovlen = len + 1;
        }
        true
    }

    /// Appends one control message (`level`/`kind` with `payload`) to the
    /// message, adding its `CMSG_SPACE` to `msg_controllen`. Returns `false`
    /// when the buffer cannot fit the header plus the payload; nothing is
    /// then pushed.
    ///
    /// The append base is the current `msg_controllen`, so after a receive it
    /// is the received length: the new message is sent after the received
    /// control bytes (fd-forwarding). Call [`Msg::clear`] first to send a
    /// fresh message instead.
    pub fn push_cmsg(&mut self, level: libc::c_int, kind: libc::c_int, payload: &[u8]) -> bool {
        let space = cmsg_space(payload.len());
        unsafe {
            let msg_ptr = self.msg_ptr();
            let used = (*msg_ptr).msg_controllen;
            if used + space > MAX_CTRL_CAP {
                return false;
            }
            let base = self.ctrl.as_ptr().cast::<u8>();
            let hdr = base.add(used).as_ptr() as *mut libc::cmsghdr;
            (*hdr).cmsg_len = size_of::<libc::cmsghdr>() + payload.len();
            (*hdr).cmsg_level = level;
            (*hdr).cmsg_type = kind;
            core::ptr::copy_nonoverlapping(
                payload.as_ptr(),
                hdr.add(1).cast::<u8>(),
                payload.len(),
            );
            (*msg_ptr).msg_controllen = used + space;
        }
        true
    }

    /// Appends an `SCM_RIGHTS` control message passing the given fds.
    /// Equivalent to `push_cmsg(SOL_SOCKET, SCM_RIGHTS, …)`.
    pub fn push_scm_rights(&mut self, fds: &[RawFd]) -> bool {
        let payload =
            unsafe { core::slice::from_raw_parts(fds.as_ptr().cast::<u8>(), size_of_val(fds)) };
        self.push_cmsg(libc::SOL_SOCKET, libc::SCM_RIGHTS, payload)
    }

    /// Empties the message: resets `msg_iovlen` and `msg_controllen` to zero
    /// so the slot can be refilled (and reused) for another op. The pooled
    /// buffers are untouched.
    pub fn clear(&mut self) {
        let msg = unsafe { self.msg.as_ptr().as_mut() };
        msg.msg_iovlen = 0;
        msg.msg_controllen = 0;
    }

    /// The wrapped `msghdr`, initialized and wired to the pooled iov/control
    /// memory by [`Msg::new`].
    pub fn msg(&mut self) -> &mut libc::msghdr {
        unsafe { self.msg.as_ptr().as_mut() }
    }

    /// The pooled `iovec` at index `i`, for setting up a send. Bounded by the
    /// current `msg_iovlen`, so entries beyond what was pushed are never
    /// exposed. Returns `None` when `i` is out of range.
    pub fn iov(&mut self, i: usize) -> Option<&mut libc::iovec> {
        if i >= self.msg().msg_iovlen {
            return None;
        }
        unsafe {
            let base = self.iov.as_ptr().cast::<MaybeUninit<libc::iovec>>();
            Some(base.add(i).as_mut().assume_init_mut())
        }
    }

    /// The control bytes: the space the pushed `cmsg`s occupy. Bounded by
    /// [`MAX_CTRL_CAP`].
    pub fn ctrl(&mut self) -> Option<&mut [u8]> {
        let len = self.msg().msg_controllen.min(MAX_CTRL_CAP);
        unsafe {
            Some(core::slice::from_raw_parts_mut(
                self.ctrl.as_ptr().as_ptr().cast::<u8>(),
                len,
            ))
        }
    }

    /// The address of the wired `msghdr`, used to build the submission entry.
    pub fn msg_ptr(&self) -> *mut libc::msghdr {
        self.msg.as_ptr().as_ptr()
    }
}

/// A reusable set of pooled, pinned argument objects for one
/// [`recvmsg`] op: the mutable counterpart of [`Msg`].
///
/// [`MsgMut::new`] allocates the same fixed-size `msghdr`/`iovec`/control
/// arguments as [`Msg`] and wires them up. The caller offers receive buffers
/// by pushing `iovec`s with [`MsgMut::push_iov`]; [`recvmsg`] then hands the
/// kernel the full control capacity [`MAX_CTRL_CAP`] and the pushed `iovec`s,
/// and on completion the kernel clamps `msg_controllen` to what actually
/// arrived and sets `msg_namelen`/`msg_flags`. Received data, flags and
/// control bytes are read back through [`MsgMut::msg`], [`MsgMut::iov`] and
/// [`MsgMut::ctrl`] after the future resolves; [`MsgMut::take_iov`] copies the
/// received `iovec` metadata out so the slot can be released while the caller
/// keeps the list of data buffers.
pub struct MsgMut {
    msg: RefMut<libc::msghdr>,
    iov: RefMut<[MaybeUninit<libc::iovec>; MAX_IOV_CAP]>,
    ctrl: RefMut<[u8; MAX_CTRL_CAP]>,
}

const _: () = assert!(size_of::<MsgMut>() == 36);

impl MsgMut {
    /// Allocates the slot's pooled arguments from the runtime's fixed buffer
    /// pool and wires the zeroed `msghdr` to them. Returns `None` when the
    /// pool cannot serve a requested allocation (a size class is exhausted).
    /// The lengths start at zero: receive buffers are declared with
    /// [`MsgMut::push_iov`].
    pub fn new() -> Option<Self> {
        let msg: RefMut<libc::msghdr> = unsafe { alloc_mut::<libc::msghdr>()?.cast() };
        let iov = unsafe {
            alloc_mut::<[MaybeUninit<libc::iovec>; MAX_IOV_CAP]>()?
                .cast::<[MaybeUninit<libc::iovec>; MAX_IOV_CAP]>()
        };
        let ctrl = unsafe { alloc_mut::<[u8; MAX_CTRL_CAP]>()?.cast::<[u8; MAX_CTRL_CAP]>() };

        unsafe {
            let msg = msg.as_ptr().as_mut();
            *msg = core::mem::zeroed();
            msg.msg_iov = iov.as_ptr().as_ptr().cast();
            msg.msg_control = ctrl.as_ptr().as_ptr().cast();
        }

        Some(Self { msg, iov, ctrl })
    }

    /// Appends one `iovec` offering a receive buffer, updating `msg_iovlen`.
    /// Returns `false` when the vec is full; the iovec is then not pushed.
    pub fn push_iov(&mut self, iov: libc::iovec) -> bool {
        unsafe {
            let msg_ptr = self.msg_ptr();
            let len = (*msg_ptr).msg_iovlen;
            if len >= MAX_IOV_CAP {
                return false;
            }
            let base = self.iov.as_ptr().cast::<MaybeUninit<libc::iovec>>();
            base.add(len).write(MaybeUninit::new(iov));
            (*msg_ptr).msg_iovlen = len + 1;
        }
        true
    }

    /// The wrapped `msghdr`, initialized and wired to the pooled iov/control
    /// memory by [`MsgMut::new`]. The kernel updates it on a receive:
    /// `msg_flags`, `msg_namelen` and `msg_controllen` are meaningful after
    /// the op resolves.
    pub fn msg(&mut self) -> &mut libc::msghdr {
        unsafe { self.msg.as_ptr().as_mut() }
    }

    /// The pooled `iovec` at index `i`, for reading what a receive consumed.
    /// Bounded by the current `msg_iovlen`. Returns `None` when `i` is out of
    /// range.
    pub fn iov(&mut self, i: usize) -> Option<&mut libc::iovec> {
        if i >= self.msg().msg_iovlen {
            return None;
        }
        unsafe {
            let base = self.iov.as_ptr().cast::<MaybeUninit<libc::iovec>>();
            Some(base.add(i).as_mut().assume_init_mut())
        }
    }

    /// The control bytes: after a receive, exactly what the kernel reported
    /// (`msg_controllen`). Bounded by [`MAX_CTRL_CAP`].
    pub fn ctrl(&mut self) -> Option<&mut [u8]> {
        let len = self.msg().msg_controllen.min(MAX_CTRL_CAP);
        unsafe {
            Some(core::slice::from_raw_parts_mut(
                self.ctrl.as_ptr().as_ptr().cast::<u8>(),
                len,
            ))
        }
    }

    /// Copies the received `iovec`s out of the pooled array and resets
    /// `msg_iovlen` to zero, so the slot's pooled arguments can be released
    /// while the caller keeps the list of data buffers the kernel filled.
    pub fn take_iov(&mut self) -> Vec<libc::iovec> {
        unsafe {
            let msg = self.msg.as_ptr().as_mut();
            let len = msg.msg_iovlen;
            let mut out = Vec::with_capacity(len);
            let base = self.iov.as_ptr().cast::<MaybeUninit<libc::iovec>>();
            for i in 0..len {
                out.push(base.add(i).read().assume_init());
            }
            msg.msg_iovlen = 0;
            out
        }
    }

    /// The address of the wired `msghdr`, used to build the submission entry.
    pub fn msg_ptr(&self) -> *mut libc::msghdr {
        self.msg.as_ptr().as_ptr()
    }
}

/// `CMSG_SPACE(n)`: the `msg_controllen` value that covers a `cmsg` carrying
/// `n` payload bytes — the `cmsg` plus trailing alignment to the next `cmsg`.
pub(crate) const fn cmsg_space(payload: usize) -> usize {
    let len = size_of::<libc::cmsghdr>() + payload;
    (len + size_of::<libc::cmsghdr>() - 1) & !(size_of::<libc::cmsghdr>() - 1)
}

/// Receives a message into `slot` via `IORING_OP_RECVMSG`, yielding the
/// number of bytes received to `done`. `flags` is passed to the kernel as-is,
/// so `MSG_PEEK | MSG_TRUNC` (the peek phase of a two-phase datagram receive)
/// is supported. Peer credentials and passed fds are read from `slot` (owned
/// by `local`) once `done` runs.
///
/// `msg` is the wired `msghdr` pointer (from `MsgMut`); it must stay valid
/// (i.e. the owning `MsgMut` kept alive in `local`) until `done` runs.
///
/// # Safety
///
/// `msg` must point at a valid, wired `msghdr` (from a [`MsgMut`]) that stays
/// alive until `done` runs.
pub unsafe fn recvmsg<G, L>(
    rt: &mut Rt<G, L>,
    fd: RawFd,
    msg: *mut libc::msghdr,
    flags: u32,
    local: L,
    done: fn(&mut Rt<G, L>, L, io::Result<usize>),
) -> io::Result<Op> {
    fn shim<G, L>(rt: &mut Rt<G, L>, args: OpShimArgs<'_, L>) {
        let OpShimArgs {
            local,
            bufs,
            outcome,
            done,
            ..
        } = args;
        // The message arguments live in the caller's local, not in a caller
        // buffer, so there is nothing here to rebuild.
        expect_bufs(bufs, 0);
        let res = transferred(outcome).map(|n| n as usize);

        if is_teardown(done) {
            drop(local);
            return;
        }
        // SAFETY: this operation registered `done` with this exact type.
        let done: fn(&mut Rt<G, L>, L, io::Result<usize>) =
            // SAFETY: the op this shim belongs to registered `done` with
            // exactly this type.
            unsafe { core::mem::transmute::<*const (), _>(done) };
        done(rt, local, res);
    }

    unsafe {
        (*msg).msg_controllen = MAX_CTRL_CAP;
    }
    let entry = io_uring::opcode::RecvMsg::new(io_uring::types::Fd(fd), msg)
        .flags(flags)
        .build();
    rt.register_op(shim::<G, L>, done as *const (), local, 0, &[], entry)
}

/// Sends the message in `slot` via `IORING_OP_SENDMSG`, typically carrying a
/// passed fd in an `SCM_RIGHTS` cmsg, yielding the number of bytes sent to
/// `done`. `msg` is the wired `msghdr` pointer (from `Msg`), which must stay
/// valid (owning `Msg` kept alive in `local`) until `done` runs.
///
/// What is sent is everything currently in the slot, i.e. the `iovec`s and
/// `cmsg`s pushed with
/// [`Msg::push_iov`]/[`Msg::push_cmsg`]/[`Msg::push_scm_rights`].
pub fn sendmsg<G, L>(
    rt: &mut Rt<G, L>,
    fd: RawFd,
    msg: *mut libc::msghdr,
    local: L,
    done: fn(&mut Rt<G, L>, L, io::Result<usize>),
) -> io::Result<Op> {
    fn shim<G, L>(rt: &mut Rt<G, L>, args: OpShimArgs<'_, L>) {
        let OpShimArgs {
            local,
            bufs,
            outcome,
            done,
            ..
        } = args;
        // The message arguments live in the caller's local, not in a caller
        // buffer, so there is nothing here to rebuild.
        expect_bufs(bufs, 0);
        let res = transferred(outcome).map(|n| n as usize);

        if is_teardown(done) {
            drop(local);
            return;
        }
        // SAFETY: this operation registered `done` with this exact type.
        let done: fn(&mut Rt<G, L>, L, io::Result<usize>) =
            // SAFETY: the op this shim belongs to registered `done` with
            // exactly this type.
            unsafe { core::mem::transmute::<*const (), _>(done) };
        done(rt, local, res);
    }

    let entry = io_uring::opcode::SendMsg::new(io_uring::types::Fd(fd), msg).build();
    rt.register_op(shim::<G, L>, done as *const (), local, 0, &[], entry)
}

// ── The portable operations ──────────────────────────────────────────
//
// `read`, `write` and `accept` are the cross-backend ones: any backend can
// provide them, so they are declared in `coio-core` as `ReadOp`/`WriteOp`/
// `AcceptOp` and reach callers as inherent methods on `Runtime`. The bodies
// below are the same as they were as free functions; only the enclosing
// signature is now a trait impl.

impl<G, L> ReadOp<G, L> for Uring {
    fn read<Buf: IoBufMut>(
        rt: &mut Runtime<G, L, Self>,
        fd: RawFd,
        mut buf: Buf,
        local: L,
        done: fn(&mut Runtime<G, L, Self>, L, Buf, io::Result<usize>),
    ) -> io::Result<Op> {
        fn shim<Buf: IoBufMut, G, L>(rt: &mut Runtime<G, L, Uring>, args: OpShimArgs<'_, L>) {
            let OpShimArgs {
                local,
                bufs,
                outcome,
                done,
                ..
            } = args;
            expect_bufs(bufs, 1);

            let res = transferred(outcome);
            // SAFETY: `read` erased this buffer's writable region on the way in,
            // and the kernel has written into exactly that region since.
            let buf: Buf = unsafe { recover(bufs[0], res.as_ref().map_or(0, |n| *n)) };

            if is_teardown(done) {
                drop((buf, local));
                return;
            }
            // SAFETY: `read` registered `done` with this exact type.
            let done: ByteDone<G, L, Uring, Buf> =
                unsafe { core::mem::transmute::<*const (), _>(done) };
            done(rt, local, buf, res.map(|n| n as usize));
        }

        let (addr, len) = buf.writable();
        let entry = io_uring::opcode::Read::new(io_uring::types::Fd(fd), addr, len).build();
        let erased = [buf.into_erased()];
        rt.register_op(
            shim::<Buf, G, L>,
            done as *const (),
            local,
            0,
            &erased,
            entry,
        )
    }
}

impl<G, L> WriteOp<G, L> for Uring {
    fn write<Buf: IoBufRef>(
        rt: &mut Runtime<G, L, Self>,
        fd: RawFd,
        buf: Buf,
        local: L,
        done: fn(&mut Runtime<G, L, Self>, L, Buf, io::Result<usize>),
    ) -> io::Result<Op> {
        fn shim<Buf: IoBufRef, G, L>(rt: &mut Runtime<G, L, Uring>, args: OpShimArgs<'_, L>) {
            let OpShimArgs {
                local,
                bufs,
                outcome,
                done,
                ..
            } = args;
            expect_bufs(bufs, 1);

            let res = transferred(outcome);
            // SAFETY: `write` erased this buffer on the way in, so the runtime
            // has held it untouched since; the op only ever read from it.
            let buf: Buf = unsafe { recover(bufs[0], res.as_ref().map_or(0, |n| *n)) };

            if is_teardown(done) {
                drop((buf, local));
                return;
            }
            // SAFETY: `write` registered `done` with this exact type.
            let done: ByteDone<G, L, Uring, Buf> =
                unsafe { core::mem::transmute::<*const (), _>(done) };
            done(rt, local, buf, res.map(|n| n as usize));
        }

        let (addr, len) = buf.readable();
        let entry = io_uring::opcode::Write::new(io_uring::types::Fd(fd), addr, len).build();
        let erased = [buf.into_erased()];
        rt.register_op(
            shim::<Buf, G, L>,
            done as *const (),
            local,
            0,
            &erased,
            entry,
        )
    }
}

impl<G, L> AcceptOp<G, L> for Uring {
    fn accept(
        rt: &mut Runtime<G, L, Self>,
        listener_fd: RawFd,
        local: L,
        done: fn(&mut Runtime<G, L, Self>, L, io::Result<RawFd>),
    ) -> io::Result<Op> {
        fn shim<G, L>(rt: &mut Runtime<G, L, Uring>, args: OpShimArgs<'_, L>) {
            let OpShimArgs {
                local,
                bufs,
                outcome,
                done,
                ..
            } = args;
            expect_bufs(bufs, 0);
            let res = match outcome {
                OpOutcome::Completed { result, .. } => match errno(result) {
                    Some(e) => Err(e),
                    None => Ok(result),
                },
                OpOutcome::Cancelled => Err(cancelled()),
            };

            if is_teardown(done) {
                drop(local);
                return;
            }
            // SAFETY: `accept` registered `done` with this exact type.
            let done: AcceptDone<G, L, Uring> =
                unsafe { core::mem::transmute::<*const (), _>(done) };
            done(rt, local, res);
        }

        let entry = io_uring::opcode::Accept::new(
            io_uring::types::Fd(listener_fd),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
        .build();
        rt.register_op(shim::<G, L>, done as *const (), local, 0, &[], entry)
    }
}
