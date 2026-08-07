//! Async io operations driven by the runtime.
//!
//! Each function submits a single `io_uring` operation for the caller's task
//! and awaits its completion. While an op is in flight it occupies a slot in
//! the runtime's io slab, which is recycled once the CQE is consumed.

use core::future::Future;
use core::mem::MaybeUninit;
use core::pin::Pin;
use core::task::Context;
use core::task::Poll;
use std::io;
use std::os::fd::RawFd;

use crate::buf::Bytes;
use crate::buf::RefMut;
use crate::task::TaskContext;

/// Submits `entry` for the caller's task and awaits its CQE result, releasing
/// the io slot on failure. On success the slot is still allocated (so the
/// caller can read per-op metadata like a read's selected buffer) and must be
/// released with [`release_io`].
async fn submit_and_await(
    ctx: TaskContext,
    entry: io_uring::squeue::Entry,
) -> io::Result<(u32, i32)> {
    let slot = ctx.with_runtime(|r| r.alloc_io_slot());
    ctx.with_task(|task| task.io.push(slot));

    if let Err(e) = ctx.push_io(entry, slot) {
        ctx.with_runtime(|r| r.free_io_slot(slot));
        ctx.with_task(|task| task.io.remove_value(slot));
        return Err(e);
    }

    let result = await_cqe(ctx, slot).await;
    if result < 0 {
        ctx.with_runtime(|r| r.free_io_slot(slot));
        ctx.with_task(|task| task.io.remove_value(slot));
        return Err(io::Error::from_raw_os_error(-result));
    }
    Ok((slot, result))
}

/// Returns an io slot allocated by [`submit_and_await`] to the runtime's free
/// list and unlinks it from the task's in-flight set.
fn release_io(ctx: TaskContext, slot: u32) {
    ctx.with_runtime(|r| r.free_io_slot(slot));
    ctx.with_task(|task| task.io.remove_value(slot));
}

/// Reads up to `max_len` bytes from `fd` into a buffer selected from the
/// runtime's provided buffer pools. The buffer is drawn from the smallest
/// size class whose slot size is at least `max_len`; `max_len` larger than
/// the largest class's slot size fails with `EFBIG`.
///
/// The returned [`Bytes`] borrows the selected slot from its slab: the slot is
/// recycled back to the ring when the last view is dropped, so the runtime
/// must still be alive while the buffer is in use.
pub async fn read(ctx: TaskContext, fd: RawFd, max_len: usize) -> io::Result<Bytes> {
    let class = ctx.with_runtime(|r| -> io::Result<u8> {
        r.provided_pool
            .class_for(max_len)
            .ok_or_else(|| io::Error::from_raw_os_error(libc::EFBIG))
    })?;

    let entry = io_uring::opcode::Read::new(
        io_uring::types::Fd(fd),
        std::ptr::null_mut(),
        max_len as u32,
    )
    .buf_group(u16::from(class))
    .build()
    .flags(io_uring::squeue::Flags::BUFFER_SELECT);

    let (slot, result) = submit_and_await(ctx, entry).await?;

    let local = ctx.with_runtime(|r| {
        let slot_ref = &r.io_slab[slot as usize];
        let local = slot_ref.bid;
        r.free_io_slot(slot);
        local
    });
    ctx.with_task(|task| task.io.remove_value(slot));

    // The kernel handed `local` to the just-completed BUFFER_SELECT read
    // above, and `result` is the number of bytes it produced, which fits in
    // the class's slot size.
    Ok(ctx.provided_bytes(class, local, result as u32))
}

/// Writes the contents of `buf` to `fd` via `IORING_OP_WRITE_FIXED`. The
/// buffer's slot is held (its borrow not released) until the kernel finishes
/// with it; `buf` is consumed and its slot recycled when it drops.
pub async fn write(ctx: TaskContext, fd: RawFd, buf: Bytes) -> io::Result<usize> {
    let len = buf.len();
    let addr = buf.as_ptr();

    let entry = io_uring::opcode::WriteFixed::new(
        io_uring::types::Fd(fd),
        addr,
        len as u32,
        0, // the whole slab is registered as fixed buffer index 0
    )
    .build();

    let (slot, result) = submit_and_await(ctx, entry).await?;
    release_io(ctx, slot);
    Ok(result as usize)
}

/// Accepts a connection on `listener_fd`, returning the new socket's fd.
pub async fn accept(ctx: TaskContext, listener_fd: RawFd) -> io::Result<RawFd> {
    let entry = io_uring::opcode::Accept::new(
        io_uring::types::Fd(listener_fd),
        std::ptr::null_mut(),
        std::ptr::null_mut(),
    )
    .build();

    let (slot, result) = submit_and_await(ctx, entry).await?;
    release_io(ctx, slot);
    Ok(result)
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
    pub fn new(ctx: TaskContext) -> Option<Self> {
        let msg: RefMut<libc::msghdr> = unsafe { ctx.alloc_mut::<libc::msghdr>()?.cast() };
        let iov = unsafe {
            ctx.alloc_mut::<[MaybeUninit<libc::iovec>; MAX_IOV_CAP]>()?
                .cast::<[MaybeUninit<libc::iovec>; MAX_IOV_CAP]>()
        };
        let ctrl = unsafe {
            ctx.alloc_mut::<[u8; MAX_CTRL_CAP]>()?
                .cast::<[u8; MAX_CTRL_CAP]>()
        };

        unsafe {
            let msg_ptr = msg.as_ptr();
            msg_ptr.write(core::mem::zeroed());
            (*msg_ptr).msg_iov = iov.as_ptr().cast();
            (*msg_ptr).msg_control = ctrl.as_ptr().cast();
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
            let hdr = base.add(used) as *mut libc::cmsghdr;
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
        let msg = unsafe { &mut *self.msg.as_ptr() };
        msg.msg_iovlen = 0;
        msg.msg_controllen = 0;
    }

    /// The wrapped `msghdr`, initialized and wired to the pooled iov/control
    /// memory by [`Msg::new`].
    pub fn msg(&mut self) -> &mut libc::msghdr {
        unsafe { &mut *self.msg.as_ptr() }
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
            Some((&mut *base.add(i)).assume_init_mut())
        }
    }

    /// The control bytes: the space the pushed `cmsg`s occupy. Bounded by
    /// [`MAX_CTRL_CAP`].
    pub fn ctrl(&mut self) -> Option<&mut [u8]> {
        let len = self.msg().msg_controllen.min(MAX_CTRL_CAP);
        unsafe {
            Some(core::slice::from_raw_parts_mut(
                self.ctrl.as_ptr().cast::<u8>(),
                len,
            ))
        }
    }

    /// The address of the wired `msghdr`, used to build the submission entry.
    pub(crate) fn msg_ptr(&self) -> *mut libc::msghdr {
        self.msg.as_ptr()
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
    pub fn new(ctx: TaskContext) -> Option<Self> {
        let msg: RefMut<libc::msghdr> = unsafe { ctx.alloc_mut::<libc::msghdr>()?.cast() };
        let iov = unsafe {
            ctx.alloc_mut::<[MaybeUninit<libc::iovec>; MAX_IOV_CAP]>()?
                .cast::<[MaybeUninit<libc::iovec>; MAX_IOV_CAP]>()
        };
        let ctrl = unsafe {
            ctx.alloc_mut::<[u8; MAX_CTRL_CAP]>()?
                .cast::<[u8; MAX_CTRL_CAP]>()
        };

        unsafe {
            let msg_ptr = msg.as_ptr();
            msg_ptr.write(core::mem::zeroed());
            (*msg_ptr).msg_iov = iov.as_ptr().cast();
            (*msg_ptr).msg_control = ctrl.as_ptr().cast();
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
        unsafe { &mut *self.msg.as_ptr() }
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
            Some((&mut *base.add(i)).assume_init_mut())
        }
    }

    /// The control bytes: after a receive, exactly what the kernel reported
    /// (`msg_controllen`). Bounded by [`MAX_CTRL_CAP`].
    pub fn ctrl(&mut self) -> Option<&mut [u8]> {
        let len = self.msg().msg_controllen.min(MAX_CTRL_CAP);
        unsafe {
            Some(core::slice::from_raw_parts_mut(
                self.ctrl.as_ptr().cast::<u8>(),
                len,
            ))
        }
    }

    /// Copies the received `iovec`s out of the pooled array and resets
    /// `msg_iovlen` to zero, so the slot's pooled arguments can be released
    /// while the caller keeps the list of data buffers the kernel filled.
    pub fn take_iov(&mut self) -> Vec<libc::iovec> {
        unsafe {
            let msg_ptr = self.msg.as_ptr();
            let len = (*msg_ptr).msg_iovlen;
            let mut out = Vec::with_capacity(len);
            let base = self.iov.as_ptr().cast::<MaybeUninit<libc::iovec>>();
            for i in 0..len {
                out.push(base.add(i).read().assume_init());
            }
            (*msg_ptr).msg_iovlen = 0;
            out
        }
    }

    /// The address of the wired `msghdr`, used to build the submission entry.
    pub(crate) fn msg_ptr(&self) -> *mut libc::msghdr {
        self.msg.as_ptr()
    }
}

/// `CMSG_SPACE(n)`: the `msg_controllen` value that covers a `cmsg` carrying
/// `n` payload bytes — the `cmsg` plus trailing alignment to the next `cmsg`.
pub(crate) const fn cmsg_space(payload: usize) -> usize {
    let len = size_of::<libc::cmsghdr>() + payload;
    (len + size_of::<libc::cmsghdr>() - 1) & !(size_of::<libc::cmsghdr>() - 1)
}

/// Receives a message into `slot` via `IORING_OP_RECVMSG`. `flags` is passed
/// to the kernel as-is, so `MSG_PEEK | MSG_TRUNC` (used by the peek phase of a
/// two-phase datagram receive) is supported. Returns the number of bytes
/// received; peer credentials and passed fds are read from `slot` after the
/// future resolves. The `iovec`s' `iov_base` data buffers are the caller's to
/// keep alive for the duration of the await.
///
/// Exactly the `iovec`s pushed with [`MsgMut::push_iov`] are offered to the
/// kernel, and the full control capacity [`MAX_CTRL_CAP`] is wired into
/// `msg_controllen` just before submission; on completion the kernel clamps it
/// to what actually arrived, so [`MsgMut::ctrl`] only exposes received bytes.
pub async fn recvmsg(
    ctx: TaskContext,
    fd: RawFd,
    slot: &mut MsgMut,
    flags: u32,
) -> io::Result<usize> {
    unsafe {
        (*slot.msg_ptr()).msg_controllen = MAX_CTRL_CAP;
    }
    let entry = io_uring::opcode::RecvMsg::new(io_uring::types::Fd(fd), slot.msg_ptr())
        .flags(flags)
        .build();

    let (slot_id, result) = submit_and_await(ctx, entry).await?;
    release_io(ctx, slot_id);
    Ok(result as usize)
}

/// Sends the message in `slot` via `IORING_OP_SENDMSG`, typically carrying a
/// passed fd in an `SCM_RIGHTS` cmsg. Returns the number of bytes sent. The
/// `iovec`s' `iov_base` data buffers are the caller's to keep alive for the
/// duration of the await.
///
/// What is sent is everything currently in the slot, i.e. the `iovec`s and
/// `cmsg`s pushed with
/// [`Msg::push_iov`]/[`Msg::push_cmsg`]/[`Msg::push_scm_rights`].
pub async fn sendmsg(ctx: TaskContext, fd: RawFd, slot: &mut Msg) -> io::Result<usize> {
    let entry = io_uring::opcode::SendMsg::new(io_uring::types::Fd(fd), slot.msg_ptr()).build();

    let (slot_id, result) = submit_and_await(ctx, entry).await?;
    release_io(ctx, slot_id);
    Ok(result as usize)
}

/// Waits until the op occupying `slot` has completed, then returns its result.
/// Used internally by the io futures; exposed so the runtime's tests can drive
/// an in-flight op directly.
pub(crate) async fn await_cqe(ctx: TaskContext, slot: u32) -> i32 {
    loop {
        let result = ctx.with_runtime(|r| {
            let slot_ref = &r.io_slab[slot as usize];
            if slot_ref.ready != 0 {
                Some(slot_ref.result)
            } else {
                None
            }
        });
        if let Some(result) = result {
            return result;
        }
        yield_now(ctx).await;
    }
}

/// Yields the current task back to the runtime loop: the first poll wakes the
/// task (re-enqueueing it) and returns `Pending`; the next poll returns
/// `Ready`.
pub fn yield_now(ctx: TaskContext) -> Yield {
    Yield { ctx, polled: false }
}

/// The future returned by [`yield_now`].
pub struct Yield {
    ctx: TaskContext,
    pub(crate) polled: bool,
}

impl Future for Yield {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        if self.polled {
            Poll::Ready(())
        } else {
            self.polled = true;
            self.ctx.wake();
            Poll::Pending
        }
    }
}
