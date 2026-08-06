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

use crate::buf::Buffer;
use crate::buf::BufferBytes;
use crate::classes::class_for;
use crate::classes::pack_bid;
use crate::pbuf::ProvidedBuffer;
use crate::runtime;
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
/// The returned [`ProvidedBuffer`] borrows memory from the pool: its slot is
/// recycled when the buffer is dropped, so the runtime must still be alive
/// while the buffer is in use.
pub async fn read(ctx: TaskContext, fd: RawFd, max_len: usize) -> io::Result<ProvidedBuffer> {
    let bgid = ctx.with_runtime(|r| -> io::Result<u16> {
        let class = class_for(&r.provided_classes, max_len)
            .ok_or_else(|| io::Error::from_raw_os_error(libc::EFBIG))?;
        Ok(class as u16)
    })?;

    let entry = io_uring::opcode::Read::new(
        io_uring::types::Fd(fd),
        std::ptr::null_mut(),
        max_len as u32,
    )
    .buf_group(bgid)
    .build()
    .flags(io_uring::squeue::Flags::BUFFER_SELECT);

    let (slot, result) = submit_and_await(ctx, entry).await?;

    let generation = runtime::active_gen().expect("read called outside an active runtime");
    let class = bgid as usize;
    let (offset, bid) = ctx.with_runtime(|r| {
        let slot_ref = &r.io_slab[slot as usize];
        let local = slot_ref.bid;
        let offset = r.provided_pools[class].slot_offset(local);
        let bid = pack_bid(class as u32, u32::from(local));
        r.free_io_slot(slot);
        (offset, bid)
    });
    ctx.with_task(|task| task.io.remove_value(slot));

    Ok(ProvidedBuffer::new(offset, bid, result as u32, generation))
}

/// Writes the contents of `buf` to `fd` via `IORING_OP_WRITE_FIXED`. The
/// buffer's slot is held until the kernel finishes with it and recycled when
/// `buf` is dropped.
pub async fn write(ctx: TaskContext, fd: RawFd, buf: BufferBytes) -> io::Result<usize> {
    let len = buf.len();
    let addr = ctx.with_runtime(|r| unsafe { r.slab.as_ptr().add(buf.offset() as usize) });

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
/// [`recvmsg`]/[`sendmsg`] op.
///
/// [`Msg::new`] allocates a `msghdr` plus — when `MAX_IOV`/`MAX_CTRL` are
/// nonzero — an array of `MAX_IOV` `iovec`s and a `MAX_CTRL`-byte control
/// buffer from the runtime's fixed buffer pool, zeroes the `msghdr`, and wires
/// its `msg_iov`/`msg_control` pointers to the pooled memory. The arguments
/// therefore never move while the slot is alive, so a slot can be reused
/// across ops. Pass it borrowed to [`recvmsg`]/[`sendmsg`], which only need
/// its address to build the submission.
///
/// Both arrays behave like static-sized vecs: the capacity is fixed in the
/// type, the current length lives in the `msghdr` (`msg_iovlen` and
/// `msg_controllen`), and entries are appended with [`Msg::push_iov`],
/// [`Msg::push_cmsg`] and [`Msg::push_scm_rights`]. The lengths
/// therefore always reflect exactly what was pushed, so a send never hands
/// the kernel uninitialized entries, and a receive resets them to the full
/// capacities before submission so the kernel can report up to that much.
///
/// `msg_controllen` carries two meanings depending on the direction, which is
/// what makes slot reuse work both ways. On a send it is the length of the
/// control bytes the caller pushed — the kernel transmits exactly that many.
/// On a receive it is an in/out capacity: [`recvmsg`] hands the kernel the
/// full `MAX_CTRL`, and the kernel clamps it to the bytes it actually wrote.
/// So after a `recvmsg` the field holds the *received* length, and calling
/// [`Msg::push_cmsg`] or [`sendmsg`] without a
/// [`Msg::clear`] in between appends to — and transmits — the
/// just-received control bytes (typically fds being forwarded) plus the new
/// entries. That is a deliberate "forward what was received and append"
/// reuse. To start a fresh send instead, call [`Msg::clear`] first, which
/// resets both lengths to zero.
///
/// `MAX_IOV`/`MAX_CTRL` are capped at what the kernel accepts
/// ([`MAX_IOV_CAP`]/[`MAX_CTRL_CAP`]); the pool may still not have a class
/// large enough for the requested capacities, in which case [`Msg::new`]
/// returns `None`. Exceeding the kernel caps is a compile-time error (the
/// coercion below forces monomorphization, which const-evaluates the check):
///
/// ```compile_fail
/// use coio::io::Msg;
/// // 1025 iovecs is more than the kernel's UIO_MAXIOV.
/// fn main() {
///     let _: fn(coio::task::TaskContext) -> Option<Msg<1025, { coio::MAX_CTRL_CAP }>> =
///         Msg::<1025, { coio::MAX_CTRL_CAP }>::new;
/// }
/// ```
///
/// ```compile_fail
/// use coio::io::Msg;
/// // A control buffer bigger than CMSG_SPACE(SCM_MAX_FD fds).
/// fn main() {
///     let _: fn(coio::task::TaskContext) -> Option<Msg<1024, 2000>> =
///         Msg::<1024, 2000>::new;
/// }
/// ```
///
/// The pool pins the *arguments*; the data buffers the `iovec`s point at are
/// still the caller's to fill and keep alive for the duration of the op.
/// After [`recvmsg`] resolves, received data, flags and control bytes are
/// read back through [`Msg::msg`], [`Msg::iov`] and [`Msg::ctrl`].
pub struct Msg<const MAX_IOV: usize = MAX_IOV_CAP, const MAX_CTRL: usize = MAX_CTRL_CAP> {
    msg: Buffer<libc::msghdr>,
    iov: Option<Buffer<[MaybeUninit<libc::iovec>; MAX_IOV]>>,
    ctrl: Option<Buffer<[u8; MAX_CTRL]>>,
}

const _: () = assert!(size_of::<Msg<4, 128>>() == 24);

impl<const MAX_IOV: usize, const MAX_CTRL: usize> Msg<MAX_IOV, MAX_CTRL> {
    /// Allocates the slot's pooled arguments from the runtime's fixed buffer
    /// pool and wires the zeroed `msghdr` to them. Returns `None` when the
    /// pool cannot serve a requested allocation (a size class is exhausted).
    /// The lengths start at zero: data is declared by pushing with
    /// [`Msg::push_iov`], [`Msg::push_cmsg`] and
    /// [`Msg::push_scm_rights`]. The reference forces the const-eval
    /// capacity check to run at monomorphization, making an over-cap
    /// instantiation a compile error.
    #[allow(clippy::let_unit_value)]
    pub fn new(ctx: TaskContext) -> Option<Self> {
        const {
            assert!(
                MAX_IOV <= MAX_IOV_CAP,
                "Msg MAX_IOV exceeds the kernel's maximum (MAX_IOV_CAP)"
            );
            assert!(
                MAX_CTRL <= MAX_CTRL_CAP,
                "Msg MAX_CTRL exceeds the kernel's maximum (MAX_CTRL_CAP)"
            );
        }
        // Safety of the casts: each strips the redundant outer `MaybeUninit`
        // that `alloc` returns. The msghdr is initialized (zeroed) below
        // before any read; the iov array stays element-wise `MaybeUninit`, so
        // no iovec is ever assumed initialized. The ctrl bytes are `u8`s, for
        // which every bit pattern is valid, so no `MaybeUninit` is needed.
        let msg: Buffer<libc::msghdr> = unsafe { ctx.alloc::<libc::msghdr>()?.cast() };
        let iov: Option<Buffer<[MaybeUninit<libc::iovec>; MAX_IOV]>> = if MAX_IOV > 0 {
            Some(unsafe { ctx.alloc::<[MaybeUninit<libc::iovec>; MAX_IOV]>()?.cast() })
        } else {
            None
        };
        let ctrl: Option<Buffer<[u8; MAX_CTRL]>> = if MAX_CTRL > 0 {
            Some(unsafe { ctx.alloc::<[u8; MAX_CTRL]>()?.cast() })
        } else {
            None
        };

        unsafe {
            let msg_ptr = msg.as_ptr();
            msg_ptr.write(core::mem::zeroed());
            if let Some(iov) = &iov {
                (*msg_ptr).msg_iov = iov.as_ptr().cast();
            }
            if let Some(ctrl) = &ctrl {
                (*msg_ptr).msg_control = ctrl.as_ptr().cast();
            }
        }

        Some(Self { msg, iov, ctrl })
    }

    /// Appends one `iovec` to the message, updating `msg_iovlen`. Returns
    /// `false` when `MAX_IOV` is zero or the vec is full; the iovec is then
    /// not pushed.
    pub fn push_iov(&mut self, iov: libc::iovec) -> bool {
        unsafe {
            let msg_ptr = self.msg_ptr();
            let len = (*msg_ptr).msg_iovlen;
            if len >= MAX_IOV {
                return false;
            }
            let Some(iov_slot) = self.iov.as_mut() else {
                return false;
            };
            let base = iov_slot.as_ptr().cast::<MaybeUninit<libc::iovec>>();
            base.add(len).write(MaybeUninit::new(iov));
            (*msg_ptr).msg_iovlen = len + 1;
        }
        true
    }

    /// Appends one control message (`level`/`kind` with `payload`) to the
    /// message, adding its `CMSG_SPACE` to `msg_controllen`. Returns `false`
    /// when `MAX_CTRL` is zero or the buffer cannot fit the header plus the
    /// payload; nothing is then pushed.
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
            if used + space > MAX_CTRL {
                return false;
            }
            let Some(ctrl_slot) = self.ctrl.as_mut() else {
                return false;
            };
            let base = ctrl_slot.as_ptr().cast::<u8>();
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
    /// buffers are untouched. Call this when switching from a receive to a
    /// fresh send; without it, pushes append to the received control bytes.
    pub fn clear(&mut self) {
        let msg = unsafe { &mut *self.msg.as_ptr() };
        msg.msg_iovlen = 0;
        msg.msg_controllen = 0;
    }

    /// The wrapped `msghdr`, initialized and wired to the pooled iov/control
    /// memory by [`Msg::new`]. The kernel updates it on a receive:
    /// `msg_flags`, `msg_namelen` and `msg_controllen` are meaningful after
    /// the op resolves.
    pub fn msg(&mut self) -> &mut libc::msghdr {
        unsafe { &mut *self.msg.as_ptr() }
    }

    /// The pooled `iovec` at index `i`, for setting up a send or reading what
    /// a receive consumed. Bounded by the current `msg_iovlen`, so entries
    /// beyond what was pushed (or offered on a receive) are never exposed.
    /// Returns `None` when `MAX_IOV` is zero or `i` is out of range.
    pub fn iov(&mut self, i: usize) -> Option<&mut libc::iovec> {
        if i >= self.msg().msg_iovlen {
            return None;
        }
        let slot = self.iov.as_mut()?;
        unsafe {
            let base = slot.as_ptr().cast::<MaybeUninit<libc::iovec>>();
            Some((&mut *base.add(i)).assume_init_mut())
        }
    }

    /// The control bytes: after a send, the space the pushed `cmsg`s occupy;
    /// after a receive, exactly what the kernel reported (`msg_controllen`).
    /// Bounded by `MAX_CTRL`. Returns `None` when `MAX_CTRL` is zero.
    pub fn ctrl(&mut self) -> Option<&mut [u8]> {
        let len = self.msg().msg_controllen.min(MAX_CTRL);
        let slot = self.ctrl.as_mut()?;
        unsafe {
            Some(core::slice::from_raw_parts_mut(
                slot.as_ptr().cast::<u8>(),
                len,
            ))
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
/// Exactly the `iovec`s pushed with [`Msg::push_iov`] are offered to the
/// kernel, and the full control capacity `MAX_CTRL` is wired into
/// `msg_controllen` just before submission; on completion the kernel clamps it
/// to what actually arrived, so [`Msg::ctrl`] only exposes received bytes.
pub async fn recvmsg<const MAX_IOV: usize, const MAX_CTRL: usize>(
    ctx: TaskContext,
    fd: RawFd,
    slot: &mut Msg<MAX_IOV, MAX_CTRL>,
    flags: u32,
) -> io::Result<usize> {
    unsafe {
        (*slot.msg_ptr()).msg_controllen = MAX_CTRL;
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
/// What is sent is everything currently in the slot. After a [`recvmsg`] that
/// includes the received control bytes, so a slot can forward them in the
/// same send; call [`Msg::clear`] first to send only freshly pushed
/// entries.
pub async fn sendmsg<const MAX_IOV: usize, const MAX_CTRL: usize>(
    ctx: TaskContext,
    fd: RawFd,
    slot: &mut Msg<MAX_IOV, MAX_CTRL>,
) -> io::Result<usize> {
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
