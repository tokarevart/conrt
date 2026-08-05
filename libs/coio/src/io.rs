//! Async io operations driven by the runtime.
//!
//! Each function submits a single `io_uring` operation for the caller's task
//! and awaits its completion. While an op is in flight it occupies a slot in
//! the runtime's io slab, which is recycled once the CQE is consumed.

use core::future::Future;
use core::pin::Pin;
use core::task::Context;
use core::task::Poll;
use std::io;
use std::os::fd::RawFd;

use crate::levels::level_for;
use crate::levels::pack_bid;
use crate::pbuf::ReadBuffer;
use crate::runtime;
use crate::task::TaskContext;
use crate::wbuf::WriteBuffer;

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
/// level whose slot size is at least `max_len`; `max_len` larger than the
/// largest level's slot size fails with `EFBIG`.
///
/// The returned [`ReadBuffer`] borrows memory from the pool: its slot is
/// recycled when the buffer is dropped, so the runtime must still be alive
/// while the buffer is in use.
pub async fn read(ctx: TaskContext, fd: RawFd, max_len: usize) -> io::Result<ReadBuffer> {
    let bgid = ctx.with_runtime(|r| -> io::Result<u16> {
        let level = level_for(&r.read_levels, max_len)
            .ok_or_else(|| io::Error::from_raw_os_error(libc::EFBIG))?;
        Ok(level as u16)
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
    let level = bgid as usize;
    let (offset, bid) = ctx.with_runtime(|r| {
        let slot_ref = &r.io_slab[slot as usize];
        let local = slot_ref.bid;
        let offset = r.buffer_pools[level].slot_offset(local);
        let bid = pack_bid(level as u32, u32::from(local));
        r.free_io_slot(slot);
        (offset, bid)
    });
    ctx.with_task(|task| task.io.remove_value(slot));

    Ok(ReadBuffer::new(offset, bid, result as u32, generation))
}

/// Acquires a zero-copy write buffer backed by a slot from the runtime's fixed
/// write buffer slab. The buffer is drawn from the smallest level whose slot
/// size is at least `size`; `size` larger than the largest level's slot size
/// fails with `EFBIG`. The buffer's [`WriteBuffer::capacity`] is the chosen
/// level's slot size; the caller fills it via [`WriteBuffer::as_mut`] and
/// records the length with [`WriteBuffer::set_len`] before passing it to
/// [`write`]. The slot is recycled when the buffer is dropped.
pub fn write_buffer(ctx: TaskContext, size: usize) -> io::Result<WriteBuffer> {
    let generation = runtime::active_gen().expect("write_buffer called outside an active runtime");
    let (bid, offset) = ctx.with_runtime(|r| -> io::Result<(u32, u32)> {
        let level = level_for(&r.write_levels, size)
            .ok_or_else(|| io::Error::from_raw_os_error(libc::EFBIG))?;
        let local = r.write_pools[level]
            .acquire()
            .ok_or_else(|| io::Error::from_raw_os_error(libc::ENOMEM))?;
        let offset = r.write_pools[level].slot_offset(local);
        Ok((pack_bid(level as u32, local), offset))
    })?;
    Ok(WriteBuffer::new(offset, bid, generation))
}

/// Writes the contents of `wb` to `fd` via `IORING_OP_WRITE_FIXED`. The
/// buffer's slot is held until the kernel finishes with it and recycled when
/// `wb` is dropped.
pub async fn write(ctx: TaskContext, fd: RawFd, wb: WriteBuffer) -> io::Result<usize> {
    let len = wb.len();
    let addr =
        ctx.with_runtime(|r| unsafe { r.slab.as_ptr().cast_mut().add(wb.offset() as usize) });

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

/// Receives a message into `msg` via `IORING_OP_RECVMSG`. `flags` is passed to
/// the kernel as-is, so `MSG_PEEK | MSG_TRUNC` (used by the peek phase of a
/// two-phase datagram receive) is supported. Returns the number of bytes
/// received; peer credentials and passed fds are read from `msg` after the
/// future resolves.
///
/// # Safety
/// `msg` must point to a valid `msghdr` that the caller keeps alive and
/// unmoved for the duration of the await.
pub async fn recvmsg(
    ctx: TaskContext,
    fd: RawFd,
    msg: *mut libc::msghdr,
    flags: u32,
) -> io::Result<usize> {
    let entry = io_uring::opcode::RecvMsg::new(io_uring::types::Fd(fd), msg)
        .flags(flags)
        .build();

    let (slot, result) = submit_and_await(ctx, entry).await?;
    release_io(ctx, slot);
    Ok(result as usize)
}

/// Sends the message in `msg` via `IORING_OP_SENDMSG`, typically carrying a
/// passed fd in its `SCM_RIGHTS` cmsg. Returns the number of bytes sent.
///
/// # Safety
/// `msg` must point to a valid `msghdr` that the caller keeps alive and
/// unmoved for the duration of the await.
pub async fn sendmsg(ctx: TaskContext, fd: RawFd, msg: *const libc::msghdr) -> io::Result<usize> {
    let entry = io_uring::opcode::SendMsg::new(io_uring::types::Fd(fd), msg).build();

    let (slot, result) = submit_and_await(ctx, entry).await?;
    release_io(ctx, slot);
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
