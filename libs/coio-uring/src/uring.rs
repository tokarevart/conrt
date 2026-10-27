//! The `io_uring` backend.
//!
//! Owns the ring and the buffer pools that back every read and fixed write, and
//! encodes the runtime's completion naming into `user_data`. Nothing here can
//! reach the op slabs or the caller's state: the whole surface is
//! [`Backend`], plus a couple of accessors for the `io_uring`-specific io
//! operations in [`crate::io`].

use std::io;
use std::os::fd::RawFd;

use coio_core::Backend;
use coio_core::Completion;
use coio_core::Op;
use coio_core::Runtime;
use coio_core::enter_active_gen;

use crate::buf::Bytes;
use crate::classes::DEFAULT_SIZE_CLASSES;
use crate::classes::SizeClass;
use crate::pools::DropGuard;
use crate::pools::Pools;
use crate::pools::build_pools;
use crate::pools::clear_current_pools;
use crate::pools::set_current_pools;

/// Construction parameters for [`Uring`].
///
/// Owned rather than borrowed so that `Backend::Config` needs no lifetime: a
/// backend cannot name a lifetime in an associated type without that lifetime
/// infecting the runtime and every operation's signature. The class tables are
/// read exactly once, at startup, so two small `Vec`s cost nothing next to
/// allocating and registering the slab itself.
#[derive(Debug, Clone)]
pub struct UringConfig {
    /// Submission and completion queue depth handed to the ring.
    pub ring_entries: u32,
    /// Provided-buffer size classes for reads. Each `size` must be a power of
    /// two, and each `count` a power of two no larger than 32768.
    pub provided_size_classes: Vec<SizeClass>,
    /// Fixed-buffer size classes for writes and op-argument allocations. Each
    /// `size` must be a power of two.
    pub size_classes: Vec<SizeClass>,
}

impl Default for UringConfig {
    fn default() -> Self {
        Self {
            ring_entries: 1024,
            provided_size_classes: DEFAULT_SIZE_CLASSES.to_vec(),
            size_classes: DEFAULT_SIZE_CLASSES.to_vec(),
        }
    }
}

/// Kept under the old name so callers written against it keep compiling.
pub type RuntimeParams = UringConfig;

// ── user_data encoding ────────────────────────────────────────────────

/// Names a completion. io_uring carries no slot of its own, so the runtime's
/// slot index is packed into `user_data` and recovered when the CQE lands.
#[repr(C)]
#[derive(Clone, Copy)]
struct UserData {
    index: u32,
    /// High bit set marks a cancel completion; the rest is the op's
    /// dispatch-time `class`.
    tag: u32,
}

const CANCEL_TAG: u32 = 1 << 31;

/// Low seven bits of the tag carry `class`, which is why it is an op count
/// bounded by the number of provided-buffer classes.
const CLASS_MASK: u32 = 0x7f;

impl From<UserData> for u64 {
    fn from(ud: UserData) -> u64 {
        u64::from(ud.index) | (u64::from(ud.tag) << 32)
    }
}

impl From<u64> for UserData {
    fn from(ud: u64) -> Self {
        UserData {
            index: ud as u32,
            tag: (ud >> 32) as u32,
        }
    }
}

/// The `user_data` for an SQE completing the op in `slot`.
fn op_user_data(slot: u32, class: u8) -> u64 {
    assert!(
        u32::from(class) <= CLASS_MASK,
        "op class does not fit the user_data tag"
    );
    UserData {
        index: slot,
        tag: u32::from(class),
    }
    .into()
}

/// The `user_data` for an `ASYNC_CANCEL` completing the cancel slot itself.
fn cancel_user_data(cancel_slot: u32) -> u64 {
    UserData {
        index: cancel_slot,
        tag: CANCEL_TAG,
    }
    .into()
}

// ── Uring ─────────────────────────────────────────────────────────────

/// The live `io_uring` ring and the shared buffer slab.
///
/// Held in an `Option` only so `begin_teardown` can close the ring at the exact
/// point the runtime chooses — see the `Backend` impl. It is `Some` for the
/// entire lifetime of the runtime except after teardown.
pub struct Uring {
    ring: Option<io_uring::IoUring>,
    pub(crate) pools: Pools,
}

impl Uring {
    /// The live ring. Only ever `None` after `begin_teardown`.
    pub(crate) fn ring_mut(&mut self) -> &mut io_uring::IoUring {
        self.ring.as_mut().expect("ring closed")
    }
}

impl Backend for Uring {
    type Config = UringConfig;
    type Request = io_uring::squeue::Entry;

    fn attach(cfg: &Self::Config) -> io::Result<Self> {
        // `setup_single_issuer`/`setup_defer_taskrun` are a kernel-version
        // optimisation, not a requirement, so a kernel that rejects them falls
        // back to a plain ring. Both attempts failing is a real error.
        let ring = io_uring::IoUring::builder()
            .setup_single_issuer()
            .setup_defer_taskrun()
            .build(cfg.ring_entries)
            .or_else(|_| io_uring::IoUring::new(cfg.ring_entries))?;

        let pools = build_pools(&ring, &cfg.provided_size_classes, &cfg.size_classes);

        Ok(Self {
            ring: Some(ring),
            pools,
        })
    }

    fn arm(&mut self, slot: u32, class: u8, mut req: Self::Request) -> io::Result<()> {
        req = req.user_data(op_user_data(slot, class));
        let mut sq = self.ring_mut().submission();
        let pushed = unsafe { sq.push(&req) };
        drop(sq);
        pushed.map_err(|_| io::Error::from_raw_os_error(libc::EAGAIN))
    }

    fn wait(&mut self) -> io::Result<()> {
        match self.ring_mut().submit_and_wait(1) {
            Ok(_) => Ok(()),
            // `EBUSY` means a submit was already pending when this one started,
            // which says nothing about whether a completion is coming. The
            // drive loop drains again either way, so it is not an error.
            Err(ref e) if e.raw_os_error() == Some(libc::EBUSY) => Ok(()),
            Err(e) => Err(e),
        }
    }

    fn next_completion(&mut self) -> Option<Completion> {
        // Popped one at a time, with no buffering. `completion()` hands back a
        // guard that mutably borrows the ring for its whole lifetime and
        // publishes the consumed `head` only on drop, and a shim dispatched
        // after this returns needs `&mut self` again to push to the submission
        // queue. Borrowing, popping one entry and dropping per completion costs
        // three relaxed/acquire/release accesses and nothing else — no
        // allocation, no copy — and each entry is still consumed exactly once,
        // since `head` is published before the next borrow. Re-reading `tail`
        // each time also picks up completions that land mid-drain.
        let mut cq = self.ring_mut().completion();
        let cqe = cq.next()?;
        let ud = UserData::from(cqe.user_data());
        let result = cqe.result();
        Some(Completion {
            slot: ud.index,
            result,
            flags: cqe.flags(),
            class: (ud.tag & CLASS_MASK) as u8,
            cancel: ud.tag & CANCEL_TAG != 0,
            // Nothing on a ring cancels an in-flight request but an
            // `ASYNC_CANCEL` the runtime submitted, so this is the kernel's own
            // report that one took effect.
            cancelled: ud.tag & CANCEL_TAG == 0 && result == -libc::ECANCELED,
        })
    }

    fn cancel(&mut self, cancel_slot: u32, target_slot: u32, class: u8) -> io::Result<bool> {
        let entry = io_uring::opcode::AsyncCancel::new(op_user_data(target_slot, class))
            .build()
            .user_data(cancel_user_data(cancel_slot));
        let mut sq = self.ring_mut().submission();
        let pushed = unsafe { sq.push(&entry) };
        drop(sq);
        pushed
            .map(|()| true)
            .map_err(|_| io::Error::from_raw_os_error(libc::EAGAIN))
    }

    fn begin_teardown(&mut self) {
        // Unregister every provided buffer ring and the fixed write buffer slab
        // while the io_uring fd is still open.
        if let Some(ring) = self.ring.as_ref() {
            let _ = self.pools.provided_pool.unregister_all(ring);
            let _ = ring.submitter().unregister_buffers();
        }
        // Close the ring. `IoUring::drop` releases its own SQ/CQ maps and then
        // closes the fd, which tears down the ring and does not return until
        // every in-flight request is cancelled and any worker that was
        // mid-execution has finished. Only after this returns is it safe for
        // the runtime to drop op locals and for this struct to free the slab.
        drop(self.ring.take());
    }
}

/// A [`Runtime`] on this crate's backend.
///
/// The core runtime names its backend explicitly, because it cannot depend on
/// this crate; this is the two-parameter spelling that lets a caller write
/// `Runtime<G, L>` and get io_uring.
pub type UringRuntime<G, L> = Runtime<G, L, Uring>;

// ── io_uring-specific runtime accessors ───────────────────────────────

/// The io_uring-specific surface of a [`Runtime`].
///
/// A foreign type takes no inherent impls, so the operations this crate adds to
/// core's runtime have to live in a trait. The portable three are inherent
/// methods instead, because core declares those and every backend implements
/// them; these four are io_uring's alone, so there is no slot in core for them
/// to be inherent in. `UringExt` is in scope wherever they are used.
pub trait UringExt<G, L> {
    /// The buffer pools backing every read's provided-buffer ring and every
    /// fixed write.
    fn pools(&self) -> &Pools;

    /// Submits everything queued for the kernel, returning how many requests
    /// were submitted.
    fn submit(&mut self) -> io::Result<usize>;

    /// Reads into a buffer the kernel picks from the provided-buffer rings.
    fn read_provided(
        &mut self,
        fd: RawFd,
        max_len: usize,
        local: L,
        done: fn(&mut Runtime<G, L, Uring>, L, io::Result<Bytes>),
    ) -> io::Result<Op>;

    /// Writes from the registered slab by address, so a pooled buffer can be
    /// sent without copying it into the fixed region first.
    fn write_fixed(
        &mut self,
        fd: RawFd,
        addr: *const u8,
        len: usize,
        local: L,
        done: fn(&mut Runtime<G, L, Uring>, L, io::Result<usize>),
    ) -> io::Result<Op>;

    /// Receives a message into `msg`, which must stay valid until `done` runs.
    ///
    /// # Safety
    ///
    /// `msg` must point at a wired `msghdr` that outlives the op.
    unsafe fn recvmsg(
        &mut self,
        fd: RawFd,
        msg: *mut libc::msghdr,
        ctrl_cap: usize,
        flags: u32,
        local: L,
        done: fn(&mut Runtime<G, L, Uring>, L, io::Result<usize>),
    ) -> io::Result<Op>;

    /// Sends the message in `msg`, which must stay valid until `done` runs.
    fn sendmsg(
        &mut self,
        fd: RawFd,
        msg: *mut libc::msghdr,
        local: L,
        done: fn(&mut Runtime<G, L, Uring>, L, io::Result<usize>),
    ) -> io::Result<Op>;
}

/// Publishes this runtime's pools as the active ones, which is how a buffer
/// view resolves its `bid` without naming the runtime's `G`/`L`. Held for the
/// duration of the run by [`block_on`].
fn install_pools_as_current<G, L>(rt: &mut Runtime<G, L, Uring>) {
    set_current_pools(&raw mut rt.backend_mut().pools);
}

/// Runs the callback runtime to completion (until no op is in flight) and
/// returns the caller's global state `G`. `boot` is invoked once with the
/// runtime to dispatch the initial ops; it is the one place a closure is
/// allowed (it runs a single time, at startup).
///
/// A panic in `boot` or the drive loop simply unwinds: dropping the runtime
/// runs the ordered teardown in its `Drop`, which quiesces the backend before
/// any op local or pool memory is released.
pub fn block_on<G, L, F>(params: &UringConfig, global: G, boot: F) -> G
where
    F: FnOnce(&mut UringRuntime<G, L>),
{
    let _gen_guard = enter_active_gen();

    // Declared *before* `rt` so it drops *after* it: op locals can own pooled
    // bytes, and `Runtime::drop` must be able to route their release through
    // the live pools. Clearing the pointer first would abort that drop.
    let _rt_guard = DropGuard::new(clear_current_pools);

    let backend = Uring::attach(params).expect("failed to create io_uring");
    let mut rt = Runtime::new(backend, global);

    install_pools_as_current(&mut rt);

    boot(&mut rt);
    rt.run();

    rt.into_global()
}

/// [`block_on`] with this crate's default configuration.
pub fn block_on_default<G, L, F>(global: G, boot: F) -> G
where
    F: FnOnce(&mut UringRuntime<G, L>),
{
    block_on(&UringConfig::default(), global, boot)
}
