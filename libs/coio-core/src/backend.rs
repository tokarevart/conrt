//! The port between the runtime and a completion backend.
//!
//! The runtime owns the op slabs, the free lists, the cancel protocol and the
//! caller's global state. A backend owns only the mechanism that turns a
//! request into a completion — an `io_uring` ring today, a readiness poller
//! later. Neither half reaches into the other: [`Backend`] methods take no
//! handle on the runtime, so a backend cannot invoke a completion callback,
//! touch `G`, or move a local. The only value that crosses the boundary is
//! [`Completion`], and the runtime validates every field of it.

use std::io;

/// One completion, already decoded by the backend.
///
/// Every field is untrusted input as far as the runtime is concerned — the
/// trait is public and implementable from outside — so the runtime checks the
/// ones that keep the slab consistent before it dereferences anything.
#[derive(Clone, Copy, Debug)]
pub struct Completion {
    /// The op slot this completes. Must be within the slab and must name a
    /// live slot: completing a free one would hand its `local` to the wrong op.
    pub slot: u32,
    /// Raw backend result: a byte count on success, or a negated errno when
    /// negative. The op's shim decodes it.
    pub result: i32,
    /// Backend completion flags. `io_uring` passes `cqe.flags` — which
    /// `IORING_CQE_F_BUFFER_SELECT` and the selected buffer id live in — while
    /// a readiness backend has nothing to report here and passes 0.
    pub flags: u32,
    /// Dispatch-time metadata the shim reads back, e.g. the read size class
    /// that named the provided-buffer group. Unused by most ops.
    pub class: u8,
    /// Whether this completes a cancel request rather than an op. The runtime
    /// routes it to the cancel slab instead of the op slab.
    pub cancel: bool,
    /// For an op completion: whether it is the result of a cancellation the
    /// runtime asked for, as opposed to the operation running.
    ///
    /// The backend decides this in its own terms — `io_uring` reports
    /// `-ECANCELED`, and nothing else on a ring cancels an in-flight request —
    /// which keeps the runtime free of any one platform's errno values.
    pub cancelled: bool,
}

/// A completion backend.
///
/// # Contract
///
/// Implementations are held to the following, each of which the runtime either
/// enforces itself or checks with a plain `assert!` rather than trusting:
///
/// * **Exactly one completion per armed request.** A request that has been
///   handed to [`Backend::arm`] or [`Backend::cancel`] must produce exactly one
///   [`Completion`], and never one before it was armed. io_uring guarantees
///   this for every submitted SQE; a backend that can lose or duplicate a
///   completion cannot satisfy the op slab's free-list invariants.
/// * **Stale completions are the backend's problem to avoid, not the runtime's
///   to survive.** Slots are recycled, so a completion naming a slot the
///   runtime has since freed is a bug in the backend. The runtime rejects it by
///   id, which is why ids come from a never-reset counter.
/// * **No reentrancy.** The runtime is single-threaded and invokes shims from
///   its drive loop; a backend must not call back into the runtime or block
///   indefinitely, and must only block in [`Backend::wait`].
/// * **`begin_teardown` must quiesce.** On return, nothing in the backend can
///   still touch caller memory — for io_uring that means the ring is closed,
///   which is what makes dropping the pools and op locals safe afterwards.
/// * **Never report a slot the runtime did not hand out.** `arm` and `cancel`
///   receive slots from the runtime and must only ever complete those.
pub trait Backend: Sized + 'static {
    /// Backend-specific construction parameters: size classes for io_uring,
    /// interest capacity for a readiness poller. Owned rather than borrowed
    /// so no lifetime has to infect the runtime.
    type Config;

    /// An armed request, as this backend encodes it. The runtime hands these
    /// back verbatim and never inspects them.
    type Request;

    /// Builds the backend and its resources from `cfg`.
    fn attach(cfg: &Self::Config) -> io::Result<Self>;

    /// Submits `req` on behalf of `slot`. `class` is the same dispatch-time
    /// metadata the runtime records on the [`Op`](crate::runtime::Op) handle,
    /// passed back so the backend can encode it into its completion.
    ///
    /// The slot is already marked live by the runtime. An error means the
    /// request was *not* submitted, so the runtime frees the slot; it will not
    /// free it for the backend.
    fn arm(&mut self, slot: u32, class: u8, req: Self::Request) -> io::Result<()>;

    /// Blocks until at least one completion is likely available.
    ///
    /// Returning `Ok(())` without a completion pending is allowed and simply
    /// causes another drain pass. An error ends the drive loop, which then
    /// tears down in-flight ops in order.
    fn wait(&mut self) -> io::Result<()>;

    /// The next completion, or `None` if none is ready. Called in a loop until
    /// it returns `None`.
    ///
    /// This must not retain any borrow: the runtime dispatches a shim between
    /// calls, and the shim may submit new requests.
    fn next_completion(&mut self) -> Option<Completion>;

    /// Requests cancellation of the op in `target_slot`, naming it by its
    /// dispatch-time `class`. The cancellation is itself a request and
    /// completes through [`Backend::next_completion`] with `cancel: true` and
    /// `slot` set to `cancel_slot`.
    ///
    /// `Ok(false)` means the request could not be submitted — a full submission
    /// queue — so no cancel completion will arrive and the runtime drops the
    /// cancel's local instead of parking it.
    fn cancel(&mut self, cancel_slot: u32, target_slot: u32, class: u8) -> io::Result<bool>;

    /// Unregisters buffers and quiesces the kernel before the runtime releases
    /// anything the backend could still be writing into.
    ///
    /// For io_uring this unregisters the provided-buffer rings and the fixed
    /// buffer slab while the fd is open, then closes the ring — which cancels
    /// every in-flight request and does not return until any worker
    /// mid-execution has finished. After this returns, no kernel operation can
    /// touch pool or op-local memory.
    fn begin_teardown(&mut self);
}
