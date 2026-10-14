//! The readiness backend.
//!
//! io_uring posts one completion per submitted request; a readiness poller
//! reports only that a descriptor is ready, so this backend issues the syscall
//! itself. That difference is the whole of it, and it is what the type below is
//! organised around.

use std::collections::VecDeque;
use std::io;
use std::os::fd::RawFd;

use coio_core::Backend;
use coio_core::Completion;

use crate::sys::Registration;

/// Construction parameters for [`Mio`].
#[derive(Debug, Clone)]
pub struct MioConfig {
    /// How many descriptors the poller should expect to track. A hint, not a
    /// cap: registering beyond it is fine, and the poller grows.
    pub capacity: usize,
}

impl Default for MioConfig {
    fn default() -> Self {
        Self { capacity: 256 }
    }
}

/// A readiness backend over a single thread's poller.
///
/// # Accounting
///
/// The drive loop ends when the runtime reports nothing in flight, and the
/// runtime's in-flight counts only fall when a completion is dispatched. So
/// every operation this backend arms must be reachable from here, or the loop
/// never ends. Two places hold them: the per-slot entries, which an event will
/// finish, and the `ready` queue, which holds completions already decided.
///
/// The `ready` queue is drained *before* the event buffer, and that
/// ordering is part of the guarantee: it is what makes a `None` from
/// [`Backend::next_completion`] mean genuinely nothing is outstanding, so the
/// drive loop never blocks on work this backend has already finished.
pub struct Mio {
    poll: mio::Poll,
    /// Events from the last `wait`, drained one at a time. Held rather than
    /// re-polled so that `next_completion` stays non-blocking and the queue
    /// borrow never outlives a completion, which is the same constraint the
    /// io_uring queue guard imposes.
    events: mio::Events,
    /// Per-slot operations, indexed by runtime slot. A slot's entry is cleared
    /// as it completes, so a stale event finds nothing and is dropped.
    pending: Vec<Option<PendingOp>>,
    /// Completions already decided, chiefly by `cancel`.
    ready: VecDeque<Completion>,
}

/// One armed operation waiting for its descriptor to become ready.
///
/// Owns the readiness registration: dropping this deregisters the descriptor,
/// which is load-bearing because the poller is level-triggered. A
/// still-registered readable descriptor is reported on every wait, so leaving
/// one behind would spin the drive loop whenever an unrelated operation stayed
/// in flight.
struct PendingOp {
    reg: Registration,
    req: Req,
}

/// The syscall an armed operation will issue once its descriptor is ready.
///
/// A `Backend::Request`, so it is public; the runtime only ever passes one
/// back to [`Backend::arm`] unchanged.
#[derive(Debug)]
pub enum Req {
    Read {
        fd: RawFd,
        addr: *mut u8,
        len: usize,
    },
    Write {
        fd: RawFd,
        addr: *const u8,
        len: usize,
    },
    Accept {
        listener_fd: RawFd,
    },
}

impl Mio {
    /// Removes an operation's registration and returns it, if it is still
    /// armed.
    fn take_pending(&mut self, slot: u32) -> Option<PendingOp> {
        let op = self.pending.get_mut(slot as usize)?.take()?;
        // Deregistering here is what keeps a completed descriptor from being
        // reported again: the poller is level-triggered, so a still-registered
        // readable fd is reported on every wait.
        op.reg.detach(&self.poll);
        Some(op)
    }

    /// Queues a completion the runtime has not been told about yet.
    fn push(&mut self, c: Completion) {
        self.ready.push_back(c);
    }
}

impl Mio {
    /// Builds a backend and its poller. See [`Backend::attach`].
    pub fn new(cfg: &MioConfig) -> io::Result<Self> {
        Self::attach(cfg)
    }
}

impl Backend for Mio {
    type Config = MioConfig;
    type Request = Req;

    fn attach(cfg: &Self::Config) -> io::Result<Self> {
        Ok(Self {
            poll: mio::Poll::new()?,
            events: mio::Events::with_capacity(cfg.capacity),
            pending: Vec::new(),
            ready: VecDeque::new(),
        })
    }

    fn arm(&mut self, slot: u32, _class: u8, req: Self::Request) -> io::Result<()> {
        let (fd, interest) = match &req {
            Req::Read { fd, .. } => (*fd, mio::Interest::READABLE),
            Req::Write { fd, .. } => (*fd, mio::Interest::WRITABLE),
            Req::Accept { listener_fd } => (*listener_fd, mio::Interest::READABLE),
        };

        // SAFETY: the runtime holds the descriptor for the operation's duration
        // — it is the operation's own — and the registration is dropped, and so
        // detached, before the slot is freed.
        let reg =
            unsafe { Registration::new(fd, &self.poll, mio::Token(slot as usize), interest)? };

        let idx = slot as usize;
        while self.pending.len() <= idx {
            self.pending.push(None);
        }
        self.pending[idx] = Some(PendingOp { reg, req });
        Ok(())
    }

    fn wait(&mut self) -> io::Result<()> {
        self.events.clear();
        loop {
            match self.poll.poll(&mut self.events, None) {
                Ok(()) => return Ok(()),
                // A signal interrupting the poll says nothing about whether a
                // descriptor is ready. The drive loop treats any error as the
                // end of the run and tears down whatever is still in flight, so
                // letting `EINTR` through here would silently drop live
                // operations on an unrelated signal.
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        }
    }

    fn next_completion(&mut self) -> Option<Completion> {
        // Decided work first, so that `None` below means the backend has
        // nothing outstanding rather than merely nothing *readable* — the drive
        // loop treats that difference as "safe to block".
        if let Some(c) = self.ready.pop_front() {
            return Some(c);
        }

        // Then the event buffer, one event at a time.
        let event = self.events.iter().next()?;
        let token = event.token();
        // An event for a slot this backend has already completed finds nothing
        // and is dropped: a stale event is the backend's to tolerate, which is
        // what `Backend` asks of one.
        let pending = self.take_pending(token.0 as u32)?;

        // A level-triggered event is a hint that the operation will not block,
        // not a promise: another consumer can take the data first, in which case
        // the syscall reports `EAGAIN` and the operation stays armed. Completing
        // it there would fail a read that was perfectly valid.
        // SAFETY: `pending.req` names a live descriptor and a buffer region the
        // caller kept alive for this operation.
        match unsafe { perform(&pending.req) } {
            Ok(result) => Some(Completion {
                slot: token.0 as u32,
                result,
                flags: 0,
                class: 0,
                cancel: false,
                cancelled: false,
            }),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                // Re-register and leave the operation armed. The descriptor is
                // already registered, so nothing is needed beyond putting the
                // entry back.
                self.pending[token.0] = Some(pending);
                None
            }
            Err(e) => {
                let result = raw_os_error(&e);
                Some(Completion {
                    slot: token.0 as u32,
                    result,
                    flags: 0,
                    class: 0,
                    cancel: false,
                    cancelled: false,
                })
            }
        }
    }

    fn cancel(&mut self, cancel_slot: u32, target_slot: u32, class: u8) -> io::Result<bool> {
        // Nothing in the kernel is in flight to interrupt — the syscall was
        // never issued — so there is no operation to cancel. The runtime learns
        // the outcome only from the completions below, and if they were not
        // produced its in-flight count would never fall and the drive loop
        // would block forever on a descriptor that is no longer registered.
        if self.take_pending(target_slot).is_none() {
            // Already finished: the runtime drops its own local and leaves the
            // target alone.
            return Ok(false);
        }

        // The cancel's own completion goes first, mirroring what a ring does.
        // `complete_cancel` ignores a successful result and leaves the slot for
        // `complete_op` to consume, so this one only has to not be mistaken for
        // a live op; queueing it second would leave it naming a slot the op's
        // completion has already freed.
        self.push(Completion {
            slot: cancel_slot,
            result: 0,
            flags: 0,
            class: 0,
            cancel: true,
            cancelled: false,
        });
        self.push(Completion {
            slot: target_slot,
            result: -libc::ECANCELED,
            flags: 0,
            class,
            cancel: false,
            cancelled: true,
        });
        Ok(true)
    }

    fn begin_teardown(&mut self) {
        // Deregistering every armed descriptor leaves the poller with nothing to
        // report, so no completion can name a slot or touch an op's local after
        // this returns. The pending table goes with it: its entries hold raw
        // pointers into caller buffers, and nothing may dereference them once
        // the runtime is dropping those buffers.
        for slot in self.pending.iter_mut().flatten() {
            slot.reg.detach(&self.poll);
        }
        self.pending.clear();
        self.ready.clear();
    }
}

/// Issues the syscall for a ready operation.
///
/// # Safety
///
/// `req` must name a live descriptor and a buffer region the caller keeps alive
/// until the op completes, which is the same requirement every operation's
/// documentation already states.
unsafe fn perform(req: &Req) -> io::Result<i32> {
    match req {
        Req::Read { fd, addr, len } => read(*fd, *addr, *len),
        Req::Write { fd, addr, len } => write(*fd, *addr, *len),
        Req::Accept { listener_fd } => accept(*listener_fd),
    }
}

fn read(fd: RawFd, addr: *mut u8, len: usize) -> io::Result<i32> {
    // SAFETY: `addr`/`len` are the buffer region the caller kept alive for
    // this operation, and the kernel writes at most `len` bytes into it.
    let n = unsafe { libc::read(fd, addr.cast(), len) };
    finish(n)
}

fn write(fd: RawFd, addr: *const u8, len: usize) -> io::Result<i32> {
    // SAFETY: `addr`/`len` are the buffer region the caller kept alive for
    // this operation, and the kernel reads at most `len` bytes from it.
    let n = unsafe { libc::write(fd, addr.cast(), len) };
    finish(n)
}

fn accept(listener_fd: RawFd) -> io::Result<i32> {
    let mut addr: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let mut addr_len = size_of::<libc::sockaddr_storage>() as libc::socklen_t;
    // SAFETY: a zeroed `sockaddr_storage` with its true length is what `accept`
    // expects; the kernel writes at most `addr_len` bytes back.
    let n = unsafe { libc::accept(listener_fd, (&raw mut addr).cast(), &raw mut addr_len) };
    // `accept` returns `c_int` rather than `ssize_t`; a descriptor is the same
    // shape as a byte count to a completion, so it widens here.
    finish(n as libc::ssize_t)
}

/// Turns a syscall's return into a completion result, negated on failure.
///
/// A negative return is `-1` with the errno in thread-local storage. The
/// runtime's shims read a negative completion result as a negated errno, which
/// is the same shape an io_uring CQE carries.
fn finish(n: libc::ssize_t) -> io::Result<i32> {
    if n < 0 {
        Err(io::Error::last_os_error())
    } else {
        // A byte count or a descriptor; both fit a completion result, and a
        // completion is only ever read as one of those.
        Ok(n as i32)
    }
}

/// The negated errno a failed syscall reports.
///
/// The runtime's shims decode a negative result, so an `io::Error` has to go
/// back to that form. `raw_os_error` is absent only for an error this backend
/// synthesised, and none are.
fn raw_os_error(e: &io::Error) -> i32 {
    -e.raw_os_error().expect("syscall error carries an errno")
}
