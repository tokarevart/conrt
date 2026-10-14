//! The platform-specific parts of the readiness backend.
//!
//! Everything not portable Unix goes through here, so adding a target is a
//! matter of writing one more `sys` module rather than touching the backend.
//! Linux is the only one implemented.

use std::io;
use std::os::fd::RawFd;

use mio::Interest;
use mio::Poll;
use mio::Token;

/// A live readiness registration, held for as long as the operation is armed.
///
/// Dropping the *registration* is what stops a completed descriptor being
/// reported again, and it is load-bearing rather than hygiene: the poller is
/// level-triggered, so a still-registered readable descriptor is reported on
/// *every* wait. Leaving one behind behind a genuinely blocked operation spins
/// the drive loop at 100% CPU.
pub struct Registration {
    fd: RawFd,
    token: Token,
    interest: Interest,
}

impl Registration {
    /// Registers `fd` with `poll` for `interest`, reporting under `token`.
    ///
    /// # Safety
    ///
    /// `fd` must stay open and valid until [`Registration::detach`] or
    /// [`Registration::drop`]. The runtime holds each operation's descriptor
    /// for exactly that long — it is the operation's own — and this is the
    /// same requirement every operation's documentation already states.
    pub unsafe fn new(
        fd: RawFd,
        poll: &Poll,
        token: Token,
        interest: Interest,
    ) -> io::Result<Self> {
        let reg = Self {
            fd,
            token,
            interest,
        };
        reg.attach(poll)?;
        Ok(reg)
    }

    /// (Re-)arms the registration, which is how a cancelled-then-rearmed
    /// operation goes back on the poller.
    pub fn attach(&self, poll: &Poll) -> io::Result<()> {
        let mut source = mio::unix::SourceFd(&self.fd);
        poll.registry()
            .register(&mut source, self.token, self.interest)
    }

    /// Removes the registration from `poll`.
    pub fn detach(&self, poll: &Poll) {
        let mut source = mio::unix::SourceFd(&self.fd);
        let _ = poll.registry().deregister(&mut source);
    }
}
