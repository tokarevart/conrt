//! A readiness-based backend for `coio`'s callback runtime.
//!
//! Where `coio-uring` posts one completion per submitted request, this backend
//! reports only that a descriptor is ready and then issues the syscall itself.
//! It therefore has no provided buffers and no fixed-buffer writes — those
//! exist to avoid copies against a registered slab, and a caller here owns its
//! memory anyway — so its whole surface is the portable `read`, `write` and
//! `accept`, which reach the runtime as inherent methods.
//!
//! # Example
//!
//! ```no_run
//! use coio_core::Op;
//! use coio_mio::MioConfig;
//! use coio_mio::MioRuntime;
//! use coio_mio::block_on;
//! use std::io;
//! use std::os::fd::RawFd;
//!
//! fn on_read(
//!     rt: &mut MioRuntime<(), ()>,
//!     _local: (),
//!     buf: Vec<u8>,
//!     res: io::Result<usize>,
//! ) {
//!     let _ = (rt, buf, res);
//! }
//!
//! fn main() {
//!     let fd: RawFd = 0;
//!     block_on(&MioConfig::default(), (), |rt: &mut MioRuntime<(), ()>| {
//!         let mut buf = vec![0u8; 64];
//!         buf.truncate(64);
//!         let _ = rt.read(fd, buf, (), on_read);
//!     });
//! }
//! ```

mod io;
mod mio;
mod sys;

use coio_core::ActiveGenGuard;
use coio_core::Backend as _;
use coio_core::Runtime;
use coio_core::enter_active_gen;

pub use crate::mio::Mio;
pub use crate::mio::MioConfig;
pub use crate::mio::Req;

/// A [`Runtime`] on this crate's backend.
pub type MioRuntime<G, L> = Runtime<G, L, Mio>;

/// Runs the callback runtime to completion (until no op is in flight) and
/// returns the caller's global state `G`. `boot` is invoked once with the
/// runtime to dispatch the initial ops; it is the one place a closure is
/// allowed (it runs a single time, at startup).
///
/// A panic in `boot` or the drive loop simply unwinds: dropping the runtime
/// runs the ordered teardown in its `Drop`, which quiesces the backend before
/// any op local is released.
pub fn block_on<G, L, F>(cfg: &MioConfig, global: G, boot: F) -> G
where
    F: FnOnce(&mut MioRuntime<G, L>),
{
    // Marks views taken during the run valid and stale ones detectable across
    // restarts, exactly as it does for a ring-based backend.
    let _gen_guard: ActiveGenGuard = enter_active_gen();

    let backend = Mio::attach(cfg).expect("failed to create the poller");
    let mut rt = Runtime::new(backend, global);

    boot(&mut rt);
    rt.run();

    rt.into_global()
}

/// [`block_on`] with this crate's default configuration.
pub fn block_on_default<G, L, F>(global: G, boot: F) -> G
where
    F: FnOnce(&mut MioRuntime<G, L>),
{
    block_on(&MioConfig::default(), global, boot)
}
