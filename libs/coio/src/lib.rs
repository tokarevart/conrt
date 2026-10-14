//! A callback io runtime, with one io operation per backend.
//!
//! This crate is a facade. The runtime itself is split between `coio-core`,
//! which owns the op slabs, the cancel protocol and the caller's global state,
//! and a backend crate — `coio-uring` today — which owns the completion
//! mechanism and whatever buffers that mechanism needs.
//!
//! # Choosing a backend
//!
//! Backends sit behind feature flags, and any number of them can be built at
//! once. [`uring`](https://docs.rs/coio-uring) is a default feature and is
//! Linux-only.
//!
//! [`runtime::Runtime`] is whichever backend suits the platform it was built
//! for: io_uring on Linux, otherwise whatever else is enabled. Every backend
//! also has its own name — [`UringRuntime`] here, `MioRuntime` once a readiness
//! backend lands — so an explicitly non-default backend is a type annotation
//! away rather than a reconfiguration.
//!
//! The portable operations are inherent methods on [`runtime::Runtime`], not
//! functions to import:
//!
//! ```
//! # #[cfg(feature = "uring")] {
//! use coio::runtime::Runtime;
//! use coio::runtime::block_on_default;
//! use std::os::fd::RawFd;
//!
//! let fd: RawFd = 1;
//! block_on_default((), |rt: &mut Runtime<(), ()>| {
//!     let _ = rt.write(fd, b"hello".as_slice(), (), |_rt, _local, _buf, res| {
//!         let _ = res;
//!     });
//! });
//! # }
//! ```
//!
//! Operations only one backend can provide stay behind that backend's feature.
//! They are methods on that backend's extension trait, because a foreign type
//! takes no inherent impls and core has no slot for a non-portable operation:
//! [`uring::UringExt::read_provided`], [`uring::UringExt::write_fixed`], and so
//! on, alongside its buffer types like [`Msg`].

/// The backend-generic runtime surface, plus this crate's default backend's
/// entry points.
pub mod runtime {
    pub use coio_core::runtime::ActiveGenGuard;
    pub use coio_core::runtime::Op;
    pub use coio_core::runtime::OpOutcome;
    pub use coio_core::runtime::OpShimArgs;

    pub use crate::default_backend::Runtime;
    pub use crate::default_backend::block_on;
    pub use crate::default_backend::block_on_default;
}

pub use coio_core::AcceptOp;
pub use coio_core::Backend;
pub use coio_core::Completion;
pub use coio_core::IoBuf;
pub use coio_core::IoBufErased;
pub use coio_core::IoBufMut;
pub use coio_core::IoBufRef;
pub use coio_core::Op;
pub use coio_core::ReadOp;
pub use coio_core::WriteOp;
#[cfg(feature = "mio")]
pub use coio_mio as mio;
#[cfg(feature = "mio")]
pub use coio_mio::Mio;
#[cfg(feature = "mio")]
pub use coio_mio::MioConfig;
#[cfg(feature = "mio")]
pub use coio_mio::MioRuntime;
/// Every backend, under its own module, whatever the platform default is.
///
/// This is the escape hatch for a build with more than one backend enabled,
/// where the root re-exports below name only the default one.
#[cfg(feature = "uring")]
pub use coio_uring as uring;
/// Each backend's own types, at the root and under flat names.
///
/// These need no selection, which is why they are not gated on the platform
/// default: picking a backend is choosing one of these type names.
#[cfg(feature = "uring")]
pub use coio_uring::Uring;
#[cfg(feature = "uring")]
pub use coio_uring::UringConfig;
#[cfg(feature = "uring")]
pub use coio_uring::UringRuntime;

/// The backend this build defaults to, under the names a caller expects.
///
/// io_uring wins on Linux because it is the only backend there today; every
/// other configuration falls through to whatever else is enabled.
#[cfg(all(feature = "uring", target_os = "linux"))]
mod default_backend {
    pub use coio_uring::UringRuntime as Runtime;
    pub use coio_uring::block_on;
    pub use coio_uring::block_on_default;
}
#[cfg(all(feature = "mio", not(all(feature = "uring", target_os = "linux"))))]
mod default_backend {
    pub use coio_mio::MioRuntime as Runtime;
    pub use coio_mio::block_on;
    pub use coio_mio::block_on_default;
}

#[cfg(not(any(all(feature = "uring", target_os = "linux"), feature = "mio")))]
compile_error!(
    "`coio` has no backend for this target: `uring` is Linux-only, so enable `uring` there or \
     `mio` anywhere. For no backend at all, depend on `coio-core` directly."
);

/// The default backend's whole surface, flat at the root.
///
/// A readiness backend has no pooled provided buffers, so this is much smaller
/// than io_uring's: its buffer types and provided-buffer operations simply do
/// not exist on a backend that has no slab.
#[cfg(all(feature = "mio", not(all(feature = "uring", target_os = "linux"))))]
pub use coio_mio::*;
/// The default backend's whole surface, flat at the root.
///
/// A glob rather than a restated list: these are exactly the items the default
/// backend already curated for its own root, and a hand-written list would be
/// one more thing to forget when the backend changes.
#[cfg(all(feature = "uring", target_os = "linux"))]
pub use coio_uring::*;
