//! The `io_uring` backend.
//!
//! Runs completion callbacks over a single `io_uring` ring and moves data
//! through a shared registered buffer slab instead of copying: reads land in
//! provided-buffer rings (`IORING_REGISTER_PBUF_RING`) and are handed back as
//! [`Bytes`], which upgrade to a writable [`BytesMut`] with
//! [`Bytes::into_mut`] while the read view is the sole holder; writes take a
//! [`BytesMut`] — from [`alloc_bytes`] or upgraded from a read — and are
//! submitted with `IORING_OP_WRITE_FIXED`. All views are borrow-tracked: a slot
//! returns to its slab only when the last view is dropped.
//!
//! The op slabs, the cancel protocol and the caller's global state belong to
//! `coio-core`'s `Runtime`; nothing here can reach them. What crosses the
//! boundary is a `Completion`, which the runtime validates before use.
//!
//! Each operation carries the caller's per-op local state `L`, moved into the
//! op on dispatch and back to a completion callback when the op finishes.
//!
//! # Example
//!
//! ```no_run
//! use coio_uring::UringConfig;
//! use coio_uring::UringRuntime;
//! use std::os::fd::RawFd;
//!
//! fn main() {
//!     let fd: RawFd = 1;
//!     coio_uring::block_on(&UringConfig::default(), (), |rt: &mut UringRuntime<(), ()>| {
//!         let _ = rt.write(fd, b"hello".as_slice(), (), |_rt, _local, _buf, res| {
//!             let _ = res;
//!         });
//!     });
//! }
//! ```

pub mod pools;

pub mod buf;
pub mod classes;
pub mod io;
mod uring;

pub use crate::buf::Bytes;
pub use crate::buf::BytesMut;
pub use crate::buf::Ref;
pub use crate::buf::RefMut;
pub use crate::buf::Slice;
pub use crate::buf::SliceMut;
pub use crate::buf::alloc;
pub use crate::buf::alloc_bytes;
pub use crate::buf::alloc_mut;
pub use crate::buf::alloc_slice_mut;
pub use crate::buf::provided_bytes;
pub use crate::classes::SizeClass;
pub use crate::io::MAX_CTRL_CAP;
pub use crate::io::MAX_IOV_CAP;
pub use crate::io::Msg;
pub use crate::io::MsgMut;
pub use crate::io::cmsg_space;
pub use crate::uring::RuntimeParams;
pub use crate::uring::Uring;
pub use crate::uring::UringConfig;
pub use crate::uring::UringExt;
pub use crate::uring::UringRuntime;
pub use crate::uring::block_on;
pub use crate::uring::block_on_default;
