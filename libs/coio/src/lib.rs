//! An `io_uring`-based callback runtime with zero-copy pooled buffers.
//!
//! `coio` runs completion callbacks over a single `io_uring`, and moves data
//! through a shared registered buffer slab instead of copying: reads land in
//! provided-buffer rings (`IORING_REGISTER_PBUF_RING`) and are handed back as
//! [`Bytes`], which upgrade to a writable [`BytesMut`] with [`Bytes::into_mut`]
//! while the read view is the sole holder; writes take a [`BytesMut`] — from
//! [`alloc_bytes`] or upgraded from a read — and are
//! submitted with `IORING_OP_WRITE_FIXED`. All views are borrow-tracked: a
//! slot returns to its slab only when the last view is dropped.
//!
//! Each io operation carries a per-op local state `L`, moved into the op on
//! dispatch and back to a completion callback when the op finishes. The
//! runtime is generic over the caller's global state `G`, which is passed in
//! once at creation and returned when the runtime shuts down; callbacks reach
//! it through `Runtime::global` / `Runtime::global_mut`.
//!
//! # Example
//!
//! ```no_run
//! use coio::io::write;
//! use coio::runtime::block_on_default;
//! use std::os::fd::RawFd;
//!
//! fn main() {
//!     let fd: RawFd = 1;
//!     block_on_default((), |rt| {
//!         let _ = write(rt, fd, b"hello".as_ptr(), 5, (), |_rt, _local, res| {
//!             let _ = res;
//!         });
//!     });
//! }
//! ```

mod buf;
pub mod classes;
pub mod io;
pub mod runtime;

pub use crate::buf::Bytes;
pub use crate::buf::BytesMut;
pub use crate::buf::Ref;
pub use crate::buf::RefMut;
pub use crate::buf::Slice;
pub use crate::buf::SliceMut;
pub use crate::buf::alloc;
pub use crate::buf::alloc_bytes;
pub use crate::buf::alloc_mut;
pub use crate::buf::provided_bytes;
pub use crate::classes::SizeClass;
pub use crate::io::MAX_CTRL_CAP;
pub use crate::io::MAX_IOV_CAP;
pub use crate::io::Msg;
pub use crate::io::MsgMut;
pub use crate::io::accept;
pub use crate::io::read;
pub use crate::io::recvmsg;
pub use crate::io::sendmsg;
pub use crate::io::write;
pub use crate::runtime::CancelStatus;
pub use crate::runtime::Op;
pub use crate::runtime::Runtime;
pub use crate::runtime::RuntimeParams;
pub use crate::runtime::block_on;
pub use crate::runtime::block_on_default;
