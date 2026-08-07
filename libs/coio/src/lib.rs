//! An `io_uring`-based async runtime with zero-copy pooled buffers.
//!
//! `coio` runs lightweight tasks over a single `io_uring`, and moves data
//! through a shared registered buffer slab instead of copying: reads land in
//! provided-buffer rings (`IORING_REGISTER_PBUF_RING`) and are handed back as
//! [`Bytes`], which upgrade to a writable [`BytesMut`] with [`Bytes::into_mut`]
//! while the read view is the sole holder; writes take a [`BytesMut`] — from
//! [`alloc_bytes`] or upgraded from a read — and are
//! submitted with `IORING_OP_WRITE_FIXED`. All views are borrow-tracked: a
//! slot returns to its slab only when the last view is dropped.
//!
//! # Example
//!
//! ```no_run
//! use coio::alloc_bytes;
//! use coio::io::write;
//! use coio::runtime::block_on_default;
//!
//! fn main() {
//!     block_on_default(
//!         |ctx, _rt, fd: std::os::fd::RawFd| async move {
//!             let mut buf = alloc_bytes(4096).unwrap();
//!             buf.copy_from_slice(b"hello");
//!             buf.set_len(5);
//!             write(ctx, fd, buf.into_bytes()).await.unwrap();
//!         },
//!         1,
//!     );
//! }
//! ```

mod buf;
pub mod classes;
pub mod io;
pub mod runtime;
pub mod task;

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
pub use crate::io::Yield;
pub use crate::io::accept;
pub use crate::io::read;
pub use crate::io::recvmsg;
pub use crate::io::sendmsg;
pub use crate::io::write;
pub use crate::io::yield_now;
pub use crate::task::JoinFuture;
pub use crate::task::JoinHandle;
