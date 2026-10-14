//! The backend-generic half of a callback io runtime.
//!
//! [`Runtime`] owns the op slabs, their intrusive free lists, the cancel
//! protocol, and the caller's global state `G`. How a request becomes a
//! completion is a [`Backend`], which the runtime cannot reach into — a backend
//! can neither invoke a completion callback nor touch `G` or an op's local, and
//! the one value that crosses the boundary ([`Completion`]) is checked by the
//! runtime before it is used.
//!
//! Each op carries its own local state `L`, moved into the op on dispatch and
//! back to a shim on completion.
//!
//! The io_uring backend and the pooled provided buffers it needs live in
//! `coio-uring`; `coio` re-exports both with io_uring as the default.

mod backend;
pub mod buf;
pub mod io;
pub mod runtime;

pub use crate::backend::Backend;
pub use crate::backend::Completion;
pub use crate::buf::IoBuf;
pub use crate::buf::IoBufErased;
pub use crate::buf::IoBufMut;
pub use crate::buf::IoBufRef;
pub use crate::buf::recover;
pub use crate::io::AcceptDone;
pub use crate::io::AcceptOp;
pub use crate::io::ByteDone;
pub use crate::io::ReadOp;
pub use crate::io::WriteOp;
pub use crate::io::cancelled;
pub use crate::io::errno;
pub use crate::io::finish_buf_op;
pub use crate::io::finish_plain_op;
pub use crate::io::is_teardown;
pub use crate::io::transferred;
pub use crate::runtime::ActiveGenGuard;
pub use crate::runtime::Op;
pub use crate::runtime::OpOutcome;
pub use crate::runtime::OpShimArgs;
pub use crate::runtime::Runtime;
pub use crate::runtime::active_gen;
pub use crate::runtime::active_gen_matches;
pub use crate::runtime::enter_active_gen;
pub use crate::runtime::is_running;
