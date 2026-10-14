//! The operations every backend provides, run against each one.
//!
//! The portable three — a read into a caller-supplied buffer, a write from one,
//! and an accept — are inherent methods on `Runtime`, so nothing here imports a
//! backend. Instantiating the same suite twice is what checks that
//! `ReadOp`/`WriteOp`/`AcceptOp` are genuinely a contract rather than one
//! backend's private shape: io_uring submits an SQE and gets a completion, this
//! backend waits for readiness and issues the syscall itself, and both have to
//! satisfy the same test.

#![cfg(any(feature = "uring", feature = "mio"))]

use std::cell::RefCell;
use std::io;
use std::os::fd::RawFd;
use std::rc::Rc;

#[cfg(feature = "mio")]
use coio::mio::MioRuntime;
#[cfg(feature = "uring")]
use coio::uring::UringRuntime;

macro_rules! portable_suite {
    (
        $modname:ident,
        runtime = $runtime:ident,
        block_on = $block_on:path
    ) => {
        mod $modname {
            use super::*;

            /// The runtime these tests drive, with its globals and locals.
            type Rt<G, L> = $runtime<G, L>;

            /// What the callbacks report back, gathered as lines.
            #[derive(Clone, Default)]
            struct Log(Rc<RefCell<Vec<String>>>);

            impl Log {
                fn push(&self, s: impl Into<String>) {
                    self.0.borrow_mut().push(s.into());
                }

                fn lines(&self) -> Vec<String> {
                    self.0.borrow().clone()
                }
            }

            /// The op's local, kept trivial so a test can prove it survived.
            struct Local(#[allow(dead_code)] u8);

            fn on_read(
                rt: &mut Rt<Log, Local>,
                _local: Local,
                buf: Vec<u8>,
                res: io::Result<usize>,
            ) {
                match res {
                    Ok(n) => rt
                        .global_mut()
                        .push(format!("read {n}: {}", String::from_utf8_lossy(&buf[..n]))),
                    Err(e) => rt.global_mut().push(format!("read err {e}")),
                }
            }

            fn on_write(
                rt: &mut Rt<Log, Local>,
                _local: Local,
                buf: &'static [u8],
                res: io::Result<usize>,
            ) {
                match res {
                    Ok(n) => rt
                        .global_mut()
                        .push(format!("wrote {n}: {}", String::from_utf8_lossy(buf))),
                    Err(e) => rt.global_mut().push(format!("write err {e}")),
                }
            }

            fn on_accept(rt: &mut Rt<Log, Local>, _local: Local, res: io::Result<RawFd>) {
                match res {
                    Ok(fd) => {
                        rt.global_mut().push(format!("accepted {fd}"));
                        unsafe { libc::close(fd) };
                    }
                    Err(e) => rt.global_mut().push(format!("accept err {e}")),
                }
            }

            /// A stream socketpair, kept open for the duration of a test. The
            /// peer matters: a read against a closed peer finishes at EOF
            /// immediately, which would make an operation that is meant to stay
            /// in flight finish instead.
            fn socketpair() -> (RawFd, RawFd) {
                let mut fds = [0i32; 2];
                assert_eq!(
                    unsafe {
                        libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr())
                    },
                    0
                );
                (fds[0], fds[1])
            }

            #[test]
            fn read_returns_the_callers_buffer_with_the_bytes_committed() {
                let (a, peer) = socketpair();
                let payload = b"hello caller";
                // Written to one end, read from the other: a socketpair carries
                // data across, so reading `a` after writing `a` would block.
                // SAFETY: `a` and `peer` are this test's descriptors.
                unsafe {
                    assert_eq!(
                        libc::write(peer, payload.as_ptr().cast(), payload.len()),
                        payload.len() as isize
                    );
                }

                let log = $block_on(Log::default(), |rt: &mut Rt<Log, Local>| {
                    let mut buf = vec![0u8; 16];
                    buf.truncate(16);
                    rt.read(a, buf, Local(0), on_read).unwrap();
                });
                assert_eq!(log.lines(), vec![format!("read 12: hello caller")]);
            }

            #[test]
            fn write_hands_the_buffer_back() {
                let (a, peer) = socketpair();
                let log = $block_on(Log::default(), |rt: &mut Rt<Log, Local>| {
                    rt.write(a, b"payload".as_slice(), Local(0), on_write)
                        .unwrap();
                });
                assert_eq!(log.lines(), vec!["wrote 7: payload"]);

                let mut buf = [0u8; 7];
                // SAFETY: `a` is this test's descriptor and `buf` is ours.
                let n = unsafe { libc::read(peer, buf.as_mut_ptr().cast(), buf.len()) };
                assert_eq!(n, 7);
                assert_eq!(&buf, b"payload");
            }

            /// A listening socket on an abstract address, so nothing is left in
            /// the filesystem for the test to clean up.
            fn listener() -> RawFd {
                // SAFETY: plain socket setup; every call is checked below.
                unsafe {
                    let fd = libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0);
                    assert!(fd >= 0, "socket: {}", io::Error::last_os_error());
                    let name = b"\0coio-portable-test\0";
                    let mut addr: libc::sockaddr_un = std::mem::zeroed();
                    addr.sun_family = libc::AF_UNIX as libc::sa_family_t;
                    assert!(
                        name.len()
                            <= size_of::<libc::sockaddr_un>() - size_of::<libc::sa_family_t>()
                    );
                    let body = &mut addr.sun_path as *mut _ as *mut u8;
                    std::ptr::copy_nonoverlapping(name.as_ptr(), body, name.len());
                    let len = (size_of::<libc::sa_family_t>() + name.len()) as libc::socklen_t;
                    assert_eq!(
                        libc::bind(fd, (&raw const addr).cast(), len),
                        0,
                        "bind: {}",
                        io::Error::last_os_error()
                    );
                    assert_eq!(
                        libc::listen(fd, 8),
                        0,
                        "listen: {}",
                        io::Error::last_os_error()
                    );
                    fd
                }
            }

            /// A connected client socket for the abstract address
            /// `listener` bound.
            fn connect_to() -> RawFd {
                // SAFETY: the abstract address matches the one `listener` bound.
                unsafe {
                    let fd = libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0);
                    assert!(fd >= 0, "socket: {}", io::Error::last_os_error());
                    let name = b"\0coio-portable-test\0";
                    let mut addr: libc::sockaddr_un = std::mem::zeroed();
                    addr.sun_family = libc::AF_UNIX as libc::sa_family_t;
                    let body = &mut addr.sun_path as *mut _ as *mut u8;
                    std::ptr::copy_nonoverlapping(name.as_ptr(), body, name.len());
                    let len = (size_of::<libc::sa_family_t>() + name.len()) as libc::socklen_t;
                    assert_eq!(
                        libc::connect(fd, (&raw const addr).cast(), len),
                        0,
                        "connect: {}",
                        io::Error::last_os_error()
                    );
                    fd
                }
            }

            #[test]
            fn accept_yields_a_descriptor() {
                let lfd = listener();
                let cfd = connect_to();

                let log = $block_on(Log::default(), |rt: &mut Rt<Log, Local>| {
                    rt.accept(lfd, Local(0), on_accept).unwrap();
                });
                let lines = log.lines();
                assert_eq!(lines.len(), 1);
                assert!(lines[0].starts_with("accepted "), "got {lines:?}");

                // SAFETY: both descriptors are this test's.
                unsafe {
                    libc::close(cfd);
                    libc::close(lfd);
                }
            }

            // ── Cancellation ──
            //
            // The interesting part of the cancel contract on a readiness
            // backend: there is no in-flight kernel request to interrupt, so the
            // backend has to *synthesise* the completion. If it does not, the
            // runtime's in-flight count never falls and the drive loop never
            // ends. These three check the ordering, the `&mut` handoff, and the
            // stale handle.

            /// Marks the op local so the op's callback can prove the cancel's
            /// ran first.
            struct Marked(Vec<u8>);

            fn on_cancelled_read(
                rt: &mut Rt<Log, Marked>,
                local: Marked,
                buf: Vec<u8>,
                res: io::Result<usize>,
            ) {
                let cancelled =
                    res.as_ref().err().map(|e| e.kind()) == Some(io::ErrorKind::Interrupted);
                rt.global_mut().push(format!(
                    "op cancelled={cancelled} held={} buf_len={}",
                    local.0.len(),
                    buf.len()
                ));
            }

            fn on_cancel_ack(rt: &mut Rt<Log, Marked>, op_local: &mut Marked, _own: Marked) {
                op_local.0.push(1);
                rt.global_mut().push("cancel ack".to_string());
            }

            #[test]
            fn a_won_cancel_runs_its_callback_first_and_then_the_ops() {
                // Nothing to read, and the peer stays open, so the read is
                // genuinely in flight and only the cancellation finishes it.
                let (a, _peer) = socketpair();

                let log = $block_on(Log::default(), |rt: &mut Rt<Log, Marked>| {
                    let op = rt
                        .read(a, vec![0u8; 4], Marked(Vec::new()), on_cancelled_read)
                        .unwrap();
                    rt.cancel(op, Marked(Vec::new()), on_cancel_ack);
                });

                let lines = log.lines();
                assert_eq!(
                    lines,
                    vec![
                        "cancel ack".to_string(),
                        // The op's callback ran, saw a cancellation, and saw
                        // the marker the cancel's callback left behind.
                        "op cancelled=true held=1 buf_len=0".to_string(),
                    ],
                    "the cancel callback must run first, and the op's callback must observe it"
                );
            }

            #[test]
            fn a_stale_cancel_never_runs_its_callback() {
                // A handle whose op already completed cannot be cancelled.
                let (a, peer) = socketpair();
                // Written to the far end: a socketpair carries data across, so
                // reading `a` after writing `a` would block forever.
                // SAFETY: `a` and `peer` are this test's descriptors.
                unsafe { libc::write(peer, b"x".as_ptr().cast(), 1) };

                let holder = Rc::new(RefCell::new(None));
                let holder_cb = holder.clone();
                let first = $block_on(Log::default(), move |rt: &mut Rt<Log, Marked>| {
                    let op = rt
                        .read(a, vec![0u8; 4], Marked(Vec::new()), on_cancelled_read)
                        .unwrap();
                    *holder_cb.borrow_mut() = Some(op);
                });
                // The read completed on its own, which is what makes the handle
                // stale; nothing cancelled it.
                assert_eq!(first.lines(), vec!["op cancelled=false held=0 buf_len=1"]);

                // The read finished on its own, so cancelling its handle now is
                // inert: no cancel callback, and no second op callback.
                let second = $block_on(Log::default(), |rt: &mut Rt<Log, Marked>| {
                    rt.cancel(holder.borrow().unwrap(), Marked(Vec::new()), on_cancel_ack);
                });
                assert_eq!(
                    second.lines(),
                    Vec::<String>::new(),
                    "a stale handle must cancel nothing"
                );
            }
        }
    };
}

#[cfg(feature = "uring")]
portable_suite! {
    uring,
    runtime = UringRuntime,
    block_on = coio::uring::block_on_default
}

#[cfg(feature = "mio")]
portable_suite! {
    mio,
    runtime = MioRuntime,
    block_on = coio::mio::block_on_default
}
