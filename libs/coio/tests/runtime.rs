//! The runtime's own test suite is written against the io_uring backend, down
//! to the pooled provided buffers and the fixed-slab writes. Without that
//! feature there is nothing here to exercise, so the file compiles to nothing
//! rather than failing to build.
#![cfg(feature = "uring")]

//! Runtime tests for the callback API.
//!
//! Every callback is a plain `fn` (no captures), so any state a callback needs
//! to observe is reached through the runtime's global state `G` or the op's
//! local state `L`. Tests carry a small `G` with an `Rc<RefCell<_>>` log.

use std::cell::Cell;
use std::cell::RefCell;
use std::os::fd::AsRawFd;
use std::os::fd::FromRawFd;
use std::os::fd::RawFd;
use std::rc::Rc;

use coio::Bytes;
use coio::BytesMut;
use coio::Msg;
use coio::RuntimeParams;
use coio::SizeClass;
use coio::alloc_bytes;
use coio::read_provided;
use coio::runtime::Op;
use coio::runtime::Runtime;
use coio::runtime::block_on;
use coio::runtime::block_on_default;
use coio::sendmsg;
use coio::uring::UringExt;
use coio::write_fixed;

/// The global state every test carries: a shared log the callbacks append to.
#[derive(Clone, Default)]
struct G(Rc<RefCell<Vec<String>>>);

impl G {
    fn push(&self, s: impl Into<String>) {
        self.0.borrow_mut().push(s.into());
    }
    fn lines(&self) -> Vec<String> {
        self.0.borrow().clone()
    }
}

fn tmpfile() -> std::fs::File {
    let path = b"/tmp\0";
    let fd = unsafe { libc::open(path.as_ptr().cast(), libc::O_TMPFILE | libc::O_RDWR, 0o600) };
    if fd < 0 {
        panic!("O_TMPFILE open failed: {}", std::io::Error::last_os_error());
    }
    unsafe { std::fs::File::from_raw_fd(fd) }
}

fn socketpair(stream: bool) -> (RawFd, RawFd) {
    let mut fds = [0i32; 2];
    let ty = if stream {
        libc::SOCK_STREAM
    } else {
        libc::SOCK_DGRAM
    };
    assert_eq!(
        unsafe { libc::socketpair(libc::AF_UNIX, ty, 0, fds.as_mut_ptr()) },
        0
    );
    (fds[0], fds[1])
}

fn params() -> RuntimeParams {
    RuntimeParams {
        ring_entries: 16,
        provided_size_classes: vec![SizeClass { size: 16, count: 8 }, SizeClass {
            size: 256,
            count: 4,
        }],
        // 256 for small buffers, 4096 for `BytesMut`, and 16384 so `Msg::new`
        // (msghdr + iov array + control buffer) can be allocated.
        size_classes: vec![
            SizeClass {
                size: 256,
                count: 4,
            },
            SizeClass {
                size: 4096,
                count: 2,
            },
            SizeClass {
                size: 16384,
                count: 2,
            },
        ],
    }
}

// ── trivial ──────────────────────────────────────────────────────────

#[test]
fn block_on_trivial_returns_global() {
    let g = block_on(&params(), G::default(), |_rt: &mut Runtime<G, ()>| {});
    assert!(g.lines().is_empty());
}

// ── read ─────────────────────────────────────────────────────────────

fn on_read(rt: &mut Runtime<G, ()>, _local: (), res: std::io::Result<Bytes>) {
    match res {
        Ok(b) => rt.global().push(format!("read {}", b.len())),
        Err(e) => rt.global().push(format!("read err {e}")),
    }
}

#[test]
fn read_from_tmpfile() {
    use std::io::Write;
    let mut f = tmpfile();
    f.write_all(b"hello coio").unwrap();
    let fd = std::os::fd::AsRawFd::as_raw_fd(&f);

    let g = block_on(&params(), G::default(), |rt| {
        read_provided(rt, fd, 64, (), on_read).unwrap();
    });
    assert_eq!(g.lines(), vec!["read 10"]);
}

#[test]
fn read_exceeding_largest_class_fails_efbig() {
    let f = tmpfile();
    let fd = std::os::fd::AsRawFd::as_raw_fd(&f);
    // The largest provided class here is 16 bytes.
    let mut err = None;
    block_on(&params(), G::default(), |rt| {
        err = read_provided(rt, fd, 1 << 20, (), on_read).err();
    });
    assert_eq!(err.and_then(|e| e.raw_os_error()), Some(libc::EFBIG));
}

// ── write ────────────────────────────────────────────────────────────

/// Carries the buffer being written so it stays alive until `done` runs.
struct WBuf(Bytes);

fn on_write(rt: &mut Runtime<G, WBuf>, local: WBuf, res: std::io::Result<usize>) {
    match res {
        Ok(n) => rt.global().push(format!("wrote {}", n.min(local.0.len()))),
        Err(e) => rt.global().push(format!("write err {e}")),
    }
}

#[test]
fn write_to_tmpfile() {
    let f = tmpfile();
    let fd = std::os::fd::AsRawFd::as_raw_fd(&f);

    let g = block_on(&params(), G::default(), |rt| {
        let mut buf = alloc_bytes(5).unwrap();
        buf.as_mut()[..5].copy_from_slice(b"bytes");
        buf.set_len(5);
        let addr = buf.as_ptr().as_ptr() as *const u8;
        let len = buf.len();
        write_fixed(rt, fd, addr, len, WBuf(buf.into_bytes()), on_write).unwrap();
    });
    assert_eq!(g.lines(), vec!["wrote 5"]);

    // Verify the bytes landed.
    use std::io::Read;
    use std::io::Seek;
    use std::io::SeekFrom;
    let mut out = unsafe { std::fs::File::from_raw_fd(libc::dup(fd)) };
    out.seek(SeekFrom::Start(0)).unwrap();
    let mut s = String::new();
    out.read_to_string(&mut s).unwrap();
    assert_eq!(s, "bytes");
}

// ── accept ───────────────────────────────────────────────────────────

fn on_accept(rt: &mut Runtime<G, ()>, _local: (), res: std::io::Result<RawFd>) {
    match res {
        Ok(fd) => {
            rt.global().push(format!("accepted {fd}"));
            unsafe { libc::close(fd) };
        }
        Err(e) => rt.global().push(format!("accept err {e}")),
    }
}

#[test]
fn accept_on_connected_listener() {
    let path = format!("/tmp/coio-accept-{}.sock", std::process::id());
    let listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
    listener.set_nonblocking(true).unwrap();
    let lfd = std::os::fd::AsRawFd::as_raw_fd(&listener);

    let g = block_on(&params(), G::default(), |rt| {
        rt.accept(lfd, (), on_accept).unwrap();
        let _c = std::os::unix::net::UnixStream::connect(&path);
    });
    assert!(
        g.lines().first().is_some_and(|l| l.starts_with("accepted")),
        "expected an accepted fd, got {:?}",
        g.lines()
    );
    std::fs::remove_file(&path).ok();
}

// ── cancel ───────────────────────────────────────────────────────────

/// Shared handle slot so a completion callback can cancel the op that just
/// completed (its handle is stale the instant it completes).
#[derive(Clone, Default)]
struct H(Rc<RefCell<Option<Op>>>);

fn on_read_then_cancel(rt: &mut Runtime<(G, H), ()>, _local: (), res: std::io::Result<Bytes>) {
    let n = res.map(|b| b.len()).unwrap_or(0);
    rt.global().0.push(format!("read {n}"));
    let h = rt.global().1.clone();
    if let Some(op) = h.0.borrow_mut().take() {
        // The handle names an op that has already run its callback, so the
        // cancellation is inert and `on_cancel_done` must never be reached.
        rt.cancel(op, (), on_cancel_done);
        rt.global().0.push("cancel requested".to_string());
    }
}

fn on_cancel_done(rt: &mut Runtime<(G, H), ()>, _op_local: &mut (), _local: ()) {
    rt.global().0.push("cancel done".to_string());
}

#[test]
fn cancel_after_completion_never_runs_the_cancel_callback() {
    use std::io::Write;
    let mut f = tmpfile();
    f.write_all(b"x").unwrap();
    let a = std::os::fd::AsRawFd::as_raw_fd(&f);
    let g = G::default();
    let h = H::default();
    let hh = h.clone();
    let gg = g.clone();
    let _ = block_on(&params(), (g, h), move |rt| {
        let op = read_provided(rt, a, 16, (), on_read_then_cancel).unwrap();
        *hh.0.borrow_mut() = Some(op);
    });
    assert_eq!(
        gg.lines(),
        vec!["read 1", "cancel requested"],
        "a stale handle must cancel nothing, so the cancel callback never runs"
    );
}

// ── sendmsg / recvmsg ────────────────────────────────────────────────

/// Carries the message buffer for a send so it outlives the op.
struct SMsg(#[allow(dead_code)] Msg);

fn on_send(rt: &mut Runtime<G, SMsg>, _local: SMsg, res: std::io::Result<usize>) {
    match res {
        Ok(n) => rt.global().push(format!("sent {n}")),
        Err(e) => rt.global().push(format!("send err {e}")),
    }
}

#[test]
fn sendmsg_roundtrip_to_a_socket() {
    let (a, b) = socketpair(false);
    let g = block_on(&params(), G::default(), |rt| {
        let mut msg = Msg::new().unwrap();
        let iov = libc::iovec {
            iov_base: b"ping".as_ptr() as *mut libc::c_void,
            iov_len: 4,
        };
        msg.push_iov(iov);
        let ptr = msg.msg_ptr();
        sendmsg(rt, a, ptr, SMsg(msg), on_send).unwrap();
    });
    assert_eq!(g.lines(), vec!["sent 4"]);

    // The datagram is now pending on b; read it back.
    let mut buf = [0u8; 8];
    let n = unsafe { libc::recv(b, buf.as_mut_ptr() as *mut libc::c_void, buf.len(), 0) };
    assert_eq!(n, 4);
    assert_eq!(&buf[..4], b"ping");
    unsafe {
        libc::close(a);
        libc::close(b);
    }
}

// ── teardown ─────────────────────────────────────────────────────────

thread_local! {
    static DROPS: RefCell<usize> = const { RefCell::new(0) };
}

/// An op-local that owns a buffer drawn from the runtime's fixed pool, so
/// dropping it returns memory to the slab. Dropping this while a kernel op is
/// still in flight is exactly what `Runtime::drop`'s ordering must prevent.
struct DropProbe(#[allow(dead_code)] BytesMut);

impl Drop for DropProbe {
    fn drop(&mut self) {
        DROPS.with(|d| *d.borrow_mut() += 1);
    }
}

fn on_probe(rt: &mut Runtime<G, DropProbe>, _local: DropProbe, _res: std::io::Result<Bytes>) {
    rt.global().push("probe done");
}

/// Same, for the batched-drain global.
fn on_probe_b(rt: &mut Runtime<B, DropProbe>, _local: DropProbe, _res: std::io::Result<Bytes>) {
    rt.global().push("probe done");
}

#[test]
fn panic_with_an_op_in_flight_drops_every_local_exactly_once() {
    DROPS.with(|d| *d.borrow_mut() = 0);

    // A pipe with nothing written to it: the read stays in flight forever, so
    // unwinding out of `boot` guarantees `Runtime::drop` has live work to tear
    // down rather than a pre-drained slab.
    let mut fds = [0 as libc::c_int; 2];
    assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
    let rfd = fds[0];

    let result = std::panic::catch_unwind(|| {
        let _ = block_on(&params(), G::default(), |rt| {
            let local = DropProbe(alloc_bytes(64).unwrap());
            read_provided(rt, rfd, 64, local, on_probe).unwrap();
            // Hand the SQE to the kernel so the read is genuinely in flight —
            // queued in the kernel's poll wait — rather than sitting in our SQ.
            rt.submit().unwrap();
            panic!("boom");
        });
    });

    unsafe {
        libc::close(fds[0]);
        libc::close(fds[1]);
    }

    assert!(
        result.is_err(),
        "the panic should propagate out of block_on"
    );
    assert_eq!(
        DROPS.with(|d| *d.borrow()),
        1,
        "the in-flight op's local must be dropped exactly once"
    );
}

// ── batched completions ─────────────────────────────────────────────

/// Global state for the drain tests: an ordered log of what completed.
#[derive(Clone, Default)]
struct B(Rc<RefCell<Vec<String>>>);

impl B {
    fn push(&self, s: impl Into<String>) {
        self.0.borrow_mut().push(s.into());
    }
    fn lines(&self) -> Vec<String> {
        self.0.borrow().clone()
    }
}

/// Identifies which read a callback belongs to.
#[derive(Clone, Copy)]
struct Idx(u32);

fn on_idx(rt: &mut Runtime<B, Idx>, local: Idx, res: std::io::Result<Bytes>) {
    let n = res.map(|b| b.len()).unwrap_or(usize::MAX);
    rt.global().push(format!("read{}={n}", local.0));
}

/// Regression test for `drain_cqes` popping one entry at a time.
///
/// Every read here is dispatched before the runtime submits anything, so all
/// six completions are sitting in the CQ before `drain_cqes` runs. They must be
/// consumed in order, exactly once each. A head/tail accounting mistake in the
/// per-entry borrow/drop cycle would double-fire a callback, skip one, or spin
/// forever here — none of which the one-op-per-test cases would catch.
#[test]
fn many_completions_in_one_batch() {
    use std::io::Write;
    let mut f = tmpfile();
    f.write_all(&[b'x'; 64]).unwrap();
    let fd = f.as_raw_fd();

    const READS: u32 = 6;
    let g = block_on(&params(), B::default(), |rt| {
        for i in 0..READS {
            read_provided(rt, fd, 8, Idx(i), on_idx).unwrap();
        }
    });

    // `io_uring` makes no ordering guarantee, so compare as a multiset: what
    // matters is that each dispatch completes exactly once, not when.
    let mut got = g.lines();
    got.sort();
    let mut expected: Vec<String> = (0..READS).map(|i| format!("read{i}=8")).collect();
    expected.sort();
    assert_eq!(got, expected, "every completion must fire exactly once");
}

/// Dispatches a new op from inside a completion callback, while the CQ still
/// holds other completions. The drain loop must keep popping after the dispatch
/// rather than stalling or losing the entries behind it.
#[derive(Clone)]
struct Chain {
    log: Rc<RefCell<Vec<String>>>,
    /// Index of the next read to dispatch. Starts past the two armed by `boot`,
    /// so each completion arms exactly one new read and the total is exact.
    next: Rc<Cell<u32>>,
    fd: RawFd,
    total: u32,
}

impl Chain {
    fn push(&self, s: impl Into<String>) {
        self.log.borrow_mut().push(s.into());
    }
    fn lines(&self) -> Vec<String> {
        self.log.borrow().clone()
    }
}

fn on_chain(rt: &mut Runtime<Chain, Idx>, local: Idx, res: std::io::Result<Bytes>) {
    let n = res.map(|b| b.len()).unwrap_or(usize::MAX);
    let (fd, next, total) = {
        let b = rt.global();
        b.push(format!("read{}={n}", local.0));
        let next = b.next.get();
        b.next.set(next + 1);
        (b.fd, next, b.total)
    };
    if next < total {
        // Dispatch from inside the drain: the CQ may still hold completions for
        // the reads `boot` armed.
        read_provided(rt, fd, 8, Idx(next), on_chain).unwrap();
    }
}

#[test]
fn dispatch_from_inside_drain_keeps_draining() {
    use std::io::Write;
    let mut f = tmpfile();
    f.write_all(&[b'x'; 64]).unwrap();
    let fd = f.as_raw_fd();

    const ROUNDS: u32 = 5;
    let chain = Chain {
        log: Rc::new(RefCell::new(Vec::new())),
        next: Rc::new(Cell::new(2)), // boot arms reads 0 and 1
        fd,
        total: ROUNDS,
    };
    let g = block_on(&params(), chain.clone(), |rt| {
        // Two reads are in flight at the outset, so the first drain sees more
        // than one completion while also starting a third.
        read_provided(rt, fd, 8, Idx(0), on_chain).unwrap();
        read_provided(rt, fd, 8, Idx(1), on_chain).unwrap();
    });

    assert_eq!(g.lines().len(), ROUNDS as usize, "got {:?}", g.lines());
    assert!(
        g.lines().iter().all(|l| l.ends_with("=8")),
        "every read should return 8 bytes: {:?}",
        g.lines()
    );
}

#[test]
fn op_local_is_dropped_when_the_submission_queue_is_full() {
    DROPS.with(|d| *d.borrow_mut() = 0);

    // One ring entry, so the second dispatch cannot get an SQE into the
    // submission queue, and the backend's `arm` reports the failure so
    // `register_op` frees the slot it just claimed. That slot still holds the
    // local, so this is the path where a `ManuallyDrop` would silently leak it.
    let params = RuntimeParams {
        ring_entries: 1,
        ..params()
    };

    let mut f = tmpfile();
    {
        use std::io::Write;
        f.write_all(b"data").unwrap();
    }
    let fd = std::os::fd::AsRawFd::as_raw_fd(&f);

    let g = block_on(&params, B::default(), |rt| {
        read_provided(rt, fd, 8, DropProbe(alloc_bytes(64).unwrap()), on_probe_b).unwrap();
        // This one cannot be pushed: the SQ is full.
        let _ = read_provided(rt, fd, 8, DropProbe(alloc_bytes(64).unwrap()), on_probe_b);
    });

    assert!(
        g.lines().iter().all(|l| l == "probe done"),
        "the first read should still complete: {:?}",
        g.lines()
    );
    assert_eq!(
        DROPS.with(|d| *d.borrow()),
        2,
        "both locals must be dropped: one by its completion, one when the undispatchable op's \
         slot is freed"
    );
}

// ── memlock headroom ────────────────────────────────────────────────
//
// `IORING_REGISTER_BUFFERS` charges the registered pages 1:1 against the
// *user's* `locked_vm` for `RLIMIT_MEMLOCK` — not the mm's, so `VmLck` in
// /proc/self/status always reads 0 and is useless for this. The observable is
// the rlimit: registering more than the shared per-UID budget allows fails with
// `ENOMEM`.
//
// The default size classes put ~2 MiB in the provided region and ~2 MiB in the
// fixed region. This test builds a runtime with the defaults, which registers
// the fixed region, and asserts it succeeds. Widening that registration over
// the provided region doubles the charge, so this is the test that would catch
// a host that cannot afford it.

/// Builds and tears down a runtime with the default (largest) pools.
///
/// If the host cannot afford to lock the registered region, `build_pools`
/// panics with the kernel's `ENOMEM` and this fails — which is exactly the
/// signal we want if the registration is ever widened.
#[test]
fn default_params_runtime_fits_in_memlock() {
    let reached_boot: bool =
        block_on_default(false, |rt: &mut Runtime<bool, ()>| *rt.global_mut() = true);
    assert!(
        reached_boot,
        "a default runtime should build its pools and reach boot"
    );
}

/// A buffer that read pulled out of the *provided* pool, written straight back
/// out with `write_fixed` — never copied into the fixed region.
///
/// The whole slab is registered as fixed buffer index 0, so a provided buffer's
/// address is a valid `write_fixed` target. The local carries the `Bytes` from
/// the read's completion into the write, so the provided slot cannot be
/// recycled while the write op is still in flight.
#[derive(Default)]
struct Proxy {
    log: Rc<RefCell<Vec<String>>>,
    dst: Rc<Cell<RawFd>>,
}

impl Proxy {
    fn lines(&self) -> Vec<String> {
        self.log.borrow().clone()
    }
}

/// The read hands its buffer to the write; the write holds it alive.
enum Relay {
    Read,
    /// Never read — held only so the provided slot cannot be recycled while
    /// the write op is in flight.
    Write(#[allow(dead_code)] Bytes),
}

fn on_relay_read(rt: &mut Runtime<Proxy, Relay>, _local: Relay, res: std::io::Result<Bytes>) {
    let bytes = match res {
        Ok(b) => b,
        Err(e) => {
            rt.global().log.borrow_mut().push(format!("read err {e}"));
            return;
        }
    };
    let addr = bytes.as_ptr().as_ptr() as *const u8;
    let len = bytes.len();
    let dst = rt.global().dst.get();
    let _ = write_fixed(rt, dst, addr, len, Relay::Write(bytes), on_relay_write);
}

fn on_relay_write(rt: &mut Runtime<Proxy, Relay>, local: Relay, res: std::io::Result<usize>) {
    let line = res
        .map(|n| format!("wrote {n}"))
        .unwrap_or_else(|e| format!("err {e}"));
    rt.global().log.borrow_mut().push(line);
    // Dropping the local here returns the provided slot to the pool.
    drop(local);
}

#[test]
fn provided_buffer_can_be_written_back_without_copying() {
    use std::io::Read;
    use std::io::Seek;
    use std::io::SeekFrom;
    use std::io::Write;

    let mut src = tmpfile();
    src.write_all(b"round trip").unwrap();
    let src_fd = std::os::fd::AsRawFd::as_raw_fd(&src);
    // Bound, or the File drops at the end of the statement and closes the fd.
    let dst = tmpfile();
    let dst_fd = std::os::fd::AsRawFd::as_raw_fd(&dst);

    let proxy = Proxy {
        log: Rc::new(RefCell::new(Vec::new())),
        dst: Rc::new(Cell::new(dst_fd)),
    };
    let g = block_on(&params(), proxy, |rt| {
        read_provided(rt, src_fd, 16, Relay::Read, on_relay_read).unwrap();
    });

    assert_eq!(
        g.lines(),
        vec!["wrote 10"],
        "the provided buffer should be written back verbatim"
    );

    let mut out = unsafe { std::fs::File::from_raw_fd(libc::dup(dst_fd)) };
    out.seek(SeekFrom::Start(0)).unwrap();
    let mut s = String::new();
    out.read_to_string(&mut s).unwrap();
    assert_eq!(s, "round trip", "contents must round-trip unchanged");
}

// ── caller-supplied buffers ──────────────────────────────────────────

struct Rbuf;

fn on_read_into(
    rt: &mut Runtime<G, Rbuf>,
    _local: Rbuf,
    buf: Vec<u8>,
    res: std::io::Result<usize>,
) {
    match res {
        Ok(n) => rt
            .global_mut()
            .push(format!("read {n}: {}", String::from_utf8_lossy(&buf[..n]))),
        Err(e) => rt.global_mut().push(format!("read err {e}")),
    }
}

#[test]
fn portable_read_returns_the_callers_buffer() {
    use std::io::Write;
    let mut f = tmpfile();
    f.write_all(b"hello caller").unwrap();
    let fd = std::os::fd::AsRawFd::as_raw_fd(&f);

    let g = block_on(&params(), G::default(), |rt| {
        // Sized first so the vector has initialized bytes to read into.
        let mut buf = vec![0u8; 12];
        buf.truncate(12);
        rt.read(fd, buf, Rbuf, on_read_into).unwrap();
    });
    assert_eq!(g.lines(), vec!["read 12: hello caller"]);
}

struct Wbuf2;
fn on_write_from(
    rt: &mut Runtime<G, Wbuf2>,
    _local: Wbuf2,
    buf: &'static [u8],
    res: std::io::Result<usize>,
) {
    match res {
        Ok(n) => rt
            .global_mut()
            .push(format!("wrote {n} from {}", String::from_utf8_lossy(buf))),
        Err(e) => rt.global_mut().push(format!("write err {e}")),
    }
}

#[test]
fn portable_write_hands_the_buffer_back_unchanged() {
    let (a, _b) = socketpair(false);
    let g = block_on(&params(), G::default(), |rt| {
        rt.write(a, b"payload".as_slice(), Wbuf2, on_write_from)
            .unwrap();
    });
    assert_eq!(g.lines(), vec!["wrote 7 from payload"]);
}

/// A buffer written from must come back whole even on a short write, so a
/// caller can resume the unwritten tail itself.
struct Tail;

fn on_short_write(
    rt: &mut Runtime<G, Tail>,
    _local: Tail,
    buf: &'static [u8],
    res: std::io::Result<usize>,
) {
    match res {
        Ok(n) => rt.global_mut().push(format!(
            "sent {n} of {} ({})",
            buf.len(),
            String::from_utf8_lossy(buf)
        )),
        Err(e) => rt.global_mut().push(format!("write err {e}")),
    }
}

#[test]
fn a_short_write_returns_the_whole_buffer() {
    let (a, _b) = socketpair(false);
    let g = block_on(&params(), G::default(), |rt| {
        rt.write(a, b"0123456789".as_slice(), Tail, on_short_write)
            .unwrap();
    });
    // Either the socket took it all, or it took a prefix and the buffer still
    // describes the whole send so the caller can resume.
    let line = &g.lines()[0];
    assert!(
        line.starts_with("sent ") && line.contains("of 10"),
        "unexpected report: {line:?}"
    );
}

/// Cancelling a read that is still in flight: the cancel's callback runs first
/// with `&mut` to the op's local, then the op's own callback sees `Cancelled`.
struct CLoc(#[allow(dead_code)] Vec<u8>);
struct CGlob(std::cell::RefCell<Vec<String>>);

fn on_cancelled_read(
    rt: &mut Runtime<CGlob, CLoc>,
    local: CLoc,
    buf: Vec<u8>,
    res: std::io::Result<usize>,
) {
    let cancelled = res.as_ref().err().map(|e| e.kind()) == Some(std::io::ErrorKind::Interrupted);
    // The cancel's callback ran first and pushed onto this local, so this one
    // sees its marker.
    let held = local.0.len();
    rt.global_mut().0.borrow_mut().push(format!(
        "op cancelled={cancelled} buf_len={} local_seen={held}",
        buf.len()
    ));
}

fn on_cancel_ack(rt: &mut Runtime<CGlob, CLoc>, op_local: &mut CLoc, _local: CLoc) {
    op_local.0.push(7);
    rt.global_mut()
        .0
        .borrow_mut()
        .push("cancel ack".to_string());
}

#[test]
fn a_won_cancel_runs_its_callback_before_the_ops() {
    // A socket with nothing to read and a peer that stays open, so the read is
    // genuinely in flight and only the cancellation can complete it. The peer
    // must outlive the op, or the read would finish at EOF and the cancel would
    // legitimately lose.
    let (a, _peer) = socketpair(false);

    let g = block_on(
        &params(),
        CGlob(std::cell::RefCell::new(Vec::new())),
        |rt| {
            let op = rt
                .read(a, vec![0u8; 4], CLoc(Vec::new()), on_cancelled_read)
                .unwrap();
            rt.cancel(op, CLoc(Vec::new()), on_cancel_ack);
        },
    );
    assert_eq!(
        g.0.borrow().clone(),
        vec![
            "cancel ack".to_string(),
            // A cancelled read committed nothing, so the buffer comes back
            // empty; what matters is that the op's callback still ran.
            "op cancelled=true buf_len=0 local_seen=1".to_string(),
        ],
        "the cancel callback must run first, and the op callback must see the marker",
    );
}
