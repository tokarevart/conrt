use coio::SizeClass;
use coio::runtime::RuntimeContext;
use coio::runtime::RuntimeParams;
use coio::runtime::block_on;

fn tmpfile() -> std::fs::File {
    use std::os::fd::FromRawFd;

    let path = b"/tmp\0";
    let fd = unsafe { libc::open(path.as_ptr().cast(), libc::O_TMPFILE | libc::O_RDWR, 0o600) };
    if fd < 0 {
        panic!("O_TMPFILE open failed: {}", std::io::Error::last_os_error());
    }
    unsafe { std::fs::File::from_raw_fd(fd) }
}

#[test]
fn block_on_trivial() {
    let rt = RuntimeParams {
        tasks_capacity: 4,
        ring_entries: 8,
        provided_size_classes: &[SizeClass { size: 16, count: 4 }],
        size_classes: &[SizeClass { size: 16, count: 4 }],
    };
    block_on(rt, |_, _, _data: ()| async move {}, ());
}

#[test]
fn block_on_with_spawn() {
    let rt = RuntimeParams {
        tasks_capacity: 4,
        ring_entries: 8,
        provided_size_classes: &[SizeClass { size: 16, count: 4 }],
        size_classes: &[SizeClass { size: 16, count: 4 }],
    };
    block_on(
        rt,
        |_, rt: RuntimeContext<u32, ()>, data: u32| async move {
            if data < 3 {
                rt.spawn(data + 1);
            }
        },
        0,
    );
}

#[test]
fn block_on_spawn_chain() {
    let rt = RuntimeParams {
        tasks_capacity: 16,
        ring_entries: 8,
        provided_size_classes: &[SizeClass { size: 16, count: 4 }],
        size_classes: &[SizeClass { size: 16, count: 4 }],
    };
    block_on(
        rt,
        |_, rt: RuntimeContext<u32, ()>, data: u32| async move {
            if data < 3 {
                rt.spawn(data + 1);
                rt.spawn(data + 2);
            }
        },
        0,
    );
}

#[test]
fn stale_runtime_context_panics_outside_block_on() {
    use std::cell::RefCell;
    use std::panic::AssertUnwindSafe;
    use std::panic::catch_unwind;

    thread_local! {
        static STASH: RefCell<Option<RuntimeContext<u32, ()>>> = const { RefCell::new(None) };
    }

    let rt = RuntimeParams {
        tasks_capacity: 4,
        ring_entries: 8,
        provided_size_classes: &[SizeClass { size: 16, count: 4 }],
        size_classes: &[SizeClass { size: 16, count: 4 }],
    };
    block_on(
        rt,
        |_, rt: RuntimeContext<u32, ()>, _data: u32| async move {
            STASH.with(|s| *s.borrow_mut() = Some(rt));
        },
        0,
    );

    let result = catch_unwind(AssertUnwindSafe(|| {
        STASH.with(|s| {
            if let Some(ctx) = s.borrow().as_ref() {
                ctx.spawn(1);
            }
        });
    }));
    assert!(result.is_err());
}

#[test]
fn stale_task_context_panics_outside_block_on() {
    use std::cell::RefCell;
    use std::panic::AssertUnwindSafe;
    use std::panic::catch_unwind;

    thread_local! {
        static STASH: RefCell<Option<coio::task::TaskContext>> = const { RefCell::new(None) };
    }

    let rt = RuntimeParams {
        tasks_capacity: 4,
        ring_entries: 8,
        provided_size_classes: &[SizeClass { size: 16, count: 4 }],
        size_classes: &[SizeClass { size: 16, count: 4 }],
    };
    block_on(
        rt,
        |ctx: coio::task::TaskContext, _rt, _data: u32| async move {
            STASH.with(|s| *s.borrow_mut() = Some(ctx));
        },
        0,
    );

    let result = catch_unwind(AssertUnwindSafe(|| {
        STASH.with(|s| {
            if let Some(ctx) = s.borrow().as_ref() {
                ctx.with_task(|_t| ());
            }
        });
    }));
    assert!(result.is_err());
}

#[test]
fn stale_runtime_context_from_previous_run_panics() {
    use std::cell::RefCell;
    use std::panic::AssertUnwindSafe;
    use std::panic::catch_unwind;

    thread_local! {
        static STASH: RefCell<Option<RuntimeContext<u32, ()>>> = const { RefCell::new(None) };
    }

    let rt = RuntimeParams {
        tasks_capacity: 4,
        ring_entries: 8,
        provided_size_classes: &[SizeClass { size: 16, count: 4 }],
        size_classes: &[SizeClass { size: 16, count: 4 }],
    };
    block_on(
        rt,
        |_, rt: RuntimeContext<u32, ()>, _data: u32| async move {
            STASH.with(|s| *s.borrow_mut() = Some(rt));
        },
        0,
    );

    let rt2 = RuntimeParams {
        tasks_capacity: 4,
        ring_entries: 8,
        provided_size_classes: &[SizeClass { size: 16, count: 4 }],
        size_classes: &[SizeClass { size: 16, count: 4 }],
    };
    block_on(
        rt2,
        |_, _rt: RuntimeContext<u32, ()>, _data: u32| async move {
            let result = catch_unwind(AssertUnwindSafe(|| {
                STASH.with(|s| {
                    if let Some(ctx) = s.borrow().as_ref() {
                        ctx.spawn(1);
                    }
                });
            }));
            assert!(
                result.is_err(),
                "stale context from a previous run must panic"
            );
        },
        0,
    );
}

#[test]
fn block_on_read_from_file() {
    use std::io::Seek;
    use std::io::SeekFrom;
    use std::io::Write;
    use std::os::fd::AsRawFd;

    use coio::io::read;

    let mut file = tmpfile();
    file.write_all(b"hello world").unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    let fd = file.as_raw_fd();

    let rt = RuntimeParams {
        tasks_capacity: 4,
        ring_entries: 8,
        provided_size_classes: &[SizeClass { size: 16, count: 4 }],
        size_classes: &[SizeClass { size: 16, count: 4 }],
    };
    block_on(
        rt,
        |ctx, _rt, fd| async move {
            let data = read(ctx, fd, 5).await.unwrap();
            assert_eq!(data.as_ref(), b"hello");
        },
        fd,
    );
}

#[test]
fn block_on_read_full_buffer() {
    use std::io::Seek;
    use std::io::SeekFrom;
    use std::io::Write;
    use std::os::fd::AsRawFd;

    use coio::io::read;

    let mut file = tmpfile();
    file.write_all(b"hello world").unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    let fd = file.as_raw_fd();

    let rt = RuntimeParams {
        tasks_capacity: 4,
        ring_entries: 8,
        provided_size_classes: &[SizeClass { size: 16, count: 4 }],
        size_classes: &[SizeClass { size: 16, count: 4 }],
    };
    block_on(
        rt,
        |ctx, _rt, fd| async move {
            let data = read(ctx, fd, 5).await.unwrap();
            assert_eq!(data.as_ref(), b"hello");
        },
        fd,
    );
}

#[test]
fn block_on_returns_main_output() {
    let rt = RuntimeParams {
        tasks_capacity: 4,
        ring_entries: 8,
        provided_size_classes: &[SizeClass { size: 16, count: 4 }],
        size_classes: &[SizeClass { size: 16, count: 4 }],
    };
    let out = block_on(rt, |_, _rt, data: u32| async move { data + 1 }, 41);
    assert_eq!(out, 42);
}

#[test]
fn join_yields_task_output() {
    let rt = RuntimeParams {
        tasks_capacity: 4,
        ring_entries: 8,
        provided_size_classes: &[SizeClass { size: 16, count: 4 }],
        size_classes: &[SizeClass { size: 16, count: 4 }],
    };
    let out = block_on(
        rt,
        |_, rt: RuntimeContext<u32, u32>, data: u32| async move {
            if data < 42
                && let Some(handle) = rt.spawn(data + 1)
            {
                let joined = handle.join().await;
                assert_eq!(joined, Some(data + 1));
            }
            data
        },
        41,
    );
    assert_eq!(out, 41);
}

#[test]
fn join_after_cancel_returns_none() {
    let rt = RuntimeParams {
        tasks_capacity: 4,
        ring_entries: 8,
        provided_size_classes: &[SizeClass { size: 16, count: 4 }],
        size_classes: &[SizeClass { size: 16, count: 4 }],
    };
    let out = block_on(
        rt,
        |_, rt: RuntimeContext<u32, u32>, data: u32| async move {
            if data < 42
                && let Some(handle) = rt.spawn(data + 1)
            {
                assert!(handle.cancel());
                let joined = handle.join().await;
                assert_eq!(joined, None);
            }
            7
        },
        41,
    );
    assert_eq!(out, 7);
}

#[test]
fn into_handle_resumes_later_join() {
    let rt = RuntimeParams {
        tasks_capacity: 4,
        ring_entries: 8,
        provided_size_classes: &[SizeClass { size: 16, count: 4 }],
        size_classes: &[SizeClass { size: 16, count: 4 }],
    };
    block_on(
        rt,
        |_, rt: RuntimeContext<u32, u32>, data: u32| async move {
            if data < 42
                && let Some(handle) = rt.spawn(data + 1)
            {
                // The target runs to completion unjoined; the handle can be
                // dropped without corrupting the output slab.
                let _ = handle.join().into_handle();
            }
            0
        },
        41,
    );
}

#[test]
fn block_on_read_many_recycles() {
    use std::io::Seek;
    use std::io::SeekFrom;
    use std::io::Write;
    use std::os::fd::AsRawFd;

    use coio::io::read;

    // More reads than the pool holds, exercising tail wrap + recycling.
    // Reads use an absolute offset of 0, so every call returns the same bytes.
    let mut file = tmpfile();
    file.write_all(b"hello world").unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    let fd = file.as_raw_fd();

    let rt = RuntimeParams {
        tasks_capacity: 4,
        ring_entries: 8,
        provided_size_classes: &[SizeClass { size: 16, count: 4 }],
        size_classes: &[SizeClass { size: 16, count: 4 }],
    };
    block_on(
        rt,
        |ctx, _rt, fd| async move {
            for _ in 0..8 {
                let data = read(ctx, fd, 16).await.unwrap();
                assert_eq!(data.as_ref(), b"hello world");
            }
        },
        fd,
    );
}

// ── sendmsg/recvmsg ───────────────────────────────────────────────

/// `CMSG_LEN(n)`: the length of a `cmsghdr` plus its `n` payload bytes, as
/// recorded in `cmsg_len`.
fn cmsg_len(payload: usize) -> usize {
    size_of::<libc::cmsghdr>() + payload
}

fn socketpair(stream: bool) -> (i32, i32) {
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

fn rt() -> RuntimeParams<'static> {
    RuntimeParams {
        tasks_capacity: 4,
        ring_entries: 8,
        provided_size_classes: &[SizeClass { size: 16, count: 4 }],
        size_classes: &[
            SizeClass { size: 16, count: 4 },
            SizeClass { size: 64, count: 4 },
            SizeClass {
                size: 128,
                count: 2,
            },
            SizeClass {
                size: 2048,
                count: 2,
            },
            SizeClass {
                size: 16384,
                count: 2,
            },
        ],
    }
}

#[test]
fn block_on_sendmsg_recvmsg_roundtrip() {
    use coio::io::Msg;
    use coio::io::MsgMut;
    use coio::io::recvmsg;
    use coio::io::sendmsg;

    let (a, b) = socketpair(true);
    block_on(
        rt(),
        |ctx, _rt, (a, b): (i32, i32)| async move {
            let mut snd = Msg::new().unwrap();
            snd.push_iov(libc::iovec {
                iov_base: b"hello".as_ptr().cast_mut().cast(),
                iov_len: 5,
            });
            let n = sendmsg(ctx, a, &mut snd).await.unwrap();
            assert_eq!(n, 5);

            let mut rcv = MsgMut::new().unwrap();
            let mut buf = [0u8; 16];
            rcv.push_iov(libc::iovec {
                iov_base: buf.as_mut_ptr().cast(),
                iov_len: buf.len(),
            });
            let n = recvmsg(ctx, b, &mut rcv, 0).await.unwrap();
            assert_eq!(n, 5);
            assert_eq!(&buf[..5], b"hello");
        },
        (a, b),
    );
    unsafe { libc::close(a) };
    unsafe { libc::close(b) };
}

#[test]
fn block_on_sendmsg_subset_of_iovecs() {
    use coio::io::Msg;
    use coio::io::MsgMut;
    use coio::io::recvmsg;
    use coio::io::sendmsg;

    let (a, b) = socketpair(true);
    block_on(
        rt(),
        |ctx, _rt, (a, b): (i32, i32)| async move {
            // A <2, 0> slot with only the first iovec filled: only that one
            // segment is declared (msg_iovlen == 1) and handed to the kernel,
            // so the uninitialized second segment is never exposed.
            let mut snd = Msg::new().unwrap();
            snd.push_iov(libc::iovec {
                iov_base: b"ab".as_ptr().cast_mut().cast(),
                iov_len: 1,
            });
            let n = sendmsg(ctx, a, &mut snd).await.unwrap();
            assert_eq!(n, 1);

            let mut rcv = MsgMut::new().unwrap();
            let mut buf = [0u8; 8];
            rcv.push_iov(libc::iovec {
                iov_base: buf.as_mut_ptr().cast(),
                iov_len: buf.len(),
            });
            let n = recvmsg(ctx, b, &mut rcv, 0).await.unwrap();
            assert_eq!(n, 1);
            assert_eq!(buf[0], b'a');
        },
        (a, b),
    );
    unsafe { libc::close(a) };
    unsafe { libc::close(b) };
}

#[test]
fn block_on_fd_passing_without_iov() {
    use std::os::fd::AsRawFd;

    use coio::io::Msg;
    use coio::io::MsgMut;
    use coio::io::recvmsg;
    use coio::io::sendmsg;

    // A file to pass; the receiver must get a new fd to the same file.
    let sent_file = tmpfile();
    let sent_fd = sent_file.as_raw_fd();
    std::mem::forget(sent_file);

    let (a, b) = socketpair(false);
    block_on(
        rt(),
        |ctx, _rt, (a, b, sent_fd): (i32, i32, i32)| async move {
            // Sender: a cmsg carrying `sent_fd`, with no data iovec.
            let mut snd = Msg::new().unwrap();
            assert!(snd.push_scm_rights(&[sent_fd]));
            let n = sendmsg(ctx, a, &mut snd).await.unwrap();
            assert_eq!(n, 0);

            // Receiver: read the cmsg back out of the pooled control buffer.
            let mut rcv = MsgMut::new().unwrap();
            let n = recvmsg(ctx, b, &mut rcv, 0).await.unwrap();
            assert_eq!(n, 0);

            let (recv_fd,) = {
                let msg = rcv.msg();
                assert_eq!(msg.msg_flags & libc::MSG_CTRUNC, 0);
                assert!(msg.msg_controllen >= cmsg_len(size_of::<i32>()));
                let hdr = msg.msg_control as *const libc::cmsghdr;
                unsafe {
                    let hdr = &*hdr;
                    assert_eq!(hdr.cmsg_level, libc::SOL_SOCKET);
                    assert_eq!(hdr.cmsg_type, libc::SCM_RIGHTS);
                    assert_eq!(hdr.cmsg_len, cmsg_len(size_of::<i32>()));
                    let data = (hdr as *const libc::cmsghdr as *const u8)
                        .add(size_of::<libc::cmsghdr>())
                        as *const i32;
                    (*data,)
                }
            };

            // A distinct fd to the same file.
            assert_ne!(recv_fd, sent_fd);
            let mut st1: libc::stat = unsafe { core::mem::zeroed() };
            let mut st2: libc::stat = unsafe { core::mem::zeroed() };
            assert_eq!(unsafe { libc::fstat(sent_fd, &mut st1) }, 0);
            assert_eq!(unsafe { libc::fstat(recv_fd, &mut st2) }, 0);
            assert_eq!(st1.st_dev, st2.st_dev);
            assert_eq!(st1.st_ino, st2.st_ino);
            unsafe { libc::close(recv_fd) };
        },
        (a, b, sent_fd),
    );
    unsafe { libc::close(a) };
    unsafe { libc::close(b) };
    unsafe { libc::close(sent_fd) };
}

#[test]
fn block_on_fd_passing_with_data() {
    use std::os::fd::AsRawFd;

    use coio::io::Msg;
    use coio::io::MsgMut;
    use coio::io::recvmsg;
    use coio::io::sendmsg;

    let sent_file = tmpfile();
    let sent_fd = sent_file.as_raw_fd();
    std::mem::forget(sent_file);

    let (a, b) = socketpair(true);
    block_on(
        rt(),
        |ctx, _rt, (a, b, sent_fd): (i32, i32, i32)| async move {
            let mut snd = Msg::new().unwrap();
            snd.push_iov(libc::iovec {
                iov_base: b"x".as_ptr().cast_mut().cast(),
                iov_len: 1,
            });
            assert!(snd.push_scm_rights(&[sent_fd]));
            let n = sendmsg(ctx, a, &mut snd).await.unwrap();
            assert_eq!(n, 1);

            let mut rcv = MsgMut::new().unwrap();
            let mut buf = [0u8; 16];
            rcv.push_iov(libc::iovec {
                iov_base: buf.as_mut_ptr().cast(),
                iov_len: buf.len(),
            });
            let n = recvmsg(ctx, b, &mut rcv, 0).await.unwrap();
            assert_eq!(n, 1);
            assert_eq!(buf[0], b'x');

            let recv_fd = {
                let msg = rcv.msg();
                assert_eq!(msg.msg_flags & libc::MSG_CTRUNC, 0);
                let hdr = msg.msg_control as *const libc::cmsghdr;
                unsafe {
                    let hdr = &*hdr;
                    assert_eq!(hdr.cmsg_level, libc::SOL_SOCKET);
                    assert_eq!(hdr.cmsg_type, libc::SCM_RIGHTS);
                    let data = (hdr as *const libc::cmsghdr as *const u8)
                        .add(size_of::<libc::cmsghdr>())
                        as *const i32;
                    *data
                }
            };

            assert_ne!(recv_fd, sent_fd);
            let mut st1: libc::stat = unsafe { core::mem::zeroed() };
            let mut st2: libc::stat = unsafe { core::mem::zeroed() };
            assert_eq!(unsafe { libc::fstat(sent_fd, &mut st1) }, 0);
            assert_eq!(unsafe { libc::fstat(recv_fd, &mut st2) }, 0);
            assert_eq!(st1.st_dev, st2.st_dev);
            assert_eq!(st1.st_ino, st2.st_ino);
            unsafe { libc::close(recv_fd) };
        },
        (a, b, sent_fd),
    );
    unsafe { libc::close(a) };
    unsafe { libc::close(b) };
    unsafe { libc::close(sent_fd) };
}

#[test]
fn block_on_read_into_vec() {
    use std::io::Seek;
    use std::io::SeekFrom;
    use std::io::Write;
    use std::os::fd::AsRawFd;

    use coio::io::read;

    let mut file = tmpfile();
    file.write_all(b"hello world").unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    let fd = file.as_raw_fd();

    let rt = RuntimeParams {
        tasks_capacity: 4,
        ring_entries: 8,
        provided_size_classes: &[SizeClass { size: 16, count: 4 }],
        size_classes: &[SizeClass { size: 16, count: 4 }],
    };
    block_on(
        rt,
        |ctx, _rt, fd| async move {
            let buf = read(ctx, fd, 16).await.unwrap();
            let data = buf.into_vec();
            assert_eq!(data, b"hello world");
        },
        fd,
    );
}

#[test]
fn block_on_write_to_file() {
    use std::io::Read;
    use std::io::Seek;
    use std::io::SeekFrom;
    use std::os::fd::AsRawFd;

    use coio::alloc_bytes;
    use coio::io::write;

    let mut file = tmpfile();
    let fd = file.as_raw_fd();

    let rt = RuntimeParams {
        tasks_capacity: 4,
        ring_entries: 8,
        provided_size_classes: &[SizeClass { size: 16, count: 4 }],
        size_classes: &[SizeClass { size: 16, count: 4 }],
    };
    block_on(
        rt,
        |ctx, _rt, fd| async move {
            let mut wb = alloc_bytes(5).unwrap();
            wb.as_mut()[..5].copy_from_slice(b"hello");
            wb.set_len(5);
            let n = write(ctx, fd, wb.into_bytes()).await.unwrap();
            assert_eq!(n, 5);
        },
        fd,
    );

    file.seek(SeekFrom::Start(0)).unwrap();
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).unwrap();
    assert_eq!(buf, b"hello");
}

#[test]
fn block_on_write_full_slot() {
    use std::io::Read;
    use std::io::Seek;
    use std::io::SeekFrom;
    use std::os::fd::AsRawFd;

    use coio::alloc_bytes;
    use coio::io::write;

    // The whole slot (buf_size) can be used for a single write.
    let mut file = tmpfile();
    let fd = file.as_raw_fd();

    let rt = RuntimeParams {
        tasks_capacity: 4,
        ring_entries: 8,
        provided_size_classes: &[SizeClass { size: 16, count: 4 }],
        size_classes: &[SizeClass { size: 16, count: 4 }],
    };
    block_on(
        rt,
        |ctx, _rt, fd| async move {
            let mut wb = alloc_bytes(16).unwrap();
            assert_eq!(wb.capacity(), 16);
            wb.as_mut().fill(0xAB);
            wb.set_len(16);
            let n = write(ctx, fd, wb.into_bytes()).await.unwrap();
            assert_eq!(n, 16);
        },
        fd,
    );

    file.seek(SeekFrom::Start(0)).unwrap();
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).unwrap();
    assert_eq!(buf, [0xAB; 16]);
}

#[test]
fn block_on_write_many_recycles() {
    use std::io::Read;
    use std::io::Seek;
    use std::io::SeekFrom;
    use std::os::fd::AsRawFd;

    use coio::alloc_bytes;
    use coio::io::write;

    // More writes than the pool holds, exercising slot recycling.
    // Writes use an absolute offset of 0, so each write overwrites the file.
    let mut file = tmpfile();
    let fd = file.as_raw_fd();

    let rt = RuntimeParams {
        tasks_capacity: 4,
        ring_entries: 8,
        provided_size_classes: &[SizeClass { size: 16, count: 4 }],
        size_classes: &[SizeClass { size: 16, count: 4 }],
    };
    block_on(
        rt,
        |ctx, _rt, fd| async move {
            for i in 0..8u32 {
                let mut wb = alloc_bytes(8).unwrap();
                let payload = format!("chunk{i:03}");
                wb.as_mut()[..8].copy_from_slice(payload.as_bytes());
                wb.set_len(8);
                let n = write(ctx, fd, wb.into_bytes()).await.unwrap();
                assert_eq!(n, 8);
            }
        },
        fd,
    );

    file.seek(SeekFrom::Start(0)).unwrap();
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).unwrap();
    assert_eq!(buf, b"chunk007");
}

#[test]
fn block_on_read_uses_proportional_levels() {
    use std::io::Seek;
    use std::io::SeekFrom;
    use std::io::Write;
    use std::os::fd::AsRawFd;

    use coio::io::read;

    // Two levels: 8-byte and 64-byte slots. A 5-byte read fits the 8-byte
    // level, a 16-byte read falls through to the 64-byte level.
    let mut file = tmpfile();
    file.write_all(b"hello world").unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    let fd = file.as_raw_fd();

    let rt = RuntimeParams {
        tasks_capacity: 4,
        ring_entries: 8,
        provided_size_classes: &[SizeClass { size: 8, count: 4 }, SizeClass {
            size: 64,
            count: 2,
        }],
        size_classes: &[SizeClass { size: 8, count: 4 }],
    };
    block_on(
        rt,
        |ctx, _rt, fd| async move {
            let data = read(ctx, fd, 5).await.unwrap();
            assert_eq!(data.as_ref(), b"hello");
            assert_eq!(data.len(), 5);

            let data = read(ctx, fd, 16).await.unwrap();
            assert_eq!(data.as_ref(), b"hello world");
            assert_eq!(data.len(), 11);
        },
        fd,
    );
}

#[test]
fn block_on_write_uses_proportional_levels() {
    use std::io::Read;
    use std::io::Seek;
    use std::io::SeekFrom;
    use std::os::fd::AsRawFd;

    use coio::alloc_bytes;
    use coio::io::write;

    // An 11-byte write needs the 64-byte level; its capacity is 64.
    let mut file = tmpfile();
    let fd = file.as_raw_fd();

    let rt = RuntimeParams {
        tasks_capacity: 4,
        ring_entries: 8,
        provided_size_classes: &[SizeClass { size: 8, count: 4 }],
        size_classes: &[SizeClass { size: 8, count: 4 }, SizeClass {
            size: 64,
            count: 2,
        }],
    };
    block_on(
        rt,
        |ctx, _rt, fd| async move {
            let mut wb = alloc_bytes(11).unwrap();
            assert_eq!(wb.capacity(), 64);
            wb.as_mut()[..11].copy_from_slice(b"hello world");
            wb.set_len(11);
            let n = write(ctx, fd, wb.into_bytes()).await.unwrap();
            assert_eq!(n, 11);
        },
        fd,
    );

    file.seek(SeekFrom::Start(0)).unwrap();
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).unwrap();
    assert_eq!(buf, b"hello world");
}

#[test]
fn block_on_read_oversized_errors() {
    use std::os::fd::AsRawFd;

    use coio::io::read;

    let file = tmpfile();
    let fd = file.as_raw_fd();

    let rt = RuntimeParams {
        tasks_capacity: 4,
        ring_entries: 8,
        provided_size_classes: &[SizeClass { size: 16, count: 4 }],
        size_classes: &[SizeClass { size: 16, count: 4 }],
    };
    block_on(
        rt,
        |ctx, _rt, fd| async move {
            let err = read(ctx, fd, 17).await.err().unwrap();
            assert_eq!(err.raw_os_error(), Some(libc::EFBIG));
        },
        fd,
    );
}

#[test]
fn block_on_alloc_bytes_oversized_errors() {
    use std::os::fd::AsRawFd;

    use coio::alloc_bytes;

    let file = tmpfile();
    let fd = file.as_raw_fd();

    let rt = RuntimeParams {
        tasks_capacity: 4,
        ring_entries: 8,
        provided_size_classes: &[SizeClass { size: 16, count: 4 }],
        size_classes: &[SizeClass { size: 16, count: 4 }],
    };
    block_on(
        rt,
        |_ctx, _rt, _fd| async move {
            let err = alloc_bytes(17).err().unwrap();
            assert_eq!(err.raw_os_error(), Some(libc::EFBIG));
        },
        fd,
    );
}

#[test]
fn block_on_read_with_default_levels() {
    use std::io::Seek;
    use std::io::SeekFrom;
    use std::io::Write;
    use std::os::fd::AsRawFd;

    use coio::io::read;

    // The default level table (two 2 MiB slabs) is used when the params are
    // left as `..Default::default()`.
    let mut file = tmpfile();
    file.write_all(b"hello world").unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    let fd = file.as_raw_fd();

    let rt = RuntimeParams {
        tasks_capacity: 4,
        ring_entries: 8,
        ..Default::default()
    };
    block_on(
        rt,
        |ctx, _rt, fd| async move {
            let data = read(ctx, fd, 5).await.unwrap();
            assert_eq!(data.as_ref(), b"hello");
        },
        fd,
    );
}
