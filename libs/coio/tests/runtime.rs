use coio::Level;
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
        read_levels: &[Level { size: 16, count: 4 }],
        write_levels: &[Level { size: 16, count: 4 }],
    };
    block_on(rt, |_, _, _data: ()| async move {}, ());
}

#[test]
fn block_on_with_spawn() {
    let rt = RuntimeParams {
        tasks_capacity: 4,
        ring_entries: 8,
        read_levels: &[Level { size: 16, count: 4 }],
        write_levels: &[Level { size: 16, count: 4 }],
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
        read_levels: &[Level { size: 16, count: 4 }],
        write_levels: &[Level { size: 16, count: 4 }],
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
        read_levels: &[Level { size: 16, count: 4 }],
        write_levels: &[Level { size: 16, count: 4 }],
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
        read_levels: &[Level { size: 16, count: 4 }],
        write_levels: &[Level { size: 16, count: 4 }],
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
        read_levels: &[Level { size: 16, count: 4 }],
        write_levels: &[Level { size: 16, count: 4 }],
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
        read_levels: &[Level { size: 16, count: 4 }],
        write_levels: &[Level { size: 16, count: 4 }],
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

    use coio::runtime::read;

    let mut file = tmpfile();
    file.write_all(b"hello world").unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    let fd = file.as_raw_fd();

    let rt = RuntimeParams {
        tasks_capacity: 4,
        ring_entries: 8,
        read_levels: &[Level { size: 16, count: 4 }],
        write_levels: &[Level { size: 16, count: 4 }],
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

    use coio::runtime::read;

    let mut file = tmpfile();
    file.write_all(b"hello world").unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    let fd = file.as_raw_fd();

    let rt = RuntimeParams {
        tasks_capacity: 4,
        ring_entries: 8,
        read_levels: &[Level { size: 16, count: 4 }],
        write_levels: &[Level { size: 16, count: 4 }],
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
        read_levels: &[Level { size: 16, count: 4 }],
        write_levels: &[Level { size: 16, count: 4 }],
    };
    let out = block_on(rt, |_, _rt, data: u32| async move { data + 1 }, 41);
    assert_eq!(out, 42);
}

#[test]
fn join_yields_task_output() {
    let rt = RuntimeParams {
        tasks_capacity: 4,
        ring_entries: 8,
        read_levels: &[Level { size: 16, count: 4 }],
        write_levels: &[Level { size: 16, count: 4 }],
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
        read_levels: &[Level { size: 16, count: 4 }],
        write_levels: &[Level { size: 16, count: 4 }],
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
        read_levels: &[Level { size: 16, count: 4 }],
        write_levels: &[Level { size: 16, count: 4 }],
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

    use coio::runtime::read;

    // More reads than the pool holds, exercising tail wrap + recycling.
    // Reads use an absolute offset of 0, so every call returns the same bytes.
    let mut file = tmpfile();
    file.write_all(b"hello world").unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    let fd = file.as_raw_fd();

    let rt = RuntimeParams {
        tasks_capacity: 4,
        ring_entries: 8,
        read_levels: &[Level { size: 16, count: 4 }],
        write_levels: &[Level { size: 16, count: 4 }],
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

#[test]
fn block_on_read_into_vec() {
    use std::io::Seek;
    use std::io::SeekFrom;
    use std::io::Write;
    use std::os::fd::AsRawFd;

    use coio::runtime::read;

    let mut file = tmpfile();
    file.write_all(b"hello world").unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    let fd = file.as_raw_fd();

    let rt = RuntimeParams {
        tasks_capacity: 4,
        ring_entries: 8,
        read_levels: &[Level { size: 16, count: 4 }],
        write_levels: &[Level { size: 16, count: 4 }],
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

    use coio::runtime::write;
    use coio::runtime::write_buffer;

    let mut file = tmpfile();
    let fd = file.as_raw_fd();

    let rt = RuntimeParams {
        tasks_capacity: 4,
        ring_entries: 8,
        read_levels: &[Level { size: 16, count: 4 }],
        write_levels: &[Level { size: 16, count: 4 }],
    };
    block_on(
        rt,
        |ctx, _rt, fd| async move {
            let mut wb = write_buffer(ctx, 5).unwrap();
            wb.as_mut()[..5].copy_from_slice(b"hello");
            wb.set_len(5);
            let n = write(ctx, fd, wb).await.unwrap();
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

    use coio::runtime::write;
    use coio::runtime::write_buffer;

    // The whole slot (buf_size) can be used for a single write.
    let mut file = tmpfile();
    let fd = file.as_raw_fd();

    let rt = RuntimeParams {
        tasks_capacity: 4,
        ring_entries: 8,
        read_levels: &[Level { size: 16, count: 4 }],
        write_levels: &[Level { size: 16, count: 4 }],
    };
    block_on(
        rt,
        |ctx, _rt, fd| async move {
            let mut wb = write_buffer(ctx, 16).unwrap();
            assert_eq!(wb.capacity(), 16);
            wb.as_mut().fill(0xAB);
            wb.set_len(16);
            let n = write(ctx, fd, wb).await.unwrap();
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

    use coio::runtime::write;
    use coio::runtime::write_buffer;

    // More writes than the pool holds, exercising slot recycling.
    // Writes use an absolute offset of 0, so each write overwrites the file.
    let mut file = tmpfile();
    let fd = file.as_raw_fd();

    let rt = RuntimeParams {
        tasks_capacity: 4,
        ring_entries: 8,
        read_levels: &[Level { size: 16, count: 4 }],
        write_levels: &[Level { size: 16, count: 4 }],
    };
    block_on(
        rt,
        |ctx, _rt, fd| async move {
            for i in 0..8u32 {
                let mut wb = write_buffer(ctx, 8).unwrap();
                let payload = format!("chunk{i:03}");
                wb.as_mut()[..8].copy_from_slice(payload.as_bytes());
                wb.set_len(8);
                let n = write(ctx, fd, wb).await.unwrap();
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

    use coio::runtime::read;

    // Two levels: 8-byte and 64-byte slots. A 5-byte read fits the 8-byte
    // level, a 16-byte read falls through to the 64-byte level.
    let mut file = tmpfile();
    file.write_all(b"hello world").unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    let fd = file.as_raw_fd();

    let rt = RuntimeParams {
        tasks_capacity: 4,
        ring_entries: 8,
        read_levels: &[Level { size: 8, count: 4 }, Level { size: 64, count: 2 }],
        write_levels: &[Level { size: 8, count: 4 }],
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

    use coio::runtime::write;
    use coio::runtime::write_buffer;

    // An 11-byte write needs the 64-byte level; its capacity is 64.
    let mut file = tmpfile();
    let fd = file.as_raw_fd();

    let rt = RuntimeParams {
        tasks_capacity: 4,
        ring_entries: 8,
        read_levels: &[Level { size: 8, count: 4 }],
        write_levels: &[Level { size: 8, count: 4 }, Level { size: 64, count: 2 }],
    };
    block_on(
        rt,
        |ctx, _rt, fd| async move {
            let mut wb = write_buffer(ctx, 11).unwrap();
            assert_eq!(wb.capacity(), 64);
            wb.as_mut()[..11].copy_from_slice(b"hello world");
            wb.set_len(11);
            let n = write(ctx, fd, wb).await.unwrap();
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

    use coio::runtime::read;

    let file = tmpfile();
    let fd = file.as_raw_fd();

    let rt = RuntimeParams {
        tasks_capacity: 4,
        ring_entries: 8,
        read_levels: &[Level { size: 16, count: 4 }],
        write_levels: &[Level { size: 16, count: 4 }],
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
fn block_on_write_buffer_oversized_errors() {
    use std::os::fd::AsRawFd;

    use coio::runtime::write_buffer;

    let file = tmpfile();
    let fd = file.as_raw_fd();

    let rt = RuntimeParams {
        tasks_capacity: 4,
        ring_entries: 8,
        read_levels: &[Level { size: 16, count: 4 }],
        write_levels: &[Level { size: 16, count: 4 }],
    };
    block_on(
        rt,
        |ctx, _rt, _fd| async move {
            let err = write_buffer(ctx, 17).err().unwrap();
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

    use coio::runtime::read;

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
