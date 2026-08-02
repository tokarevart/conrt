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
        buf_count: 4,
        buf_size: 16,
    };
    block_on(rt, |_, _, _data: ()| async move {}, ());
}

#[test]
fn block_on_with_spawn() {
    let rt = RuntimeParams {
        tasks_capacity: 4,
        ring_entries: 8,
        buf_count: 4,
        buf_size: 16,
    };
    block_on(
        rt,
        |_, rt: RuntimeContext<u32>, data: u32| async move {
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
        buf_count: 4,
        buf_size: 16,
    };
    block_on(
        rt,
        |_, rt: RuntimeContext<u32>, data: u32| async move {
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
        static STASH: RefCell<Option<RuntimeContext<u32>>> = const { RefCell::new(None) };
    }

    let rt = RuntimeParams {
        tasks_capacity: 4,
        ring_entries: 8,
        buf_count: 4,
        buf_size: 16,
    };
    block_on(
        rt,
        |_, rt: RuntimeContext<u32>, _data: u32| async move {
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
        buf_count: 4,
        buf_size: 16,
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
        static STASH: RefCell<Option<RuntimeContext<u32>>> = const { RefCell::new(None) };
    }

    let rt = RuntimeParams {
        tasks_capacity: 4,
        ring_entries: 8,
        buf_count: 4,
        buf_size: 16,
    };
    block_on(
        rt,
        |_, rt: RuntimeContext<u32>, _data: u32| async move {
            STASH.with(|s| *s.borrow_mut() = Some(rt));
        },
        0,
    );

    let rt2 = RuntimeParams {
        tasks_capacity: 4,
        ring_entries: 8,
        buf_count: 4,
        buf_size: 16,
    };
    block_on(
        rt2,
        |_, _rt: RuntimeContext<u32>, _data: u32| async move {
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
        buf_count: 4,
        buf_size: 16,
    };
    block_on(
        rt,
        |ctx, _rt, fd| async move {
            let data = read(ctx, fd, 5).await.unwrap();
            assert_eq!(data, b"hello");
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
        buf_count: 4,
        buf_size: 16,
    };
    block_on(
        rt,
        |ctx, _rt, fd| async move {
            let data = read(ctx, fd, 16).await.unwrap();
            assert_eq!(data, b"hello world");
        },
        fd,
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
        buf_count: 4,
        buf_size: 16,
    };
    block_on(
        rt,
        |ctx, _rt, fd| async move {
            for _ in 0..8 {
                let data = read(ctx, fd, 16).await.unwrap();
                assert_eq!(data, b"hello world");
            }
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

    let mut file = tmpfile();
    let fd = file.as_raw_fd();

    let rt = RuntimeParams {
        tasks_capacity: 4,
        ring_entries: 8,
        buf_count: 4,
        buf_size: 16,
    };
    block_on(
        rt,
        |ctx, _rt, fd| async move {
            let n = write(ctx, fd, b"hello".to_vec()).await.unwrap();
            assert_eq!(n, 5);
        },
        fd,
    );

    file.seek(SeekFrom::Start(0)).unwrap();
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).unwrap();
    assert_eq!(buf, b"hello");
}
