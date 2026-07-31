use coio::runtime::Runtime;
use coio::runtime::RuntimeContext;

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
fn runtime_new_success() {
    let rt = Runtime::new(4, 8);
    assert!(rt.is_ok());
}

#[test]
fn runtime_new_zero_entries_fails() {
    let rt = Runtime::new(4, 0);
    assert!(rt.is_err());
}

#[test]
fn block_on_trivial() {
    let rt = Runtime::new(4, 8).unwrap();
    rt.block_on(|_, _, _data: ()| async move {}, ());
}

#[test]
fn block_on_with_spawn() {
    let rt = Runtime::new(4, 8).unwrap();
    rt.block_on(
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
    let rt = Runtime::new(16, 8).unwrap();
    rt.block_on(
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

    let rt = Runtime::new(4, 8).unwrap();
    rt.block_on(
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

    let rt = Runtime::new(4, 8).unwrap();
    rt.block_on(
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

    let rt = Runtime::new(4, 8).unwrap();
    rt.block_on(
        |_, rt: RuntimeContext<u32>, _data: u32| async move {
            STASH.with(|s| *s.borrow_mut() = Some(rt));
        },
        0,
    );

    let rt2 = Runtime::new(4, 8).unwrap();
    rt2.block_on(
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

    let rt = Runtime::new(4, 8).unwrap();
    rt.block_on(
        |ctx, _rt, fd| async move {
            let data = read(ctx, fd, vec![0u8; 5]).await.unwrap();
            assert_eq!(data, b"hello");
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

    let rt = Runtime::new(4, 8).unwrap();
    rt.block_on(
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
