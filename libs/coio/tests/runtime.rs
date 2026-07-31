use coio::runtime::Runtime;
use coio::runtime::RuntimeContext;

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
