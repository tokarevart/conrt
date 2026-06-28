//! Demonstrate the difference between glibc's clone wrapper (requires a
//! pre-allocated stack) and clone3 (can share the parent's stack like fork).
//!
//! Usage:
//!
//!     cargo run --example stack_demo -- glibc   # libc::clone with 1 MB stack
//!     cargo run --example stack_demo -- clone3  # clone3 with stack=0
//!     cargo run --example stack_demo -- null    # libc::clone with NULL stack
//! (proves EINVAL)

use std::ffi::CString;
use std::mem;
use std::process::ExitCode;

fn main() -> ExitCode {
    let method = match std::env::args().nth(1) {
        Some(m) => m,
        None => {
            eprintln!("Usage: stack_demo <glibc|clone3|null>");
            return ExitCode::FAILURE;
        }
    };

    match method.as_str() {
        "glibc" => demo_glibc_clone(),
        "clone3" => demo_clone3(),
        "null" => demo_null_stack(),
        _ => {
            eprintln!("Usage: stack_demo <glibc|clone3|null>");
            ExitCode::FAILURE
        }
    }
}

/// glibc's clone wrapper with a proper 1 MB stack.
fn demo_glibc_clone() -> ExitCode {
    let mut stack = vec![0u8; 1024 * 1024];
    let stack_top = {
        let ptr = stack.as_mut_ptr() as usize + stack.len();
        (ptr & !15) as *mut libc::c_void
    };

    let c_cmd = CString::new("/bin/true").unwrap();

    extern "C" fn child(arg: *mut libc::c_void) -> libc::c_int {
        let cmd = unsafe { &*(arg as *const CString) };
        let argv = [cmd.as_ptr(), std::ptr::null()];
        unsafe {
            libc::execvp(cmd.as_ptr(), argv.as_ptr());
        }
        1
    }

    let ret = unsafe {
        libc::clone(
            child as extern "C" fn(*mut libc::c_void) -> libc::c_int,
            stack_top,
            libc::SIGCHLD,
            &c_cmd as *const CString as *mut libc::c_void,
        )
    };

    if ret < 0 {
        eprintln!("glibc clone failed: {}", std::io::Error::last_os_error());
        return ExitCode::FAILURE;
    }

    let pid = nix::unistd::Pid::from_raw(ret);
    eprintln!("glibc clone: child PID = {}", pid);
    eprintln!("  -> 1 MB stack allocated via vec![0u8; 1 << 20] (see strace for mmap)");

    wait_and_exit(pid)
}

/// clone3 with stack=0, stack_size=0 (parent's COW stack, like fork).
fn demo_clone3() -> ExitCode {
    let args = libc::clone_args {
        flags: 0,
        pidfd: 0,
        child_tid: 0,
        parent_tid: 0,
        exit_signal: libc::SIGCHLD as u64,
        stack: 0,
        stack_size: 0,
        tls: 0,
        set_tid: 0,
        set_tid_size: 0,
        cgroup: 0,
    };

    let ret = unsafe {
        libc::syscall(
            libc::SYS_clone3,
            &args as *const libc::clone_args as i64,
            mem::size_of::<libc::clone_args>() as i64,
        )
    };

    if ret < 0 {
        eprintln!("clone3 failed: {}", std::io::Error::last_os_error());
        return ExitCode::FAILURE;
    }

    if ret == 0 {
        // Child — exec /bin/true
        let cmd = CString::new("/bin/true").unwrap();
        let argv = [cmd.as_ptr(), std::ptr::null()];
        unsafe {
            libc::execvp(cmd.as_ptr(), argv.as_ptr());
        }
        std::process::exit(1);
    }

    let pid = nix::unistd::Pid::from_raw(ret as i32);
    eprintln!("clone3: child PID = {}", pid);
    eprintln!("  -> No stack allocation (stack=0, kernel uses COW)");

    wait_and_exit(pid)
}

/// glibc's clone with NULL stack — proves the wrapper rejects it.
fn demo_null_stack() -> ExitCode {
    extern "C" fn child(_arg: *mut libc::c_void) -> libc::c_int {
        0
    }

    let ret = unsafe {
        libc::clone(
            child as extern "C" fn(*mut libc::c_void) -> libc::c_int,
            std::ptr::null_mut(), // NULL stack!
            libc::SIGCHLD,
            std::ptr::null_mut(),
        )
    };

    if ret < 0 {
        eprintln!(
            "glibc clone with NULL stack -> {}",
            std::io::Error::last_os_error()
        );
        ExitCode::FAILURE
    } else {
        eprintln!("UNEXPECTED: clone with NULL stack returned PID {}", ret);
        ExitCode::SUCCESS
    }
}

fn wait_and_exit(pid: nix::unistd::Pid) -> ExitCode {
    match nix::sys::wait::waitpid(pid, None) {
        Ok(nix::sys::wait::WaitStatus::Exited(_, code)) => ExitCode::from(code as u8),
        Ok(nix::sys::wait::WaitStatus::Signaled(_, sig, _)) => {
            ExitCode::from(128 + sig as i32 as u8)
        }
        Ok(_) => ExitCode::FAILURE,
        Err(e) => {
            eprintln!("waitpid failed: {}", e);
            ExitCode::FAILURE
        }
    }
}
