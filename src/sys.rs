//! Raw Linux syscall wrappers.
//!
//! Every function mirrors the kernel interface directly — no hidden retries,
//! no buffer reinterpretation, no null-termination guarantees. Caller
//! manages all layout and owns all buffers.

#![allow(dead_code)]

use std::ffi::c_int;
use std::io;

use libc::pid_t;

use crate::cstring::CString;

macro_rules! syscall_unchecked {
    ($nr:expr $(, $a:expr)*) => {{
        unsafe { libc::syscall($nr as i64, $($a as i64),*) as isize }
    }};
}

macro_rules! syscall {
    ($nr:expr $(, $a:expr)*) => {{
        let ret = syscall_unchecked!($nr $(, $a)*);
        if ret < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(ret)
        }
    }};
}

/// `read(fd, buf, count)` — raw syscall, does NOT retry on `EINTR`.
#[inline]
pub unsafe fn read_value<T>(fd: c_int, buf: &mut T) -> io::Result<isize> {
    syscall!(libc::SYS_read, fd, buf as *mut _, size_of::<T>())
}

/// `read(fd, buf, count)` — raw syscall, does NOT retry on `EINTR`.
#[inline]
pub fn read(fd: c_int, buf: &mut [u8]) -> io::Result<isize> {
    syscall!(libc::SYS_read, fd, buf.as_mut_ptr(), buf.len())
}

/// `write(fd, buf, count)` — raw syscall, does NOT retry on `EINTR`.
#[inline]
pub fn write_value<T>(fd: c_int, buf: &T) -> io::Result<isize> {
    syscall!(libc::SYS_write, fd, buf as *const _, size_of::<T>())
}

/// `write(fd, buf, count)` — raw syscall, does NOT retry on `EINTR`.
#[inline]
pub fn write(fd: c_int, buf: &[u8]) -> io::Result<isize> {
    syscall!(libc::SYS_write, fd, buf.as_ptr(), buf.len())
}

/// `close(fd)` — raw syscall. Kernel guarantees the fd is released even on
/// `EINTR`. Return value discarded intentionally.
#[inline]
pub fn close(fd: c_int) {
    syscall_unchecked!(libc::SYS_close, fd);
}

/// `pipe2(pipefd, flags)` — caller owns the buffer; any `repr(C)` pair of
/// `i32` works.
#[inline]
pub fn pipe2(pipefd: &mut [c_int; 2], flags: c_int) -> io::Result<()> {
    syscall!(libc::SYS_pipe2, pipefd as *mut _, flags).map(|_| ())
}

/// `sethostname(name, len)` — caller provides pointer + length. No null
/// terminator needed.
#[inline]
pub fn sethostname(name: &[u8]) -> io::Result<()> {
    let (ptr, len) = (name.as_ptr(), name.len());
    syscall!(libc::SYS_sethostname, ptr, len).map(|_| ())
}

/// `wait4(pid, status, options, rusage)` — raw syscall. Returns child pid
/// on success. Pass `None` for rusage to ignore resource stats.
#[inline]
pub fn wait4(
    pid: pid_t,
    status: &mut c_int,
    options: c_int,
    rusage: Option<&mut libc::rusage>,
) -> io::Result<pid_t> {
    let rusage = rusage.map_or(std::ptr::null_mut(), |r| r as *mut _);
    syscall!(libc::SYS_wait4, pid, status as *mut _, options, rusage).map(|r| r as _)
}

/// `execvp(argv)` — thin wrapper over `libc::execvp` for PATH search.
#[inline]
pub fn execvp(argv: &[Option<CString>]) -> io::Error {
    assert!(
        argv.last().expect("argv must not be empty").is_none(),
        "argv must be null-terminated"
    );

    let argv = argv.as_ptr().cast();
    unsafe { libc::execvp(*argv, argv) };
    io::Error::last_os_error()
}

/// `clone3(args, size)` — raw syscall. Returns 0 in the child, child pid
/// in the parent. Caller interprets the return value.
#[inline]
pub unsafe fn clone3(args: &libc::clone_args) -> io::Result<isize> {
    syscall!(
        libc::SYS_clone3,
        args as *const _,
        size_of::<libc::clone_args>()
    )
}
