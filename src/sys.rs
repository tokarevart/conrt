//! Raw Linux syscall wrappers.
//!
//! Every function mirrors the kernel interface directly — no hidden retries,
//! no buffer reinterpretation, no null-termination guarantees. Caller
//! manages all layout and owns all buffers.

use std::io;

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
pub unsafe fn read<T>(fd: i32, buf: *mut T, count: usize) -> io::Result<isize> {
    syscall!(libc::SYS_read, fd, buf, count)
}

/// `write(fd, buf, count)` — raw syscall, does NOT retry on `EINTR`.
#[inline]
pub unsafe fn write<T>(fd: i32, buf: *const T, count: usize) -> io::Result<isize> {
    syscall!(libc::SYS_write, fd, buf, count)
}

/// `close(fd)` — raw syscall. Kernel guarantees the fd is released even on
/// `EINTR`. Return value discarded intentionally.
#[inline]
pub unsafe fn close(fd: i32) {
    syscall_unchecked!(libc::SYS_close, fd);
}

/// `pipe2(pipefd, flags)` — caller owns the buffer; any `repr(C)` pair of
/// `i32` works.
#[inline]
pub unsafe fn pipe2<I32Pair>(pipefd: *mut I32Pair, flags: i32) -> io::Result<()> {
    syscall!(libc::SYS_pipe2, pipefd, flags).map(|_| ())
}

/// `sethostname(name, len)` — caller provides pointer + length. No null
/// terminator needed.
#[inline]
pub unsafe fn sethostname(name: *const u8, len: usize) -> io::Result<()> {
    syscall!(libc::SYS_sethostname, name, len).map(|_| ())
}

/// `wait4(pid, status, options)` — raw syscall. Returns child pid on
/// success. Pass `0` for rusage (NULL).
#[inline]
pub unsafe fn wait4(pid: i32, status: *mut i32, options: i32) -> io::Result<i32> {
    syscall!(libc::SYS_wait4, pid, status, options, 0usize).map(|r| r as i32)
}

/// `execvp(argv)` — thin wrapper over `libc::execvp` for PATH search.
/// Always returns `io::Error`.
#[inline]
pub fn execvp(argv: *const *const libc::c_char) -> io::Error {
    unsafe { libc::execvp(*argv, argv) };
    io::Error::last_os_error()
}

/// `clone3(args, size)` — raw syscall. Returns 0 in the child, child pid
/// in the parent. Caller interprets the return value.
#[inline]
pub unsafe fn clone3(args: *const libc::clone_args, size: usize) -> io::Result<isize> {
    syscall!(libc::SYS_clone3, args, size)
}
