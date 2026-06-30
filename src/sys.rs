//! Raw Linux syscall wrappers.
//!
//! Every function mirrors the kernel interface directly — no hidden retries,
//! no buffer reinterpretation, no null-termination guarantees. Caller
//! manages all layout and owns all buffers.

#![allow(dead_code)]

use core::ffi::c_char;
use core::ffi::c_int;
use core::ptr::NonNull;
use std::io;

use libc::pid_t;

use crate::cstring::CStr;
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
pub fn sethostname(name: &str) -> io::Result<()> {
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

/// Owned argv array for execvp. Guarantees the last element is `None`.
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct Argv {
    args: Vec<Option<CString>>,
}

impl Argv {
    pub fn new(command: Vec<CString>) -> Self {
        let mut args: Vec<Option<CString>> = unsafe { core::mem::transmute::<_, _>(command) };
        args.push(None);
        Self { args }
    }

    pub fn as_slice(&self) -> &ArgvSlice {
        let slice = self.args.as_slice();
        unsafe { core::mem::transmute::<_, _>(slice) }
    }

    pub fn as_raw(&self) -> *const *const c_char {
        self.args.as_ptr().cast()
    }

    pub fn into_inner(self) -> Vec<Option<CString>> {
        self.args
    }

    pub fn into_command(mut self) -> Vec<CString> {
        assert!(self.args.pop().is_none());
        unsafe { core::mem::transmute::<_, _>(self.args) }
    }
}

/// Borrowed argv slice. Invariant: the last element is `None`.
#[repr(transparent)]
#[derive(PartialEq, Eq, Debug)]
pub struct ArgvSlice {
    args: [Option<CString>],
}

impl ArgvSlice {
    pub fn as_raw(&self) -> *const *const c_char {
        self.args.as_ptr().cast()
    }
}

/// `execvp(argv)` — thin wrapper over `libc::execvp` for PATH search.
#[inline]
pub fn execvp(argv: &ArgvSlice) -> io::Error {
    unsafe { libc::execvp(*argv.as_raw(), argv.as_raw()) };
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

/// `mount(source, target, fstype, flags, data)` — raw syscall.
/// All string pointers must be valid null-terminated C strings or null.
#[inline]
pub fn mount(
    source: Option<CStr>,
    target: CStr,
    fstype: Option<CStr>,
    flags: u64,
    data: Option<NonNull<()>>,
) -> io::Result<()> {
    let data = data.map(|d| d.as_ptr()).unwrap_or(std::ptr::null_mut());

    syscall!(
        libc::SYS_mount,
        CStr::as_raw_option(source),
        target.as_raw(),
        CStr::as_raw_option(fstype),
        flags,
        data
    )
    .map(|_| ())
}

/// `pivot_root(new_root, put_old)` — raw syscall.
#[inline]
pub fn pivot_root(new_root: *const libc::c_char, put_old: *const libc::c_char) -> io::Result<()> {
    syscall!(libc::SYS_pivot_root, new_root, put_old).map(|_| ())
}

/// `umount2(target, flags)` — raw syscall.
#[inline]
pub fn umount2(target: *const libc::c_char, flags: c_int) -> io::Result<()> {
    syscall!(libc::SYS_umount2, target, flags).map(|_| ())
}

/// `chdir(path)` — raw syscall. `path` must be a valid null-terminated C
/// string.
#[inline]
pub fn chdir(path: CStr) -> io::Result<()> {
    syscall!(libc::SYS_chdir, path.as_raw()).map(|_| ())
}

/// `mkdir(path, mode)` — raw syscall.
#[inline]
pub fn mkdir(path: *const libc::c_char, mode: libc::mode_t) -> io::Result<()> {
    syscall!(libc::SYS_mkdir, path, mode).map(|_| ())
}

/// `rmdir(path)` — raw syscall.
#[inline]
pub fn rmdir(path: *const libc::c_char) -> io::Result<()> {
    syscall!(libc::SYS_rmdir, path).map(|_| ())
}

/// `chroot(path)` — raw syscall.
#[inline]
pub fn chroot(path: CStr) -> io::Result<()> {
    syscall!(libc::SYS_chroot, path.as_raw()).map(|_| ())
}
