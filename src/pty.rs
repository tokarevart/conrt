use std::io;
use std::os::fd::RawFd;

use crate::sys;

/// A PTY master fd. The fd is closed on drop.
pub struct PtyMaster(RawFd);

/// A PTY slave fd. The fd is closed on drop.
pub struct PtySlave(RawFd);

impl PtyMaster {
    pub fn raw_fd(&self) -> RawFd {
        self.0
    }
}

impl Drop for PtyMaster {
    fn drop(&mut self) {
        sys::close(self.0);
    }
}

impl PtySlave {
    /// Make this PTY slave the controlling terminal of the current process.
    ///
    /// Consumes `self`:
    /// 1. `setsid()` — create a new session.
    /// 2. `ioctl(TIOCSCTTY)` — attach the slave as the controlling terminal.
    /// 3. `dup2(slave, 0)`, `dup2(slave, 1)`, `dup2(slave, 2)` — redirect
    ///    stdin, stdout, and stderr to the slave.
    ///
    /// The original slave fd is closed on drop.  dup2 guarantees the
    /// duplicated 0/1/2 refer to the same open file description, so closing
    /// the original fd does not affect them.
    pub fn make_controlling(self) -> io::Result<()> {
        sys::setsid()?;

        let ret = unsafe { libc::ioctl(self.0, libc::TIOCSCTTY, 0) };
        if ret != 0 {
            return Err(io::Error::last_os_error());
        }

        for fd in [0, 1, 2] {
            sys::dup2(self.0, fd)?;
        }

        Ok(())
    }
}

impl Drop for PtySlave {
    fn drop(&mut self) {
        sys::close(self.0);
    }
}

/// Allocate a PTY pair. Returns `(master, slave)`.
pub fn open_pty() -> io::Result<(PtyMaster, PtySlave)> {
    let mut master: RawFd = -1;
    let mut slave: RawFd = -1;
    let ret = unsafe {
        libc::openpty(
            &mut master,
            &mut slave,
            std::ptr::null_mut(),
            std::ptr::null(),
            std::ptr::null(),
        )
    };
    if ret != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok((PtyMaster(master), PtySlave(slave)))
}

/// Disable echo and output processing on the terminal, but keep signal keys
/// (Ctrl+C etc.) working. Used for PTY output relay where we don't read stdin.
/// Returns the original `termios` so it can be restored later.
pub fn disable_echo_output() -> io::Result<libc::termios> {
    let mut termios: libc::termios = unsafe { std::mem::zeroed() };
    let ret = unsafe { libc::tcgetattr(libc::STDIN_FILENO, &mut termios) };
    if ret != 0 {
        return Err(io::Error::last_os_error());
    }
    let original = termios;
    termios.c_lflag &= !(libc::ECHO | libc::ECHONL);
    termios.c_oflag &= !libc::OPOST;
    let ret = unsafe { libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, &termios) };
    if ret != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(original)
}

/// Put the terminal attached to stdin into raw mode.
/// Returns the original `termios` so it can be restored later.
pub fn set_raw_terminal() -> io::Result<libc::termios> {
    let mut termios: libc::termios = unsafe { std::mem::zeroed() };
    let ret = unsafe { libc::tcgetattr(libc::STDIN_FILENO, &mut termios) };
    if ret != 0 {
        return Err(io::Error::last_os_error());
    }
    let original = termios;
    unsafe { libc::cfmakeraw(&mut termios) };
    let ret = unsafe { libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, &termios) };
    if ret != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(original)
}

/// Restore terminal to a previous state.
pub fn restore_terminal(termios: &libc::termios) -> io::Result<()> {
    let ret = unsafe { libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, termios) };
    if ret != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}
