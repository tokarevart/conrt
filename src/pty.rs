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

/// Relay data between the PTY master and the real terminal.
///
/// Reads from `master` → writes to stdout.
/// Reads from stdin → writes to `master`.
/// Returns when the PTY slave is closed (child exited) or stdin reaches EOF.
pub fn relay_pty(master: RawFd) -> io::Result<()> {
    let mut buf = [0u8; 4096];

    let mut poll_fds = [
        libc::pollfd {
            fd: libc::STDIN_FILENO,
            events: libc::POLLIN,
            revents: 0,
        },
        libc::pollfd {
            fd: master,
            events: libc::POLLIN,
            revents: 0,
        },
    ];

    loop {
        let ret = unsafe { libc::poll(poll_fds.as_mut_ptr(), 2, -1) };
        if ret < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(err);
        }

        if poll_fds[0].revents & (libc::POLLHUP | libc::POLLERR) != 0 {
            return Ok(());
        }
        if poll_fds[0].revents & libc::POLLIN != 0 {
            let n = sys::read(libc::STDIN_FILENO, &mut buf)?;
            if n == 0 {
                return Ok(());
            }
            if let Err(e) = sys::write(master, &buf[..n as usize]) {
                if e.raw_os_error() == Some(libc::EIO) {
                    return Ok(());
                }
                return Err(e);
            }
        }

        if poll_fds[1].revents & (libc::POLLHUP | libc::POLLERR) != 0 {
            return Ok(());
        }
        if poll_fds[1].revents & libc::POLLIN != 0 {
            match sys::read(master, &mut buf) {
                Ok(0) => return Ok(()),
                Ok(n) => {
                    sys::write(libc::STDOUT_FILENO, &buf[..n as usize])?;
                }
                Err(e) if e.raw_os_error() == Some(libc::EIO) => return Ok(()),
                Err(e) => return Err(e),
            }
        }
    }
}
