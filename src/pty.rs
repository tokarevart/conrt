use std::io;
use std::os::fd::RawFd;

use crate::sys;

/// A PTY pair (master + slave).
pub struct Pty {
    pub master: RawFd,
    pub slave: RawFd,
}

impl Pty {
    pub fn open() -> io::Result<Self> {
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
        Ok(Self { master, slave })
    }
}

impl Drop for Pty {
    fn drop(&mut self) {
        if self.master >= 0 {
            unsafe { libc::close(self.master) };
        }
        if self.slave >= 0 {
            unsafe { libc::close(self.slave) };
        }
    }
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

        if poll_fds[0].revents & libc::POLLIN != 0 {
            let n = sys::read(libc::STDIN_FILENO, &mut buf)?;
            if n == 0 {
                break;
            }
            if let Err(e) = sys::write(master, &buf[..n as usize]) {
                if e.raw_os_error() == Some(libc::EIO) {
                    break;
                }
                return Err(e);
            }
        }

        if poll_fds[1].revents & libc::POLLIN != 0 {
            match sys::read(master, &mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    sys::write(libc::STDOUT_FILENO, &buf[..n as usize])?;
                }
                Err(e) => {
                    if e.raw_os_error() == Some(libc::EIO) {
                        break;
                    }
                    return Err(e);
                }
            }
        }

        if poll_fds[0].revents & (libc::POLLHUP | libc::POLLERR) != 0 {
            break;
        }
        if poll_fds[1].revents & (libc::POLLHUP | libc::POLLERR) != 0 {
            break;
        }
    }

    Ok(())
}
