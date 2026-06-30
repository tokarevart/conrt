use std::ffi::c_int;
use std::io;

use crate::sys;

pub struct OneshotSignal {
    rfd: c_int,
    wfd: c_int,
}

impl OneshotSignal {
    pub fn new() -> io::Result<Self> {
        let mut fds = [-1, -1];
        sys::pipe2(&mut fds, libc::O_CLOEXEC)?;
        Ok(Self {
            rfd: fds[0],
            wfd: fds[1],
        })
    }

    pub fn signal(self) {
        sys::close(self.rfd);
        sys::write(self.wfd, &[1]).ok();
        sys::close(self.wfd);
        std::mem::forget(self);
    }

    pub fn wait(self) -> io::Result<()> {
        sys::close(self.wfd);
        let ret = sys::read(self.rfd, &mut [0u8]);
        sys::close(self.rfd);
        std::mem::forget(self);
        match ret {
            Ok(1) => Ok(()),
            Ok(_) => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "signal pipe closed",
            )),
            Err(e) => Err(e),
        }
    }
}

impl Drop for OneshotSignal {
    fn drop(&mut self) {
        sys::close(self.rfd);
        sys::close(self.wfd);
    }
}
