use std::io;

use crate::sys;
use crate::sys::FdPair;

pub struct OneshotSignal {
    fds: FdPair,
}

impl OneshotSignal {
    pub fn new() -> io::Result<Self> {
        let mut fds = FdPair {
            read: -1,
            write: -1,
        };
        sys::pipe2(&mut fds, libc::O_CLOEXEC)?;
        Ok(Self { fds })
    }

    pub fn signal(self) {
        sys::close(self.fds.read);
        sys::write(self.fds.write, &[1]).ok();
        sys::close(self.fds.write);
        std::mem::forget(self);
    }

    pub fn wait(self) -> io::Result<()> {
        sys::close(self.fds.write);
        let ret = sys::read(self.fds.read, &mut [0u8]);
        sys::close(self.fds.read);
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
        sys::close(self.fds.read);
        sys::close(self.fds.write);
    }
}
