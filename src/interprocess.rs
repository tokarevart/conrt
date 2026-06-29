use std::io;

use crate::sys;

#[repr(C)]
pub struct OneshotSignal {
    rfd: i32,
    wfd: i32,
}

impl OneshotSignal {
    pub fn new() -> io::Result<Self> {
        let mut s = Self { rfd: -1, wfd: -1 };
        unsafe { sys::pipe2(&mut s as *mut Self as *mut (), libc::O_CLOEXEC) }?;
        Ok(s)
    }

    pub fn signal(self) {
        unsafe {
            sys::close(self.rfd);
            let val = 1u8;
            sys::write(self.wfd, &val as *const u8 as *const (), 1).ok();
            sys::close(self.wfd);
        }
        std::mem::forget(self);
    }

    pub fn wait(self) -> io::Result<()> {
        unsafe { sys::close(self.wfd) };
        let mut val = 0u8;
        let ret = unsafe { sys::read(self.rfd, &mut val as *mut u8 as *mut (), 1) };
        unsafe { sys::close(self.rfd) };
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
        unsafe {
            sys::close(self.rfd);
            sys::close(self.wfd);
        }
    }
}
