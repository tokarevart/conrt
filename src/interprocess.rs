use std::io;

#[repr(C)]
pub struct OneshotSignal {
    rfd: i32,
    wfd: i32,
}

impl OneshotSignal {
    pub fn new() -> io::Result<Self> {
        let mut s = Self { rfd: -1, wfd: -1 };
        let ret = unsafe { libc::pipe2(&mut s as *mut Self as *mut i32, libc::O_CLOEXEC) };
        if ret < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(s)
    }

    pub fn signal(self) {
        unsafe {
            libc::close(self.rfd);
            let val = 1u8;
            libc::write(self.wfd, &val as *const u8 as *const libc::c_void, 1);
            libc::close(self.wfd);
            std::mem::forget(self);
        }
    }

    pub fn wait(self) -> io::Result<()> {
        let ret = unsafe {
            libc::close(self.wfd);
            let mut val = 0u8;
            let ret = libc::read(self.rfd, &mut val as *mut u8 as *mut libc::c_void, 1);
            libc::close(self.rfd);
            ret
        };
        std::mem::forget(self);
        match ret {
            1 => Ok(()),
            0 => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "signal pipe closed",
            )),
            _ => Err(io::Error::last_os_error()),
        }
    }
}

impl Drop for OneshotSignal {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.rfd);
            libc::close(self.wfd);
        }
    }
}
