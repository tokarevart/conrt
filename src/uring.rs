use std::os::fd::RawFd;
use std::ptr;

use io_uring::opcode;
use io_uring::types;

pub fn accept(sq: &mut io_uring::squeue::SubmissionQueue, fd: RawFd, user_data: u64) {
    let entry = opcode::Accept::new(types::Fd(fd), ptr::null_mut(), ptr::null_mut())
        .build()
        .user_data(user_data);
    unsafe {
        sq.push(&entry).expect("submission queue full");
    }
}

pub fn read(sq: &mut io_uring::squeue::SubmissionQueue, fd: RawFd, buf: &mut [u8], user_data: u64) {
    let entry = opcode::Read::new(types::Fd(fd), buf.as_mut_ptr(), buf.len() as u32)
        .build()
        .user_data(user_data);
    unsafe {
        sq.push(&entry).expect("submission queue full");
    }
}

pub fn write(sq: &mut io_uring::squeue::SubmissionQueue, fd: RawFd, buf: &[u8], user_data: u64) {
    let entry = opcode::Write::new(types::Fd(fd), buf.as_ptr(), buf.len() as u32)
        .build()
        .user_data(user_data);
    unsafe {
        sq.push(&entry).expect("submission queue full");
    }
}
