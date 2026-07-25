#![allow(dead_code)]

use std::os::fd::RawFd;

use io_uring::opcode;
use io_uring::types;

pub fn push_accept(sq: &mut io_uring::squeue::SubmissionQueue, fd: RawFd, user_data: u64) {
    let entry = opcode::Accept::new(types::Fd(fd), std::ptr::null_mut(), std::ptr::null_mut())
        .build()
        .user_data(user_data);
    unsafe {
        sq.push(&entry).expect("submission queue full");
    }
}

pub fn push_read(
    sq: &mut io_uring::squeue::SubmissionQueue,
    fd: RawFd,
    buf: &mut [u8],
    user_data: u64,
) {
    let entry = opcode::Read::new(types::Fd(fd), buf.as_mut_ptr(), buf.len() as u32)
        .build()
        .user_data(user_data);
    unsafe {
        sq.push(&entry).expect("submission queue full");
    }
}

pub fn push_write(
    sq: &mut io_uring::squeue::SubmissionQueue,
    fd: RawFd,
    buf: &[u8],
    user_data: u64,
) {
    let entry = opcode::Write::new(types::Fd(fd), buf.as_ptr(), buf.len() as u32)
        .build()
        .user_data(user_data);
    unsafe {
        sq.push(&entry).expect("submission queue full");
    }
}

pub fn push_recvmsg(
    sq: &mut io_uring::squeue::SubmissionQueue,
    fd: RawFd,
    msghdr: *mut libc::msghdr,
    flags: u32,
    user_data: u64,
) {
    let entry = opcode::RecvMsg::new(types::Fd(fd), msghdr)
        .flags(flags)
        .build()
        .user_data(user_data);
    unsafe {
        sq.push(&entry).expect("submission queue full");
    }
}

pub fn push_sendmsg(
    sq: &mut io_uring::squeue::SubmissionQueue,
    fd: RawFd,
    msghdr: *const libc::msghdr,
    user_data: u64,
) {
    let entry = opcode::SendMsg::new(types::Fd(fd), msghdr)
        .build()
        .user_data(user_data);
    unsafe {
        sq.push(&entry).expect("submission queue full");
    }
}
