#![allow(dead_code)]

use std::os::fd::RawFd;
// use std::pin::Pin;
use std::ptr;

// use std::task::Poll;
use io_uring::opcode;
use io_uring::types;

// #[derive(Hash, PartialOrd, Ord, PartialEq, Eq, Default, Debug)]
// pub struct PendOnce {
//     pended: bool,
// }

// impl PendOnce {
//     pub fn new() -> Self {
//         Self::default()
//     }

//     pub fn pended(&self) -> bool {
//         self.pended
//     }
// }

// impl Future for PendOnce {
//     type Output = ();

//     fn poll(self: Pin<&mut Self>, _: &mut core::task::Context<'_>) ->
// Poll<Self::Output> {         if self.pended {
//             Poll::Ready(())
//         } else {
//             Poll::Pending
//         }
//     }
// }

// pub fn pend_once() -> PendOnce {
//     PendOnce::new()
// }

pub fn push_accept(sq: &mut io_uring::squeue::SubmissionQueue, fd: RawFd, user_data: u64) {
    let entry = opcode::Accept::new(types::Fd(fd), ptr::null_mut(), ptr::null_mut())
        .build()
        .user_data(user_data);
    unsafe {
        sq.push(&entry).expect("submission queue full");
    }
}

// pub async fn accept(ring: &mut io_uring::IoUring, fd: RawFd, user_data: u64)
// {     let mut sq = ring.submission();
//     push_accept(&mut sq, fd, user_data);
//     pend_once().await
// }

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

// pub async fn read(ring: &mut io_uring::IoUring, fd: RawFd, buf: &mut [u8],
// user_data: u64) {     let mut sq = ring.submission();
//     push_read(&mut sq, fd, buf, user_data);
//     pend_once().await
// }

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

// pub async fn write(ring: &mut io_uring::IoUring, fd: RawFd, buf: &[u8],
// user_data: u64) {     let mut sq = ring.submission();
//     push_write(&mut sq, fd, buf, user_data);
//     pend_once().await
// }
