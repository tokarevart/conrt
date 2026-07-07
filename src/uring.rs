use std::io;
use std::os::fd::RawFd;

use io_uring::IoUring;
use io_uring::cqueue;
use io_uring::opcode;
use io_uring::types;

pub struct Ring {
    ring: IoUring,
}

impl Ring {
    pub fn new(entries: u32) -> io::Result<Self> {
        let ring = IoUring::new(entries)?;
        Ok(Self { ring })
    }

    /// Submit a multi-shot poll for the given fd. Multi-shot means the poll
    /// stays armed after firing — no re-arm needed.
    pub fn poll_add(&mut self, fd: RawFd, events: u32, user_data: u64) {
        let entry = opcode::PollAdd::new(types::Fd(fd), events)
            .multi(true)
            .build()
            .user_data(user_data);

        let mut sq = self.ring.submission();
        unsafe {
            sq.push(&entry).expect("submission queue full");
        }
    }

    /// Submit all queued SQEs and wait for at least `want` completions.
    pub fn submit_and_wait(&self, want: usize) -> io::Result<usize> {
        self.ring.submit_and_wait(want)
    }

    /// Get the completion queue to drain ready events.
    pub fn completion(&mut self) -> cqueue::CompletionQueue<'_, io_uring::cqueue::Entry> {
        self.ring.completion()
    }
}
