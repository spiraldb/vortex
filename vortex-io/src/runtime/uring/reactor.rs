// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! The io_uring a worker owns: its in-flight reads and its wakeup eventfd.

use std::collections::VecDeque;
use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;
use std::os::fd::FromRawFd;
use std::os::fd::OwnedFd;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use futures::channel::oneshot;
use io_uring::IoUring;
use io_uring::opcode;
use io_uring::squeue;
use io_uring::types;
use vortex_buffer::Alignment;
use vortex_buffer::BufferMut;
use vortex_buffer::ByteBuffer;

use super::pool::BufferPool;

/// The `user_data` of the eventfd read that wakes a parked worker.
const WAKE_TOKEN: u64 = u64::MAX;

/// The largest read a single submission asks for. Longer reads continue as short reads do.
const MAX_READ_CHUNK: usize = 1 << 30;

/// The result a read delivers to the future waiting on it.
pub(super) type ReadResult = io::Result<ByteBuffer>;

/// Wakes one worker parked on its ring.
#[derive(Clone)]
pub(super) struct WakeFd(Arc<WakeInner>);

struct WakeInner {
    fd: OwnedFd,
    parked: AtomicBool,
}

impl WakeFd {
    pub(super) fn new() -> io::Result<Self> {
        // Blocking, since io_uring answers a read of an empty non-blocking eventfd with EAGAIN
        // rather than waiting for it to be signalled.
        // SAFETY: eventfd has no memory-safety preconditions; a negative return is an error.
        let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: `fd` is a freshly created descriptor nothing else owns.
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };
        Ok(Self(Arc::new(WakeInner {
            fd,
            parked: AtomicBool::new(false),
        })))
    }

    pub(super) fn set_parked(&self, parked: bool) {
        self.0.parked.store(parked, Ordering::SeqCst);
    }

    /// Wakes the worker if it is parked, returning whether it was.
    pub(super) fn unpark(&self) -> bool {
        if self.0.parked.swap(false, Ordering::SeqCst) {
            self.wake();
            true
        } else {
            false
        }
    }

    /// Signals the eventfd whether or not the worker is parked.
    pub(super) fn wake(&self) {
        let one: u64 = 1;
        // SAFETY: writes 8 bytes from a live u64 to a descriptor this value owns. The counter
        // cannot realistically overflow, and a failed write leaves nothing to recover, so the
        // result is ignored.
        unsafe {
            libc::write(
                self.0.fd.as_raw_fd(),
                (&raw const one).cast(),
                size_of::<u64>(),
            )
        };
    }
}

/// A read for a worker to perform on its ring.
pub(super) struct ReadRequest {
    pub(super) file: Arc<File>,
    pub(super) offset: u64,
    pub(super) len: usize,
    pub(super) alignment: Alignment,
    /// The destination for a read too large or too aligned for the pool, already `len` long.
    /// `None` reads into a pooled buffer of the worker that performs the read.
    pub(super) buffer: Option<BufferMut<u8>>,
    pub(super) reply: oneshot::Sender<ReadResult>,
}

/// One read the kernel may still be writing into.
struct ReadOp {
    /// Keeps the descriptor open while the kernel reads from it.
    file: Arc<File>,
    /// The destination, already at its final length. Its heap allocation does not move.
    buffer: BufferMut<u8>,
    /// The pool size class `buffer` came from, if it goes back to the worker's pool.
    pooled: Option<u32>,
    alignment: Alignment,
    offset: u64,
    filled: usize,
    reply: oneshot::Sender<ReadResult>,
}

impl ReadOp {
    fn entry(&mut self, token: u64) -> squeue::Entry {
        let remaining = (self.buffer.len() - self.filled).min(MAX_READ_CHUNK);
        // SAFETY: `filled < len`, so the pointer stays within the buffer's initialized length.
        let ptr = unsafe { self.buffer.as_mut_slice().as_mut_ptr().add(self.filled) };
        opcode::Read::new(
            types::Fd(self.file.as_raw_fd()),
            ptr,
            u32::try_from(remaining).unwrap_or(u32::MAX),
        )
        .offset(self.offset + self.filled as u64)
        .build()
        .user_data(token)
    }
}

/// A worker's ring.
pub(super) struct Reactor {
    ring: IoUring,
    /// In-flight reads, indexed by their `user_data`.
    ops: Vec<Option<ReadOp>>,
    free: Vec<usize>,
    in_flight: usize,
    /// Entries that did not fit in the submission queue.
    backlog: VecDeque<squeue::Entry>,
    /// Entries pushed to the submission queue since the last `io_uring_enter`.
    unsubmitted: usize,
    wake: WakeFd,
    pool: BufferPool,
    /// The eventfd read's destination. Boxed so the kernel's pointer stays valid.
    wake_buf: Box<u64>,
    wake_armed: bool,
}

impl Reactor {
    /// Creates a ring that only the calling thread may enter, signalled through `wake`.
    pub(super) fn new(entries: u32, wake: WakeFd) -> io::Result<Self> {
        // A ring is only ever entered by the worker that created it.
        let ring = IoUring::builder()
            .setup_single_issuer()
            .build(entries)
            .or_else(|_| IoUring::new(entries))?;
        Ok(Self {
            ring,
            ops: Vec::new(),
            free: Vec::new(),
            in_flight: 0,
            backlog: VecDeque::new(),
            unsubmitted: 0,
            wake,
            pool: BufferPool::new(),
            wake_buf: Box::new(0),
            wake_armed: false,
        })
    }

    /// Queues `request` on this ring. The submission is flushed the next time the worker polls
    /// its ring.
    pub(super) fn read(&mut self, request: ReadRequest) {
        let ReadRequest {
            file,
            offset,
            len,
            alignment,
            buffer,
            reply,
        } = request;
        if len == 0 {
            drop(reply.send(Ok(ByteBuffer::empty_aligned(alignment))));
            return;
        }
        let (buffer, pooled) = match buffer {
            Some(buffer) => (buffer, None),
            None => {
                let (class, buffer) = self.pool.take(len);
                (buffer, Some(class))
            }
        };
        let op = ReadOp {
            file,
            buffer,
            pooled,
            alignment,
            offset,
            filled: 0,
            reply,
        };
        let token = match self.free.pop() {
            Some(token) => {
                self.ops[token] = Some(op);
                token
            }
            None => {
                self.ops.push(Some(op));
                self.ops.len() - 1
            }
        };
        self.in_flight += 1;
        let entry = self.ops[token]
            .as_mut()
            .map(|op| op.entry(token as u64))
            .unwrap_or_else(|| unreachable!("the op was just inserted"));
        self.push(entry);
    }

    fn push(&mut self, entry: squeue::Entry) {
        if self.backlog.is_empty() {
            // SAFETY: every entry points into a buffer owned by `self.ops` (or `wake_buf`) that
            // stays alive and unmoved until its completion is reaped.
            if unsafe { self.ring.submission().push(&entry) }.is_ok() {
                self.unsubmitted += 1;
                return;
            }
        }
        self.backlog.push_back(entry);
    }

    /// Moves backlogged entries into the submission queue as space allows.
    fn fill_from_backlog(&mut self) {
        let mut sq = self.ring.submission();
        while let Some(entry) = self.backlog.front() {
            // SAFETY: see `push`.
            if unsafe { sq.push(entry) }.is_err() {
                break;
            }
            self.backlog.pop_front();
            self.unsubmitted += 1;
        }
    }

    /// Flushes submissions and handles every completion already posted, without blocking.
    /// Returns the number of completions handled.
    pub(super) fn poll(&mut self) -> usize {
        self.enter(0);
        self.reap()
    }

    /// Whether reads are queued that the kernel has not been told about yet.
    pub(super) fn has_unsubmitted(&self) -> bool {
        self.unsubmitted > 0 || !self.backlog.is_empty()
    }

    /// Blocks until something completes on the ring, including a wakeup from another thread.
    pub(super) fn wait(&mut self) {
        if !self.wake_armed {
            let entry = opcode::Read::new(
                types::Fd(self.wake.0.fd.as_raw_fd()),
                (&raw mut *self.wake_buf).cast(),
                u64::BITS / 8,
            )
            .build()
            .user_data(WAKE_TOKEN);
            self.push(entry);
            self.wake_armed = true;
        }
        self.enter(1);
        self.reap();
    }

    /// Enters the kernel if there is anything to submit or `want` completions to wait for.
    fn enter(&mut self, want: usize) {
        loop {
            self.fill_from_backlog();
            if self.unsubmitted == 0 && want == 0 {
                return;
            }
            match self.ring.submit_and_wait(want) {
                Ok(submitted) => {
                    self.unsubmitted = self.unsubmitted.saturating_sub(submitted);
                    if self.backlog.is_empty() {
                        return;
                    }
                    // The queue had no room for the backlog: make room by reaping.
                    self.reap();
                }
                Err(err) if err.raw_os_error() == Some(libc::EINTR) => {
                    if want > 0 {
                        return;
                    }
                }
                Err(err) if err.raw_os_error() == Some(libc::EBUSY) => {
                    // The completion queue is full. Reap and try again.
                    self.reap();
                }
                Err(err) => {
                    tracing::error!("io_uring_enter failed: {err}");
                    return;
                }
            }
        }
    }

    fn reap(&mut self) -> usize {
        let completions: Vec<(u64, i32)> = self
            .ring
            .completion()
            .map(|cqe| (cqe.user_data(), cqe.result()))
            .collect();
        for &(token, result) in &completions {
            match usize::try_from(token) {
                Ok(token) if token < self.ops.len() => self.complete(token, result),
                _ => self.wake_armed = false,
            }
        }
        completions.len()
    }

    fn complete(&mut self, token: usize, result: i32) {
        let Some(mut op) = self.ops[token].take() else {
            tracing::error!("io_uring completion for unknown read {token}");
            return;
        };
        let outcome = match result {
            n if n > 0 => {
                op.filled += n as usize;
                if op.filled < op.buffer.len() {
                    // A short read: continue from where it stopped.
                    let entry = op.entry(token as u64);
                    self.ops[token] = Some(op);
                    self.push(entry);
                    return;
                }
                Ok(())
            }
            0 => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "failed to fill whole buffer",
            )),
            n if n == -libc::EINTR || n == -libc::EAGAIN => {
                let entry = op.entry(token as u64);
                self.ops[token] = Some(op);
                self.push(entry);
                return;
            }
            n => Err(io::Error::from_raw_os_error(-n)),
        };
        self.in_flight -= 1;
        self.free.push(token);
        let ReadOp {
            buffer,
            pooled,
            alignment,
            reply,
            ..
        } = op;
        let bytes = outcome.map(|()| match pooled {
            Some(class) => self.pool.freeze(class, buffer, alignment),
            None => buffer.freeze(),
        });
        // The reader may have given up on the read; then there is no one to tell.
        drop(reply.send(bytes));
    }

    /// Waits for every in-flight read, so no buffer is freed while the kernel writes into it.
    pub(super) fn shutdown(&mut self) {
        while self.in_flight > 0 {
            self.enter(1);
            self.reap();
        }
    }
}
