// Remove these
#![allow(dead_code)]
#![allow(clippy::all)]
#![allow(clippy::nursery)]

mod read;
mod write;

#[cfg(feature = "tokio")]
pub mod tokio;
mod fusio;

use std::future::Future;
use std::io;
use std::ops::Range;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::read::VortexReadAt;
use crate::write::VortexWrite;

/// Generic trait for runtimes to implement.
pub trait Fs {
    type FileRead: VortexReadAt;
    type FileWrite: VortexWrite;

    /// Open a file. Async operations on the file will be performed within the current runtime.
    fn open(&self, path: &str) -> impl Future<Output = io::Result<Self::FileRead>> + Send;

    fn create(&self, path: &str) -> impl Future<Output = io::Result<Self::FileWrite>> + Send;
}

pub trait IoScheduler: Sized {
    /// The asynchronous runtime used for running submitted tasks.
    type Fs;

    /// Submit a new read operation against the given `reader`.
    ///
    /// Returns a future that resolves to the bytes over the given range. The future will be
    /// executed on our runtime.
    fn submit<R>(&self, reader: R, options: ReadOptions) -> ReadRangeFut<Self, R>
    where
        R: VortexReadAt;
}

/// Options for the read request.
///
/// Any endpoint or backing-store specific options should not live here, they should be handled
/// by the thing that creates the VortexReadAt.
pub struct ReadOptions {
    pub range: Range<u64>,
}

// ReadRangeFut is a future that resolves to have a specific range of data instead there.
pub struct ReadRangeFut<S: Sized, R> {
    scheduler: S,
    reader: R,
    options: ReadOptions,
}

// Implement using the reader traits here instead.

impl<S, R> Future for ReadRangeFut<S, R>
where
    S: IoScheduler,
    R: VortexReadAt,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        todo!()
    }
}
