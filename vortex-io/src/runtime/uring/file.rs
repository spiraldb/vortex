// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use futures::FutureExt;
use futures::channel::oneshot;
use futures::future::BoxFuture;
use vortex_array::buffer::BufferHandle;
use vortex_array::memory::BufferAllocatorRef;
use vortex_buffer::Alignment;
use vortex_buffer::BufferMut;
use vortex_error::VortexResult;
use vortex_error::vortex_err;

use super::pool::BufferPool;
use super::reactor::ReadRequest;
use super::submit_read;
use crate::CoalesceConfig;
use crate::VortexReadAt;
use crate::runtime::Handle;
use crate::std_file::read_exact_at;

/// The widest gap between two reads that are still merged into one.
const COALESCE_DISTANCE: u64 = 256 << 10;

/// The largest read merging produces.
const COALESCE_MAX_SIZE: u64 = 2 << 20;

/// Concurrent reads a single file keeps in flight. A ring absorbs many more than a thread pool
/// would, and each costs only a submission queue entry.
const CONCURRENCY: usize = 128;

/// A local file read through the io_uring of the [`UringRuntime`](super::UringRuntime) worker
/// that polls each read.
///
/// A read polled anywhere else, such as on the thread opening the file, runs as a blocking
/// positional read on `handle`'s blocking pool instead.
pub struct UringFileReadAt {
    uri: Arc<str>,
    file: Arc<File>,
    handle: Handle,
    allocator: BufferAllocatorRef,
}

impl UringFileReadAt {
    /// Opens a file for reading.
    pub fn open(path: impl AsRef<Path>, handle: Handle) -> VortexResult<Self> {
        Self::open_with_allocator(path, handle, BufferAllocatorRef::statically_allocated())
    }

    /// Opens a file for reading into buffers from `allocator`.
    pub fn open_with_allocator(
        path: impl AsRef<Path>,
        handle: Handle,
        allocator: BufferAllocatorRef,
    ) -> VortexResult<Self> {
        let path = path.as_ref();
        Ok(Self {
            uri: path.to_string_lossy().to_string().into(),
            file: Arc::new(File::open(path)?),
            handle,
            allocator,
        })
    }
}

impl VortexReadAt for UringFileReadAt {
    fn uri(&self) -> Option<&Arc<str>> {
        Some(&self.uri)
    }

    fn coalesce_config(&self) -> Option<CoalesceConfig> {
        // Merged reads are spread over every worker's ring, so moderate ones keep every core
        // copying at once, and narrower gaps copy fewer bytes nobody asked for.
        Some(CoalesceConfig::new(COALESCE_DISTANCE, COALESCE_MAX_SIZE))
    }

    fn concurrency(&self) -> usize {
        CONCURRENCY
    }

    fn size(&self) -> BoxFuture<'static, VortexResult<u64>> {
        let file = Arc::clone(&self.file);
        async move { Ok(file.metadata()?.len()) }.boxed()
    }

    fn read_at(
        &self,
        offset: u64,
        length: usize,
        alignment: Alignment,
    ) -> BoxFuture<'static, VortexResult<BufferHandle>> {
        let file = Arc::clone(&self.file);
        let handle = self.handle.clone();
        let allocator = self.allocator.clone();
        async move {
            // Reads the pool serves land in a buffer of the worker that performs them, which
            // returns to that worker once dropped. Anything else gets a buffer of its own.
            let buffer = (!BufferPool::fits(length, alignment))
                .then(|| exact_buffer(&allocator, length, alignment));
            let (reply, read) = oneshot::channel();
            let request = ReadRequest {
                file: Arc::clone(&file),
                offset,
                len: length,
                alignment,
                buffer,
                reply,
            };
            let bytes = match submit_read(request) {
                Ok(()) => read.await.map_err(|_| {
                    vortex_err!("io_uring worker stopped before the read finished")
                })??,
                Err(request) => {
                    let mut buffer = request
                        .buffer
                        .unwrap_or_else(|| exact_buffer(&allocator, length, alignment));
                    handle
                        .spawn_blocking(move || {
                            read_exact_at(&file, buffer.as_mut_slice(), offset)?;
                            VortexResult::Ok(buffer.freeze())
                        })
                        .await?
                }
            };
            Ok(BufferHandle::new_host(bytes))
        }
        .boxed()
    }
}

/// A buffer of `length` bytes that a read must fill before anything reads it.
fn exact_buffer(
    allocator: &BufferAllocatorRef,
    length: usize,
    alignment: Alignment,
) -> BufferMut<u8> {
    let mut buffer = allocator.with_capacity_aligned::<u8>(length, alignment);
    // SAFETY: the read fills every byte before the buffer is frozen, or fails and the buffer is
    // dropped unread.
    unsafe { buffer.set_len(length) };
    buffer
}
