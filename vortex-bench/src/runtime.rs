// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! The runtime benchmarks drive Vortex work on, and the local file reader that goes with it.

use std::any::Any;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;

use clap::ValueEnum;
use memmap2::Mmap;
use vortex::array::memory::MemorySessionExt;
use vortex::buffer::ByteBuffer;
use vortex::error::VortexResult;
use vortex::file::VortexFile;
use vortex::file::VortexOpenOptions;
use vortex::io::VortexReadAt;
#[cfg(target_os = "linux")]
use vortex::io::runtime::BlockingRuntime;
#[cfg(target_os = "linux")]
use vortex::io::runtime::uring::UringFileReadAt;
#[cfg(target_os = "linux")]
use vortex::io::runtime::uring::UringRuntime;
use vortex::io::session::RuntimeSessionExt;
use vortex::io::std_file::FileReadAt;
use vortex::utils::parallelism::get_available_parallelism;

use crate::SESSION;

/// The runtime Vortex work is scheduled on.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, ValueEnum)]
pub enum VortexRuntime {
    /// The Tokio runtime the benchmark binary runs on, shared with every other format.
    Tokio,
    /// Vortex's thread-per-core runtime, whose workers each own an io_uring. Linux only.
    #[default]
    IoUring,
}

/// How Vortex reads local files.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, ValueEnum)]
#[repr(u8)]
pub enum VortexReader {
    /// Positional reads on the runtime's blocking thread pool.
    Blocking,
    /// Reads submitted to the io_uring of the worker that issues them. Needs the io_uring
    /// runtime, and otherwise falls back to blocking reads.
    IoUring,
    /// The whole file memory-mapped, as the Python bindings open local files. Segments are slices
    /// of the page cache: nothing is copied, and no read is ever waited on. The fastest choice for
    /// files the page cache holds, as the benchmarks' files are after their warm-up.
    #[default]
    Mmap,
}

/// Whether [`SESSION`] runs on the io_uring runtime.
static URING: AtomicBool = AtomicBool::new(false);

/// The [`VortexReader`] [`open_local`] uses.
static READER: AtomicU8 = AtomicU8::new(VortexReader::Mmap as u8);

/// Chooses how [`open_local`] reads files.
pub fn set_reader(reader: VortexReader) {
    READER.store(reader as u8, Ordering::Relaxed);
}

/// Moves Vortex work onto `kind`, with `workers` threads, returning the runtime to keep alive for
/// the rest of the run, or `None` when Vortex stays on Tokio.
///
/// The handle is installed on [`SESSION`], so every Vortex spawn lands on the runtime's workers.
/// A kernel that refuses to create a ring falls back to Tokio with a warning.
pub fn install(kind: VortexRuntime, workers: usize) -> Option<Box<dyn Any>> {
    if kind == VortexRuntime::Tokio {
        return None;
    }
    #[cfg(target_os = "linux")]
    match UringRuntime::with_workers(workers) {
        Ok(uring) => {
            tracing::info!("Running Vortex on {} io_uring workers", uring.workers());
            drop(SESSION.clone().with_handle(uring.handle()));
            URING.store(true, Ordering::Relaxed);
            Some(Box::new(uring))
        }
        Err(error) => {
            tracing::warn!("io_uring is unavailable, running Vortex on Tokio: {error}");
            None
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = workers;
        tracing::warn!("io_uring needs Linux, running Vortex on Tokio");
        None
    }
}

/// One worker per available core, as many as a default multi-threaded Tokio runtime starts.
pub fn available_workers() -> usize {
    get_available_parallelism().unwrap_or(1).max(1)
}

/// Whether Vortex runs on the io_uring runtime.
pub fn uring_active() -> bool {
    URING.load(Ordering::Relaxed)
}

/// Opens a Vortex file on local disk with `options`, read the way [`set_reader`] chose.
pub async fn open_file(
    path: impl AsRef<Path>,
    options: VortexOpenOptions,
) -> VortexResult<VortexFile> {
    if READER.load(Ordering::Relaxed) == VortexReader::Mmap as u8 {
        let file = std::fs::File::open(path)?;
        // SAFETY: benchmark files are immutable once written. A file changed or truncated while
        // mapped gives wrong data or SIGBUS, as with any memory-mapped reader.
        let mmap = unsafe { Mmap::map(&file)? };
        return options.open_buffer(ByteBuffer::from(mmap));
    }
    options.open(open_local(path)?).await
}

/// Opens a local file for Vortex to read, with the reader chosen by [`set_reader`]: through the
/// workers' rings, or as blocking reads on the runtime's blocking pool. A memory-mapped reader
/// has no [`VortexReadAt`], so it reads as blocking here; [`open_file`] maps the file instead.
pub fn open_local(path: impl AsRef<Path>) -> VortexResult<Arc<dyn VortexReadAt>> {
    #[cfg(target_os = "linux")]
    if uring_active() && READER.load(Ordering::Relaxed) == VortexReader::IoUring as u8 {
        return Ok(Arc::new(UringFileReadAt::open_with_allocator(
            path,
            SESSION.handle(),
            SESSION.allocator(),
        )?));
    }
    Ok(Arc::new(FileReadAt::open_with_allocator(
        path,
        SESSION.handle(),
        SESSION.allocator(),
    )?))
}
