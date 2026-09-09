// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Repro for #9817: a Vortex runtime driven from inside `tokio::runtime::Runtime::block_on`
//! loses wakeups delivered by Tokio's blocking pool, and stalls forever.
//!
//! The stalling cases are `#[ignore]`d so CI stays green, and they fail on a timeout rather than
//! hanging, so a regression is a test failure rather than a wedged job. Run them with
//! `cargo test -p vortex-io --features object_store,tokio --test tokio_context_blocking_stall --
//! --ignored --test-threads=1`.

#![cfg(all(feature = "object_store", feature = "tokio"))]
#![expect(clippy::tests_outside_test_module)]

use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use futures::StreamExt;
use futures::TryStreamExt;
use futures::stream;
use tokio::runtime::Builder;
use tokio::runtime::Runtime;
use vortex_buffer::Alignment;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_err;
use vortex_io::filesystem::FileSystem;
use vortex_io::object_store::ObjectStoreFileSystem;
use vortex_io::runtime::BlockingRuntime;
use vortex_io::runtime::current::CurrentThreadRuntime;

const TIMEOUT: Duration = Duration::from_secs(15);
const FILE_BYTES: usize = 1 << 20;
const CHUNK: usize = 4096;
/// Enough wakeups to hit the race reliably; it typically stalls between 130 and 200.
const WAKEUPS: usize = 1000;

/// Runs `body` on a worker thread, failing if it has not finished within [`TIMEOUT`].
fn with_timeout<F>(body: F) -> VortexResult<()>
where
    F: FnOnce() -> VortexResult<()> + Send + 'static,
{
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let result = body();
        drop(tx.send(result));
    });
    match rx.recv_timeout(TIMEOUT) {
        Ok(result) => result,
        Err(_) => vortex_bail!("stalled: no progress after {TIMEOUT:?}"),
    }
}

fn current_thread_runtime() -> VortexResult<Runtime> {
    Ok(Builder::new_current_thread().enable_all().build()?)
}

fn multi_thread_runtime() -> VortexResult<Runtime> {
    Ok(Builder::new_multi_thread().enable_all().build()?)
}

/// The minimal shape of the bug, with no Vortex I/O involved at all: awaiting Tokio blocking
/// tasks from a Vortex runtime whose driving thread sits inside `Runtime::block_on`.
fn await_tokio_blocking_tasks(rt: Runtime) -> VortexResult<()> {
    rt.block_on(async {
        CurrentThreadRuntime::new().block_on(async {
            for i in 0..WAKEUPS {
                let value = tokio::task::spawn_blocking(move || i)
                    .await
                    .map_err(|e| vortex_err!("blocking task failed: {e}"))?;
                assert_eq!(value, i);
            }
            Ok(())
        })
    })
}

#[test]
#[ignore = "reproduces #9817: stalls inside Runtime::block_on"]
fn tokio_blocking_tasks_under_current_thread_runtime() -> VortexResult<()> {
    with_timeout(|| await_tokio_blocking_tasks(current_thread_runtime()?))
}

/// Worker-thread count is not the variable: a multi-thread runtime stalls identically.
#[test]
#[ignore = "reproduces #9817: stalls inside Runtime::block_on"]
fn tokio_blocking_tasks_under_multi_thread_runtime() -> VortexResult<()> {
    with_timeout(|| await_tokio_blocking_tasks(multi_thread_runtime()?))
}

/// The control: the same Tokio runtime and the same blocking tasks, driven from a thread that is
/// *not* inside `Runtime::block_on`.
#[test]
fn tokio_blocking_tasks_from_thread_outside_block_on() -> VortexResult<()> {
    with_timeout(|| {
        let rt = multi_thread_runtime()?;
        let handle = rt.handle().clone();
        CurrentThreadRuntime::new().block_on(async move {
            for i in 0..WAKEUPS {
                let value = handle
                    .spawn_blocking(move || i)
                    .await
                    .map_err(|e| vortex_err!("blocking task failed: {e}"))?;
                assert_eq!(value, i);
            }
            Ok(())
        })
    })
}

fn fixture() -> VortexResult<(tempfile::TempDir, String)> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("data.vortex");
    std::fs::write(&path, vec![7u8; FILE_BYTES])?;
    let path = path.to_string_lossy().into_owned();
    Ok((dir, path))
}

/// Reads a local file through [`ObjectStoreFileSystem`], whose `LocalFileSystem` reaches Tokio's
/// blocking pool via `Handle::try_current()` on the calling thread.
fn read_local_file(path: String) -> VortexResult<()> {
    let vortex_rt = CurrentThreadRuntime::new();
    let fs = ObjectStoreFileSystem::local(vortex_rt.handle());
    vortex_rt.block_on(async move {
        let source = fs.open_read(&path).await?;
        stream::iter(
            (0..FILE_BYTES / CHUNK)
                .map(|i| source.read_at((i * CHUNK) as u64, CHUNK, Alignment::none())),
        )
        .buffer_unordered(1)
        .try_for_each(|_| async { Ok(()) })
        .await
    })
}

/// How the bug reaches Vortex in practice: scans of a local file driven from async code.
#[test]
#[ignore = "reproduces #9817: stalls inside Runtime::block_on"]
fn local_object_store_reads_inside_block_on() -> VortexResult<()> {
    let (dir, path) = fixture()?;
    with_timeout(move || {
        let result = current_thread_runtime()?.block_on(async move { read_local_file(path) });
        drop(dir);
        result
    })
}

/// The control: with no ambient Tokio runtime, `LocalFileSystem` reads inline and the same
/// workload completes.
#[test]
fn local_object_store_reads_without_tokio_context() -> VortexResult<()> {
    let (dir, path) = fixture()?;
    with_timeout(move || {
        let result = read_local_file(path);
        drop(dir);
        result
    })
}
