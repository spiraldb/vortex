// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::io::Write;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use futures::FutureExt;
use futures::StreamExt;
use futures::stream;
use rstest::rstest;
use vortex_buffer::Alignment;
use vortex_error::VortexResult;

use super::UringFileReadAt;
use super::UringRuntime;
use crate::VortexReadAt;
use crate::runtime::BlockingRuntime;

fn runtime(workers: usize) -> UringRuntime {
    UringRuntime::with_workers(workers).expect("io_uring is available")
}

#[rstest]
#[case(1)]
#[case(4)]
fn spawned_tasks_all_run(#[case] workers: usize) {
    let runtime = runtime(workers);
    let handle = runtime.handle();
    let count = Arc::new(AtomicUsize::new(0));
    let tasks = (0..1000)
        .map(|_| {
            let count = Arc::clone(&count);
            handle.spawn(async move {
                count.fetch_add(1, Ordering::Relaxed);
            })
        })
        .collect::<Vec<_>>();
    runtime.block_on(futures::future::join_all(tasks));
    assert_eq!(count.load(Ordering::Relaxed), 1000);
}

#[test]
fn nested_spawns_and_cpu_work_complete() {
    let runtime = runtime(3);
    let handle = runtime.handle();
    let total = runtime.block_on(handle.spawn_nested(|handle| async move {
        let parts = (0..64u64)
            .map(|i| handle.spawn_cpu(move || i * 2))
            .collect::<Vec<_>>();
        futures::future::join_all(parts)
            .await
            .into_iter()
            .sum::<u64>()
    }));
    assert_eq!(total, (0..64u64).map(|i| i * 2).sum::<u64>());
}

#[test]
fn a_panicking_task_does_not_stop_its_worker() {
    let runtime = runtime(1);
    let handle = runtime.handle();
    let panicked = AssertUnwindSafe(runtime.block_on(async {
        AssertUnwindSafe(handle.spawn(async { panic!("boom") }))
            .catch_unwind()
            .await
    }));
    assert!(panicked.0.is_err());
    // The only worker still runs tasks.
    assert_eq!(runtime.block_on(handle.spawn(async { 7 })), 7);
}

#[test]
fn block_on_stream_yields_every_item() {
    let runtime = runtime(2);
    let items = runtime
        .block_on_stream(stream::iter(0..10).then(|i| async move { i + 1 }))
        .collect::<Vec<_>>();
    assert_eq!(items, (1..11).collect::<Vec<_>>());
}

fn temp_file(len: usize) -> (tempfile::NamedTempFile, Vec<u8>) {
    let bytes = (0..len)
        .map(|i| u8::try_from(i % 251).unwrap_or(0))
        .collect::<Vec<_>>();
    let mut file = tempfile::NamedTempFile::new().expect("temp file");
    file.write_all(&bytes).expect("write");
    file.flush().expect("flush");
    (file, bytes)
}

#[rstest]
#[case(0, 0)]
#[case(1, 17)]
#[case(4096, 100_000)]
#[case(0, 3 << 20)]
fn reads_on_a_worker_go_through_the_ring(
    #[case] offset: usize,
    #[case] len: usize,
) -> VortexResult<()> {
    let runtime = runtime(2);
    let (file, bytes) = temp_file(4 << 20);
    let read = Arc::new(UringFileReadAt::open(file.path(), runtime.handle())?);
    let buffer = runtime.block_on(
        runtime
            .handle()
            .spawn(async move { read.read_at(offset as u64, len, Alignment::new(64)).await }),
    )?;
    let buffer = buffer.unwrap_host();
    assert_eq!(buffer.as_slice(), &bytes[offset..offset + len]);
    assert!(buffer.is_aligned(Alignment::new(64)));
    Ok(())
}

#[test]
fn many_concurrent_reads_complete() -> VortexResult<()> {
    let runtime = runtime(4);
    let (file, bytes) = temp_file(1 << 20);
    let read = Arc::new(UringFileReadAt::open(file.path(), runtime.handle())?);
    let reads = (0..2000u64)
        .map(|i| {
            let read = Arc::clone(&read);
            let offset = (i * 523) % ((1 << 20) - 1000);
            let start = usize::try_from(offset).unwrap_or(0);
            runtime.handle().spawn(async move {
                let buffer = read.read_at(offset, 1000, Alignment::none()).await?;
                VortexResult::Ok((start, buffer))
            })
        })
        .collect::<Vec<_>>();
    for result in runtime.block_on(futures::future::join_all(reads)) {
        let (offset, buffer) = result?;
        assert_eq!(
            buffer.unwrap_host().as_slice(),
            &bytes[offset..offset + 1000]
        );
    }
    Ok(())
}

#[test]
fn reading_past_the_end_fails() -> VortexResult<()> {
    let runtime = runtime(1);
    let (file, _) = temp_file(100);
    let read = Arc::new(UringFileReadAt::open(file.path(), runtime.handle())?);
    let result = runtime.block_on(
        runtime
            .handle()
            .spawn(async move { read.read_at(90, 20, Alignment::none()).await }),
    );
    assert!(result.is_err());
    Ok(())
}

#[test]
fn reads_off_the_workers_fall_back_to_blocking_reads() -> VortexResult<()> {
    let runtime = runtime(1);
    let (file, bytes) = temp_file(1000);
    let read = UringFileReadAt::open(file.path(), runtime.handle())?;
    let buffer = runtime.block_on(read.read_at(10, 500, Alignment::none()))?;
    assert_eq!(buffer.unwrap_host().as_slice(), &bytes[10..510]);
    assert_eq!(runtime.block_on(read.size())?, 1000);
    Ok(())
}

#[test]
fn large_reads_spread_over_every_ring() -> VortexResult<()> {
    let runtime = runtime(4);
    let (file, bytes) = temp_file(8 << 20);
    let read = Arc::new(UringFileReadAt::open(file.path(), runtime.handle())?);
    let len = 1 << 20;
    let reads = (0..16u64)
        .map(|i| {
            let read = Arc::clone(&read);
            runtime.handle().spawn(async move {
                let offset = (i * 333_333) % (7 << 20);
                let buffer = read.read_at(offset, len, Alignment::new(64)).await?;
                VortexResult::Ok((usize::try_from(offset).unwrap_or(0), buffer))
            })
        })
        .collect::<Vec<_>>();
    for result in runtime.block_on(futures::future::join_all(reads)) {
        let (offset, buffer) = result?;
        let buffer = buffer.unwrap_host();
        assert!(buffer.is_aligned(Alignment::new(64)));
        assert_eq!(buffer.as_slice(), &bytes[offset..offset + len]);
    }
    Ok(())
}

#[test]
fn dropped_read_buffers_are_reused() -> VortexResult<()> {
    let runtime = runtime(1);
    let (file, bytes) = temp_file(1 << 20);
    let read = Arc::new(UringFileReadAt::open(file.path(), runtime.handle())?);
    let read_once = |offset: u64| {
        let read = Arc::clone(&read);
        runtime.block_on(
            runtime
                .handle()
                .spawn(async move { read.read_at(offset, 100_000, Alignment::none()).await }),
        )
    };
    let first = read_once(0)?.unwrap_host();
    let address = first.as_slice().as_ptr();
    drop(first);
    let second = read_once(5)?.unwrap_host();
    assert_eq!(second.as_slice().as_ptr(), address);
    assert_eq!(second.as_slice(), &bytes[5..100_005]);
    Ok(())
}

/// A lone large read is divided among the idle rings, and its parts land in one buffer.
#[rstest]
#[case(0, 4 << 20)]
#[case(12_345, (1 << 20) + 777)]
#[case(1, 256 << 10)]
fn a_lone_large_read_is_divided_among_the_rings(
    #[case] offset: usize,
    #[case] len: usize,
) -> VortexResult<()> {
    let runtime = runtime(4);
    let (file, bytes) = temp_file(6 << 20);
    let read = Arc::new(UringFileReadAt::open(file.path(), runtime.handle())?);
    let buffer = runtime.block_on(
        runtime
            .handle()
            .spawn(async move { read.read_at(offset as u64, len, Alignment::new(64)).await }),
    )?;
    let buffer = buffer.unwrap_host();
    assert!(buffer.is_aligned(Alignment::new(64)));
    assert_eq!(buffer.as_slice(), &bytes[offset..offset + len]);
    Ok(())
}

#[test]
fn a_divided_read_past_the_end_fails() -> VortexResult<()> {
    let runtime = runtime(4);
    let (file, _) = temp_file(1 << 20);
    let read = Arc::new(UringFileReadAt::open(file.path(), runtime.handle())?);
    let result = runtime.block_on(
        runtime
            .handle()
            .spawn(async move { read.read_at(512 << 10, 1 << 20, Alignment::none()).await }),
    );
    assert!(result.is_err());
    Ok(())
}
