// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Drives a V2 scan for DataFusion.
//!
//! A planner says what it needs: compute, or reads. This driver decides where each runs. The
//! opener composes the planners over a [`PreparedScan`] and hands the driver the first; the
//! driver runs it and everything it emits on planning runs it owns: the root run on a task that
//! awaits its reads in place, the first chunk it spawns on that run too, each later chunk on a
//! task of its own, or on the root run when the engine's partitions already fill the cores.
//! Every read is a `spawn_io` task of the file's segment source. The DataFusion thread polling
//! the stream does no scan work itself.

use std::future;
use std::sync::Arc;
use std::task::Poll;

use arrow_array::RecordBatch;
use futures::SinkExt;
use futures::StreamExt;
use futures::TryStreamExt;
use futures::channel::mpsc;
use futures::stream;
use futures::stream::BoxStream;
use vortex::array::ArrayRef;
use vortex::error::VortexResult;
use vortex::io::request::IoSource;
use vortex::io::runtime::Handle;
use vortex::layout::scan::v2::PreparedScan;
use vortex::scan::planning::driver::Progress;
use vortex::scan::planning::driver::Run;
use vortex::scan::planning::planner::Planner;
use vortex::scan::planning::planner::WorkScope;
use vortex_utils::parallelism::get_available_parallelism;

/// Maps a scan's batches to Arrow on the task that produced them.
type MapBatch = Arc<dyn Fn(ArrayRef) -> VortexResult<RecordBatch> + Send + Sync>;

/// Runs one partition's V2 scan on DataFusion's runtime.
pub(crate) struct ScanDriver {
    handle: Handle,
    /// Chunks run beside each other per worker thread; at 1 they run on the scan's own run.
    concurrency: usize,
    /// Whether batches are yielded in row order, which holds them until the scan is done.
    ordered: bool,
}

impl ScanDriver {
    pub(crate) fn new(handle: Handle, concurrency: usize, ordered: bool) -> Self {
        Self {
            handle,
            concurrency,
            ordered,
        }
    }

    /// Runs `root`, the first planner composed over `scan`, and everything it emits, and
    /// streams their batches mapped by `map`.
    /// How many chunks run beside each other.
    pub(crate) fn in_flight(&self) -> usize {
        self.concurrency * get_available_parallelism().unwrap_or(1)
    }

    pub(crate) fn run(
        self,
        scan: Arc<PreparedScan>,
        root: Box<dyn Planner>,
        map: impl Fn(ArrayRef) -> VortexResult<RecordBatch> + Send + Sync + 'static,
    ) -> BoxStream<'static, VortexResult<RecordBatch>> {
        let map: MapBatch = Arc::new(map);
        let in_flight = self.in_flight();
        let inline = in_flight <= 1;
        let scope = WorkScope {
            file_ordinal: 0,
            rows: scan.rows().clone(),
        };
        let io = scan.io();
        let (chunk_handle, chunk_io, chunk_map) =
            (self.handle.clone(), Arc::clone(&io), Arc::clone(&map));
        let batches = run_stream(self.handle, root, scope, io.session(), map, inline)
            .map_ok(move |yielded| match yielded {
                Yielded::Batch(start, batch) => {
                    stream::once(future::ready(Ok((start, batch)))).boxed()
                }
                Yielded::Spawned(chunks) => {
                    let streams: Vec<_> = chunks
                        .into_iter()
                        .map(|(scope, chunk)| {
                            chunk_stream(
                                &chunk_handle,
                                chunk,
                                scope,
                                chunk_io.session(),
                                Arc::clone(&chunk_map),
                            )
                        })
                        .collect();
                    stream::iter(streams).flatten_unordered(None).boxed()
                }
            })
            .try_flatten_unordered(in_flight);
        // The prepared scan holds the file's shared cache: keep it until the stream ends, so
        // scans of the file that start meanwhile find it.
        let batches = batches.chain(stream::poll_fn(move |_| {
            let _ = &scan;
            Poll::Ready(None)
        }));
        if !self.ordered {
            return batches.map_ok(|(_, batch)| batch).boxed();
        }
        stream::once(async move {
            let mut held: Vec<(u64, RecordBatch)> = batches.try_collect().await?;
            // Morsels finish in whatever order their reads arrive.
            held.sort_by_key(|(start, _)| *start);
            VortexResult::Ok(stream::iter(held.into_iter().map(|(_, batch)| Ok(batch))))
        })
        .try_flatten()
        .boxed()
    }
}

/// What the root run's stream yields: a mapped batch with the row it starts at, or a chunk a
/// planner of the run spawned.
enum Yielded {
    Batch(u64, RecordBatch),
    /// Chunks the run spawned together, to be run beside each other from the same moment, so
    /// their first reads are registered together and coalesce.
    Spawned(Vec<(WorkScope, Box<dyn Planner>)>),
}

/// Runs `root` over `scope` on a planning run of its own, on one task that drives the run to
/// its end and awaits only its reads, and yields its batches and the chunks it spawns, those
/// spawned between two hand-offs together. The first chunk the run spawns, and every chunk when
/// `inline`, runs on this run as another root, so a partition of one chunk never hands it to a
/// task of its own.
fn run_stream(
    handle: Handle,
    root: Box<dyn Planner>,
    scope: WorkScope,
    io: Arc<dyn IoSource>,
    map: MapBatch,
    inline: bool,
) -> BoxStream<'static, VortexResult<Yielded>> {
    let (mut send, recv) = mpsc::channel(1);
    let task = handle.spawn(async move {
        let mut run = Run::new();
        run.admit(root, scope, Arc::clone(&io));
        let mut admitted = 0;
        let mut spawned = Vec::new();
        loop {
            let yielded = match run.advance()? {
                Progress::Batch(batch) => {
                    Some(Yielded::Batch(batch.scope.rows.start, map(batch.array)?))
                }
                Progress::Spawn(scope, child) if inline || admitted == 0 => {
                    admitted += 1;
                    run.admit(child, scope, Arc::clone(&io));
                    continue;
                }
                Progress::Spawn(scope, child) => {
                    spawned.push((scope, child));
                    continue;
                }
                Progress::RootDone(_) if run.live_roots() > 0 => continue,
                Progress::RootDone(_) | Progress::Idle => None,
                Progress::Waiting => {
                    if spawned.is_empty() {
                        future::poll_fn(|cx| run.poll_completion(cx)).await?;
                        continue;
                    }
                    None
                }
            };
            // Everything spawned since the last hand-off goes out together, ahead of a batch.
            if !spawned.is_empty()
                && send
                    .send(Ok(Yielded::Spawned(std::mem::take(&mut spawned))))
                    .await
                    .is_err()
            {
                return Ok(());
            }
            match yielded {
                Some(yielded) => {
                    if let Yielded::Batch(start, batch) = &yielded {
                        tracing::debug!(
                            row_start = start,
                            rows = batch.num_rows(),
                            "v2 batch sent"
                        );
                    }
                    if send.send(Ok(yielded)).await.is_err() {
                        return Ok(());
                    }
                }
                None if run.live_roots() > 0 => {
                    future::poll_fn(|cx| run.poll_completion(cx)).await?;
                }
                None => return Ok(()),
            }
        }
    });
    let errors = stream::once(task)
        .filter_map(|result: VortexResult<()>| future::ready(result.err().map(Err)));
    stream::select(recv, errors).boxed()
}

/// Runs a spawned chunk's `root` over `scope` on a planning run of its own, on one task that
/// drives the run to its end and awaits only its reads, and yields its mapped batches with the
/// row each starts at. A planner the chunk spawns in turn runs as another root of the same run.
///
/// One task per chunk, not one per step: a chunk's run waits for reads many times, and a task
/// hop at each wait is latency on the chunk's critical path that no other chunk's work fills.
fn chunk_stream(
    handle: &Handle,
    root: Box<dyn Planner>,
    scope: WorkScope,
    io: Arc<dyn IoSource>,
    map: MapBatch,
) -> BoxStream<'static, VortexResult<(u64, RecordBatch)>> {
    let (mut send, recv) = mpsc::channel(1);
    let task = handle.spawn(async move {
        tracing::debug!(
            row_start = scope.rows.start,
            row_end = scope.rows.end,
            "v2 chunk run started"
        );
        let mut run = Run::new();
        run.admit(root, scope, Arc::clone(&io));
        loop {
            match run.advance()? {
                Progress::Batch(batch) => {
                    let start = batch.scope.rows.start;
                    let batch = map(batch.array);
                    if let Ok(batch) = &batch {
                        tracing::debug!(
                            row_start = start,
                            rows = batch.num_rows(),
                            "v2 batch sent"
                        );
                    }
                    let failed = batch.is_err();
                    if send.send(batch.map(|batch| (start, batch))).await.is_err() || failed {
                        return Ok(());
                    }
                }
                Progress::Spawn(scope, child) => {
                    run.admit(child, scope, Arc::clone(&io));
                }
                Progress::RootDone(_) if run.live_roots() > 0 => {}
                Progress::RootDone(_) | Progress::Idle => return Ok(()),
                Progress::Waiting => future::poll_fn(|cx| run.poll_completion(cx)).await?,
            }
        }
    });
    let errors = stream::once(task)
        .filter_map(|result: VortexResult<()>| future::ready(result.err().map(Err)));
    stream::select(recv, errors).boxed()
}
