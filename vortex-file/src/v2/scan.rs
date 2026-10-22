// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Drives a V2 scan of a file for callers without an engine of their own.
//!
//! A planner says what it needs: compute, or reads. This driver decides where each runs. It
//! composes the planners over a [`PreparedScan`], the pruning plans first when the scan has a
//! filter and the chunks of the rows they keep after, and runs them on planning runs it owns:
//! the root run stepped on `spawn_cpu` tasks as the stream is polled, each spawned chunk on a
//! task of its own, up to the concurrency, or on the root run at a concurrency of one. Every
//! read is a `spawn_io` task of the file's segment source.

use std::future;
use std::sync::Arc;
use std::task::Poll;

use futures::SinkExt;
use futures::StreamExt;
use futures::TryStreamExt;
use futures::channel::mpsc;
use futures::stream;
use futures::stream::BoxStream;
use vortex_array::ArrayRef;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_io::request::IoSource;
use vortex_io::runtime::Handle;
use vortex_layout::scan::v2::ChunkPlanner;
use vortex_layout::scan::v2::PreparedScan;
use vortex_layout::scan::v2::PrunePlanner;
use vortex_layout::scan::v2::ScanBuilder;
use vortex_mask::Mask;
use vortex_scan::planning::driver::Progress;
use vortex_scan::planning::driver::Run;
use vortex_scan::planning::planner::Planner;
use vortex_scan::planning::planner::WorkScope;
use vortex_utils::parallelism::get_available_parallelism;

/// Runs a V2 scan on the session's runtime.
pub struct ScanDriver {
    handle: Handle,
    concurrency: usize,
    ordered: bool,
}

impl ScanDriver {
    /// A driver spawning on `handle`, running `concurrency` chunks beside each other per worker
    /// thread, or every chunk on the scan's own run at one, and yielding batches in row order
    /// when `ordered`, which holds them until the scan is done.
    pub fn new(handle: Handle, concurrency: usize, ordered: bool) -> Self {
        assert!(concurrency > 0);
        Self {
            handle,
            concurrency,
            ordered,
        }
    }

    /// Prepares `builder` where compute runs, composes its planners, and streams their batches.
    /// Preparation errors are the stream's first item.
    pub fn run(self, builder: ScanBuilder) -> BoxStream<'static, VortexResult<ArrayRef>> {
        let handle = self.handle.clone();
        stream::once(async move {
            handle
                .spawn_cpu(move || {
                    let scan = Arc::new(builder.prepare()?);
                    VortexResult::Ok(self.execute(scan))
                })
                .await
        })
        .try_flatten()
        .boxed()
    }

    fn execute(self, scan: Arc<PreparedScan>) -> BoxStream<'static, VortexResult<ArrayRef>> {
        let in_flight = self.concurrency * get_available_parallelism().unwrap_or(1);
        let inline = in_flight <= 1;
        let scope = WorkScope {
            file_ordinal: 0,
            rows: scan.rows().clone(),
        };
        let io = scan.io();
        let root = compose(&scan);
        let (chunk_handle, chunk_io) = (self.handle.clone(), Arc::clone(&io));
        let batches = run_stream(self.handle, root, scope, io.session(), inline)
            .map_ok(|yielded| stream::iter(yielded.into_iter().map(VortexResult::Ok)))
            .try_flatten()
            .map_ok(move |yielded| match yielded {
                Yielded::Batch(start, batch) => {
                    stream::once(future::ready(Ok((start, batch)))).boxed()
                }
                Yielded::Spawn(scope, chunk) => {
                    chunk_stream(&chunk_handle, chunk, scope, chunk_io.session())
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
            let mut held: Vec<(u64, ArrayRef)> = batches.try_collect().await?;
            // Morsels finish in whatever order their reads arrive.
            held.sort_by_key(|(start, _)| *start);
            VortexResult::Ok(stream::iter(held.into_iter().map(|(_, batch)| Ok(batch))))
        })
        .try_flatten()
        .boxed()
    }
}

/// The planners of a scan, composed: with a filter, its pruning plans first over the whole scan,
/// continuing into the chunks of the rows they keep; without, the chunks directly.
fn compose(scan: &Arc<PreparedScan>) -> Box<dyn Planner> {
    let rows = scan.rows().clone();
    let chunks = {
        let scan = Arc::clone(scan);
        let rows = rows.clone();
        move |kept: Option<Mask>| -> Box<dyn Planner> {
            let selection = scan.selection().clone();
            Box::new(ChunkPlanner::new(scan, rows, selection, kept))
        }
    };
    if scan.filter().is_some() {
        Box::new(PrunePlanner::new(Arc::clone(scan), rows, Box::new(chunks)))
    } else {
        chunks(None)
    }
}

/// What the root run's stream yields: a batch with the row it starts at, or a chunk a planner of
/// the run spawned.
enum Yielded {
    Batch(u64, ArrayRef),
    Spawn(WorkScope, Box<dyn Planner>),
}

/// What one CPU step of the root run produced: the chunks it spawned, then how it ended.
struct Stepped {
    spawned: Vec<(WorkScope, Box<dyn Planner>)>,
    end: End,
}

enum End {
    Batch(u64, ArrayRef),
    Done,
    Waiting,
}

/// The state of the root run's stream.
struct RunState {
    handle: Handle,
    /// Taken while a CPU step runs on its task and put back with its result.
    run: Option<Run>,
    io: Arc<dyn IoSource>,
    /// Whether a spawned chunk runs on this run, as another root, instead of being yielded.
    inline: bool,
    /// How the last step ended, once the chunks it spawned have been yielded.
    pending: Option<End>,
}

/// Runs `root` over `scope` on a planning run of its own, reading through `io`, and yields its
/// batches and, unless `inline`, the chunks it spawns. One CPU step, on a `spawn_cpu` task, runs
/// the run until it has a batch, waits for a read, or is done, so every chunk the run can plan
/// at once is yielded together.
fn run_stream(
    handle: Handle,
    root: Box<dyn Planner>,
    scope: WorkScope,
    io: Arc<dyn IoSource>,
    inline: bool,
) -> BoxStream<'static, VortexResult<Vec<Yielded>>> {
    let mut run = Run::new();
    run.admit(root, scope, Arc::clone(&io));
    let state = RunState {
        handle,
        run: Some(run),
        io,
        inline,
        pending: None,
    };
    stream::try_unfold(state, |mut state| async move {
        loop {
            let run = state
                .run
                .as_mut()
                .ok_or_else(|| vortex_err!("the scan's run is missing"))?;
            match state.pending.take() {
                None => {}
                Some(End::Waiting) => future::poll_fn(|cx| run.poll_completion(cx)).await?,
                Some(End::Done | End::Batch(..)) => return Ok(None),
            }
            let run = state
                .run
                .take()
                .ok_or_else(|| vortex_err!("the scan's run is missing"))?;
            let (io, inline) = (Arc::clone(&state.io), state.inline);
            let (run, stepped) = state
                .handle
                .spawn_cpu(move || {
                    let mut run = run;
                    let stepped = step(&mut run, &io, inline);
                    (run, stepped)
                })
                .await;
            state.run = Some(run);
            let Stepped { spawned, end } = stepped?;
            let mut yielded: Vec<_> = spawned
                .into_iter()
                .map(|(scope, chunk)| Yielded::Spawn(scope, chunk))
                .collect();
            match end {
                End::Batch(start, batch) => yielded.push(Yielded::Batch(start, batch)),
                end => state.pending = Some(end),
            }
            if !yielded.is_empty() {
                return Ok(Some((yielded, state)));
            }
        }
    })
    .boxed()
}

/// Advances `run` until it has a batch, waits, or is done, admitting each chunk it spawns as
/// another root when `inline` and collecting it otherwise.
fn step(run: &mut Run, io: &Arc<dyn IoSource>, inline: bool) -> VortexResult<Stepped> {
    let mut spawned = Vec::new();
    let end = loop {
        match run.advance()? {
            Progress::Batch(batch) => break End::Batch(batch.scope.rows.start, batch.array),
            Progress::Spawn(scope, child) if inline => {
                run.admit(child, scope, Arc::clone(io));
            }
            Progress::Spawn(scope, child) => spawned.push((scope, child)),
            Progress::RootDone(_) if run.live_roots() > 0 => {}
            Progress::RootDone(_) | Progress::Idle => break End::Done,
            Progress::Waiting => break End::Waiting,
        }
    };
    Ok(Stepped { spawned, end })
}

/// Runs a spawned chunk's `root` over `scope` on a planning run of its own, on one task that
/// drives the run to its end and awaits only its reads, and yields its batches with the row each
/// starts at. A planner the chunk spawns in turn runs as another root of the same run.
fn chunk_stream(
    handle: &Handle,
    root: Box<dyn Planner>,
    scope: WorkScope,
    io: Arc<dyn IoSource>,
) -> BoxStream<'static, VortexResult<(u64, ArrayRef)>> {
    let (mut send, recv) = mpsc::channel(1);
    let task = handle.spawn(async move {
        let mut run = Run::new();
        run.admit(root, scope, Arc::clone(&io));
        loop {
            match run.advance()? {
                Progress::Batch(batch) => {
                    if send
                        .send(Ok((batch.scope.rows.start, batch.array)))
                        .await
                        .is_err()
                    {
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
