// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::future;
use std::sync::Arc;

use futures::StreamExt;
use futures::TryStreamExt;
use futures::stream;
use futures::stream::BoxStream;
use vortex_array::ArrayRef;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_io::request::IoService;
use vortex_io::session::RuntimeSessionExt;
use vortex_scan::planning::driver::Progress;
use vortex_scan::planning::driver::Run;
use vortex_scan::planning::planner::Planner;
use vortex_scan::planning::planner::PlannerOutput;
use vortex_scan::planning::planner::State;
use vortex_scan::planning::planner::WorkScope;
use vortex_session::VortexSession;
use vortex_utils::parallelism::get_available_parallelism;

use super::FilePlanner;

/// Executes the file planner's children with bounded concurrency and optional file ordering.
pub(in crate::scan) fn execute_file<A: 'static + Send>(
    planner: FilePlanner,
    session: VortexSession,
    io: Arc<dyn IoService>,
    map_fn: Arc<dyn Fn(ArrayRef) -> VortexResult<A> + Send + Sync>,
    ordered: bool,
    concurrency: usize,
) -> BoxStream<'static, VortexResult<A>> {
    let concurrency = concurrency * get_available_parallelism().unwrap_or(1);
    let handle = session.handle();
    let children = stream::try_unfold(planner, |mut planner| async move {
        while planner.state() != State::Done {
            match planner.compute()? {
                PlannerOutput::Planner(scope, child) => {
                    return Ok(Some(((scope, child), planner)));
                }
                PlannerOutput::Continue => {}
                PlannerOutput::Done => return Ok(None),
                _ => vortex_bail!("file planner must emit child planners"),
            }
        }
        Ok(None)
    });
    // Each child has its own Run so its CPU work can execute on a different runtime thread.
    // Buffering also bounds how far the file planner advances ahead of consumed output.
    let tasks = children.map_ok(move |(scope, child)| {
        let io = Arc::clone(&io);
        let map_fn = Arc::clone(&map_fn);
        handle.spawn(async move {
            run_planner(child, scope, io)
                .await?
                .into_iter()
                .map(|array| map_fn(array))
                .collect::<VortexResult<Vec<_>>>()
        })
    });
    let batches = if ordered {
        tasks.try_buffered(concurrency).boxed()
    } else {
        tasks.try_buffer_unordered(concurrency).boxed()
    };
    batches
        .map_ok(|batches| stream::iter(batches.into_iter().map(Ok)))
        .try_flatten()
        .boxed()
}

async fn run_planner(
    planner: Box<dyn Planner>,
    scope: WorkScope,
    io: Arc<dyn IoService>,
) -> VortexResult<Vec<ArrayRef>> {
    let mut run = Run::new();
    run.admit(planner, scope, io.session());
    let mut batches = Vec::new();
    loop {
        match run.advance()? {
            Progress::Batch(batch) => batches.push(batch),
            Progress::RootDone(_) | Progress::Idle => break,
            Progress::Waiting => future::poll_fn(|cx| run.poll_completion(cx)).await?,
        }
    }
    batches.sort_by_key(|batch| batch.scope.rows.start);
    Ok(batches.into_iter().map(|batch| batch.array).collect())
}
