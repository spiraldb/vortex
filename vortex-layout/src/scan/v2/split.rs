// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::future;
use std::sync::Arc;
use std::task::Poll;

use futures::FutureExt;
use futures::SinkExt;
use futures::StreamExt;
use futures::channel::mpsc;
use futures::future::Either;
use futures::stream::FuturesUnordered;
use vortex_array::ArrayRef;
use vortex_error::VortexResult;
use vortex_io::request::IoService;
use vortex_io::runtime::Handle;
use vortex_scan::planning::driver::Progress;
use vortex_scan::planning::driver::Run;
use vortex_scan::planning::planner::Planner;
use vortex_scan::planning::planner::WorkScope;

/// Batches of one split mapped at once. Two keep the next batch converting while the consumer
/// takes the last, without decoding so far ahead that batches leave the cache before they are
/// read.
const MAX_MAPPING: usize = 2;

/// One split of a scan, ready to be admitted to a planning run.
///
/// The split's planner projects the rows of `scope`, and its morsels emit one batch per
/// projection split with selected rows, in whatever order their reads arrive.
pub(super) struct SplitPlan {
    /// The rows of the scan's root row domain the split covers.
    pub(super) scope: WorkScope,
    /// The split's first planner.
    pub(super) root: Box<dyn Planner>,
}

impl SplitPlan {
    /// Publishes mapped batches as they finish, bounding how far projection runs ahead of its
    /// consumer. Unordered scans do not need to retain the whole split to sort its output.
    ///
    /// Each batch is mapped on a task of its own, so decoding and converting a split's morsels
    /// spreads over the runtime's workers while the run plans the next morsel, and a large split
    /// does not finish on one thread after the others have gone idle.
    pub(super) async fn run_unordered<A: Send + 'static>(
        self,
        io: Arc<dyn IoService>,
        map: Arc<dyn Fn(ArrayRef) -> VortexResult<A> + Send + Sync>,
        handle: Handle,
        mut send: mpsc::Sender<VortexResult<A>>,
    ) -> VortexResult<()> {
        let mut run = Run::new();
        run.admit(self.root, self.scope, io.session());
        let mut mapping = FuturesUnordered::new();
        let mut planned = false;
        loop {
            while let Some(Some(batch)) = mapping.next().now_or_never() {
                if !forward(&mut send, batch).await {
                    return Ok(());
                }
            }
            if planned || mapping.len() >= MAX_MAPPING {
                let Some(batch) = mapping.next().await else {
                    return Ok(());
                };
                if !forward(&mut send, batch).await {
                    return Ok(());
                }
                continue;
            }
            match run.advance()? {
                Progress::Batch(batch) => {
                    let map = Arc::clone(&map);
                    mapping.push(handle.spawn_cpu(move || map(batch.array)));
                }
                Progress::RootDone(_) | Progress::Idle => planned = true,
                // Hand on mapped batches while the run waits for its reads.
                Progress::Waiting => loop {
                    let next = future::poll_fn(|cx| match mapping.poll_next_unpin(cx) {
                        Poll::Ready(Some(batch)) => Poll::Ready(Either::Left(batch)),
                        _ => run.poll_completion(cx).map(Either::Right),
                    })
                    .await;
                    match next {
                        Either::Left(batch) => {
                            if !forward(&mut send, batch).await {
                                return Ok(());
                            }
                        }
                        Either::Right(read) => {
                            read?;
                            break;
                        }
                    }
                },
            }
        }
    }

    /// Runs the split on a planning run of its own, reading through a session of `io`, and
    /// returns its batches in row order.
    ///
    /// The run advances inside this future, on whichever thread polls it, and the future awaits
    /// its reads between steps. Nothing is handed to another thread.
    pub(super) async fn run(self, io: Arc<dyn IoService>) -> VortexResult<Vec<ArrayRef>> {
        let mut run = Run::new();
        run.admit(self.root, self.scope, io.session());
        let mut batches = Vec::new();
        loop {
            match run.advance()? {
                Progress::Batch(batch) => batches.push(batch),
                Progress::RootDone(_) | Progress::Idle => break,
                Progress::Waiting => future::poll_fn(|cx| run.poll_completion(cx)).await?,
            }
        }
        // Morsels finish in whatever order their reads arrive.
        batches.sort_by_key(|batch| batch.scope.rows.start);
        Ok(batches.into_iter().map(|batch| batch.array).collect())
    }
}

/// Sends a mapped batch, returning whether the scan should go on: the consumer is still there
/// and the batch is not an error.
async fn forward<A>(send: &mut mpsc::Sender<VortexResult<A>>, batch: VortexResult<A>) -> bool {
    let failed = batch.is_err();
    send.send(batch).await.is_ok() && !failed
}
