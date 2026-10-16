// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::future;
use std::sync::Arc;

use futures::SinkExt;
use futures::StreamExt;
use futures::channel::mpsc;
use futures::stream::BoxStream;
use vortex_array::ArrayRef;
use vortex_error::VortexResult;
use vortex_io::request::IoService;
use vortex_io::runtime::Handle;
use vortex_scan::planning::driver::Progress;
use vortex_scan::planning::driver::Run;
use vortex_scan::planning::planner::Planner;
use vortex_scan::planning::planner::WorkScope;

/// One split of a scan, ready to be admitted to a planning run.
///
/// The split's planner projects the rows of `scope`. Each morsel emits its selected batches
/// in row order, and morsels run in whatever order their reads arrive.
pub(super) struct SplitPlan {
    /// The rows of the scan's root row domain the split covers.
    pub(super) scope: WorkScope,
    /// The split's first planner.
    pub(super) root: Box<dyn Planner>,
}

impl SplitPlan {
    /// Streams a split whose planner emits its batches in row order, without retaining earlier
    /// batches until the split finishes. Sparse projection splits each contain one morsel.
    pub(super) fn stream<A: Send + 'static>(
        self,
        io: Arc<dyn IoService>,
        map: impl Fn(ArrayRef) -> VortexResult<A> + Send + 'static,
    ) -> BoxStream<'static, VortexResult<A>> {
        Box::pin(async_stream::try_stream! {
            let mut run = Run::new();
            run.admit(self.root, self.scope, io.session());
            loop {
                match run.advance()? {
                    Progress::Batch(batch) => yield map(batch.array)?,
                    Progress::RootDone(_) | Progress::Idle => break,
                    Progress::Waiting => future::poll_fn(|cx| run.poll_completion(cx)).await?,
                }
            }
        })
    }

    /// Runs an ordered split on a worker, keeping at most one undelivered mapped batch.
    /// Dropping the stream cancels its worker; worker errors and panics reach the consumer.
    pub(super) fn spawn_stream<A: Send + 'static>(
        self,
        io: Arc<dyn IoService>,
        map: impl Fn(ArrayRef) -> VortexResult<A> + Send + 'static,
        handle: &Handle,
    ) -> BoxStream<'static, VortexResult<A>> {
        // Each futures sender has one reserved slot. With no additional capacity, send waits
        // for the consumer before this task decodes another batch that may retain segment bytes.
        let (mut sender, mut receiver) = mpsc::channel(0);
        let task = handle.spawn(async move {
            let mut stream = self.stream(io, map);
            while let Some(batch) = stream.next().await {
                if sender.send(batch?).await.is_err() {
                    break;
                }
            }
            VortexResult::Ok(())
        });
        Box::pin(async_stream::try_stream! {
            while let Some(batch) = receiver.next().await {
                yield batch;
            }
            task.await?;
        })
    }

    /// Runs the split on a planning run of its own, reading through a session of `io`, and
    /// maps each batch as it arrives before returning the results in row order. Mapping here
    /// lets a decoder release the batch's compressed buffers while later batches still read.
    ///
    /// The run advances inside this future, on whichever thread polls it, and the future awaits
    /// its reads between steps. Nothing is handed to another thread.
    pub(super) async fn run<A>(
        self,
        io: Arc<dyn IoService>,
        map: impl Fn(ArrayRef) -> VortexResult<A>,
    ) -> VortexResult<Vec<A>> {
        let mut run = Run::new();
        run.admit(self.root, self.scope, io.session());
        let mut batches = Vec::new();
        loop {
            match run.advance()? {
                Progress::Batch(batch) => {
                    batches.push((batch.scope.rows.start, map(batch.array)?));
                }
                Progress::RootDone(_) | Progress::Idle => break,
                Progress::Waiting => future::poll_fn(|cx| run.poll_completion(cx)).await?,
            }
        }
        // Morsels finish in whatever order their reads arrive.
        batches.sort_by_key(|(start, _)| *start);
        Ok(batches.into_iter().map(|(_, batch)| batch).collect())
    }
}
