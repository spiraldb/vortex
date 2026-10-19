// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Query-wide admission and execution of large child projections.
//!
//! Permits cover the estimated decoded bytes of whole touched chunks until their task completes.
//! Retained output, encoded buffers and allocator overhead are outside this estimate; this is not
//! a limit on the query's total heap. Cancellation drops child handles and their IO sessions.

use std::future::poll_fn;
use std::sync::Arc;
use std::sync::OnceLock;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
#[cfg(test)]
use std::sync::atomic::Ordering;

use async_lock::Mutex;
use async_lock::Semaphore;
use async_lock::SemaphoreGuardArc;
use futures::FutureExt;
use futures::StreamExt;
use futures::TryStreamExt;
use futures::future::BoxFuture;
use futures::stream;
use vortex_array::ArrayRef;
use vortex_array::ExecutionCtx;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::BoolArray;
use vortex_array::arrays::Chunked;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::chunked::ChunkedArrayExt;
use vortex_array::dtype::DType;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_ensure;
use vortex_io::request::IoService;
use vortex_io::session::RuntimeSessionExt;
use vortex_scan::planning::driver::Progress;
use vortex_scan::planning::driver::Run;
use vortex_scan::planning::planner::WorkScope;
use vortex_scan::planning::selection::SelectedRows;
use vortex_session::VortexSession;
use vortex_utils::parallelism::get_available_parallelism;

use super::ScanPlans;
use super::SegmentLocation;
use super::projection::ProjectionPlanner;
use crate::plan::physical::PlanRef;
use crate::plan::physical::pipeline::Split;
use crate::plan::physical::pipeline::ValueCache;

const BUDGET_BYTES: u64 = 32 * 1024 * 1024;
const PERMIT_BYTES: u64 = 256 * 1024;

pub(super) mod splits;

#[cfg(test)]
mod tests;

#[derive(Debug)]
pub(super) struct ChildWork {
    pub(super) split: Split,
    /// Estimated bytes of whole touched chunks, including validity.
    pub(super) bytes: u64,
}

struct ByteBudget {
    permits: Arc<Semaphore>,
    acquisition: Mutex<()>,
}

impl Default for ByteBudget {
    fn default() -> Self {
        Self {
            permits: Arc::new(Semaphore::new((BUDGET_BYTES / PERMIT_BYTES) as usize)),
            acquisition: Mutex::new(()),
        }
    }
}

impl ByteBudget {
    async fn acquire(&self, bytes: u64) -> VortexResult<Vec<SemaphoreGuardArc>> {
        vortex_ensure!(
            bytes > 0 && bytes <= BUDGET_BYTES,
            "Child work exceeds its admission budget"
        );
        // Acquire a whole weight before admitting another claimant, so partial acquisitions
        // cannot fill the budget and deadlock one another. Completed tasks release independently.
        let _acquisition = self.acquisition.lock().await;
        let count = usize::try_from(bytes.div_ceil(PERMIT_BYTES))?;
        let mut permits = Vec::with_capacity(count);
        for _ in 0..count {
            permits.push(self.permits.acquire_arc().await);
        }
        Ok(permits)
    }
}

pub(in crate::scan) struct Children {
    session: VortexSession,
    locations: Arc<[SegmentLocation]>,
    io: Arc<dyn IoService>,
    workers: usize,
    budget: OnceLock<ByteBudget>,
    #[cfg(test)]
    admitted: AtomicUsize,
}

pub(in crate::scan) fn child_executor(
    session: VortexSession,
    locations: Arc<[SegmentLocation]>,
    io: Arc<dyn IoService>,
) -> Option<Arc<Children>> {
    let workers = get_available_parallelism().unwrap_or(1);
    (workers > 1).then(|| {
        Arc::new(Children {
            session,
            locations,
            io,
            workers,
            budget: OnceLock::new(),
            #[cfg(test)]
            admitted: Default::default(),
        })
    })
}

impl Children {
    #[cfg(test)]
    pub(in crate::scan) fn admitted(&self) -> usize {
        self.admitted.load(Ordering::Relaxed)
    }

    pub(super) fn execute(
        self: Arc<Self>,
        plan: PlanRef,
        work: Vec<ChildWork>,
        values: ValueCache,
    ) -> BoxFuture<'static, VortexResult<Vec<ArrayRef>>> {
        async move {
            let workers = self.workers;
            stream::iter(work)
                .map(|work| {
                    let children = Arc::clone(&self);
                    let plan = plan.clone();
                    let values = values.clone();
                    async move {
                        let permits = children
                            .budget
                            .get_or_init(ByteBudget::default)
                            .acquire(work.bytes)
                            .await?;
                        #[cfg(test)]
                        children.admitted.fetch_add(1, Ordering::Relaxed);
                        let handle = children.session.handle();
                        handle
                            .spawn(async move {
                                let _permits = permits;
                                let scope = WorkScope {
                                    file_ordinal: 0,
                                    rows: work.split.rows,
                                };
                                let plans = ScanPlans {
                                    session: children.session.clone(),
                                    locations: Arc::clone(&children.locations),
                                    projection: plan,
                                    row_offset: 0,
                                    values,
                                    // Eligible child plans contain only fixed-width segment leaves.
                                    // They cannot recursively wait for this same byte budget.
                                    children: None,
                                };
                                let root = ProjectionPlanner::new(
                                    plans,
                                    SelectedRows {
                                        scope: scope.clone(),
                                        mask: work.split.mask,
                                    },
                                );
                                let mut run = Run::new();
                                run.admit(Box::new(root), scope, children.io.session());
                                let mut result = Vec::new();
                                let mut exec = children.session.create_execution_ctx();
                                loop {
                                    match run.advance()? {
                                        Progress::Batch(batch) => {
                                            materialize(batch.array, &mut exec, &mut result)?;
                                        }
                                        Progress::RootDone(_) | Progress::Idle => break,
                                        Progress::Waiting => {
                                            poll_fn(|cx| run.poll_completion(cx)).await?
                                        }
                                    }
                                }
                                Ok::<_, vortex_error::VortexError>(result)
                            })
                            .await
                    }
                })
                .buffered(workers)
                .try_collect::<Vec<Vec<ArrayRef>>>()
                .await
                .map(|arrays| arrays.into_iter().flatten().collect())
        }
        .boxed()
    }
}

/// Decode on the child task, preserving chunk boundaries so the parent need only concatenate
/// once. Canonicalizing the whole job would copy its chunks into an intermediate buffer first.
fn materialize(
    array: ArrayRef,
    exec: &mut ExecutionCtx,
    output: &mut Vec<ArrayRef>,
) -> VortexResult<()> {
    if let Some(chunks) = array.as_opt::<Chunked>() {
        for chunk in chunks.iter_chunks() {
            materialize(chunk.clone(), exec, output)?;
        }
        return Ok(());
    }
    output.push(match array.dtype() {
        DType::Primitive(..) => array.execute::<PrimitiveArray>(exec)?.into_array(),
        DType::Bool(..) => array.execute::<BoolArray>(exec)?.into_array(),
        _ => vortex_bail!("Child projection is not fixed-width"),
    });
    Ok(())
}
