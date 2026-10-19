// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::collections::VecDeque;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;
use std::time::Instant;

use futures::executor::block_on;
use futures::task::AtomicWaker;
use parking_lot::Mutex as SyncMutex;
use vortex_array::ArrayContext;
use vortex_array::arrays::ChunkedArray;
use vortex_array::assert_arrays_eq;
use vortex_array::buffer::BufferHandle;
use vortex_array::dtype::PType;
use vortex_array::serde::SerializeOptions;
use vortex_buffer::Alignment;
use vortex_buffer::Buffer;
use vortex_buffer::ByteBufferMut;
use vortex_error::vortex_err;
use vortex_io::request::Completion;
use vortex_io::request::IoBatch;
use vortex_io::request::IoIntent;
use vortex_io::request::IoOwnerId;
use vortex_io::request::IoResult;
use vortex_io::request::IoSource;
use vortex_io::request::IoTarget;
use vortex_session::registry::ReadContext;

use super::*;
use crate::plan::physical::ConcatPlan;
use crate::plan::physical::SegmentScanPlan;
use crate::plan::physical::pipeline::Split;
use crate::segments::SegmentId;
use crate::test::new_session;

#[test]
fn byte_budget_prevents_over_admission_and_cancellation_releases_partial_acquisitions()
-> VortexResult<()> {
    let budget = ByteBudget::default();
    let held = block_on(budget.acquire(BUDGET_BYTES * 3 / 4))?;
    let mut waiting = Box::pin(budget.acquire(BUDGET_BYTES / 2));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(waiting.poll_unpin(&mut cx).is_pending());
    // The waiting acquisition has taken the remaining quarter. Cancelling it must return
    // those permits and the acquisition lock, even while the first job is still running.
    drop(waiting);
    let mut quarter = Box::pin(budget.acquire(BUDGET_BYTES / 4));
    let Poll::Ready(permits) = quarter.poll_unpin(&mut cx) else {
        panic!("cancelled admission leaked permits")
    };
    let quarter = permits?;
    let mut extra = Box::pin(budget.acquire(1));
    assert!(extra.poll_unpin(&mut cx).is_pending());
    drop(quarter);
    let Poll::Ready(permits) = extra.poll_unpin(&mut cx) else {
        panic!("released permits did not wake admission")
    };
    drop(permits?);
    drop(held);
    let mut whole = Box::pin(budget.acquire(BUDGET_BYTES));
    assert!(matches!(whole.poll_unpin(&mut cx), Poll::Ready(Ok(_))));
    Ok(())
}

#[test]
fn admission_rejects_zero_and_oversized_work() {
    let budget = ByteBudget::default();
    assert!(block_on(budget.acquire(0)).is_err());
    assert!(block_on(budget.acquire(BUDGET_BYTES + 1)).is_err());
}

#[derive(Default)]
struct Gates {
    sources: SyncMutex<Vec<Arc<Gate>>>,
    bytes: Vec<BufferHandle>,
}

impl IoService for Gates {
    fn session(&self) -> Arc<dyn IoSource> {
        let gate = Arc::new(Gate {
            bytes: self.bytes.clone(),
            ready: AtomicBool::new(false),
            fail: AtomicBool::new(false),
            requests: SyncMutex::new(VecDeque::new()),
            wake: AtomicWaker::new(),
            cleared: AtomicUsize::new(0),
        });
        self.sources.lock().push(Arc::clone(&gate));
        gate
    }
}

struct Gate {
    bytes: Vec<BufferHandle>,
    ready: AtomicBool,
    fail: AtomicBool,
    requests: SyncMutex<VecDeque<Completion>>,
    wake: AtomicWaker,
    cleared: AtomicUsize,
}

impl Gate {
    fn open(&self) {
        self.ready.store(true, Ordering::Release);
        self.wake.wake();
    }
}

impl IoSource for Gate {
    fn submit(&self, owner: IoOwnerId, batch: IoBatch) -> VortexResult<()> {
        for request in batch
            .into_iter()
            .filter(|request| request.intent == IoIntent::Fetch)
        {
            let IoTarget::Range { offset, .. } = request.target else {
                return Err(vortex_err!("unexpected size request"));
            };
            self.requests.lock().push_back(Completion {
                owner,
                request: request.request,
                result: Ok(IoResult::Bytes(
                    self.bytes[usize::try_from(offset)?].clone(),
                )),
            });
        }
        Ok(())
    }

    fn poll(&self) -> VortexResult<Option<Completion>> {
        if self.fail.load(Ordering::Acquire) {
            return Err(vortex_err!("injected child IO failure"));
        }
        Ok(if self.ready.load(Ordering::Acquire) {
            self.requests.lock().pop_front()
        } else {
            None
        })
    }

    fn poll_completion(&self, cx: &mut Context<'_>) -> Poll<VortexResult<Completion>> {
        self.wake.register(cx.waker());
        match self.poll() {
            Ok(Some(completion)) => Poll::Ready(Ok(completion)),
            Ok(None) => Poll::Pending,
            Err(error) => Poll::Ready(Err(error)),
        }
    }

    fn wait(&self) -> VortexResult<Completion> {
        Err(vortex_err!("child executor must not block on IO"))
    }
    fn release(&self, owner: IoOwnerId) {
        self.requests
            .lock()
            .retain(|request| request.owner != owner);
    }
    fn clear(&self) {
        self.requests.lock().clear();
        self.cleared.fetch_add(1, Ordering::Relaxed);
    }
}

type Fixture = (Arc<Children>, Arc<Gates>, PlanRef, Vec<ChildWork>);

#[test]
fn materialization_preserves_chunk_buffers_instead_of_concatenating_twice() -> VortexResult<()> {
    let session = new_session();
    let chunks = [
        PrimitiveArray::from_option_iter([Some(1_i64), None]),
        PrimitiveArray::from_option_iter([None, Some(4_i64)]),
    ];
    let array = ChunkedArray::try_new(
        chunks.iter().cloned().map(IntoArray::into_array),
        chunks[0].dtype().clone(),
    )?
    .into_array();
    let mut output = Vec::new();
    materialize(array, &mut session.create_execution_ctx(), &mut output)?;
    assert_eq!(output.len(), chunks.len());
    for (actual, expected) in output.into_iter().zip(chunks) {
        let actual = actual.execute::<PrimitiveArray>(&mut session.create_execution_ctx())?;
        assert_eq!(
            actual.as_slice::<i64>().as_ptr(),
            expected.as_slice::<i64>().as_ptr()
        );
        assert_arrays_eq!(actual, expected, &mut session.create_execution_ctx());
    }
    Ok(())
}

fn fixture() -> VortexResult<Fixture> {
    let session = new_session().with_tokio();
    let mut service = Gates::default();
    let mut plans = Vec::new();
    for id in 0_u32..3 {
        let array = Buffer::from_iter(u64::from(id) * 8..u64::from(id + 1) * 8).into_array();
        let ctx = ArrayContext::empty();
        let buffers = array.serialize(
            &ctx,
            &session,
            &SerializeOptions {
                offset: 0,
                include_padding: true,
            },
        )?;
        let mut bytes = ByteBufferMut::empty_aligned(Alignment::new(64));
        for buffer in buffers {
            bytes.extend_from_slice(buffer.as_ref());
        }
        service.bytes.push(BufferHandle::new_host(bytes.freeze()));
        plans.push(
            SegmentScanPlan::new(
                array.dtype().clone(),
                8,
                SegmentId::from(id),
                ReadContext::new(ctx.to_ids()),
                None,
            )
            .into_plan(),
        );
    }
    let service = Arc::new(service);
    let children = Arc::new(Children {
        session,
        io: Arc::<Gates>::clone(&service),
        workers: 4,
        budget: OnceLock::new(),
        admitted: Default::default(),
        locations: (0..3)
            .map(|offset| SegmentLocation {
                offset,
                length: 0,
                alignment: Alignment::new(64),
            })
            .collect(),
    });
    let plan = ConcatPlan::try_new(DType::Primitive(PType::U64, false.into()), plans)?.into_plan();
    // Inflate the estimate to exercise admission without a 48 MiB test fixture.
    let work = (0..3)
        .map(|id| ChildWork {
            split: Split::all(id * 8..(id + 1) * 8),
            bytes: BUDGET_BYTES / 2,
        })
        .collect();
    Ok((children, service, plan, work))
}

async fn until(mut condition: impl FnMut() -> bool) {
    let start = Instant::now();
    while !condition() {
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "child execution did not make progress"
        );
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn independent_children_respect_the_shared_budget_and_return_in_order() -> VortexResult<()> {
    let (children, service, plan, work) = fixture()?;
    let task = tokio::spawn(Arc::clone(&children).execute(plan, work, ValueCache::default()));
    until(|| service.sources.lock().len() == 2).await;
    let first = Arc::clone(&service.sources.lock()[0]);
    let second = Arc::clone(&service.sources.lock()[1]);
    assert!(!task.is_finished());
    assert_eq!(first.requests.lock().len(), 1);
    assert_eq!(second.requests.lock().len(), 1);
    // The later job releases capacity while the first job is still held. The third job
    // must start without losing output order or exceeding the two-job byte budget.
    second.open();
    until(|| service.sources.lock().len() == 3).await;
    assert!(!first.ready.load(Ordering::Relaxed));
    service.sources.lock()[2].open();
    first.open();
    let arrays = task.await.expect("child task panicked")?;
    assert_eq!(arrays.len(), 3);
    for (index, array) in arrays.into_iter().enumerate() {
        let start = u64::try_from(index)? * 8;
        assert_arrays_eq!(
            array,
            Buffer::from_iter(start..start + 8).into_array(),
            &mut children.session.create_execution_ctx()
        );
    }
    assert!(
        service
            .sources
            .lock()
            .iter()
            .all(|source| source.cleared.load(Ordering::Relaxed) == 1)
    );
    Ok(())
}

#[rstest::rstest]
#[tokio::test]
async fn cancelled_or_failed_parent_releases_child_io_and_budget(
    #[values(false, true)] failure: bool,
) -> VortexResult<()> {
    let (children, service, plan, work) = fixture()?;
    let task = tokio::spawn(Arc::clone(&children).execute(plan, work, ValueCache::default()));
    until(|| service.sources.lock().len() == 2).await;
    if failure {
        let first = Arc::clone(&service.sources.lock()[0]);
        first.fail.store(true, Ordering::Release);
        first.open();
        assert!(
            task.await
                .expect("task panicked")
                .unwrap_err()
                .to_string()
                .contains("injected child IO failure")
        );
    } else {
        task.abort();
        assert!(
            task.await
                .expect_err("task should be cancelled")
                .is_cancelled()
        );
    }
    until(|| {
        service
            .sources
            .lock()
            .iter()
            .all(|source| source.cleared.load(Ordering::Relaxed) == 1)
    })
    .await;
    assert!(
        service
            .sources
            .lock()
            .iter()
            .all(|source| source.requests.lock().is_empty())
    );
    let mut all = Box::pin(
        children
            .budget
            .get()
            .expect("budget initialized")
            .acquire(BUDGET_BYTES),
    );
    assert!(matches!(
        all.poll_unpin(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Ok(_))
    ));
    Ok(())
}
