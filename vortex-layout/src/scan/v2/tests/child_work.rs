// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use futures::future::BoxFuture;
use vortex_utils::parallelism::get_available_parallelism;

use super::*;
use crate::layouts::list::ListLayout;
use crate::plan::physical::PlanRef;
use crate::plan::physical::pipeline::ChildExecutor;
use crate::plan::physical::pipeline::ChildWork;
use crate::plan::physical::pipeline::ValueCache;

struct CountChildren {
    inner: Arc<dyn ChildExecutor>,
    tasks: Arc<AtomicUsize>,
}

impl ChildExecutor for CountChildren {
    fn execute(
        self: Arc<Self>,
        plan: PlanRef,
        work: Vec<ChildWork>,
        values: ValueCache,
    ) -> BoxFuture<'static, VortexResult<Vec<ArrayRef>>> {
        self.tasks.fetch_add(work.len(), Ordering::Relaxed);
        Arc::clone(&self.inner).execute(plan, work, values)
    }
}

async fn store_array(
    session: &VortexSession,
    segments: &Arc<TestSegments>,
    array: ArrayRef,
    chunk_rows: usize,
) -> VortexResult<LayoutRef> {
    let dtype = array.dtype().clone();
    let (mut id, eof) = SequenceId::root().split();
    let chunks = (0..array.len())
        .step_by(chunk_rows)
        .map(|start| {
            Ok((
                id.advance(),
                array.slice(start..(start + chunk_rows).min(array.len()))?,
            ))
        })
        .collect::<Vec<_>>();
    chunked_flat()
        .write_stream(
            ArrayContext::empty().into(),
            Arc::<TestSegments>::clone(segments),
            SequentialStreamAdapter::new(dtype, stream::iter(chunks)).sendable(),
            eof,
            session,
        )
        .await
}

#[rstest]
#[case::empty(vec![], false)]
#[case::empty_rows(vec![0, 2], false)]
#[case::null_row(vec![3], false)]
#[case::large_row(vec![1], true)]
#[case::mixed(vec![0, 1, 3], true)]
#[case::all(vec![0, 1, 2, 3], true)]
#[tokio::test]
async fn large_list_children_match_input_and_v1(
    #[case] selected: Vec<u64>,
    #[case] parallel: bool,
    #[values(false, true)] nested: bool,
) -> VortexResult<()> {
    let session = new_session().with_tokio();
    let segments = Arc::new(TestSegments::default());
    let primitive = PrimitiveArray::from_option_iter(
        (0_i64..262144).map(|value| (value % 7 != 0).then_some(value)),
    )
    .into_array();
    let mut elements = primitive.clone();
    let mut layout = store_array(&session, &segments, primitive, 8192).await?;
    if nested {
        let offsets = Buffer::from_iter((0_u64..=2048).map(|row| row * 128)).into_array();
        let valid = BoolArray::from_iter((0..2048).map(|row| row % 11 != 0)).into_array();
        elements = ListArray::try_new(elements, offsets.clone(), Validity::Array(valid.clone()))?
            .into_array();
        layout = ListLayout::new(
            elements.dtype().clone(),
            layout,
            store_array(&session, &segments, offsets, 2049).await?,
            Some(store_array(&session, &segments, valid, 2048).await?),
        )
        .into_layout();
    }
    let half = u64::try_from(elements.len())? / 2;
    let offsets = Buffer::from(vec![0_u64, 0, half, half, 2 * half]).into_array();
    let valid = BoolArray::from_iter([true, true, true, false]).into_array();
    let input =
        ListArray::try_new(elements, offsets.clone(), Validity::Array(valid.clone()))?.into_array();
    let layout = ListLayout::new(
        input.dtype().clone(),
        layout,
        store_array(&session, &segments, offsets, 5).await?,
        Some(store_array(&session, &segments, valid, 4).await?),
    )
    .into_layout();
    let segments: Arc<dyn SegmentSource> = segments;
    let make = || {
        Ok(builder(&session, &segments, &layout)?
            .with_row_indices(StrictSortedBuffer::try_new(Buffer::from(selected.clone()))?))
    };
    let mut scan = PreparedScan::try_new(make()?)?;
    let tasks = Arc::new(AtomicUsize::new(0));
    scan.plans.children = scan.plans.children.map(|inner| {
        Arc::new(CountChildren {
            inner,
            tasks: Arc::clone(&tasks),
        }) as Arc<dyn ChildExecutor>
    });
    let actual = collect(input.dtype().clone(), scan.execute_stream()?).await?;
    assert_arrays_eq!(
        actual,
        input.take(Buffer::from(selected.clone()).into_array())?,
        &mut session.create_execution_ctx()
    );
    assert_eq!(
        tasks.load(Ordering::Relaxed) > 1,
        parallel && get_available_parallelism().unwrap_or(1) > 1
    );
    assert_same_as_v1(&session, make).await
}
