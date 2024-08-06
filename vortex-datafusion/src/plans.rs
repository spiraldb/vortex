//! Physical operators needed to implement scanning of Vortex arrays with pushdown.

use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_array::cast::AsArray;
use arrow_array::types::UInt64Type;
use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion_common::Result as DFResult;
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion_expr::Expr;
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, PlanProperties,
};
use futures::{ready, Stream};
use lazy_static::lazy_static;
use pin_project::pin_project;
use vortex::array::ChunkedArray;
use vortex::arrow::FromArrowArray;
use vortex::compute::take;
use vortex::{ArrayDType, ArrayData, IntoArray, IntoArrayVariant, IntoCanonical};

use crate::datatype::infer_schema;
use crate::eval::ExpressionEvaluator;
use crate::expr::{make_conjunction, simplify_expr};

/// Physical plan operator that applies a set of [filters][Expr] against the input, producing a
/// row mask that can be used downstream to force a take against the corresponding struct array
/// chunks but for different columns.
pub(crate) struct RowSelectorExec {
    filter_exprs: Vec<Expr>,

    filter_projection: Vec<usize>,

    // cached PlanProperties object. We do not make use of this.
    cached_plan_props: PlanProperties,

    // Full array. We only access partitions of this data.
    chunked_array: ChunkedArray,
}

lazy_static! {
    static ref ROW_SELECTOR_SCHEMA_REF: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        "row_idx",
        DataType::UInt64,
        false
    )]));
}

impl RowSelectorExec {
    pub(crate) fn new(
        filter_exprs: &[Expr],
        filter_projection: Vec<usize>,
        chunked_array: &ChunkedArray,
    ) -> Self {
        let cached_plan_props = PlanProperties::new(
            EquivalenceProperties::new(ROW_SELECTOR_SCHEMA_REF.clone()),
            Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        );

        Self {
            filter_exprs: filter_exprs.to_owned(),
            filter_projection: filter_projection.clone(),
            chunked_array: chunked_array.clone(),
            cached_plan_props,
        }
    }
}

impl Debug for RowSelectorExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RowSelectorExec")
            .field("filter_exprs", &self.filter_exprs)
            .finish()
    }
}

impl DisplayAs for RowSelectorExec {
    fn fmt_as(
        &self,
        _display_format_type: DisplayFormatType,
        f: &mut Formatter,
    ) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl ExecutionPlan for RowSelectorExec {
    fn name(&self) -> &str {
        RowSelectorExec::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cached_plan_props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // No children
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        assert_eq!(
            partition, 0,
            "single partitioning only supported by RowSelectorExec"
        );

        // Derive a schema using the provided set of fields.

        let filter_schema = Arc::new(
            infer_schema(self.chunked_array.dtype())
                .project(self.filter_projection.as_slice())
                .unwrap(),
        );

        let conjunction_expr =
            simplify_expr(&make_conjunction(&self.filter_exprs)?, filter_schema)?;

        Ok(Box::pin(RowIndicesStream {
            chunked_array: self.chunked_array.clone(),
            chunk_idx: 0,
            filter_projection: self.filter_projection.clone(),
            conjunction_expr,
        }))
    }
}

/// [RecordBatchStream] of row indices, emitted by the [RowSelectorExec] physical plan node.
pub(crate) struct RowIndicesStream {
    chunked_array: ChunkedArray,
    chunk_idx: usize,
    conjunction_expr: Expr,
    filter_projection: Vec<usize>,
}

impl Stream for RowIndicesStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if this.chunk_idx >= this.chunked_array.nchunks() {
            return Poll::Ready(None);
        }

        let next_chunk = this
            .chunked_array
            .chunk(this.chunk_idx)
            .expect("chunk index in-bounds");
        this.chunk_idx += 1;

        // Get the unfiltered record batch.
        // Since this is a one-shot, we only want to poll the inner future once, to create the
        // initial batch for us to process.
        let vortex_struct = next_chunk
            .into_struct()
            .expect("chunks must be StructArray")
            .project(this.filter_projection.as_slice())
            .expect("projection should succeed");

        // TODO(adamg): Filter on vortex arrays
        let array =
            ExpressionEvaluator::eval(vortex_struct.into_array(), &this.conjunction_expr).unwrap();
        let selection = array.into_canonical().unwrap().into_arrow();

        // Convert the `selection` BooleanArray into a UInt64Array of indices.
        let selection_indices = selection
            .as_boolean()
            .values()
            .set_indices()
            .map(|idx| idx as u64);

        let indices = Arc::new(UInt64Array::from_iter_values(selection_indices)) as ArrayRef;
        let indices_batch = RecordBatch::try_new(ROW_SELECTOR_SCHEMA_REF.clone(), vec![indices])?;

        Poll::Ready(Some(Ok(indices_batch)))
    }
}

impl RecordBatchStream for RowIndicesStream {
    fn schema(&self) -> SchemaRef {
        ROW_SELECTOR_SCHEMA_REF.clone()
    }
}

/// Physical that receives a stream of row indices from a child operator, and uses that to perform
/// a `take` operation on tha backing Vortex array.
pub(crate) struct TakeRowsExec {
    plan_properties: PlanProperties,

    // Array storing the indices used to take the plan nodes.
    projection: Vec<usize>,

    // Input plan, a stream of indices on which we perform a take against the original dataset.
    input: Arc<dyn ExecutionPlan>,

    output_schema: SchemaRef,

    // The original Vortex array holding the fields we have not decoded yet.
    table: ChunkedArray,
}

impl TakeRowsExec {
    pub(crate) fn new(
        schema_ref: SchemaRef,
        projection: &[usize],
        row_indices: Arc<dyn ExecutionPlan>,
        table: &ChunkedArray,
    ) -> Self {
        let output_schema = Arc::new(schema_ref.project(projection).unwrap());
        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        );

        Self {
            plan_properties,
            projection: projection.to_owned(),
            input: row_indices,
            output_schema: output_schema.clone(),
            table: table.clone(),
        }
    }
}

impl Debug for TakeRowsExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TakeRowsExec")
            .field("projection", &self.projection)
            .field("output_schema", &self.output_schema)
            .finish()
    }
}

impl DisplayAs for TakeRowsExec {
    fn fmt_as(&self, _display_type: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl ExecutionPlan for TakeRowsExec {
    fn name(&self) -> &str {
        TakeRowsExec::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        // Get the row indices for the given chunk.
        let row_indices_stream = self.input.execute(partition, context)?;

        Ok(Box::pin(TakeRowsStream {
            row_indices_stream,
            chunk_idx: 0,
            output_projection: self.projection.clone(),
            output_schema: self.output_schema.clone(),
            vortex_array: self.table.clone(),
        }))
    }
}

/// Stream of outputs emitted by the [TakeRowsExec] physical operator.
#[pin_project]
pub(crate) struct TakeRowsStream<F> {
    // Stream of row indices arriving from upstream operator.
    #[pin]
    row_indices_stream: F,

    // The current chunk. Every time we receive a new RecordBatch from the upstream operator
    // we treat it as a set of row-indices that are zero-indexed relative to this chunk number
    // in the `vortex_array`.
    chunk_idx: usize,

    // Projection based on the schema here
    output_projection: Vec<usize>,
    output_schema: SchemaRef,

    // The original Vortex array we're taking from
    vortex_array: ChunkedArray,
}

impl<F> Stream for TakeRowsStream<F>
where
    F: Stream<Item = DFResult<RecordBatch>>,
{
    type Item = DFResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        // Get the indices provided by the upstream operator.
        let record_batch = match ready!(this.row_indices_stream.poll_next(cx)) {
            None => {
                // Row indices stream is complete, we are also complete.
                return Poll::Ready(None);
            }
            Some(result) => result?,
        };

        assert!(
            *this.chunk_idx <= this.vortex_array.nchunks(),
            "input yielded too many RecordBatches"
        );

        let row_indices =
            ArrayData::from_arrow(record_batch.column(0).as_primitive::<UInt64Type>(), false)
                .into_array();

        // If no columns in the output projection, we send back a RecordBatch with empty schema.
        // This is common for COUNT queries.
        if this.output_projection.is_empty() {
            let opts = RecordBatchOptions::new().with_row_count(Some(row_indices.len()));
            return Poll::Ready(Some(Ok(RecordBatch::try_new_with_options(
                Arc::new(Schema::empty()),
                vec![],
                &opts,
            )
            .unwrap())));
        }

        let chunk = this
            .vortex_array
            .chunk(*this.chunk_idx)
            .expect("streamed too many chunks")
            .into_struct()
            .expect("chunks must be struct-encoded");

        *this.chunk_idx += 1;

        // TODO(aduffy): this re-decodes the fields from the filter schema, which is wasteful.
        //  We should find a way to avoid decoding the filter columns and only decode the other
        //  columns, then stitch the StructArray back together from those.
        let projected_for_output = chunk.project(this.output_projection).unwrap();
        let decoded = take(&projected_for_output.into_array(), &row_indices)
            .expect("take")
            .into_canonical()
            .expect("into_canonical")
            .into_arrow();

        // Send back a single record batch of the decoded data.
        let output_batch = RecordBatch::from(decoded.as_struct());

        Poll::Ready(Some(Ok(output_batch)))
    }
}

impl<F> RecordBatchStream for TakeRowsStream<F>
where
    F: Stream<Item = DFResult<RecordBatch>>,
{
    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow_array::{RecordBatch, UInt64Array};
    use datafusion_expr::{and, col, lit};
    use itertools::Itertools;
    use vortex::array::StructArray;
    use vortex::array::{BoolArray, ChunkedArray, PrimitiveArray};
    use vortex::validity::Validity;
    use vortex::{ArrayDType, IntoArray};
    use vortex_dtype::FieldName;

    use crate::plans::{RowIndicesStream, ROW_SELECTOR_SCHEMA_REF};

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn test_filtering_stream() {
        let chunk = StructArray::try_new(
            Arc::new([FieldName::from("a"), FieldName::from("b")]),
            vec![
                PrimitiveArray::from(vec![0u64, 1, 2]).into_array(),
                BoolArray::from(vec![false, false, true]).into_array(),
            ],
            3,
            Validity::NonNullable,
        )
        .unwrap()
        .into_array();

        let dtype = chunk.dtype().clone();
        let chunked_array =
            ChunkedArray::try_new(vec![chunk.clone(), chunk.clone()], dtype).unwrap();

        let filtering_stream = RowIndicesStream {
            chunked_array: chunked_array.clone(),
            chunk_idx: 0,
            conjunction_expr: and((col("a")).eq(lit(2u64)), col("b").eq(lit(true))),
            filter_projection: vec![0, 1],
        };

        let rows: Vec<RecordBatch> = futures::executor::block_on_stream(filtering_stream)
            .try_collect()
            .unwrap();

        assert_eq!(rows.len(), 2);

        // The output of row selection is a RecordBatch of indices that can be used as selectors
        // against the original RecordBatch.
        assert_eq!(
            rows[0],
            RecordBatch::try_new(
                ROW_SELECTOR_SCHEMA_REF.clone(),
                vec![Arc::new(UInt64Array::from(vec![2u64])),]
            )
            .unwrap()
        );

        assert_eq!(
            rows[1],
            RecordBatch::try_new(
                ROW_SELECTOR_SCHEMA_REF.clone(),
                vec![Arc::new(UInt64Array::from(vec![2u64])),]
            )
            .unwrap()
        );
    }
}
