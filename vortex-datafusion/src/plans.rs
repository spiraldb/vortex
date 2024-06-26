//! Physical operators needed to implement scanning of Vortex arrays with pushdown.

use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_array::cast::AsArray;
use arrow_array::types::UInt64Type;
use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::compute::cast;
use datafusion::execution::context::SessionState;
use datafusion_common::{DFSchema, Result as DFResult};
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion_expr::Expr;
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, PlanProperties,
};
use futures::{ready, Stream};
use lazy_static::lazy_static;
use pin_project::pin_project;
use vortex::array::struct_::StructArray;
use vortex::arrow::FromArrowArray;
use vortex::compute::take::take;
use vortex::{ArrayDType, ArrayData, IntoArray, IntoCanonical};

use crate::datatype::infer_schema;
use crate::expr::{make_conjunction, simplify_expr};

/// Physical plan operator that applies a set of [filters][Expr] against the input, producing a
/// row mask that can be used downstream to force a take against the corresponding struct array
/// chunks but for different columns.
pub(crate) struct RowSelectorExec {
    filter_exprs: Vec<Expr>,

    // cached PlanProperties object. We do not make use of this.
    cached_plan_props: PlanProperties,

    // A Vortex struct array that contains all columns necessary for executing the filter
    // expressions.
    filter_struct: StructArray,
}

lazy_static! {
    static ref ROW_SELECTOR_SCHEMA_REF: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        "row_idx",
        DataType::UInt64,
        false
    )]));
}

impl RowSelectorExec {
    pub(crate) fn new(filter_exprs: &[Expr], filter_struct: &StructArray) -> Self {
        let cached_plan_props = PlanProperties::new(
            EquivalenceProperties::new(ROW_SELECTOR_SCHEMA_REF.clone()),
            Partitioning::RoundRobinBatch(1),
            ExecutionMode::Bounded,
        );

        Self {
            filter_exprs: filter_exprs.to_owned(),
            filter_struct: filter_struct.clone(),
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
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        assert_eq!(
            partition, 0,
            "single partitioning only supported by TakeOperator"
        );

        let stream_schema = Arc::new(infer_schema(self.filter_struct.dtype()));

        let filter_struct = self.filter_struct.clone();
        let inner = Box::pin(async move {
            let arrow_array = filter_struct.into_canonical().unwrap().into_arrow();
            Ok(RecordBatch::from(arrow_array.as_struct()))
        });

        let conjunction_expr = simplify_expr(
            &make_conjunction(&self.filter_exprs)?,
            stream_schema.clone(),
        )?;

        Ok(Box::pin(RowIndicesStream {
            inner,
            polled_inner: false,
            conjunction_expr,
            schema_ref: stream_schema,
            context: context.clone(),
        }))
    }
}

/// [RecordBatchStream] of row indices, emitted by the [RowSelectorExec] physical plan node.
#[pin_project::pin_project]
pub(crate) struct RowIndicesStream<F> {
    /// The inner future that returns `DFResult<RecordBatch>`.
    #[pin]
    inner: F,

    polled_inner: bool,

    conjunction_expr: Expr,
    schema_ref: SchemaRef,
    context: Arc<TaskContext>,
}

impl<F> Stream for RowIndicesStream<F>
where
    F: Future<Output = DFResult<RecordBatch>>,
{
    type Item = DFResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        // If we have already polled the one-shot future with the filter records, indicate
        // that the stream has finished.
        if *this.polled_inner {
            return Poll::Ready(None);
        }

        // Get the unfiltered record batch.
        // Since this is a one-shot, we only want to poll the inner future once, to create the
        // initial batch for us to process.
        //
        // We want to avoid ever calling it again.
        let record_batch = ready!(this.inner.poll(cx))?;
        *this.polled_inner = true;

        // Using a local SessionContext, generate a physical plan to execute the conjunction query
        // against the filter columns.
        //
        // The result of a conjunction expression is a BooleanArray containing `true` for rows
        // where the conjunction was satisfied, and `false` otherwise.
        let session_state = SessionState::new_with_config_rt(
            this.context.session_config().clone(),
            this.context.runtime_env().clone(),
        );
        let df_schema = DFSchema::try_from(this.schema_ref.clone())?;
        let physical_expr =
            session_state.create_physical_expr(this.conjunction_expr.clone(), &df_schema)?;
        let selection = physical_expr
            .evaluate(&record_batch)?
            .into_array(record_batch.num_rows())?;

        // Convert the `selection` BooleanArray into a UInt64Array of indices.
        let selection_indices: Vec<u64> = selection
            .as_boolean()
            .clone()
            .values()
            .set_indices()
            .map(|idx| idx as u64)
            .collect();

        let indices: ArrayRef = Arc::new(UInt64Array::from(selection_indices));
        let indices_batch = RecordBatch::try_new(ROW_SELECTOR_SCHEMA_REF.clone(), vec![indices])?;

        Poll::Ready(Some(Ok(indices_batch)))
    }
}

impl<F> RecordBatchStream for RowIndicesStream<F>
where
    F: Future<Output = DFResult<RecordBatch>>,
{
    fn schema(&self) -> SchemaRef {
        self.schema_ref.clone()
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
    table: StructArray,
}

impl TakeRowsExec {
    pub(crate) fn new(
        schema_ref: SchemaRef,
        projection: &[usize],
        row_indices: Arc<dyn ExecutionPlan>,
        table: &StructArray,
    ) -> Self {
        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(schema_ref.clone()),
            Partitioning::RoundRobinBatch(1),
            ExecutionMode::Bounded,
        );

        let output_schema = Arc::new(schema_ref.project(projection).unwrap());

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
        assert_eq!(
            partition, 0,
            "single partitioning only supported by TakeOperator"
        );

        let row_indices_stream = self.input.execute(partition, context)?;

        Ok(Box::pin(TakeRowsStream {
            row_indices_stream,
            completed: false,
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

    completed: bool,

    // Projection based on the schema here
    output_projection: Vec<usize>,
    output_schema: SchemaRef,

    // The original Vortex array we're taking from
    vortex_array: StructArray,
}

impl<F> Stream for TakeRowsStream<F>
where
    F: Stream<Item = DFResult<RecordBatch>>,
{
    type Item = DFResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        // If `poll_next` has already fired, return None indicating end of the stream.
        if *this.completed {
            return Poll::Ready(None);
        }

        // Get the indices provided by the upstream operator.
        let record_batch = match ready!(this.row_indices_stream.poll_next(cx)) {
            None => {
                // Row indices stream is complete, we are also complete.
                // This should never happen right now given we only emit one recordbatch upstream.
                return Poll::Ready(None);
            }
            Some(result) => {
                *this.completed = true;
                result?
            }
        };

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

        // Assemble the output columns using the row indices.
        // NOTE(aduffy): this re-decodes the fields from the filter schema, which is unnecessary.
        let mut columns = Vec::new();
        for (output_idx, src_idx) in this.output_projection.iter().enumerate() {
            let encoded = this.vortex_array.field(*src_idx).expect("field access");
            let decoded = take(&encoded, &row_indices)
                .expect("take")
                .into_canonical()
                .expect("into_canonical")
                .into_arrow();
            let data_type = this.output_schema.field(output_idx).data_type();

            columns.push(cast(&decoded, data_type).expect("cast"));
        }

        // Send back a single record batch of the decoded data.
        let output_batch = RecordBatch::try_new(this.output_schema.clone(), columns)
            .expect("RecordBatch::try_new");

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

    use arrow_array::{BooleanArray, RecordBatch, UInt64Array};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion_expr::{and, col, lit};
    use itertools::Itertools;

    use crate::plans::{RowIndicesStream, ROW_SELECTOR_SCHEMA_REF};

    #[tokio::test]
    async fn test_filtering_stream() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::UInt64, false),
            Field::new("b", DataType::Boolean, false),
        ]));

        let _schema = schema.clone();
        let inner = Box::pin(async move {
            Ok(RecordBatch::try_new(
                _schema,
                vec![
                    Arc::new(UInt64Array::from(vec![0u64, 1, 2])),
                    Arc::new(BooleanArray::from(vec![false, false, true])),
                ],
            )
            .unwrap())
        });

        let _schema = schema.clone();
        let filtering_stream = RowIndicesStream {
            inner,
            polled_inner: false,
            conjunction_expr: and((col("a") % lit(2)).eq(lit(0)), col("b").is_true()),
            schema_ref: _schema,
            context: Arc::new(Default::default()),
        };

        let rows: Vec<RecordBatch> = futures::executor::block_on_stream(filtering_stream)
            .try_collect()
            .unwrap();

        assert_eq!(rows.len(), 1);

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
    }
}
