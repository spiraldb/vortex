//! Connectors to enable DataFusion to read Vortex data.

use std::any::Any;
use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_array::{RecordBatch, StructArray as ArrowStructArray};
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::arrow::buffer::NullBuffer;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionState;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion::prelude::SessionContext;
use datafusion_common::{
    exec_datafusion_err, DataFusionError, Result as DFResult, ScalarValue, ToDFSchema,
};
use datafusion_common::tree_node::{TreeNodeRecursion, TreeNodeVisitor};
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::simplify::SimplifyContext;
use datafusion_expr::{BinaryExpr, Expr, Operator, TableProviderFilterPushDown, TableType};
use datafusion_physical_expr::EquivalenceProperties;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::{Stream, StreamExt};
use pin_project::pin_project;
use vortex::array::chunked::ChunkedArray;
use vortex::array::struct_::StructArray;
use vortex::{Array, ArrayDType, IntoArray, IntoCanonical};
use vortex_dtype::DType;
use vortex_error::{vortex_bail, VortexResult};

use crate::datatype::infer_schema;

mod datatype;

pub trait SessionContextExt {
    fn read_vortex(&self, array: Array) -> DFResult<DataFrame>;
}

impl SessionContextExt for SessionContext {
    fn read_vortex(&self, array: Array) -> DFResult<DataFrame> {
        assert!(
            matches!(array.dtype(), DType::Struct(_, _)),
            "Vortex arrays must have struct type"
        );

        let vortex_table = VortexInMemoryTableProvider::try_new(array)
            .map_err(|error| DataFusionError::Internal(format!("vortex error: {error}")))?;

        self.read_table(Arc::new(vortex_table))
    }
}

/// A [`TableProvider`] that exposes an existing Vortex Array to the DataFusion SQL engine.
///
/// Only arrays that have a top-level [struct type](vortex_dtype::StructDType) can be exposed as
/// a table to DataFusion.
#[derive(Debug, Clone)]
pub(crate) struct VortexInMemoryTableProvider {
    array: Array,
    schema_ref: SchemaRef,
}

impl VortexInMemoryTableProvider {
    /// Build a new table provider from an existing [struct type](vortex_dtype::StructDType) array.
    pub fn try_new(array: Array) -> VortexResult<Self> {
        if !matches!(array.dtype(), DType::Struct(_, _)) {
            vortex_bail!(InvalidArgument: "only DType::Struct arrays can produce a table provider");
        }

        let arrow_schema = infer_schema(array.dtype());
        let schema_ref = SchemaRef::new(arrow_schema);

        Ok(Self { array, schema_ref })
    }
}

#[async_trait]
impl TableProvider for VortexInMemoryTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema_ref)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Plan an array scan.
    ///
    /// Currently, projection pushdown is supported, but not filter pushdown.
    /// The array is flattened directly into the nearest Arrow-compatible encoding.
    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let filter_expr = if filters.is_empty() {
            None
        } else {
            Some(make_simplified_conjunction(
                filters,
                self.schema_ref.clone(),
            )?)
        };

        println!("simplified filter: {filter_expr:?}");

        let partitioning = if let Ok(chunked_array) = ChunkedArray::try_from(&self.array) {
            Partitioning::RoundRobinBatch(chunked_array.nchunks())
        } else {
            Partitioning::UnknownPartitioning(1)
        };

        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(self.schema_ref.clone()),
            partitioning,
            ExecutionMode::Bounded,
        );

        Ok(Arc::new(VortexMemoryExec {
            array: self.array.clone(),
            projection: projection.cloned(),
            filter_expr,
            plan_properties,
        }))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        // TODO(aduffy): add support for filter pushdown
        Ok(filters
            .iter()
            .map(|expr| {
                match expr {
                    // Several expressions can be pushed down.
                    Expr::BinaryExpr(_)
                    | Expr::IsNotNull(_)
                    | Expr::IsNull(_)
                    | Expr::IsTrue(_)
                    | Expr::IsFalse(_)
                    | Expr::IsNotTrue(_)
                    | Expr::IsNotFalse(_)
                    | Expr::Cast(_) => TableProviderFilterPushDown::Exact,

                    // All other expressions should be handled outside of the TableProvider
                    // via the normal DataFusion operator chain.
                    _ => TableProviderFilterPushDown::Unsupported,
                }

                TableProviderFilterPushDown::Exact
            })
            .collect())
    }
}

struct ValidationVisitor {}

impl ValidationVisitor {

}

impl TreeNodeVisitor for ValidationVisitor {
    type Node = Expr;

    fn f_down(&mut self, node: &Self::Node) -> DFResult<TreeNodeRecursion> {

    }
}

/// A mask determining the rows in an Array that should be treated as valid for query processing.
/// The vector is used to determine the take order of a set of things, or otherwise we determine
/// that we want to perform cross-filtering of the larger columns, if we so choose.
pub(crate) struct RowSelection {
    selection: NullBuffer,
}

/// Convert a set of expressions that must all match into a single AND expression.
///
/// # Returns
///
/// If conversion is successful, the result will be a
/// [binary expression node][datafusion_expr::Expr::BinaryExpr] containing the conjunction.
///
/// Note that the set of operators must be provided here instead.
///
/// # Simplification
///
/// Simplification will occur as part of this process, so constant folding and similar optimizations
/// will be applied before returning the final expression.
fn make_simplified_conjunction(filters: &[Expr], schema: SchemaRef) -> DFResult<Expr> {
    let init = Box::new(Expr::Literal(ScalarValue::Boolean(Some(true))));
    let conjunction = filters.iter().fold(init, |conj, item| {
        Box::new(Expr::BinaryExpr(BinaryExpr::new(
            conj,
            Operator::And,
            Box::new(item.clone()),
        )))
    });

    let schema = schema.to_dfschema_ref()?;

    // simplify the expression.
    let props = ExecutionProps::new();
    let context = SimplifyContext::new(&props).with_schema(schema);
    let simplifier = ExprSimplifier::new(context);

    simplifier.simplify(*conjunction)
}

/// Physical plan node for scans against an in-memory, possibly chunked Vortex Array.
#[derive(Debug, Clone)]
struct VortexMemoryExec {
    array: Array,
    filter_expr: Option<Expr>,
    projection: Option<Vec<usize>>,
    plan_properties: PlanProperties,
}

impl DisplayAs for VortexMemoryExec {
    fn fmt_as(&self, _display_type: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl VortexMemoryExec {
    /// Read a single array chunk from the source as a RecordBatch.
    ///
    /// `array` must be a [`StructArray`] or flatten into one. Passing a different Array variant
    /// may cause a panic.
    fn execute_single_chunk(
        array: Array,
        projection: &Option<Vec<usize>>,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let data = array
            .into_canonical()
            .map_err(|vortex_error| DataFusionError::Execution(format!("{}", vortex_error)))?
            .into_array();

        // Construct the RecordBatch by flattening each struct field and transmuting to an ArrayRef.
        let struct_array = StructArray::try_from(data).expect("array must be StructArray");

        let field_order = if let Some(projection) = projection {
            projection.clone()
        } else {
            (0..struct_array.names().len()).collect()
        };

        let projected_struct =
            struct_array
                .project(field_order.as_slice())
                .map_err(|vortex_err| {
                    exec_datafusion_err!("projection pushdown to Vortex failed: {vortex_err}")
                })?;
        let batch = RecordBatch::from(
            projected_struct
                .into_canonical()
                .expect("struct arrays must flatten")
                .into_arrow()
                .as_any()
                .downcast_ref::<ArrowStructArray>()
                .expect("vortex StructArray must convert to arrow StructArray"),
        );
        Ok(Box::pin(VortexRecordBatchStream {
            schema_ref: batch.schema(),
            inner: futures::stream::iter(vec![batch]),
        }))
    }
}

#[pin_project]
struct VortexRecordBatchStream<I> {
    schema_ref: SchemaRef,

    #[pin]
    inner: I,
}

impl<I> Stream for VortexRecordBatchStream<I>
where
    I: Stream<Item = RecordBatch>,
{
    type Item = DFResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        match this.inner.poll_next_unpin(cx) {
            Poll::Ready(Some(batch)) => Poll::Ready(Some(Ok(batch))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<I> RecordBatchStream for VortexRecordBatchStream<I>
where
    I: Stream<Item = RecordBatch>,
{
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema_ref)
    }
}

impl ExecutionPlan for VortexMemoryExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // Leaf node
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let chunk = if let Ok(chunked_array) = ChunkedArray::try_from(&self.array) {
            chunked_array
                .chunk(partition)
                .ok_or_else(|| exec_datafusion_err!("partition not found"))?
        } else {
            self.array.clone()
        };

        Self::execute_single_chunk(chunk, &self.projection, context)
    }
}

#[cfg(test)]
mod test {
    use arrow_array::types::Int64Type;
    use datafusion::arrow::array::AsArray;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::{col, count_distinct, lit};
    use vortex::array::primitive::PrimitiveArray;
    use vortex::array::struct_::StructArray;
    use vortex::array::varbin::VarBinArray;
    use vortex::validity::Validity;
    use vortex::IntoArray;
    use vortex_dtype::{DType, Nullability};

    use crate::SessionContextExt;

    #[tokio::test]
    async fn test_datafusion_simple() {
        let names = VarBinArray::from_vec(
            vec![
                "Washington",
                "Adams",
                "Jefferson",
                "Madison",
                "Monroe",
                "Adams",
            ],
            DType::Utf8(Nullability::NonNullable),
        );
        let term_start = PrimitiveArray::from_vec(
            vec![1789u16, 1797, 1801, 1809, 1817, 1825],
            Validity::NonNullable,
        );

        let presidents = StructArray::from_fields(&[
            ("president", names.into_array()),
            ("term_start", term_start.into_array()),
        ])
        .into_array();

        let ctx = SessionContext::new();

        let df = ctx.read_vortex(presidents).unwrap();

        let distinct_names = df
            .filter(col("term_start").gt_eq(lit(1795)))
            .unwrap()
            .aggregate(vec![], vec![count_distinct(col("president"))])
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(distinct_names.len(), 1);

        assert_eq!(
            *distinct_names[0]
                .column(0)
                .as_primitive::<Int64Type>()
                .values()
                .first()
                .unwrap(),
            4i64
        );
    }
}
