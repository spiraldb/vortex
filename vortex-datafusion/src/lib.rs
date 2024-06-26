//! Connectors to enable DataFusion to read Vortex data.

use std::any::Any;
use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_array::{RecordBatch, StructArray as ArrowStructArray};
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionState;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::prelude::SessionContext;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion_common::{exec_datafusion_err, DataFusionError, Result as DFResult};
use datafusion_expr::{Expr, Operator, TableProviderFilterPushDown, TableType};
use datafusion_physical_expr::EquivalenceProperties;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::{Stream, StreamExt};
use itertools::Itertools;
use pin_project::pin_project;
use vortex::array::chunked::ChunkedArray;
use vortex::array::struct_::StructArray;
use vortex::{Array, ArrayDType, IntoArrayVariant, IntoCanonical};
use vortex_dtype::DType;
use vortex_error::VortexResult;

use crate::datatype::infer_schema;
use crate::plans::{RowSelectorExec, TakeRowsExec};

mod datatype;
mod expr;
mod plans;

/// Optional configurations to pass when loading a [VortexMemTable].
#[derive(Default, Debug, Clone)]
pub struct VortexMemTableOptions {
    pub disable_pushdown: bool,
}

impl VortexMemTableOptions {
    pub fn with_disable_pushdown(mut self, disable_pushdown: bool) -> Self {
        self.disable_pushdown = disable_pushdown;
        self
    }
}

pub trait SessionContextExt {
    fn read_vortex(&self, array: Array) -> DFResult<DataFrame> {
        self.read_vortex_opts(array, VortexMemTableOptions::default())
    }

    fn read_vortex_opts(&self, array: Array, options: VortexMemTableOptions)
        -> DFResult<DataFrame>;
}

impl SessionContextExt for SessionContext {
    fn read_vortex_opts(
        &self,
        array: Array,
        options: VortexMemTableOptions,
    ) -> DFResult<DataFrame> {
        assert!(
            matches!(array.dtype(), DType::Struct(_, _)),
            "Vortex arrays must have struct type"
        );

        let vortex_table = VortexMemTable::try_new(array, options)
            .map_err(|error| DataFusionError::Internal(format!("vortex error: {error}")))?;

        self.read_table(Arc::new(vortex_table))
    }
}

/// A [`TableProvider`] that exposes an existing Vortex Array to the DataFusion SQL engine.
///
/// Only arrays that have a top-level [struct type](vortex_dtype::StructDType) can be exposed as
/// a table to DataFusion.
#[derive(Debug, Clone)]
pub struct VortexMemTable {
    array: Array,
    schema_ref: SchemaRef,
    options: VortexMemTableOptions,
}

impl VortexMemTable {
    /// Build a new table provider from an existing [struct type](vortex_dtype::StructDType) array.
    ///
    /// # Panics
    ///
    /// Creation will panic if the provided array is not of `DType::Struct` type.
    pub fn try_new(array: Array, options: VortexMemTableOptions) -> VortexResult<Self> {
        let arrow_schema = infer_schema(array.dtype());
        let schema_ref = SchemaRef::new(arrow_schema);

        Ok(Self {
            array,
            schema_ref,
            options,
        })
    }
}

#[async_trait]
impl TableProvider for VortexMemTable {
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
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        fn get_filter_projection(exprs: &[Expr], schema: SchemaRef) -> Vec<usize> {
            let referenced_columns: HashSet<String> =
                exprs.iter().flat_map(get_column_references).collect();

            let projection: Vec<usize> = referenced_columns
                .iter()
                .map(|col_name| schema.column_with_name(col_name).unwrap().0)
                .sorted()
                .collect();

            projection
        }

        let filter_exprs = if filters.is_empty() {
            None
        } else {
            Some(filters)
        };

        let partitioning = if let Ok(chunked_array) = ChunkedArray::try_from(&self.array) {
            Partitioning::RoundRobinBatch(chunked_array.nchunks())
        } else {
            Partitioning::UnknownPartitioning(1)
        };

        let output_projection: Vec<usize> = match projection {
            None => (0..self.schema_ref.fields().len()).collect(),
            Some(proj) => proj.clone(),
        };

        match filter_exprs {
            // If there is a filter expression, we execute in two phases, first performing a filter
            // on the input to get back row indices, and then taking the remaining struct columns
            // using the calculated indices from the filter.
            Some(filter_exprs) => {
                let filter_projection =
                    get_filter_projection(filter_exprs, self.schema_ref.clone());

                Ok(make_filter_then_take_plan(
                    self.schema_ref.clone(),
                    filter_exprs,
                    filter_projection,
                    self.array.clone(),
                    output_projection.clone(),
                    state,
                ))
            }

            // If no filters were pushed down, we materialize the entire StructArray into a
            // RecordBatch and let DataFusion process the entire query.
            _ => {
                let output_schema = Arc::new(
                    self.schema_ref
                        .project(output_projection.as_slice())
                        .expect("project output schema"),
                );
                let plan_properties = PlanProperties::new(
                    EquivalenceProperties::new(output_schema),
                    partitioning,
                    ExecutionMode::Bounded,
                );

                Ok(Arc::new(VortexScanExec {
                    array: self.array.clone(),
                    scan_projection: output_projection.clone(),
                    plan_properties,
                }))
            }
        }
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        // In the case the caller has configured this provider with filter pushdown disabled,
        // do not attempt to apply any filters at scan time.
        if self.options.disable_pushdown {
            return Ok(filters
                .iter()
                .map(|_| TableProviderFilterPushDown::Unsupported)
                .collect());
        }

        filters
            .iter()
            .map(|expr| {
                if can_be_pushed_down(expr)? {
                    Ok(TableProviderFilterPushDown::Exact)
                } else {
                    Ok(TableProviderFilterPushDown::Unsupported)
                }
            })
            .try_collect()
    }
}

/// Construct an operator plan that executes in two stages.
///
/// The first plan stage only materializes the columns related to the provided set of filter
/// expressions. It evaluates the filters into a row selection.
///
/// The second stage receives the row selection above and dispatches a `take` on the remaining
/// columns.
fn make_filter_then_take_plan(
    schema: SchemaRef,
    filter_exprs: &[Expr],
    filter_projection: Vec<usize>,
    array: Array,
    output_projection: Vec<usize>,
    _session_state: &SessionState,
) -> Arc<dyn ExecutionPlan> {
    let struct_array = StructArray::try_from(array).unwrap();

    let filter_struct = struct_array
        .project(filter_projection.as_slice())
        .expect("projecting filter struct");

    let row_selector_op = Arc::new(RowSelectorExec::new(filter_exprs, &filter_struct));

    Arc::new(TakeRowsExec::new(
        schema.clone(),
        &output_projection,
        row_selector_op.clone(),
        &struct_array,
    ))
}

/// Check if the given expression tree can be pushed down into the scan.
fn can_be_pushed_down(expr: &Expr) -> DFResult<bool> {
    // If the filter references a column not known to our schema, we reject the filter for pushdown.
    fn is_supported(expr: &Expr) -> bool {
        match expr {
            Expr::BinaryExpr(binary_expr) => {
                // Both the left and right sides must be column expressions, scalars, or casts.

                match binary_expr.op {
                    // Initially, we will only support pushdown for basic boolean operators
                    Operator::Eq
                    | Operator::NotEq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq => true,

                    // TODO(aduffy): add support for LIKE
                    // TODO(aduffy): add support for basic mathematical ops +-*/
                    // TODO(aduffy): add support for conjunctions, assuming all of the
                    //  left and right are valid expressions.
                    _ => false,
                }
            }
            Expr::IsNotNull(_)
            | Expr::IsNull(_)
            | Expr::IsTrue(_)
            | Expr::IsFalse(_)
            | Expr::IsNotTrue(_)
            | Expr::IsNotFalse(_)
            | Expr::Column(_)
            | Expr::Literal(_)
            // TODO(aduffy): ensure that cast can be pushed down.
            | Expr::Cast(_) => true,
            _ => false,
        }
    }

    // Visitor that traverses the expression tree and tracks if any unsupported expressions were
    // encountered.
    struct IsSupportedVisitor {
        supported_expressions_only: bool,
    }

    impl TreeNodeVisitor<'_> for IsSupportedVisitor {
        type Node = Expr;

        fn f_down(&mut self, node: &Self::Node) -> DFResult<TreeNodeRecursion> {
            if !is_supported(node) {
                self.supported_expressions_only = false;
                return Ok(TreeNodeRecursion::Stop);
            }

            Ok(TreeNodeRecursion::Continue)
        }
    }

    let mut visitor = IsSupportedVisitor {
        supported_expressions_only: true,
    };

    // Traverse the tree.
    // At the end of the traversal, the internal state of `visitor` will indicate if there were
    // unsupported expressions encountered.
    expr.visit(&mut visitor)?;

    Ok(visitor.supported_expressions_only)
}

/// Extract out the columns from our table referenced by the expression.
fn get_column_references(expr: &Expr) -> HashSet<String> {
    let mut references = HashSet::new();

    expr.apply(|node| match node {
        Expr::Column(col) => {
            references.insert(col.name.clone());

            Ok(TreeNodeRecursion::Continue)
        }
        _ => Ok(TreeNodeRecursion::Continue),
    })
    .unwrap();

    references
}

/// Physical plan node for scans against an in-memory, possibly chunked Vortex Array.
#[derive(Debug, Clone)]
struct VortexScanExec {
    array: Array,
    scan_projection: Vec<usize>,
    plan_properties: PlanProperties,
}

impl DisplayAs for VortexScanExec {
    fn fmt_as(&self, _display_type: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// Read a single array chunk from the source as a RecordBatch.
///
/// # Errors
/// This function will return an Error if `array` is not struct-typed. It will also return an
/// error if the projection references columns
fn execute_unfiltered(
    array: Array,
    projection: &Vec<usize>,
) -> DFResult<SendableRecordBatchStream> {
    // Construct the RecordBatch by flattening each struct field and transmuting to an ArrayRef.
    let struct_array = array
        .clone()
        .into_struct()
        .map_err(|vortex_error| DataFusionError::Execution(format!("{}", vortex_error)))?;

    let projected_struct = struct_array
        .project(projection.as_slice())
        .map_err(|vortex_err| {
            exec_datafusion_err!("projection pushdown to Vortex failed: {vortex_err}")
        })?;
    let batch = RecordBatch::from(
        projected_struct
            .into_canonical()
            .expect("struct arrays must canonicalize")
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

// Row selector stream.
// I.e., send a stream of RowSelector which allows us to pass in a bunch of binary arrays
// back down to the other systems here instead.

#[pin_project]
pub(crate) struct VortexRecordBatchStream<I> {
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

impl ExecutionPlan for VortexScanExec {
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
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let chunk = if let Ok(chunked_array) = ChunkedArray::try_from(&self.array) {
            chunked_array
                .chunk(partition)
                .ok_or_else(|| exec_datafusion_err!("partition not found"))?
        } else {
            self.array.clone()
        };

        execute_unfiltered(chunk, &self.scan_projection)
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
    use vortex::{Array, IntoArray};
    use vortex_dtype::{DType, Nullability};

    use crate::{SessionContextExt, VortexMemTableOptions};

    fn presidents_array() -> Array {
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

        StructArray::from_fields(&[
            ("president", names.into_array()),
            ("term_start", term_start.into_array()),
        ])
        .into_array()
    }

    #[tokio::test]
    async fn test_datafusion_pushdown() {
        let ctx = SessionContext::new();

        let df = ctx.read_vortex(presidents_array()).unwrap();

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

    #[tokio::test]
    async fn test_datafusion_no_pushdown() {
        let ctx = SessionContext::new();

        let df = ctx
            .read_vortex_opts(
                presidents_array(),
                // Disable pushdown. We run this test to make sure that the naive codepath also
                // produces correct results and does not panic anywhere.
                VortexMemTableOptions::default().with_disable_pushdown(true),
            )
            .unwrap();

        let distinct_names = df
            .filter(col("term_start").gt_eq(lit(1795)))
            .unwrap()
            .filter(col("term_start").lt(lit(2000)))
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
