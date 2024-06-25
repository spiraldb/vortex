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
use datafusion::arrow::buffer::NullBuffer;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionState;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion::prelude::SessionContext;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion_common::{exec_datafusion_err, DataFusionError, Result as DFResult, ToDFSchema};
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::simplify::SimplifyContext;
use datafusion_expr::{Expr, Operator, TableProviderFilterPushDown, TableType};
use datafusion_physical_expr::EquivalenceProperties;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::{Stream, StreamExt};
use itertools::Itertools;
use pin_project::pin_project;
use vortex::array::bool::BoolArray;
use vortex::array::chunked::ChunkedArray;
use vortex::{Array, ArrayDType, IntoArrayVariant, IntoCanonical};
use vortex_dtype::DType;
use vortex_error::{vortex_bail, VortexResult};

use crate::datatype::infer_schema;

mod datatype;
mod plans;

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

        let filter_exprs: Option<Vec<Expr>> = if filters.is_empty() {
            None
        } else {
            Some(filters.iter().cloned().collect())
        };

        let filter_projection = filter_exprs
            .clone()
            .map(|exprs| get_filter_projection(exprs.as_slice(), self.schema_ref.clone()));

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

        match (filter_exprs, filter_projection) {
            // If there is a filter expression, we execute in two phases, first performing a filter
            // on the input to get back row indices, and then taking the remaning struct columns
            // using the calculcated indices from the filter.
            (Some(filter_exprs), Some(filter_projection)) => Ok(make_filter_then_take_plan(
                self.schema_ref.clone(),
                filter_exprs,
                filter_projection,
                self.array.clone(),
                projection.clone(),
                plan_properties,
            )),

            // If no filters were pushed down, we materialize the entire StructArray into a
            // RecordBatch and let DataFusion process the entire query.
            _ => Ok(Arc::new(VortexScanExec {
                array: self.array.clone(),
                filter_exprs: None,
                filter_projection: None,
                scan_projection: projection.cloned(),
                plan_properties,
            })),
        }
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        // Get the set of column filters supported.
        let schema_columns: HashSet<String> = self
            .schema_ref
            .fields
            .iter()
            .map(|field| field.name().clone())
            .collect();

        filters
            .iter()
            .map(|expr| {
                if can_be_pushed_down(*expr, &schema_columns)? {
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
    _schema: SchemaRef,
    _filter_exprs: Vec<Expr>,
    _filter_projection: Vec<usize>,
    _array: Array,
    _output_projection: Option<&Vec<usize>>,
    _plan_properties: PlanProperties,
) -> Arc<dyn ExecutionPlan> {
    // Create a struct array necessary to run the filter operations.

    todo!()
}

/// Check if the given expression tree can be pushed down into the scan.
fn can_be_pushed_down(expr: &Expr, schema_columns: &HashSet<String>) -> DFResult<bool> {
    // If the filter references a column not known to our schema, we reject the filter for pushdown.
    // TODO(aduffy): is this necessary? Under what conditions would this happen?
    let column_refs = get_column_references(expr);
    if !column_refs.is_subset(&schema_columns) {
        return Ok(false);
    }

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

/// A mask determining the rows in an Array that should be treated as valid for query processing.
/// The vector is used to determine the take order of a set of things, or otherwise we determine
/// that we want to perform cross-filtering of the larger columns, if we so choose.
pub(crate) struct RowSelection {
    selection: NullBuffer,
}

impl RowSelection {
    /// Construct a new RowSelection with all elements initialized to selected (true).
    pub(crate) fn new_selected(len: usize) -> Self {
        Self {
            selection: NullBuffer::new_valid(len),
        }
    }

    /// Construct a new RowSelection with all elements initialized to unselected (false).
    pub(crate) fn new_unselected(len: usize) -> Self {
        Self {
            selection: NullBuffer::new_null(len),
        }
    }
}

impl RowSelection {
    // Based on the boolean array outputs of the other vector here.
    // We want to be careful when comparing things based on the infra for pushdown here.
    pub(crate) fn refine(&mut self, matches: &BoolArray) -> &mut Self {
        let matches = matches.boolean_buffer();

        // If nothing matches, we return a new value to set to false here.
        if matches.count_set_bits() == 0 {
            return self;
        }

        // Use an internal BoolArray to perform the logic here.
        // Once we have this setup, it might just work this way.
        self
    }
}

/// Convert a set of expressions that must all match into a single AND expression.
///
/// # Returns
///
/// If conversion is successful, the result will be a
/// [binary expression node][datafusion_expr::Expr::BinaryExpr] containing the conjunction.
fn make_simplified(expr: &Expr, schema: SchemaRef) -> DFResult<Expr> {
    let schema = schema.to_dfschema_ref()?;

    // simplify the expression.
    let props = ExecutionProps::new();
    let context = SimplifyContext::new(&props).with_schema(schema);
    let simplifier = ExprSimplifier::new(context);

    simplifier.simplify(expr.clone())
}

/// Physical plan node for scans against an in-memory, possibly chunked Vortex Array.
#[derive(Debug, Clone)]
struct VortexScanExec {
    array: Array,
    filter_exprs: Option<Vec<Expr>>,
    filter_projection: Option<Vec<usize>>,
    scan_projection: Option<Vec<usize>>,
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
    array: &Array,
    projection: &Option<Vec<usize>>,
) -> DFResult<SendableRecordBatchStream> {
    // Construct the RecordBatch by flattening each struct field and transmuting to an ArrayRef.
    let struct_array = array
        .clone()
        .into_struct()
        .map_err(|vortex_error| DataFusionError::Execution(format!("{}", vortex_error)))?;

    let field_order = if let Some(projection) = projection {
        projection.clone()
    } else {
        (0..struct_array.names().len()).collect()
    };

    let projected_struct = struct_array
        .project(field_order.as_slice())
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

        execute_unfiltered(&chunk, &self.scan_projection)
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
