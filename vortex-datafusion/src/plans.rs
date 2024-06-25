//! Physical operators needed to implement scanning of Vortex arrays with pushdown.

use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion_common::Result as DFResult;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_expr::Expr;
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, PlanProperties,
};
use lazy_static::lazy_static;
use vortex::array::struct_::StructArray;

/// Physical plan operator that applies a set of [filters][Expr] against the input, producing a
/// row mask that can be used downstream to force a take against the corresponding struct array
/// chunks but for different columns.
pub(crate) struct RowSelectorExec {
    filter_exprs: Vec<Expr>,
    filter_projection: Vec<usize>,

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
    pub(crate) fn new(
        filter_exprs: &Vec<Expr>,
        filter_projection: &Vec<usize>,
        filter_struct: &StructArray,
    ) -> Self {
        let cached_plan_props = PlanProperties::new(
            EquivalenceProperties::new(ROW_SELECTOR_SCHEMA_REF.clone()),
            Partitioning::RoundRobinBatch(1),
            ExecutionMode::Bounded,
        );

        Self {
            filter_exprs: filter_exprs.clone(),
            filter_projection: filter_projection.clone(),
            filter_struct: filter_struct.clone(),
            cached_plan_props,
        }
    }
}

impl Debug for RowSelectorExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RowSelectorExec").finish()
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
        panic!("with_new_children not supported for RowSelectorExec")
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        assert_eq!(
            partition, 0,
            "single partitioning only supported by TakeOperator"
        );

        todo!("need to implement this")
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

    // A record batch holding the fields that were relevant to executing the upstream filter expression.
    // These fields have already been decoded, so we hold them separately and "paste" them together
    // with the fields we decode from `table` below.
    filter_struct: RecordBatch,

    // The original Vortex array holding the fields we have not decoded yet.
    table: StructArray,
}

impl TakeRowsExec {
    pub(crate) fn new(
        schema_ref: SchemaRef,
        projection: &Vec<usize>,
        row_indices: Arc<dyn ExecutionPlan>,
        output_schema: SchemaRef,
        table: StructArray,
    ) -> Self {
        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(schema_ref.clone()),
            Partitioning::RoundRobinBatch(1),
            ExecutionMode::Bounded,
        );

        Self {
            plan_properties,
            projection: projection.clone(),
            input: row_indices,
            output_schema: output_schema.clone(),
            filter_struct: RecordBatch::new_empty(output_schema.clone()),
            table,
        }
    }
}

impl Debug for TakeRowsExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Take").finish()
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
        panic!("unsupported with_new_children for {:?}", &self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        assert_eq!(
            partition, 0,
            "single partitioning only supported by TakeOperator"
        );

        todo!()
    }
}
