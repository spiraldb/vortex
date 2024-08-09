//! Connectors to enable DataFusion to read Vortex data.

#![allow(clippy::nonminimal_bool)]

use std::any::Any;
use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_array::RecordBatch;
use arrow_schema::{DataType, SchemaRef};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{exec_datafusion_err, DataFusionError, Result as DFResult};
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_expr::{Expr, Operator};
use datafusion_physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::Stream;
use itertools::Itertools;
use memory::{VortexMemTable, VortexMemTableOptions};
use persistent::config::VortexTableOptions;
use persistent::provider::VortexFileTableProvider;
use vortex::array::ChunkedArray;
use vortex::{Array, ArrayDType, IntoArrayVariant};
use vortex_error::vortex_err;

pub mod memory;
pub mod persistent;

mod datatype;
mod eval;
mod expr;
mod plans;

const SUPPORTED_BINARY_OPS: &[Operator] = &[
    Operator::Eq,
    Operator::NotEq,
    Operator::Gt,
    Operator::GtEq,
    Operator::Lt,
    Operator::LtEq,
];

fn supported_data_types(dt: DataType) -> bool {
    dt.is_integer()
        || dt.is_signed_integer()
        || dt.is_floating()
        || dt.is_null()
        || dt == DataType::Boolean
        || dt == DataType::Binary
        || dt == DataType::Utf8
        || dt == DataType::Binary
        || dt == DataType::BinaryView
        || dt == DataType::Utf8View
}

pub trait SessionContextExt {
    fn register_mem_vortex<S: AsRef<str>>(&self, name: S, array: Array) -> DFResult<()> {
        self.register_mem_vortex_opts(name, array, VortexMemTableOptions::default())
    }

    fn register_mem_vortex_opts<S: AsRef<str>>(
        &self,
        name: S,
        array: Array,
        options: VortexMemTableOptions,
    ) -> DFResult<()>;

    fn read_mem_vortex(&self, array: Array) -> DFResult<DataFrame> {
        self.read_mem_vortex_opts(array, VortexMemTableOptions::default())
    }

    fn read_mem_vortex_opts(
        &self,
        array: Array,
        options: VortexMemTableOptions,
    ) -> DFResult<DataFrame>;

    fn register_disk_vortex_opts<S: AsRef<str>>(
        &self,
        name: S,
        url: ObjectStoreUrl,
        options: VortexTableOptions,
    ) -> DFResult<()>;

    fn read_disk_vortex_opts(
        &self,
        url: ObjectStoreUrl,
        options: VortexTableOptions,
    ) -> DFResult<DataFrame>;
}

impl SessionContextExt for SessionContext {
    fn register_mem_vortex_opts<S: AsRef<str>>(
        &self,
        name: S,
        array: Array,
        options: VortexMemTableOptions,
    ) -> DFResult<()> {
        if !array.dtype().is_struct() {
            return Err(vortex_err!(
                "Vortex arrays must have struct type, found {}",
                array.dtype()
            )
            .into());
        }

        let vortex_table = VortexMemTable::new(array, options);
        self.register_table(name.as_ref(), Arc::new(vortex_table))
            .map(|_| ())
    }

    fn read_mem_vortex_opts(
        &self,
        array: Array,
        options: VortexMemTableOptions,
    ) -> DFResult<DataFrame> {
        if !array.dtype().is_struct() {
            vortex_bail!(
                "Vortex arrays must have struct type, found {}",
                array.dtype()
            )
            .into();
        }

        let vortex_table = VortexMemTable::new(array, options);

        self.read_table(Arc::new(vortex_table))
    }

    fn register_disk_vortex_opts<S: AsRef<str>>(
        &self,
        name: S,
        url: ObjectStoreUrl,
        options: VortexTableOptions,
    ) -> DFResult<()> {
        let provider = Arc::new(VortexFileTableProvider::try_new(url, options)?);
        self.register_table(name.as_ref(), provider as _)?;

        Ok(())
    }

    fn read_disk_vortex_opts(
        &self,
        url: ObjectStoreUrl,
        options: VortexTableOptions,
    ) -> DFResult<DataFrame> {
        let provider = Arc::new(VortexFileTableProvider::try_new(url, options)?);
        self.read_table(provider)
    }
}

fn can_be_pushed_down(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryExpr(expr)
            if expr.op.is_logic_operator() || SUPPORTED_BINARY_OPS.contains(&expr.op) =>
        {
            can_be_pushed_down(expr.left.as_ref()) & can_be_pushed_down(expr.right.as_ref())
        }
        Expr::Column(_) => true,
        Expr::Literal(lit) => supported_data_types(lit.data_type()),
        _ => false,
    }
}

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
#[derive(Clone)]
struct VortexScanExec {
    array: ChunkedArray,
    scan_projection: Vec<usize>,
    plan_properties: PlanProperties,
}

impl Debug for VortexScanExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VortexScanExec")
            .field("array_length", &self.array.len())
            .field("array_dtype", &self.array.dtype())
            .field("scan_projection", &self.scan_projection)
            .field("plan_properties", &self.plan_properties)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for VortexScanExec {
    #[allow(clippy::use_debug)]
    fn fmt_as(&self, _display_type: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

pub(crate) struct VortexRecordBatchStream {
    schema_ref: SchemaRef,

    idx: usize,
    num_chunks: usize,
    chunks: ChunkedArray,

    projection: Vec<usize>,
}

impl Stream for VortexRecordBatchStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.idx >= this.num_chunks {
            return Poll::Ready(None);
        }

        // Grab next chunk, project and convert to Arrow.
        let chunk = this
            .chunks
            .chunk(this.idx)
            .expect("nchunks should match precomputed");
        this.idx += 1;

        let struct_array = chunk
            .clone()
            .into_struct()
            .map_err(|vortex_error| DataFusionError::Execution(format!("{}", vortex_error)))?;

        let projected_struct =
            struct_array
                .project(this.projection.as_slice())
                .map_err(|vortex_err| {
                    exec_datafusion_err!("projection pushdown to Vortex failed: {vortex_err}")
                })?;

        Poll::Ready(Some(Ok(projected_struct.into())))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.num_chunks, Some(self.num_chunks))
    }
}

impl RecordBatchStream for VortexRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema_ref)
    }
}

impl ExecutionPlan for VortexScanExec {
    fn name(&self) -> &str {
        VortexScanExec::static_name()
    }

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
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        // Send back a stream of RecordBatch that returns the next element of the chunk each time.
        Ok(Box::pin(VortexRecordBatchStream {
            schema_ref: self.schema().clone(),
            idx: 0,
            num_chunks: self.array.nchunks(),
            chunks: self.array.clone(),
            projection: self.scan_projection.clone(),
        }))
    }
}
