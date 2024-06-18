//! Connectors to enable DataFusion to read Vortex data.

use std::any::Any;
use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_array::{RecordBatch, StructArray as ArrowStructArray};
use arrow_schema::Schema;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionState;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion_common::{exec_datafusion_err, exec_err, DataFusionError, Result as DFResult};
use datafusion_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion_physical_expr::EquivalenceProperties;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::{Stream, StreamExt};
use pin_project::pin_project;
use vortex::array::chunked::ChunkedArray;
use vortex::array::r#struct::StructArray;
use vortex::{Array, ArrayDType, ArrayFlatten, IntoArray};
use vortex_dtype::DType;
use vortex_error::{vortex_bail, VortexResult};

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

        let arrow_schema = Schema::try_from(array.dtype())?;
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
        if !filters.is_empty() {
            return exec_err!("vortex does not support filter pushdown");
        }

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
            .map(|_| TableProviderFilterPushDown::Unsupported)
            .collect())
    }
}

/// Physical plan node for scans against an in-memory, possibly chunked Vortex Array.
#[derive(Debug, Clone)]
struct VortexMemoryExec {
    array: Array,
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
            .flatten()
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
                .flatten()
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

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
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
    use std::sync::Arc;

    use datafusion::arrow::array::AsArray;
    use datafusion::arrow::datatypes::UInt64Type;
    use datafusion::prelude::SessionContext;
    use vortex::array::primitive::PrimitiveArray;
    use vortex::array::r#struct::StructArray;
    use vortex::array::varbin::VarBinArray;
    use vortex::validity::Validity;
    use vortex::IntoArray;
    use vortex_dtype::{DType, FieldName, Nullability};

    use crate::VortexInMemoryTableProvider;

    #[tokio::test]
    async fn test_datafusion_simple() {
        let names = VarBinArray::from_vec(
            vec!["Washington", "Adams", "Jefferson", "Madison", "Monroe"],
            DType::Utf8(Nullability::NonNullable),
        );
        let term_start =
            PrimitiveArray::from_vec(vec![1789u16, 1797, 1801, 1809, 1817], Validity::NonNullable);
        let presidents = StructArray::try_new(
            Arc::new([FieldName::from("president"), FieldName::from("term_start")]),
            vec![names.into_array(), term_start.into_array()],
            5,
            Validity::NonNullable,
        )
        .unwrap();

        let presidents_table =
            Arc::new(VortexInMemoryTableProvider::try_new(presidents.into_array()).unwrap());
        let session_ctx = SessionContext::new();

        session_ctx
            .register_table("presidents", presidents_table)
            .unwrap();

        let df_term_start = session_ctx
            .sql("SELECT SUM(term_start) FROM presidents WHERE president <> 'Madison'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(df_term_start.len(), 1);
        assert_eq!(
            *df_term_start[0]
                .column(0)
                .as_primitive::<UInt64Type>()
                .values()
                .first()
                .unwrap(),
            vec![1789u64, 1797, 1801, 1817].into_iter().sum::<u64>()
        );
    }
}
