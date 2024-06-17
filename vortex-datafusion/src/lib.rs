//! Connectors to enable DataFusion to read Vortex data.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_expr::{Expr, TableType};
use vortex::{Array, ArrayDType};
use vortex_dtype::DType;
use vortex_error::{vortex_bail, VortexResult};

#[derive(Debug, Clone)]
pub struct VortexInMemoryTableProvider {
    array: Array,
    schema_ref: SchemaRef,
}

impl VortexInMemoryTableProvider {
    pub fn try_new(array: Array) -> VortexResult<Self> {
        if !matches!(array.dtype(), DType::Struct(_, _)) {
            vortex_bail!(InvalidArgument: "only DType::Struct arrays can produce a table provider");
        }

        Ok(Self { array })
    }
}

// Create a table provider that is able to perform basic pushdown over
// the datasources inherent in the stream readers.

#[async_trait]
impl TableProvider for VortexInMemoryTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        // Convert our schema type into the other types.
    }

    fn table_type(&self) -> TableType {
        todo!()
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        todo!()
    }
}
