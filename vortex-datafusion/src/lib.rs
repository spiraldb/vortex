//! Connectors to enable DataFusion to read Vortex data.

use std::any::Any;
use std::sync::Arc;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::execution::context::SessionState;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::Statistics;

#[derive(Debug, Clone)]
pub struct VortexFileFormat {}

#[async_trait]
impl FileFormat for VortexFileFormat {
    fn as_any(&self) -> &dyn Any {
        todo!()
    }

    async fn infer_schema(&self, state: &SessionState, store: &Arc<dyn object_store::ObjectStore>, objects: &[object_store::ObjectMeta]) -> datafusion_common::Result<SchemaRef> {
        todo!()
    }

    async fn infer_stats(&self, state: &SessionState, store: &Arc<dyn object_store::ObjectStore>, table_schema: SchemaRef, object: &object_store::ObjectMeta) -> datafusion_common::Result<Statistics> {
        Statistics::
        todo!()
    }

    async fn create_physical_plan(&self, state: &SessionState, conf: FileScanConfig, filters: Option<&Arc<dyn PhysicalExpr>>) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        todo!()
    }
}