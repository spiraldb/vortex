use std::any::Any;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionState;
use datafusion_common::{DFSchema, Result as DFResult, Statistics};
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_expr::utils::conjunction;
use datafusion_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_physical_plan::ExecutionPlan;

use super::config::VortexTableConfig;
use crate::persistent::execution::VortexExec;

#[allow(dead_code)]
pub struct VortexFileTableProvider {
    schema_ref: SchemaRef,
    object_store_url: ObjectStoreUrl,
    config: VortexTableConfig,
}

impl VortexFileTableProvider {
    pub fn try_new(object_store_url: ObjectStoreUrl, config: VortexTableConfig) -> DFResult<Self> {
        Ok(Self {
            schema_ref: config.schema.clone().unwrap(),
            object_store_url,
            config,
        })
    }
}

#[async_trait]
impl TableProvider for VortexFileTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema_ref)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let df_schema = DFSchema::try_from(self.schema())?;
        let predicate = conjunction(filters.to_vec());
        let predicate = predicate
            .map(|predicate| state.create_physical_expr(predicate, &df_schema))
            .transpose()?;

        let metrics = ExecutionPlanMetricsSet::new();

        // TODO: Point at some files and/or ranges
        let file_scan_config = FileScanConfig::new(self.object_store_url.clone(), self.schema())
            .with_file_group(
                self.config
                    .data_files
                    .iter()
                    .cloned()
                    .map(|f| f.into())
                    .collect(),
            );

        let exec =
            VortexExec::try_new(file_scan_config, metrics, projection, predicate)?.into_arc();

        Ok(exec)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }

    fn statistics(&self) -> Option<Statistics> {
        None
    }
}
