use std::fmt;
use std::sync::Arc;

use datafusion::datasource::physical_plan::{FileScanConfig, FileStream};
use datafusion_common::Result as DFResult;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, PlanProperties,
};

use crate::persistent::opener::VortexFileOpener;

#[derive(Debug)]
pub struct VortexExec {
    file_scan_config: FileScanConfig,
    metrics: ExecutionPlanMetricsSet,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    plan_properties: PlanProperties,
    projection: Option<Vec<usize>>,
}

impl VortexExec {
    pub fn try_new(
        file_scan_config: FileScanConfig,
        metrics: ExecutionPlanMetricsSet,
        projection: Option<&Vec<usize>>,
        predicate: Option<Arc<dyn PhysicalExpr>>,
    ) -> DFResult<Self> {
        let partitioning = Partitioning::UnknownPartitioning(1);
        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(file_scan_config.file_schema.clone()),
            partitioning,
            ExecutionMode::Bounded,
        );
        let projection = projection.cloned();

        Ok(Self {
            file_scan_config,
            metrics,
            predicate,
            projection,
            plan_properties,
        })
    }
    pub(crate) fn into_arc(self) -> Arc<dyn ExecutionPlan> {
        Arc::new(self) as _
    }
}

impl DisplayAs for VortexExec {
    fn fmt_as(&self, _t: DisplayFormatType, _f: &mut fmt::Formatter) -> fmt::Result {
        todo!()
    }
}

impl ExecutionPlan for VortexExec {
    fn name(&self) -> &str {
        "VortexExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
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
        let object_store = context
            .runtime_env()
            .object_store(&self.file_scan_config.object_store_url)?;
        let opener = VortexFileOpener {
            object_store,
            projection: self.projection.clone(),
            batch_size: None,
            predicate: self.predicate.clone(),
        };
        let stream = FileStream::new(&self.file_scan_config, partition, opener, &self.metrics)?;

        Ok(Box::pin(stream))
    }
}
