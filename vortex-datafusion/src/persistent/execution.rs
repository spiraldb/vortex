use std::fmt;
use std::sync::Arc;

use datafusion::datasource::physical_plan::{FileScanConfig, FileStream};
use datafusion_common::Result as DFResult;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};

use crate::persistent::opener::VortexFileOpener;

#[derive(Debug)]
pub struct VortexExec {
    file_scan_config: FileScanConfig,
    metrics: ExecutionPlanMetricsSet,
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
        todo!()
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
        let opener = VortexFileOpener { object_store };
        let stream = FileStream::new(&self.file_scan_config, partition, opener, &self.metrics)?;

        Ok(Box::pin(stream))
    }
}
