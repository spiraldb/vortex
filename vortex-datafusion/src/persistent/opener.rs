use std::sync::Arc;

use arrow_array::RecordBatch;
use datafusion::datasource::physical_plan::{FileMeta, FileOpenFuture, FileOpener};
use datafusion_common::Result as DFResult;
use datafusion_physical_expr::PhysicalExpr;
use futures::{FutureExt as _, TryStreamExt};
use object_store::ObjectStore;
use vortex::Context;
use vortex_serde::io::ObjectStoreReadAt;
use vortex_serde::layouts::reader::builder::VortexLayoutReaderBuilder;
use vortex_serde::layouts::reader::context::{LayoutContext, LayoutDeserializer};
use vortex_serde::layouts::reader::projections::Projection;

pub struct VortexFileOpener {
    pub ctx: Arc<Context>,
    pub object_store: Arc<dyn ObjectStore>,
    pub batch_size: Option<usize>,
    pub projection: Option<Vec<usize>>,
    pub predicate: Option<Arc<dyn PhysicalExpr>>,
}

impl FileOpener for VortexFileOpener {
    fn open(&self, file_meta: FileMeta) -> DFResult<FileOpenFuture> {
        let read_at =
            ObjectStoreReadAt::new(self.object_store.clone(), file_meta.location().clone());

        let mut builder = VortexLayoutReaderBuilder::new(
            read_at,
            LayoutDeserializer::new(self.ctx.clone(), Arc::new(LayoutContext::default())),
        );

        if let Some(batch_size) = self.batch_size {
            builder = builder.with_batch_size(batch_size);
        }

        if let Some(_predicate) = self.predicate.as_ref() {
            log::warn!("Missing logic to turn a physical expression into a RowFilter");
        }

        if let Some(projection) = self.projection.as_ref() {
            builder = builder.with_projection(Projection::new(projection))
        }

        Ok(async move {
            let reader = builder.build().await?;
            let stream = reader
                .map_ok(RecordBatch::from)
                .map_err(std::convert::Into::into);
            Ok(Box::pin(stream) as _)
        }
        .boxed())
    }
}
