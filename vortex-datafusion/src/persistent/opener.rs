use std::sync::{Arc, RwLock};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::datasource::physical_plan::{FileMeta, FileOpenFuture, FileOpener};
use datafusion_common::Result as DFResult;
use datafusion_physical_expr::PhysicalExpr;
use futures::{FutureExt as _, StreamExt, TryStreamExt};
use object_store::ObjectStore;
use vortex::Context;
use vortex_expr::datafusion::convert_expr_to_vortex;
use vortex_serde::io::ObjectStoreReadAt;
use vortex_serde::layouts::{
    build_selection, LayoutContext, LayoutDeserializer, LayoutMessageCache, LayoutReaderBuilder,
    Projection,
};

pub struct VortexFileOpener {
    pub ctx: Arc<Context>,
    pub object_store: Arc<dyn ObjectStore>,
    pub batch_size: Option<usize>,
    pub projection: Option<Vec<usize>>,
    pub predicate: Option<Arc<dyn PhysicalExpr>>,
    pub arrow_schema: SchemaRef,
}

impl FileOpener for VortexFileOpener {
    fn open(&self, file_meta: FileMeta) -> DFResult<FileOpenFuture> {
        let read_at =
            ObjectStoreReadAt::new(self.object_store.clone(), file_meta.location().clone());

        let deserializer =
            LayoutDeserializer::new(self.ctx.clone(), Arc::new(LayoutContext::default()));

        let message_cache = Arc::new(RwLock::new(LayoutMessageCache::default()));

        let mut builder = LayoutReaderBuilder::new(read_at.clone(), deserializer.clone())
            .with_message_cache(message_cache.clone());

        if let Some(batch_size) = self.batch_size {
            builder = builder.with_batch_size(batch_size);
        }

        let expr = self
            .predicate
            .clone()
            .map(convert_expr_to_vortex)
            .transpose()?;

        if let Some(projection) = self.projection.as_ref() {
            builder = builder.with_projection(Projection::new(projection))
        }

        Ok(async {
            if let Some(expr) = expr {
                let selection = build_selection(read_at, expr, deserializer, message_cache).await?;
                builder = builder.with_row_selection(selection);
            }

            Ok(Box::pin(
                builder
                    .build()
                    .await?
                    .map_ok(RecordBatch::try_from)
                    .map(|r| r.and_then(|inner| inner))
                    .map_err(|e| e.into()),
            ) as _)
        }
        .boxed())
    }
}
