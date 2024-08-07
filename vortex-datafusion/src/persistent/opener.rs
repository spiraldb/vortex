use std::sync::Arc;

use arrow_array::cast::as_struct_array;
use arrow_array::RecordBatch;
use datafusion::datasource::physical_plan::{FileMeta, FileOpenFuture, FileOpener};
use datafusion_common::Result as DFResult;
use datafusion_physical_expr::PhysicalExpr;
use futures::{FutureExt as _, TryStreamExt};
use object_store::ObjectStore;
use vortex::IntoCanonical;
use vortex_serde::io::ObjectStoreReadAt;
use vortex_serde::layouts::reader::builder::VortexLayoutReaderBuilder;
use vortex_serde::layouts::reader::context::LayoutDeserializer;
use vortex_serde::layouts::reader::projections::Projection;

pub struct VortexFileOpener {
    pub object_store: Arc<dyn ObjectStore>,
    pub batch_size: Option<usize>,
    pub projection: Option<Vec<usize>>,
    pub predicate: Option<Arc<dyn PhysicalExpr>>,
}

impl FileOpener for VortexFileOpener {
    fn open(&self, file_meta: FileMeta) -> DFResult<FileOpenFuture> {
        let read_at =
            ObjectStoreReadAt::new(self.object_store.clone(), file_meta.location().clone());

        let mut builder = VortexLayoutReaderBuilder::new(read_at, LayoutDeserializer::default());

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
                .map_ok(|array| {
                    let arrow = array
                        .into_canonical()
                        .expect("struct arrays must canonicalize")
                        .into_arrow();
                    let struct_array = as_struct_array(arrow.as_ref());
                    let rb = RecordBatch::from(struct_array);

                    rb
                })
                .map_err(|e| e.into());

            Ok(Box::pin(stream) as _)
        }
        .boxed())
    }
}
