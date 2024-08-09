use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::datasource::physical_plan::{FileMeta, FileOpenFuture, FileOpener};
use datafusion_common::Result as DFResult;
use datafusion_physical_expr::PhysicalExpr;
use futures::{FutureExt as _, TryStreamExt};
use object_store::ObjectStore;
use vortex_error::VortexResult;
use vortex_serde::io::ObjectStoreReadAt;
use vortex_serde::layouts::reader::builder::VortexLayoutReaderBuilder;
use vortex_serde::layouts::reader::context::LayoutDeserializer;
use vortex_serde::layouts::reader::projections::Projection;

use crate::expr::convert_expr_to_vortex;

pub struct VortexFileOpener {
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

        let mut builder = VortexLayoutReaderBuilder::new(read_at, LayoutDeserializer::default());

        if let Some(batch_size) = self.batch_size {
            builder = builder.with_batch_size(batch_size);
        }

        let predicate = if let Some(predicate) = self.predicate.as_ref() {
            if let Ok(vortex_predicate) =
                convert_expr_to_vortex(predicate.clone(), self.arrow_schema.as_ref())
            {
                log::info!("got some predicate here!");
                Some(vortex_predicate)
            } else {
                None
            }
        } else {
            None
        };

        if let Some(projection) = self.projection.as_ref() {
            builder = builder.with_projection(Projection::new(projection))
        }

        Ok(async move {
            let reader = builder.build().await?;

            let stream = reader
                .and_then(move |array| {
                    let predicate = predicate.clone();
                    async move {
                        let array = if let Some(predicate) = predicate.as_ref() {
                            // println!("eval!");
                            let predicate_result = predicate.evaluate(&array)?;

                            vortex::compute::filter(&array, &predicate_result)?
                        } else {
                            array
                        };

                        VortexResult::Ok(RecordBatch::from(array))
                    }
                })
                .map_err(|e| e.into());
            Ok(Box::pin(stream) as _)
        }
        .boxed())
    }
}
