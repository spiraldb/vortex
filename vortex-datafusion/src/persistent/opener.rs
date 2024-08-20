use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::datasource::physical_plan::{FileMeta, FileOpenFuture, FileOpener};
use datafusion_common::Result as DFResult;
use datafusion_physical_expr::PhysicalExpr;
use futures::{FutureExt as _, TryStreamExt};
use itertools::Itertools;
use object_store::ObjectStore;
use vortex::Context;
use vortex_expr::datafusion::{convert_expr_to_vortex, extract_columns_from_expr};
use vortex_serde::io::ObjectStoreReadAt;
use vortex_serde::layouts::reader::builder::LayoutReaderBuilder;
use vortex_serde::layouts::reader::context::{LayoutContext, LayoutDeserializer};
use vortex_serde::layouts::reader::filtering::RowFilter;
use vortex_serde::layouts::reader::projections::Projection;

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

        let mut builder = LayoutReaderBuilder::new(
            read_at,
            LayoutDeserializer::new(self.ctx.clone(), Arc::new(LayoutContext::default())),
        );

        if let Some(batch_size) = self.batch_size {
            builder = builder.with_batch_size(batch_size);
        }

        let predicate_projection =
            extract_columns_from_expr(self.predicate.as_ref(), self.arrow_schema.clone())?;

        if let Some(predicate) = self
            .predicate
            .clone()
            .map(convert_expr_to_vortex)
            .transpose()?
        {
            builder = builder.with_row_filter(RowFilter::new(predicate));
        }

        if let Some(projection) = self.projection.as_ref() {
            let mut projection = projection.clone();
            for col_idx in predicate_projection.into_iter() {
                if !projection.contains(&col_idx) {
                    projection.push(col_idx);
                }
            }

            builder = builder.with_projection(Projection::new(projection))
        }

        let original_projection_len = self.projection.as_ref().map(|v| v.len());

        Ok(async move {
            let reader = builder.build().await?;

            let stream = reader
                .and_then(move |array| async move {
                    let rb = RecordBatch::from(array);

                    // If we had a projection, we cut the record batch down to the desired columns
                    if let Some(len) = original_projection_len {
                        Ok(rb.project(&(0..len).collect_vec())?)
                    } else {
                        Ok(rb)
                    }
                })
                .map_err(|e| e.into());
            Ok(Box::pin(stream) as _)
        }
        .boxed())
    }
}
