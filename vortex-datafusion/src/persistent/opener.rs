use std::sync::{Arc, RwLock};

use arrow_array::builder::BooleanBufferBuilder;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::datasource::physical_plan::{FileMeta, FileOpenFuture, FileOpener};
use datafusion_common::Result as DFResult;
use datafusion_physical_expr::PhysicalExpr;
use futures::{FutureExt as _, StreamExt, TryStreamExt};
use object_store::ObjectStore;
use vortex::array::BoolArray;
use vortex::{Array, Context, IntoArray, IntoArrayVariant as _};
use vortex_error::VortexResult;
use vortex_expr::datafusion::convert_expr_to_vortex;
use vortex_expr::VortexExpr;
use vortex_serde::io::{ObjectStoreReadAt, VortexReadAt};
use vortex_serde::layouts::{
    null_as_false, LayoutContext, LayoutDeserializer, LayoutMessageCache, LayoutReaderBuilder,
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

        // if let Some(expr) = expr.as_ref() {
        //     builder = builder.with_row_filter(vortex_serde::layouts::RowFilter::new(expr.clone()));
        // }

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

async fn build_selection<R: VortexReadAt + Unpin + Send + 'static>(
    reader: R,
    expr: Arc<dyn VortexExpr>,
    deserializer: LayoutDeserializer,
    message_cache: Arc<RwLock<LayoutMessageCache>>,
) -> VortexResult<Array> {
    let mut builder = LayoutReaderBuilder::new(reader, deserializer);
    let footer = builder.read_footer().await?;
    let fields =
        footer.resolve_references(expr.references().into_iter().collect::<Vec<_>>().as_ref())?;
    let projection = Projection::Flat(fields);
    builder = builder.with_projection(projection);
    builder = builder.with_message_cache(message_cache);

    let mut stream = builder.build().await?;
    let mut bool_builder = BooleanBufferBuilder::new(0);

    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let chunk = expr.evaluate(&batch)?;
        let bool = null_as_false(chunk.into_bool()?)?.into_bool()?;
        bool_builder.append_buffer(&bool.boolean_buffer());
    }

    BoolArray::try_new(
        bool_builder.finish(),
        vortex::validity::Validity::NonNullable,
    )
    .map(|a| a.into_array())
}
