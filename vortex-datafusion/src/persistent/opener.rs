use std::sync::Arc;

use arrow_array::{Array as _, BooleanArray, RecordBatch};
use arrow_schema::SchemaRef;
use datafusion::arrow::buffer::{buffer_bin_and_not, BooleanBuffer};
use datafusion::datasource::physical_plan::{FileMeta, FileOpenFuture, FileOpener};
use datafusion_common::Result as DFResult;
use datafusion_physical_expr::PhysicalExpr;
use futures::{FutureExt as _, TryStreamExt};
use object_store::ObjectStore;
use vortex::array::BoolArray;
use vortex::arrow::FromArrowArray;
use vortex::{Array, Context, IntoArrayVariant as _};
use vortex_error::VortexResult;
use vortex_serde::io::ObjectStoreReadAt;
use vortex_serde::layouts::reader::builder::VortexLayoutReaderBuilder;
use vortex_serde::layouts::reader::context::{LayoutContext, LayoutDeserializer};
use vortex_serde::layouts::reader::projections::Projection;

use crate::expr::convert_expr_to_vortex;

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

        let mut builder = VortexLayoutReaderBuilder::new(
            read_at,
            LayoutDeserializer::new(self.ctx.clone(), Arc::new(LayoutContext::default())),
        );

        if let Some(batch_size) = self.batch_size {
            builder = builder.with_batch_size(batch_size);
        }

        let predicate = self
            .predicate
            .clone()
            .map(|predicate| convert_expr_to_vortex(predicate, self.arrow_schema.as_ref()))
            .transpose()?;

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
                            let predicate_result = predicate.evaluate(&array)?;

                            let filter_array = null_as_false(&predicate_result.into_bool()?)?;
                            vortex::compute::filter(&array, &filter_array)?
                        } else {
                            array
                        };

                        RecordBatch::try_from(array)
                    }
                })
                .map_err(|e| e.into());
            Ok(Box::pin(stream) as _)
        }
        .boxed())
    }
}

/// Mask all null values of a Arrow boolean array to false
fn null_as_false(array: &BoolArray) -> VortexResult<Array> {
    let array = BooleanArray::from(array.boolean_buffer());

    let boolean_array = match array.nulls() {
        None => array,
        Some(nulls) => {
            let inner_bool_buffer = array.values();
            let buff = buffer_bin_and_not(
                inner_bool_buffer.inner(),
                inner_bool_buffer.offset(),
                nulls.buffer(),
                nulls.offset(),
                inner_bool_buffer.len(),
            );
            let bool_buffer =
                BooleanBuffer::new(buff, inner_bool_buffer.offset(), inner_bool_buffer.len());
            BooleanArray::from(bool_buffer)
        }
    };

    Ok(Array::from_arrow(&boolean_array, false))
}
