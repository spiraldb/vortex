use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::arrow::compute::prep_null_mask_filter;
use datafusion::datasource::physical_plan::{FileMeta, FileOpenFuture, FileOpener};
use datafusion_common::{DataFusionError, Result as DFResult};
use datafusion_physical_expr::PhysicalExpr;
use futures::{FutureExt as _, TryStreamExt};
use itertools::Itertools;
use object_store::ObjectStore;
use vortex::array::BoolArray;
use vortex::arrow::FromArrowArray;
use vortex::{Array, Context, IntoArrayVariant as _, IntoCanonical};
use vortex_error::VortexResult;
use vortex_serde::io::ObjectStoreReadAt;
use vortex_serde::layouts::reader::builder::LayoutReaderBuilder;
use vortex_serde::layouts::reader::context::{LayoutContext, LayoutDeserializer};
use vortex_serde::layouts::reader::projections::Projection;

use crate::expr::{convert_expr_to_vortex, extract_columns_from_expr, VortexPhysicalExpr};

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

        let predicate = self
            .predicate
            .clone()
            .map(|predicate| -> DFResult<Arc<dyn VortexPhysicalExpr>> {
                let vtx_expr = convert_expr_to_vortex(predicate, self.arrow_schema.as_ref())
                    .map_err(|e| DataFusionError::External(e.into()))?;

                DFResult::Ok(vtx_expr)
            })
            .transpose()?;

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
                .and_then(move |array| {
                    let predicate = predicate.clone();
                    async move {
                        let array = if let Some(predicate) = predicate {
                            let predicate_result = predicate.evaluate(&array)?;

                            let filter_array = null_as_false(predicate_result.into_bool()?)?;
                            vortex::compute::filter(&array, &filter_array)?
                        } else {
                            array
                        };

                        let rb = RecordBatch::from(array);

                        // If we had a projection, we cut the record batch down to the desired columns
                        if let Some(len) = original_projection_len {
                            Ok(rb.project(&(0..len).collect_vec())?)
                        } else {
                            Ok(rb)
                        }
                    }
                })
                .map_err(|e| e.into());
            Ok(Box::pin(stream) as _)
        }
        .boxed())
    }
}

/// Mask all null values of a Arrow boolean array to false
fn null_as_false(array: BoolArray) -> VortexResult<Array> {
    let arrow_array = array.into_canonical()?.into_arrow();
    let boolean_array = match arrow_array.null_count() {
        0 => arrow_array.as_boolean(),
        _ => &prep_null_mask_filter(arrow_array.as_boolean()),
    };

    Ok(Array::from_arrow(boolean_array, false))
}

#[cfg(test)]
mod tests {
    use vortex::array::BoolArray;
    use vortex::validity::Validity;
    use vortex::IntoArrayVariant;

    use crate::persistent::opener::null_as_false;

    #[test]
    fn coerces_nulls() {
        let bool_array = BoolArray::from_vec(
            vec![true, true, false, false],
            Validity::Array(BoolArray::from(vec![true, false, true, false]).into()),
        );
        let non_null_array = null_as_false(bool_array).unwrap().into_bool().unwrap();
        assert_eq!(
            non_null_array.boolean_buffer().iter().collect::<Vec<_>>(),
            vec![true, false, false, false]
        );
    }
}
