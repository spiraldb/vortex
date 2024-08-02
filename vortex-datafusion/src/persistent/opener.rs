use std::sync::Arc;

use arrow_array::cast::as_struct_array;
use arrow_array::RecordBatch;
use datafusion::datasource::physical_plan::{FileMeta, FileOpenFuture, FileOpener};
use datafusion_common::Result as DFResult;
use futures::{FutureExt as _, TryStreamExt};
use object_store::ObjectStore;
use vortex::IntoCanonical;
use vortex_serde::file::reader::VortexBatchReaderBuilder;
use vortex_serde::io::ObjectStoreReadAt;

pub struct VortexFileOpener {
    pub object_store: Arc<dyn ObjectStore>,
}

impl FileOpener for VortexFileOpener {
    fn open(&self, file_meta: FileMeta) -> DFResult<FileOpenFuture> {
        let object_store = self.object_store.clone();
        DFResult::Ok(
            async move {
                let read_at = ObjectStoreReadAt::new(object_store, file_meta.location().clone());

                let reader = VortexBatchReaderBuilder::new(read_at).build().await?;

                let stream = reader
                    .map_ok(|array| {
                        let arrow = array
                            .into_canonical()
                            .expect("struct arrays must canonicalize")
                            .into_arrow();
                        let struct_array = as_struct_array(arrow.as_ref());
                        RecordBatch::from(struct_array)
                    })
                    .map_err(|e| e.into());

                DFResult::Ok(Box::pin(stream) as _)
            }
            .boxed(),
        )
    }
}
