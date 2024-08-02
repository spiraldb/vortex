use std::sync::Arc;

use arrow_array::{RecordBatch, StructArray as ArrowStructArray};
use arrow_schema::ArrowError;
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

                let reader = VortexBatchReaderBuilder::new(read_at)
                    .build()
                    .await
                    .unwrap();

                let stream = reader
                    .map_ok(|a| {
                        RecordBatch::from(
                            a.into_canonical()
                                .expect("struct arrays must canonicalize")
                                .into_arrow()
                                .as_any()
                                .downcast_ref::<ArrowStructArray>()
                                .expect("vortex StructArray must convert to arrow StructArray"),
                        )
                    })
                    .map_err(|e| ArrowError::from_external_error(Box::new(e)));

                DFResult::Ok(Box::pin(stream) as _)
            }
            .boxed(),
        )
    }
}
