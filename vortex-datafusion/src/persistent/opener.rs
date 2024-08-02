use std::sync::Arc;

use datafusion::datasource::physical_plan::{FileMeta, FileOpenFuture, FileOpener};
use datafusion_common::Result as DFResult;
use futures::FutureExt;
use object_store::ObjectStore;
use vortex_serde::file::reader::VortexBatchReaderBuilder;
use vortex_serde::io::ObjectStoreReadAt;

pub struct VortexFileOpener {
    object_store: Arc<dyn ObjectStore>,
}

impl FileOpener for VortexFileOpener {
    fn open(&self, file_meta: FileMeta) -> DFResult<FileOpenFuture> {
        let object_store = self.object_store.clone();
        Ok(async move {
            let read_at = ObjectStoreReadAt::new(&object_store, file_meta.location());

            let _reader = VortexBatchReaderBuilder::new(read_at)
                .build()
                .await
                .unwrap();
            todo!()
        }
        .boxed())
    }
}
