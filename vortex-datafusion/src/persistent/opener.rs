use std::sync::Arc;

use datafusion::datasource::physical_plan::{FileMeta, FileOpenFuture, FileOpener};
use datafusion_common::Result as DFResult;
use object_store::ObjectStore;

pub struct VortexFileOpener {
    object_store: Arc<dyn ObjectStore>,
}

impl FileOpener for VortexFileOpener {
    fn open(&self, _file_meta: FileMeta) -> DFResult<FileOpenFuture> {
        // let object_store =
        todo!()
    }
}
