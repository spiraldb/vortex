use datafusion::datasource::physical_plan::{FileMeta, FileOpenFuture, FileOpener};
use datafusion_common::Result as DFResult;

pub mod execution;
pub mod table;

pub struct VortexFileOpener {}

impl FileOpener for VortexFileOpener {
    fn open(&self, _file_meta: FileMeta) -> DFResult<FileOpenFuture> {
        todo!()
    }
}

#[cfg(test)]
mod tests {}
