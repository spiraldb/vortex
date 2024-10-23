use std::future::Future;
use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, SchemaRef};
use futures::StreamExt;
use vortex::arrow::infer_schema;
use vortex::Array;
use vortex_error::{VortexError, VortexResult};

use super::LayoutBatchStream;
use crate::io::VortexReadAt;

fn vortex_to_arrow_error(error: VortexError) -> ArrowError {
    ArrowError::ExternalError(Box::new(error))
}

fn vortex_to_arrow(result: VortexResult<Array>) -> Result<RecordBatch, ArrowError> {
    result
        .and_then(RecordBatch::try_from)
        .map_err(vortex_to_arrow_error)
}

pub trait AsyncRuntime {
    fn block_on<F: Future>(&self, fut: F) -> F::Output;
}

pub struct VortexRecordBatchReader<'a, R, S>
where
    S: AsyncRuntime,
{
    stream: LayoutBatchStream<R>,
    arrow_schema: SchemaRef,
    spawn: &'a S,
}

impl<'a, R, S> VortexRecordBatchReader<'a, R, S>
where
    R: VortexReadAt + Unpin + 'static,
    S: AsyncRuntime,
{
    pub fn new(
        stream: LayoutBatchStream<R>,
        spawn: &'a S,
    ) -> VortexResult<VortexRecordBatchReader<'a, R, S>> {
        let arrow_schema = Arc::new(infer_schema(stream.schema().dtype())?);
        Ok(VortexRecordBatchReader {
            stream,
            arrow_schema,
            spawn,
        })
    }
}

impl<'a, R, S> Iterator for VortexRecordBatchReader<'a, R, S>
where
    R: VortexReadAt + Unpin + 'static,
    S: AsyncRuntime,
{
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let maybe_result = self.spawn.block_on(self.stream.next());
        maybe_result.map(vortex_to_arrow)
    }
}

impl<'a, R, S> RecordBatchReader for VortexRecordBatchReader<'a, R, S>
where
    R: VortexReadAt + Unpin + 'static,
    S: AsyncRuntime,
{
    fn schema(&self) -> SchemaRef {
        self.arrow_schema.clone()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        self.next().transpose()
    }
}
