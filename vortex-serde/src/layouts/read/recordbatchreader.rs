use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, SchemaRef};
use futures::StreamExt;
use vortex::arrow::infer_schema;
use vortex::Array;
use vortex_error::{VortexError, VortexResult};

use super::LayoutBatchStream;
use crate::io::{VortexReadAt, TOKIO_RUNTIME};

fn vortex_to_arrow_error(error: VortexError) -> ArrowError {
    ArrowError::ExternalError(Box::new(error))
}

fn vortex_to_arrow(result: VortexResult<Array>) -> Result<RecordBatch, ArrowError> {
    result
        .and_then(RecordBatch::try_from)
        .map_err(vortex_to_arrow_error)
}

pub struct VortexRecordBatchReader<R: VortexReadAt + Unpin + 'static> {
    stream: LayoutBatchStream<R>,
    arrow_schema: SchemaRef,
}

impl<R: VortexReadAt + Unpin + 'static> VortexRecordBatchReader<R> {
    pub fn new(stream: LayoutBatchStream<R>) -> VortexResult<VortexRecordBatchReader<R>> {
        let arrow_schema = Arc::new(infer_schema(stream.schema().dtype())?);
        Ok(VortexRecordBatchReader {
            stream,
            arrow_schema,
        })
    }
}

impl<R: VortexReadAt + Unpin + 'static> Iterator for VortexRecordBatchReader<R> {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let maybe_result = TOKIO_RUNTIME.block_on(self.stream.next());
        maybe_result.map(vortex_to_arrow)
    }
}

impl<R: VortexReadAt + Unpin + 'static> RecordBatchReader for VortexRecordBatchReader<R> {
    fn schema(&self) -> SchemaRef {
        self.arrow_schema.clone()
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        self.next().transpose()
    }
}
