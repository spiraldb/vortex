use std::pin::Pin;
use std::task::{Context, Poll};

use futures_util::Stream;
use pin_project::pin_project;
use vortex::OwnedArray;
use vortex_dtype::DType;
use vortex_error::VortexResult;

/// NOTE: similar to Arrow RecordBatchReader.
pub trait ArrayReader: Stream<Item = VortexResult<OwnedArray>> {
    #[allow(dead_code)]
    fn dtype(&self) -> &DType;
}

/// Wrap a DType with a stream of array chunks to implement an ArrayReader.
#[pin_project]
pub struct ArrayReaderImpl<S> {
    dtype: DType,
    #[pin]
    inner: S,
}

impl<S: Stream<Item = VortexResult<OwnedArray>>> ArrayReader for ArrayReaderImpl<S> {
    #[inline]
    fn dtype(&self) -> &DType {
        &self.dtype
    }
}

impl<S: Stream<Item = VortexResult<OwnedArray>>> Stream for ArrayReaderImpl<S> {
    type Item = VortexResult<OwnedArray>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().inner.poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}
