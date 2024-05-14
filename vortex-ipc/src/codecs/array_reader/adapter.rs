use std::pin::Pin;
use std::task::Poll;

use futures_util::Stream;
use pin_project::pin_project;
use vortex::Array;
use vortex_dtype::DType;
use vortex_error::VortexResult;

use crate::codecs::ArrayReader;

/// An adapter for a stream of array chunks to implement an ArrayReader.
#[pin_project]
pub struct ArrayReaderAdapter<S> {
    dtype: DType,
    #[pin]
    inner: S,
}

impl<S> ArrayReaderAdapter<S> {
    pub fn new(dtype: DType, inner: S) -> Self {
        Self { dtype, inner }
    }
}

impl<S> ArrayReader for ArrayReaderAdapter<S>
where
    S: Stream<Item = VortexResult<Array>>,
{
    fn dtype(&self) -> &DType {
        &self.dtype
    }
}

impl<S> Stream for ArrayReaderAdapter<S>
where
    S: Stream<Item = VortexResult<Array>>,
{
    type Item = VortexResult<Array>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.project().inner.poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}
