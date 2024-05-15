use std::future::Future;

use futures_util::TryFutureExt;
use futures_util::TryStreamExt;
use vortex::array::chunked::ChunkedArray;
use vortex::{Array, IntoArray};
use vortex_error::VortexResult;

use crate::array_stream::take_rows::TakeRows;
use crate::array_stream::ArrayStream;
use crate::array_stream::ArrayStreamAdapter;

pub trait ArrayStreamExt: ArrayStream {
    fn collect(self) -> impl Future<Output = VortexResult<Array>>
    where
        Self: Sized,
    {
        self.collect_chunked()
            .map_ok(|chunked| chunked.into_array())
    }

    fn collect_chunked(self) -> impl Future<Output = VortexResult<ChunkedArray>>
    where
        Self: Sized,
    {
        async {
            let dtype = self.dtype().clone();
            let chunks: Vec<Array> = self.try_collect().await.unwrap();
            ChunkedArray::try_new(chunks, dtype)
        }
    }

    fn take_rows(self, indices: &Array) -> VortexResult<impl ArrayStream>
    where
        Self: Sized,
    {
        Ok(ArrayStreamAdapter::new(
            self.dtype().clone(),
            TakeRows::try_new(self, indices)?,
        ))
    }
}

impl<R: ArrayStream> ArrayStreamExt for R {}
