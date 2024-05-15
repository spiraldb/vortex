use vortex::Array;
use vortex_error::VortexResult;

use crate::array_stream::take_rows::TakeRows;
use crate::array_stream::ArrayStream;
use crate::array_stream::ArrayStreamAdapter;

pub trait ArrayStreamExt: ArrayStream {
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
