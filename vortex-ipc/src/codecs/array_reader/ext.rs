use std::pin::Pin;

use vortex::Array;
use vortex_error::VortexResult;

use crate::codecs::array_reader::take::Take;
use crate::codecs::array_reader::ArrayReaderAdapter;
use crate::codecs::ArrayReader;

pub trait ArrayReaderExt: ArrayReader {
    fn pinned(self) -> Pin<Box<dyn ArrayReader>>
    where
        Self: Sized + 'static,
    {
        Box::pin(self)
    }

    fn take_indices(self, indices: &Array) -> VortexResult<impl ArrayReader>
    where
        Self: Sized,
    {
        Ok(ArrayReaderAdapter::new(
            self.dtype().clone(),
            Take::try_new(self, indices)?,
        ))
    }
}

impl<R: ArrayReader> ArrayReaderExt for R {}
