use vortex::Array;
use vortex_error::VortexResult;

use super::take::Take;
use crate::codecs::array_reader::ArrayReaderAdapter;
use crate::codecs::ArrayReader;

pub trait ArrayReaderExt: ArrayReader {
    fn take_rows(self, indices: &Array) -> VortexResult<impl ArrayReader>
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
