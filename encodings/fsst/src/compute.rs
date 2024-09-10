use vortex::array::varbin_scalar;
use vortex::compute::unary::{scalar_at_unchecked, ScalarAtFn};
use vortex::compute::{filter, slice, take, ArrayCompute, FilterFn, SliceFn, TakeFn};
use vortex::{Array, ArrayDType, IntoArray};
use vortex_buffer::Buffer;
use vortex_error::VortexResult;
use vortex_scalar::Scalar;

use crate::FSSTArray;

impl ArrayCompute for FSSTArray {
    fn filter(&self) -> Option<&dyn FilterFn> {
        Some(self)
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn slice(&self) -> Option<&dyn SliceFn> {
        Some(self)
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }
}

impl SliceFn for FSSTArray {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<Array> {
        // Slicing an FSST array leaves the symbol table unmodified,
        // only slicing the `codes` array.
        Ok(Self::try_new(
            self.dtype().clone(),
            self.symbols(),
            self.symbol_lengths(),
            slice(&self.codes(), start, stop)?,
            slice(&self.uncompressed_lengths(), start, stop)?,
        )?
        .into_array())
    }
}

impl TakeFn for FSSTArray {
    // Take on an FSSTArray is a simple take on the codes array.
    fn take(&self, indices: &Array) -> VortexResult<Array> {
        Ok(Self::try_new(
            self.dtype().clone(),
            self.symbols(),
            self.symbol_lengths(),
            take(&self.codes(), indices)?,
            take(&self.uncompressed_lengths(), indices)?,
        )?
        .into_array())
    }
}

impl ScalarAtFn for FSSTArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        Ok(self.scalar_at_unchecked(index))
    }

    fn scalar_at_unchecked(&self, index: usize) -> Scalar {
        let compressed = scalar_at_unchecked(&self.codes(), index);
        let binary_datum = compressed.value().as_buffer().unwrap().unwrap();

        self.with_decompressor(|decompressor| {
            let decoded_buffer: Buffer = decompressor.decompress(binary_datum.as_slice()).into();

            varbin_scalar(decoded_buffer, self.dtype())
        })
    }
}

impl FilterFn for FSSTArray {
    // Filtering an FSSTArray filters the codes array, leaving the symbols array untouched
    fn filter(&self, predicate: &Array) -> VortexResult<Array> {
        Ok(Self::try_new(
            self.dtype().clone(),
            self.symbols(),
            self.symbol_lengths(),
            filter(&self.codes(), predicate)?,
            filter(&self.uncompressed_lengths(), predicate)?,
        )?
        .into_array())
    }
}
