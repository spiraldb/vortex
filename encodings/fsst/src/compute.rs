use vortex::array::varbin_scalar;
use vortex::compute::unary::{scalar_at_unchecked, ScalarAtFn};
use vortex::compute::{filter, slice, take, ArrayCompute, FilterFn, SliceFn, TakeFn};
use vortex::{Array, ArrayDType, IntoArray};
use vortex_buffer::Buffer;
use vortex_error::{vortex_err, VortexResult, VortexUnwrap};
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
            slice(self.codes(), start, stop)?,
            slice(self.uncompressed_lengths(), start, stop)?,
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
            take(self.codes(), indices)?,
            take(self.uncompressed_lengths(), indices)?,
        )?
        .into_array())
    }
}

impl ScalarAtFn for FSSTArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        let compressed = scalar_at_unchecked(self.codes(), index);
        let binary_datum = compressed
            .value()
            .as_buffer()?
            .ok_or_else(|| vortex_err!("Expected a binary scalar, found {}", compressed.dtype()))?;

        self.with_decompressor(|decompressor| {
            let decoded_buffer: Buffer = decompressor.decompress(binary_datum.as_slice()).into();
            Ok(varbin_scalar(decoded_buffer, self.dtype()))
        })
    }

    fn scalar_at_unchecked(&self, index: usize) -> Scalar {
        <Self as ScalarAtFn>::scalar_at(self, index).vortex_unwrap()
    }
}

impl FilterFn for FSSTArray {
    // Filtering an FSSTArray filters the codes array, leaving the symbols array untouched
    fn filter(&self, predicate: &Array) -> VortexResult<Array> {
        Ok(Self::try_new(
            self.dtype().clone(),
            self.symbols(),
            self.symbol_lengths(),
            filter(self.codes(), predicate)?,
            filter(self.uncompressed_lengths(), predicate)?,
        )?
        .into_array())
    }
}
