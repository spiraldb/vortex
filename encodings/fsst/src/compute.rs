use vortex::array::varbin_scalar;
use vortex::compute::unary::{scalar_at, ScalarAtFn};
use vortex::compute::{filter, slice, take, ArrayCompute, FilterFn, SliceFn, TakeFn};
use vortex::validity::ArrayValidity;
use vortex::{Array, ArrayDType, IntoArray};
use vortex_buffer::Buffer;
use vortex_error::{vortex_bail, VortexResult};
use vortex_scalar::Scalar;

use crate::FSSTArray;

impl ArrayCompute for FSSTArray {
    fn slice(&self) -> Option<&dyn SliceFn> {
        Some(self)
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn filter(&self) -> Option<&dyn FilterFn> {
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
            slice(&self.codes(), start, stop)?,
        )?
        .into_array())
    }
}

impl TakeFn for FSSTArray {
    // Take on an FSSTArray is a simple take on the codes array.
    fn take(&self, indices: &Array) -> VortexResult<Array> {
        let new_codes = take(&self.codes(), indices)?;

        Ok(Self::try_new(self.dtype().clone(), self.symbols(), new_codes)?.into_array())
    }
}

impl ScalarAtFn for FSSTArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        // Check validity and short-circuit to null
        if !self.is_valid(index) {
            return Ok(Scalar::null(self.dtype().clone()));
        }

        let compressed = scalar_at(&self.codes(), index)?;
        let binary_datum = match compressed.value().as_buffer()? {
            Some(b) => b,
            None => vortex_bail!("non-nullable scalar must unwrap"),
        };

        let decompressor = self.decompressor()?;
        let decoded_buffer: Buffer = decompressor.decompress(binary_datum.as_slice()).into();

        Ok(varbin_scalar(decoded_buffer, self.dtype()))
    }
}

impl FilterFn for FSSTArray {
    // Filtering an FSSTArray filters the codes array, leaving the symbols array untouched
    fn filter(&self, predicate: &Array) -> VortexResult<Array> {
        let filtered_codes = filter(&self.codes(), predicate)?;
        Ok(Self::try_new(self.dtype().clone(), self.symbols(), filtered_codes)?.into_array())
    }
}
