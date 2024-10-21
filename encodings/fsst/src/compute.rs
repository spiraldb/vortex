use fsst::Symbol;
use vortex::array::{varbin_scalar, ConstantArray};
use vortex::compute::unary::{scalar_at_unchecked, ScalarAtFn};
use vortex::compute::{
    compare, filter, slice, take, ArrayCompute, FilterFn, MaybeCompareFn, Operator, SliceFn, TakeFn,
};
use vortex::validity::Validity;
use vortex::variants::BoolArrayTrait;
use vortex::{Array, ArrayDType, IntoArray, IntoArrayVariant};
use vortex_buffer::Buffer;
use vortex_dtype::DType;
use vortex_error::{vortex_err, VortexResult, VortexUnwrap};
use vortex_scalar::Scalar;

use crate::FSSTArray;

impl ArrayCompute for FSSTArray {
    fn compare(&self, other: &Array, operator: Operator) -> Option<VortexResult<Array>> {
        MaybeCompareFn::maybe_compare(self, other, operator)
    }

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

impl MaybeCompareFn for FSSTArray {
    fn maybe_compare(&self, other: &Array, operator: Operator) -> Option<VortexResult<Array>> {
        match (ConstantArray::try_from(other), operator) {
            (Ok(constant_array), Operator::Eq | Operator::NotEq) => Some(compare_fsst_constant(
                self,
                &constant_array,
                operator == Operator::Eq,
            )),
            _ => None,
        }
    }
}

/// Return an array where true means the value is null and false indicates non-null.
///
/// This is the inverse of the normal validity buffer.
fn is_null(array: &FSSTArray) -> VortexResult<Array> {
    match array.validity() {
        Validity::NonNullable | Validity::AllValid => {
            Ok(ConstantArray::new(false, array.len()).into_array())
        }
        Validity::AllInvalid => Ok(ConstantArray::new(true, array.len()).into_array()),
        Validity::Array(validity_array) => validity_array.into_bool()?.invert(),
    }
}

fn is_non_null(array: &FSSTArray) -> VortexResult<Array> {
    match array.validity() {
        Validity::NonNullable | Validity::AllValid => {
            Ok(ConstantArray::new(true, array.len()).into_array())
        }
        Validity::AllInvalid => Ok(ConstantArray::new(false, array.len()).into_array()),
        Validity::Array(validity_array) => Ok(validity_array),
    }
}

/// Specialized compare function implementation used when performing equals or not equals against
/// a constant.
fn compare_fsst_constant(
    left: &FSSTArray,
    right: &ConstantArray,
    equal: bool,
) -> VortexResult<Array> {
    let symbols = left.symbols().into_primitive()?;
    let symbols_u64 = symbols.maybe_null_slice::<u64>();

    let symbol_lens = left.symbol_lengths().into_primitive()?;
    let symbol_lens_u8 = symbol_lens.maybe_null_slice::<u8>();

    let mut compressor = fsst::CompressorBuilder::new();
    for (symbol, symbol_len) in symbols_u64.iter().zip(symbol_lens_u8.iter()) {
        compressor.insert(Symbol::from_slice(&symbol.to_le_bytes()), *symbol_len as _);
    }
    let compressor = compressor.build();

    let encoded_scalar = match left.dtype() {
        DType::Utf8(_) => right
            .scalar_value()
            .as_buffer_string()?
            .map(|scalar| Buffer::from(compressor.compress(scalar.as_bytes()))),
        DType::Binary(_) => right
            .scalar_value()
            .as_buffer()?
            .map(|scalar| Buffer::from(compressor.compress(scalar.as_slice()))),

        _ => unreachable!("FSSTArray can only have string or binary data type"),
    };

    match encoded_scalar {
        None => {
            if equal {
                // Equality comparison to null scalar becomes is_null
                is_null(left)
            } else {
                is_non_null(left)
            }
        }
        Some(encoded_scalar) => {
            let rhs = ConstantArray::new(encoded_scalar, left.len());

            compare(
                left.codes(),
                rhs,
                if equal { Operator::Eq } else { Operator::NotEq },
            )
        }
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
