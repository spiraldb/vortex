use vortex::array::{BoolArray, ConstantArray};
use vortex::compute::unary::{scalar_at, scalar_at_unchecked, ScalarAtFn};
use vortex::compute::{
    compare, filter, slice, take, ArrayCompute, FilterFn, MaybeCompareFn, Operator, SliceFn, TakeFn,
};
use vortex::stats::{ArrayStatistics, Stat};
use vortex::validity::Validity;
use vortex::{Array, ArrayDType, AsArray, IntoArray};
use vortex_error::{VortexExpect, VortexResult};
use vortex_scalar::{PValue, Scalar};

use crate::{match_each_alp_float_ptype, ALPArray, ALPFloat};

impl ArrayCompute for ALPArray {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn slice(&self) -> Option<&dyn SliceFn> {
        Some(self)
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }

    fn compare(&self, other: &Array, operator: Operator) -> Option<VortexResult<Array>> {
        MaybeCompareFn::maybe_compare(self, other, operator)
    }

    fn filter(&self) -> Option<&dyn FilterFn> {
        Some(self)
    }
}

impl ScalarAtFn for ALPArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        Ok(self.scalar_at_unchecked(index))
    }

    fn scalar_at_unchecked(&self, index: usize) -> Scalar {
        if let Some(patches) = self.patches() {
            if patches.with_dyn(|a| a.is_valid(index)) {
                // We need to make sure the value is actually in the patches array
                return scalar_at_unchecked(&patches, index);
            }
        }

        let encoded_val = scalar_at_unchecked(&self.encoded(), index);

        match_each_alp_float_ptype!(self.ptype(), |$T| {
            let encoded_val: <$T as ALPFloat>::ALPInt = encoded_val.as_ref().try_into().unwrap();
            Scalar::primitive(<$T as ALPFloat>::decode_single(
                encoded_val,
                self.exponents(),
            ), self.dtype().nullability())
        })
    }
}

impl TakeFn for ALPArray {
    fn take(&self, indices: &Array) -> VortexResult<Array> {
        // TODO(ngates): wrap up indices in an array that caches decompression?
        Ok(Self::try_new(
            take(&self.encoded(), indices)?,
            self.exponents(),
            self.patches().map(|p| take(&p, indices)).transpose()?,
        )?
        .into_array())
    }
}

impl SliceFn for ALPArray {
    fn slice(&self, start: usize, end: usize) -> VortexResult<Array> {
        Ok(Self::try_new(
            slice(&self.encoded(), start, end)?,
            self.exponents(),
            self.patches().map(|p| slice(&p, start, end)).transpose()?,
        )?
        .into_array())
    }
}

impl FilterFn for ALPArray {
    fn filter(&self, predicate: &Array) -> VortexResult<Array> {
        Ok(Self::try_new(
            filter(&self.encoded(), predicate)?,
            self.exponents(),
            self.patches().map(|p| filter(&p, predicate)).transpose()?,
        )?
        .into_array())
    }
}

impl MaybeCompareFn for ALPArray {
    fn maybe_compare(&self, array: &Array, operator: Operator) -> Option<VortexResult<Array>> {
        if ConstantArray::try_from(array).is_ok()
            || array
                .statistics()
                .get_as::<bool>(Stat::IsConstant)
                .unwrap_or_default()
        {
            let rhs = scalar_at(array, 0).vortex_expect("should be scalar");
            let pvalue = rhs
                .value()
                .as_pvalue()
                .vortex_expect("Expected primitive value");

            match pvalue {
                Some(PValue::F32(f)) => {
                    let encoded = f32::encode_single(f, self.exponents());
                    match encoded {
                        Ok(encoded) => {
                            let s = ConstantArray::new(encoded, self.len());
                            Some(compare(&self.encoded(), s.as_array_ref(), operator))
                        }
                        Err(exception) => {
                            if let Some(patches) = self.patches().as_ref() {
                                let s = ConstantArray::new(exception, self.len());
                                Some(compare(patches, s.as_array_ref(), operator))
                            } else {
                                Some(Ok(BoolArray::from_vec(
                                    vec![false; self.len()],
                                    Validity::AllValid,
                                )
                                .into_array()))
                            }
                        }
                    }
                }
                Some(PValue::F64(f)) => {
                    let encoded = f64::encode_single(f, self.exponents());
                    match encoded {
                        Ok(encoded) => {
                            let s = ConstantArray::new(encoded, self.len());
                            Some(compare(&self.encoded(), s.as_array_ref(), operator))
                        }
                        Err(exception) => {
                            if let Some(patches) = self.patches().as_ref() {
                                let s = ConstantArray::new(exception, self.len());
                                Some(compare(patches, s.as_array_ref(), operator))
                            } else {
                                Some(Ok(BoolArray::from_vec(
                                    vec![true; self.len()],
                                    Validity::AllValid,
                                )
                                .into_array()))
                            }
                        }
                    }
                }
                None => Some(Ok(BoolArray::from_vec(
                    vec![false; self.len()],
                    Validity::AllValid,
                )
                .into_array())),
                _ => unreachable!(),
            }
        } else {
            None
        }
    }
}
