use vortex::array::{BoolArray, ConstantArray};
use vortex::arrow::FromArrowArray;
use vortex::compute::unary::{scalar_at, scalar_at_unchecked, ScalarAtFn};
use vortex::compute::{compare, slice, take, ArrayCompute, CompareFn, Operator, SliceFn, TakeFn};
use vortex::stats::ArrayStatistics;
use vortex::validity::{ArrayValidity, Validity};
use vortex::{Array, ArrayDType, AsArray, IntoArray, IntoCanonical};
use vortex_error::VortexResult;
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

impl CompareFn for ALPArray {
    fn compare(&self, array: &Array, operator: Operator) -> VortexResult<Array> {
        if array.statistics().compute_is_constant().unwrap_or_default() {
            let rhs = scalar_at(array, 0).expect("should be scalar");
            let pvalue = rhs.value().as_pvalue().expect("Expected primitive value");

            match pvalue {
                Some(PValue::F32(f)) => {
                    let encoded = f32::encode_single(f, self.exponents());
                    match encoded {
                        Ok(encoded) => {
                            let s = ConstantArray::new(encoded, self.len());
                            compare(&self.encoded(), s.as_array_ref(), operator)
                        }
                        Err(exception) => {
                            if let Some(patches) = self.patches().as_ref() {
                                let s = ConstantArray::new(exception, self.len());
                                compare(patches, s.as_array_ref(), operator)
                            } else {
                                Ok(
                                    BoolArray::from_vec(
                                        vec![false; self.len()],
                                        Validity::AllValid,
                                    )
                                    .into_array(),
                                )
                            }
                        }
                    }
                }
                Some(PValue::F64(f)) => {
                    let encoded = f64::encode_single(f, self.exponents());
                    match encoded {
                        Ok(encoded) => {
                            let s = ConstantArray::new(encoded, self.len());
                            compare(&self.encoded(), s.as_array_ref(), operator)
                        }
                        Err(exception) => {
                            if let Some(patches) = self.patches().as_ref() {
                                let s = ConstantArray::new(exception, self.len());
                                compare(patches, s.as_array_ref(), operator)
                            } else {
                                Ok(
                                    BoolArray::from_vec(vec![true; self.len()], Validity::AllValid)
                                        .into_array(),
                                )
                            }
                        }
                    }
                }
                None => {
                    // Is `null == null => true`?
                    let bools = (0..self.len()).map(|index| !self.is_valid(index)).collect();
                    Ok(BoolArray::from_vec(bools, Validity::AllValid).into_array())
                }
                _ => unreachable!(),
            }
        } else {
            let lhs = self.clone().into_canonical()?.into_arrow();
            let rhs = array.clone().into_canonical()?.into_arrow();

            use arrow_ord::cmp;
            let array = match operator {
                Operator::Eq => cmp::eq(&lhs.as_ref(), &rhs.as_ref())?,
                Operator::NotEq => cmp::neq(&lhs.as_ref(), &rhs.as_ref())?,
                Operator::Gt => cmp::gt(&lhs.as_ref(), &rhs.as_ref())?,
                Operator::Gte => cmp::gt_eq(&lhs.as_ref(), &rhs.as_ref())?,
                Operator::Lt => cmp::lt(&lhs.as_ref(), &rhs.as_ref())?,
                Operator::Lte => cmp::lt_eq(&lhs.as_ref(), &rhs.as_ref())?,
            };

            Ok(Array::from_arrow(&array, true))
        }
    }
}
