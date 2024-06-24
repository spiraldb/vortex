use std::ops::{BitAnd, BitOr, BitXor, Not};
use std::sync::Arc;

use arrow_buffer::BooleanBuffer;
use num_traits::AsPrimitive;
use vortex::validity::Validity;
use vortex::ToArrayData;
use vortex::{
    compute::{
        compare::CompareFn, slice::SliceFn, take::TakeFn, unary::fill_forward::FillForwardFn,
        unary::scalar_at::ScalarAtFn, ArrayCompute,
    },
    encoding::ArrayEncodingRef,
    stats::StatsSet,
    validity::ArrayValidity,
    ArrayDType, ArrayData, ArrayTrait, IntoArray,
};
use vortex::{Array, IntoCanonical};
use vortex_dtype::{match_each_integer_ptype, Nullability};
use vortex_error::{vortex_bail, VortexResult};
use vortex_expr::Operator;
use vortex_scalar::{Scalar, ScalarValue};

use super::{ByteBoolArray, ByteBoolMetadata};

impl ArrayCompute for ByteBoolArray {
    fn compare(&self) -> Option<&dyn CompareFn> {
        Some(self)
    }

    fn fill_forward(&self) -> Option<&dyn FillForwardFn> {
        None
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

impl ScalarAtFn for ByteBoolArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        if index >= self.len() {
            vortex_bail!(OutOfBounds: index, 0, self.len());
        }

        let scalar = match self.is_valid(index).then(|| self.buffer()[index] == 1) {
            Some(b) => Scalar::new(self.dtype().clone(), ScalarValue::Bool(b)),
            None => Scalar::null(self.dtype().clone()),
        };

        Ok(scalar)
    }
}

impl SliceFn for ByteBoolArray {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<Array> {
        let length = stop - start;

        let validity = self.validity().slice(start, stop)?;

        let slice_metadata = Arc::new(ByteBoolMetadata {
            validity: validity.to_metadata(length)?,
        });

        ArrayData::try_new(
            self.encoding(),
            self.dtype().clone(),
            slice_metadata,
            Some(self.buffer().slice(start..stop)),
            validity.into_array().into_iter().collect::<Vec<_>>().into(),
            StatsSet::new(),
        )
        .map(Array::Data)
    }
}

impl TakeFn for ByteBoolArray {
    fn take(&self, indices: &Array) -> VortexResult<Array> {
        let validity = self.validity();
        let indices = indices.clone().as_primitive();
        let bools = self.maybe_null_slice();

        let arr = match validity {
            Validity::AllValid | Validity::NonNullable => {
                let bools = match_each_integer_ptype!(indices.ptype(), |$I| {
                    indices.maybe_null_slice::<$I>()
                    .iter()
                    .map(|&idx| {
                        let idx: usize = idx.as_();
                        bools[idx]
                    })
                    .collect::<Vec<_>>()
                });

                Self::from(bools).into_array()
            }
            Validity::AllInvalid => Self::from(vec![None; indices.len()]).into_array(),

            Validity::Array(_) => {
                let bools = match_each_integer_ptype!(indices.ptype(), |$I| {
                    indices.maybe_null_slice::<$I>()
                    .iter()
                    .map(|&idx| {
                        let idx = idx.as_();
                        if validity.is_valid(idx) {
                            Some(bools[idx])
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<Option<_>>>()
                });

                Self::from(bools).into_array()
            }
        };

        Ok(arr)
    }
}

impl CompareFn for ByteBoolArray {
    fn compare(&self, other: &Array, op: Operator) -> VortexResult<Array> {
        let canonical = other.clone().into_canonical()?.into_bool()?;
        let lhs = BooleanBuffer::from(self.maybe_null_slice());
        let rhs = canonical.boolean_buffer();

        let result_buf = match op {
            Operator::Eq => lhs.bitxor(&rhs).not(),
            Operator::NotEq => lhs.bitxor(&rhs),

            Operator::Gt => lhs.bitand(&rhs.not()),
            Operator::Gte => lhs.bitor(&rhs.not()),
            Operator::Lt => lhs.not().bitand(&rhs),
            Operator::Lte => lhs.not().bitor(&rhs),
        };

        let mut validity = Vec::with_capacity(self.len());

        let lhs_validity = self.validity();
        let rhs_validity = canonical.validity();

        for idx in 0..self.len() {
            let l = lhs_validity.is_valid(idx);
            let r = rhs_validity.is_valid(idx);
            validity.push(l & r);
        }

        ByteBoolArray::try_from_vec(Vec::from_iter(result_buf.iter()), validity)
            .map(ByteBoolArray::into_array)
    }
}

impl FillForwardFn for ByteBoolArray {
    fn fill_forward(&self) -> VortexResult<Array> {
        if self.dtype().nullability() == Nullability::NonNullable {
            return Ok(self.to_array_data().into_array());
        }

        let validity = self.logical_validity().to_null_buffer()?.unwrap();
        let bools = self.maybe_null_slice();
        let mut last_value = bool::default();

        let filled = bools
            .iter()
            .zip(validity.inner().iter())
            .map(|(&v, is_valid)| {
                if is_valid {
                    last_value = v
                }

                last_value
            })
            .collect::<Vec<_>>();

        Ok(Self::from(filled).into_array())
    }
}

#[cfg(test)]
mod tests {
    use vortex::{
        compute::{compare::compare, slice::slice, unary::scalar_at::scalar_at},
        AsArray as _,
    };

    use super::*;

    #[test]
    fn test_slice() {
        let original = vec![Some(true), Some(true), None, Some(false), None];
        let vortex_arr = ByteBoolArray::from(original.clone());

        let sliced_arr = slice(vortex_arr.as_array_ref(), 1, 4).unwrap();
        let sliced_arr = ByteBoolArray::try_from(sliced_arr).unwrap();

        let s = scalar_at(sliced_arr.as_array_ref(), 0).unwrap();
        assert_eq!(s.into_value().as_bool().unwrap(), Some(true));

        let s = scalar_at(sliced_arr.as_array_ref(), 1).unwrap();
        assert!(!sliced_arr.is_valid(1));
        assert!(s.is_null());
        assert_eq!(s.into_value().as_bool().unwrap(), None);

        let s = scalar_at(sliced_arr.as_array_ref(), 2).unwrap();
        assert_eq!(s.into_value().as_bool().unwrap(), Some(false));
    }

    #[test]
    fn test_compare_all_equal() {
        let lhs = ByteBoolArray::from(vec![true; 5]);
        let rhs = ByteBoolArray::from(vec![true; 5]);

        let arr = compare(lhs.as_array_ref(), rhs.as_array_ref(), Operator::Eq).unwrap();

        for i in 0..arr.len() {
            let s = scalar_at(arr.as_array_ref(), i).unwrap();
            assert!(s.is_valid());
            assert_eq!(s.value(), &ScalarValue::Bool(true));
        }
    }

    #[test]
    fn test_compare_all_different() {
        let lhs = ByteBoolArray::from(vec![false; 5]);
        let rhs = ByteBoolArray::from(vec![true; 5]);

        let arr = compare(lhs.as_array_ref(), rhs.as_array_ref(), Operator::Eq).unwrap();

        for i in 0..arr.len() {
            let s = scalar_at(&arr, i).unwrap();
            assert!(s.is_valid());
            assert_eq!(s.value(), &ScalarValue::Bool(false));
        }
    }

    #[test]
    fn test_compare_with_nulls() {
        let lhs = ByteBoolArray::from(vec![true; 5]);
        let rhs = ByteBoolArray::from(vec![Some(true), Some(true), Some(true), Some(false), None]);

        let arr = compare(lhs.as_array_ref(), rhs.as_array_ref(), Operator::Eq).unwrap();

        for i in 0..3 {
            let s = scalar_at(&arr, i).unwrap();
            assert!(s.is_valid());
            assert_eq!(s.value(), &ScalarValue::Bool(true));
        }

        let s = scalar_at(&arr, 3).unwrap();
        assert!(s.is_valid());
        assert_eq!(s.value(), &ScalarValue::Bool(false));

        let s = scalar_at(&arr, 4).unwrap();
        assert!(s.is_null());
    }
}
