use std::ops::{BitAnd, BitOr, BitXor, Not};
use std::sync::Arc;

use arrow_array::{ArrayRef as ArrowArrayRef, BooleanArray as ArrowBoolArray};
use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder};
use num_traits::AsPrimitive;
use vortex_dtype::{match_each_integer_ptype, Nullability};
use vortex_error::{vortex_bail, VortexResult};
use vortex_expr::Operator;
use vortex_scalar::Scalar;

use super::{ByteBoolArray, ByteBoolMetadata};
use crate::ToArrayData;
use crate::{
    compute::{
        as_arrow::AsArrowArray, compare::CompareFn, fill::FillForwardFn, scalar_at::ScalarAtFn,
        slice::SliceFn, take::TakeFn, ArrayCompute,
    },
    encoding::ArrayEncodingRef,
    stats::StatsSet,
    validity::ArrayValidity,
    ArrayDType, ArrayData, ArrayTrait, IntoArray,
};

impl ArrayCompute for ByteBoolArray {
    fn as_arrow(&self) -> Option<&dyn crate::compute::as_arrow::AsArrowArray> {
        Some(self)
    }

    fn compare(&self) -> Option<&dyn crate::compute::compare::CompareFn> {
        Some(self)
    }

    fn fill_forward(&self) -> Option<&dyn crate::compute::fill::FillForwardFn> {
        None
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn slice(&self) -> Option<&dyn crate::compute::slice::SliceFn> {
        Some(self)
    }

    fn take(&self) -> Option<&dyn crate::compute::take::TakeFn> {
        Some(self)
    }
}

impl ScalarAtFn for ByteBoolArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        if index >= self.len() {
            vortex_bail!(OutOfBounds: index, 0, self.len());
        }

        let scalar = match self.is_valid(index).then(|| self.buffer()[index] == 1) {
            Some(b) => Scalar::new(
                vortex_dtype::DType::Bool(self.validity().nullability()),
                vortex_scalar::ScalarValue::Bool(b),
            ),
            None => Scalar::null(self.dtype().clone()),
        };

        Ok(scalar)
    }
}

impl AsArrowArray for ByteBoolArray {
    fn as_arrow(&self) -> VortexResult<ArrowArrayRef> {
        let nulls = self.logical_validity().to_null_buffer()?;

        let mut builder = BooleanBufferBuilder::new(self.len());
        builder.append_slice(self.as_ref());

        Ok(Arc::new(ArrowBoolArray::new(builder.finish(), nulls)))
    }
}

impl SliceFn for ByteBoolArray {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<crate::Array> {
        match stop.checked_sub(start) {
            None => vortex_bail!(ComputeError:
                "{}..{} is an invalid slicing range", start, stop
            ),
            Some(length) => {
                let validity = self.validity().slice(start, stop)?;

                let validity_bools = validity
                    .to_logical(length)
                    .to_present_null_buffer()
                    .unwrap();

                let x = Vec::from_iter(validity_bools.inner().into_iter());

                println!("{x:?}");

                let slice_metadata = Arc::new(ByteBoolMetadata {
                    validity: validity.to_metadata(length)?,
                    length,
                });

                ArrayData::try_new(
                    self.encoding(),
                    self.dtype().clone(),
                    slice_metadata,
                    Some(self.buffer().slice(start..stop)),
                    validity
                        .into_array_data()
                        .into_iter()
                        .collect::<Vec<_>>()
                        .into(),
                    StatsSet::new(),
                )
                .map(crate::Array::Data)
            }
        }
    }
}

impl TakeFn for ByteBoolArray {
    fn take(&self, indices: &crate::Array) -> VortexResult<crate::Array> {
        let validity = self.validity();
        let indices = indices.clone().flatten_primitive()?;
        let bools = self.as_ref();

        let bools = match_each_integer_ptype!(indices.ptype(), |$I| {
            indices.typed_data::<$I>()
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

        Ok(Self::from(bools).into_array())
    }
}

impl CompareFn for ByteBoolArray {
    fn compare(
        &self,
        other: &crate::Array,
        op: vortex_expr::Operator,
    ) -> VortexResult<crate::Array> {
        let flattened = other.clone().flatten_bool()?;
        let lhs = BooleanBuffer::from(self.as_ref());
        let rhs = flattened.boolean_buffer();

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
        let rhs_validity = flattened.validity();

        for idx in 0..self.len() {
            let l = lhs_validity.is_valid(idx);
            let r = rhs_validity.is_valid(idx);
            validity.push(l & r);
        }

        ByteBoolArray::try_with_validity(Vec::from_iter(result_buf.iter()), validity)
            .map(ByteBoolArray::into_array)
    }
}

impl FillForwardFn for ByteBoolArray {
    fn fill_forward(&self) -> VortexResult<crate::Array> {
        if self.dtype().nullability() == Nullability::NonNullable {
            return Ok(self.to_array_data().into_array());
        }

        let validity = self.logical_validity().to_null_buffer()?.unwrap();
        let bools = self.as_ref();
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
    use arrow_array::cast::AsArray as _;

    use super::*;
    use crate::{
        compute::{scalar_at::scalar_at, slice::slice},
        AsArray,
    };

    #[test]
    fn test_as_arrow() {
        let original = vec![Some(true), Some(true), None, Some(false), None];

        let vortex_arr = ByteBoolArray::from(original.clone());
        let arrow_arr = ArrowBoolArray::from(original);

        let converted_arr = AsArrowArray::as_arrow(&vortex_arr).unwrap();
        let bool_converted_arr = converted_arr.as_boolean();

        for (idx, (expected, output)) in arrow_arr.iter().zip(bool_converted_arr.iter()).enumerate()
        {
            assert_eq!(
                expected, output,
                "The item at index {} doesn't match - expected {:?} but got {:?}",
                idx, expected, output
            );
        }
    }

    #[test]
    fn test_slice() {
        let original = vec![Some(true), Some(true), None, Some(false), None];
        let vortex_arr = ByteBoolArray::from(original.clone());

        let validity = vortex_arr.validity();

        let validity_bools = validity
            .to_logical(vortex_arr.len())
            .to_present_null_buffer()
            .unwrap();

        let x = Vec::from_iter(validity_bools.inner().into_iter());

        println!("{x:?}");

        println!("--- --- --- --- ---");

        for idx in 0..vortex_arr.len() {
            let s = scalar_at(vortex_arr.as_array_ref(), idx).unwrap();
            println!("{s:?}");
        }

        let sliced_arr = slice(vortex_arr.as_array_ref(), 1, 4).unwrap();
        let sliced_arr = ByteBoolArray::try_from(sliced_arr).unwrap();

        println!("--- --- --- --- ---");

        for idx in 0..sliced_arr.len() {
            let s = scalar_at(sliced_arr.as_array_ref(), idx).unwrap();
            println!("{s:?}");
        }

        let s = scalar_at(sliced_arr.as_array_ref(), 0).unwrap();
        assert_eq!(s.into_value().as_bool().unwrap(), Some(true));

        let s = scalar_at(sliced_arr.as_array_ref(), 1).unwrap();
        assert!(!sliced_arr.is_valid(1));
        assert!(s.is_null());
        assert_eq!(s.into_value().as_bool().unwrap(), None);

        let s = scalar_at(sliced_arr.as_array_ref(), 2).unwrap();
        assert_eq!(s.into_value().as_bool().unwrap(), Some(false));
    }
}
