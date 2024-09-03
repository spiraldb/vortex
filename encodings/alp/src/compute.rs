use arrow_array::builder::BooleanBufferBuilder;
use vortex::array::BoolArray;
use vortex::compute::unary::{scalar_at_unchecked, ScalarAtFn};
use vortex::compute::{slice, take, ArrayCompute, CompareFn, Operator, SliceFn, TakeFn};
use vortex::stats::{ArrayStatistics, Stat};
use vortex::validity::{ArrayValidity, Validity};
use vortex::variants::PrimitiveArrayTrait;
use vortex::{Array, ArrayDType, IntoArray};
use vortex_dtype::{DType, PType};
use vortex_error::{vortex_err, VortexResult};
use vortex_scalar::Scalar;

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

    fn compare(&self) -> Option<&dyn CompareFn> {
        Some(self)
    }
}

impl ScalarAtFn for ALPArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        Ok(self.scalar_at_unchecked(index))
    }

    fn scalar_at_unchecked(&self, index: usize) -> Scalar {
        if let Some(patches) = self.patches().and_then(|p| {
            p.with_dyn(|arr| {
                // We need to make sure the value is actually in the patches array
                arr.is_valid(index)
            })
            .then_some(p)
        }) {
            return scalar_at_unchecked(&patches, index);
        }

        let encoded_val = scalar_at_unchecked(&self.encoded(), index);

        match_each_alp_float_ptype!(self.ptype(), |$T| {
            let encoded_val: <$T as ALPFloat>::ALPInt = encoded_val.as_ref().try_into().unwrap();
            Scalar::from(<$T as ALPFloat>::decode_single(
                encoded_val,
                self.exponents(),
            ))
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
        if let Some(true) = array.statistics().get_as::<bool>(Stat::IsConstant) {
            let scalar_value = scalar_at_unchecked(array, 0);
            match self.dtype() {
                DType::Primitive(PType::F32, _) => {
                    let value = f32::try_from(scalar_value)?;
                    let mut values = Vec::with_capacity(self.len());
                    let op_fn = operator.to_fn();

                    for batch in self.f32_iter().ok_or(vortex_err!("Expected DType"))? {
                        for v in batch.data() {
                            values.push(op_fn(value, *v));
                        }
                    }

                    Ok(
                        BoolArray::from_vec(values, self.logical_validity().into_validity())
                            .into_array(),
                    )
                }
                DType::Primitive(PType::F64, _) => {
                    let value = f64::try_from(scalar_value)?;
                    let mut values = Vec::with_capacity(self.len());
                    let op_fn = operator.to_fn();

                    for batch in self.f64_iter().ok_or(vortex_err!("Expected DType"))? {
                        for v in batch.data() {
                            values.push(op_fn(value, *v));
                        }
                    }

                    Ok(
                        BoolArray::from_vec(values, self.logical_validity().into_validity())
                            .into_array(),
                    )
                }
                _ => unreachable!(),
            }
        } else {
            let mut values = BooleanBufferBuilder::new(self.len());
            let mut validity = BooleanBufferBuilder::new(self.len());
            match self.dtype() {
                DType::Primitive(PType::F32, _) => {
                    let iter = self.f32_iter().ok_or(vortex_err!("Expected DType"))?;
                    let rhs = array
                        .with_dyn(|a| a.as_primitive_array_unchecked().f32_iter())
                        .ok_or(vortex_err!(
                            InvalidArgument:
                            "Both sides of a `compare` should be of the same DType"
                        ))?;

                    let op_fn = operator.to_fn();

                    for (l_batch, r_batch) in iter.zip(rhs) {
                        for (&l, &r) in l_batch.data().iter().zip(r_batch.data().iter()) {
                            values.append(op_fn(l, r));
                        }
                    }

                    for idx in 0..self.len() {
                        validity.append(self.is_valid(idx) & array.with_dyn(|a| a.is_valid(idx)));
                    }

                    Ok(BoolArray::from_vec(
                        values.finish().into_iter().collect::<Vec<_>>(),
                        Validity::from(validity.finish()),
                    )
                    .into_array())
                }
                DType::Primitive(PType::F64, _) => {
                    let iter = self.f64_iter().ok_or(vortex_err!("Expected DType"))?;
                    let rhs = array
                        .with_dyn(|a| a.as_primitive_array_unchecked().f64_iter())
                        .ok_or(vortex_err!(
                            InvalidArgument:
                            "Both sides of a `compare` should be of the same DType"
                        ))?;

                    let op_fn = operator.to_fn();

                    for (l_batch, r_batch) in iter.zip(rhs) {
                        for (&l, &r) in l_batch.data().iter().zip(r_batch.data().iter()) {
                            values.append(op_fn(l, r));
                        }
                    }

                    for idx in 0..self.len() {
                        validity.append(self.is_valid(idx) & array.with_dyn(|a| a.is_valid(idx)));
                    }

                    Ok(BoolArray::from_vec(
                        values.finish().into_iter().collect::<Vec<_>>(),
                        Validity::from(validity.finish()),
                    )
                    .into_array())
                }
                _ => unreachable!(),
            }
        }
    }
}
