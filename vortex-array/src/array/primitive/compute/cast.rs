use vortex_dtype::{match_each_native_ptype, DType, NativePType, PType};
use vortex_error::{vortex_err, VortexResult};

use crate::array::primitive::PrimitiveArray;
use crate::compute::unary::CastFn;
use crate::validity::Validity;
use crate::{Array, ArrayDType, IntoArray};

impl CastFn for PrimitiveArray {
    fn cast(&self, dtype: &DType) -> VortexResult<Array> {
        let ptype = PType::try_from(dtype)?;

        // Short-cut if we can just change the nullability
        if self.ptype() == ptype && !self.dtype().is_nullable() && dtype.is_nullable() {
            return Ok(PrimitiveArray::new(
                self.buffer().clone(),
                self.ptype(),
                Validity::AllValid,
            )
            .into_array());
        }

        // FIXME(ngates): #260 - check validity and nullability
        match_each_native_ptype!(ptype, |$T| {
            Ok(PrimitiveArray::from_vec(
                cast::<$T>(self)?,
                self.validity().clone(),
            ).into_array())
        })
    }
}

fn cast<T: NativePType>(array: &PrimitiveArray) -> VortexResult<Vec<T>> {
    match_each_native_ptype!(array.ptype(), |$E| {
        array
            .maybe_null_slice::<$E>()
            .iter()
            // TODO(ngates): allow configurable checked/unchecked casting
            .map(|&v| {
                T::from(v).ok_or_else(|| {
                    vortex_err!(ComputeError: "Failed to cast {} to {:?}", v, T::PTYPE)
                })
            })
            .collect()
    })
}

#[cfg(test)]
mod test {
    use vortex_dtype::PType;
    use vortex_error::VortexError;

    use crate::{compute, IntoArray};

    #[test]
    fn cast_u32_u8() {
        let arr = vec![0u32, 10, 200].into_array();
        let p = compute::unary::try_cast(&arr, PType::U8.into())
            .unwrap()
            .as_primitive();
        assert_eq!(p.maybe_null_slice::<u8>(), vec![0u8, 10, 200]);
    }

    #[test]
    fn cast_u32_f32() {
        let arr = vec![0u32, 10, 200].into_array();
        let u8arr = compute::unary::try_cast(&arr, PType::F32.into())
            .unwrap()
            .as_primitive();
        assert_eq!(u8arr.maybe_null_slice::<f32>(), vec![0.0f32, 10., 200.]);
    }

    #[test]
    fn cast_i32_u32() {
        let arr = vec![-1i32].into_array();
        let error = compute::unary::try_cast(&arr, PType::U32.into())
            .err()
            .unwrap();
        let VortexError::ComputeError(s, _) = error else {
            unreachable!()
        };
        assert_eq!(s.to_string(), "Failed to cast -1 to U32");
    }
}
