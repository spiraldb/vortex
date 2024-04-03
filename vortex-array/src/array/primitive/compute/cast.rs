use vortex_error::{vortex_err, VortexResult};
use vortex_schema::DType;

use crate::array::primitive::compute::PrimitiveTrait;
use crate::array::primitive::PrimitiveArray;
use crate::array::{Array, ArrayRef};
use crate::compute::cast::CastFn;
use crate::match_each_native_ptype;
use crate::ptype::{NativePType, PType};

impl<T: NativePType> CastFn for &dyn PrimitiveTrait<T> {
    fn cast(&self, dtype: &DType) -> VortexResult<ArrayRef> {
        // TODO(ngates): check validity
        let into_ptype = PType::try_from(dtype)?;
        if into_ptype == self.ptype() {
            Ok(self.to_array())
        } else {
            match_each_native_ptype!(into_ptype, |$P| {
                Ok(PrimitiveArray::from_nullable(
                    cast::<T, $P>(self.typed_data())?,
                    self.validity_view().map(|v| v.to_validity()),
                ).into_array())
            })
        }
    }
}

fn cast<P: NativePType, T: NativePType>(array: &[P]) -> VortexResult<Vec<T>> {
    array
        .iter()
        // TODO(ngates): allow configurable checked/unchecked casting
        .map(|&v| {
            T::from(v)
                .ok_or_else(|| vortex_err!(ComputeError: "Failed to cast {} to {:?}", v, T::PTYPE))
        })
        .collect()
}

#[cfg(test)]
mod test {
    use vortex_error::VortexError;

    use crate::array::downcast::DowncastArrayBuiltin;
    use crate::array::IntoArray;
    use crate::compute;
    use crate::ptype::PType;

    #[test]
    fn cast_u32_u8() {
        let arr = vec![0u32, 10, 200].into_array();
        let u8arr = compute::cast::cast(&arr, PType::U8.into()).unwrap();
        assert_eq!(u8arr.as_primitive().typed_data::<u8>(), vec![0u8, 10, 200]);
    }

    #[test]
    fn cast_u32_f32() {
        let arr = vec![0u32, 10, 200].into_array();
        let u8arr = compute::cast::cast(&arr, PType::F32.into()).unwrap();
        assert_eq!(
            u8arr.as_primitive().typed_data::<f32>(),
            vec![0.0f32, 10., 200.]
        );
    }

    #[test]
    fn cast_i32_u32() {
        let arr = vec![-1i32].into_array();
        let error = compute::cast::cast(&arr, PType::U32.into()).err().unwrap();
        let VortexError::ComputeError(s, _) = error else {
            unreachable!()
        };
        assert_eq!(s.to_string(), "Failed to cast -1 to U32");
    }
}
