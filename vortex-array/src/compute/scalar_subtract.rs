use vortex_error::{vortex_err, VortexResult};

use crate::scalar::Scalar;
use crate::{Array, OwnedArray};

pub trait ScalarSubtractFn {
    fn scalar_subtract(&self, to_subtract: Scalar) -> VortexResult<OwnedArray>;
}

pub fn scalar_subtract(array: &Array, to_subtract: Scalar) -> VortexResult<OwnedArray> {
    array.with_dyn(|c| {
        let option = c
            .scalar_subtract()
            .map(|t| t.scalar_subtract(to_subtract.clone()));
        option.unwrap_or_else(|| {
            Err(vortex_err!(
                NotImplemented: "scalar_subtract",
                array.encoding().id().name()
            ))
        })
    })
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::IntoArray;

    #[test]
    fn test_scalar_subtract_unsigned() {
        let values = vec![1u16, 2, 3].into_array();
        let results = scalar_subtract(&values, 1u16.into())
            .unwrap()
            .flatten_primitive()
            .unwrap()
            .typed_data::<u16>()
            .to_vec();
        assert_eq!(results, &[0u16, 1, 2]);
    }

    #[test]
    fn test_scalar_subtract_signed() {
        let values = vec![1i64, 2, 3].into_array();
        let to_subtract = -1i64;
        let results = scalar_subtract(&values, to_subtract.into())
            .unwrap()
            .flatten_primitive()
            .unwrap()
            .typed_data::<i64>()
            .to_vec();
        assert_eq!(results, &[2i64, 3, 4]);
    }

    #[test]
    fn test_scalar_subtract_float() {
        let values = vec![1.0f64, 2.0, 3.0].into_array();
        let to_subtract = -1f64;
        let results = scalar_subtract(&values, to_subtract.into())
            .unwrap()
            .flatten_primitive()
            .unwrap()
            .typed_data::<f64>()
            .to_vec();
        assert_eq!(results, &[2.0f64, 3.0, 4.0]);
    }

    #[test]
    fn test_scalar_subtract_unsigned_underflow() {
        let values = vec![u8::MIN, 2, 3].into_array();
        let _results =
            scalar_subtract(&values, 1u8.into()).expect_err("should fail with underflow");
        let values = vec![u16::MIN, 2, 3].into_array();
        let _results =
            scalar_subtract(&values, 1u16.into()).expect_err("should fail with underflow");
        let values = vec![u32::MIN, 2, 3].into_array();
        let _results =
            scalar_subtract(&values, 1u32.into()).expect_err("should fail with underflow");
        let values = vec![u64::MIN, 2, 3].into_array();
        let _results =
            scalar_subtract(&values, 1u64.into()).expect_err("should fail with underflow");
    }

    #[test]
    fn test_scalar_subtract_signed_overflow() {
        let values = vec![i8::MAX, 2, 3].into_array();
        let to_subtract = -1i8;
        let _results =
            scalar_subtract(&values, to_subtract.into()).expect_err("should fail with overflow");
        let values = vec![i16::MAX, 2, 3].into_array();
        let _results =
            scalar_subtract(&values, to_subtract.into()).expect_err("should fail with overflow");
        let values = vec![i32::MAX, 2, 3].into_array();
        let _results =
            scalar_subtract(&values, to_subtract.into()).expect_err("should fail with overflow");
        let values = vec![i64::MAX, 2, 3].into_array();
        let _results =
            scalar_subtract(&values, to_subtract.into()).expect_err("should fail with overflow");
    }

    #[test]
    fn test_scalar_subtract_signed_underflow() {
        let values = vec![i8::MIN, 2, 3].into_array();
        let _results =
            scalar_subtract(&values, 1i8.into()).expect_err("should fail with underflow");
        let values = vec![i16::MIN, 2, 3].into_array();
        let _results =
            scalar_subtract(&values, 1i16.into()).expect_err("should fail with underflow");
        let values = vec![i32::MIN, 2, 3].into_array();
        let _results =
            scalar_subtract(&values, 1i32.into()).expect_err("should fail with underflow");
        let values = vec![i64::MIN, 2, 3].into_array();
        let _results =
            scalar_subtract(&values, 1i64.into()).expect_err("should fail with underflow");
    }

    #[test]
    fn test_scalar_subtract_float_underflow_is_ok() {
        let values = vec![f32::MIN, 2.0, 3.0].into_array();
        let _results = scalar_subtract(&values, 1.0f32.into()).unwrap();
        let _results = scalar_subtract(&values, f32::MAX.into()).unwrap();
    }

    #[test]
    fn test_scalar_subtract_type_mismatch_fails() {
        let values = vec![1.0f64, 2.0, 3.0].into_array();
        // Subtracting non-equivalent dtypes should fail
        let to_subtract = 1u64;
        let _results =
            scalar_subtract(&values, to_subtract.into()).expect_err("Expected type mismatch error");
    }
}
