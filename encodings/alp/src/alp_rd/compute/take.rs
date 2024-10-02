use vortex::compute::{take, TakeFn};
use vortex::{Array, ArrayDType, IntoArray};
use vortex_error::VortexResult;

use crate::ALPRDArray;

impl TakeFn for ALPRDArray {
    fn take(&self, indices: &Array) -> VortexResult<Array> {
        let left_parts_exceptions = self
            .left_parts_exceptions()
            .map(|array| take(&array, indices))
            .transpose()?;

        Ok(ALPRDArray::try_new(
            self.dtype().clone(),
            take(self.left_parts(), indices)?,
            self.left_parts_dict(),
            take(self.right_parts(), indices)?,
            self.right_bit_width(),
            left_parts_exceptions,
        )?
        .into_array())
    }
}

#[cfg(test)]
mod test {
    use rstest::rstest;
    use vortex::array::PrimitiveArray;
    use vortex::compute::take;
    use vortex::IntoArrayVariant;

    use crate::{ALPRDFloat, Encoder};

    #[rstest]
    #[case(0.1f32, 0.2f32, 3e25f32)]
    #[case(0.1f64, 0.2f64, 3e100f64)]
    fn test_take<T: ALPRDFloat>(#[case] a: T, #[case] b: T, #[case] outlier: T) {
        let array = PrimitiveArray::from(vec![a, b, outlier]);
        let encoded = Encoder::new(&[a, b]).encode(&array);

        assert!(encoded.left_parts_exceptions().is_some());

        let taken = take(encoded.as_ref(), PrimitiveArray::from(vec![0, 2]).as_ref())
            .unwrap()
            .into_primitive()
            .unwrap();

        assert_eq!(taken.maybe_null_slice::<T>(), &[a, outlier]);
    }
}
