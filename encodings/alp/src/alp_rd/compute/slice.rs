use vortex::compute::{slice, SliceFn};
use vortex::{Array, ArrayDType, IntoArray};
use vortex_error::VortexResult;

use crate::ALPRDArray;

impl SliceFn for ALPRDArray {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<Array> {
        let left_parts_exceptions = self
            .left_parts_exceptions()
            .map(|array| slice(&array, start, stop))
            .transpose()?;

        Ok(ALPRDArray::try_new(
            self.dtype().clone(),
            slice(self.left_parts(), start, stop)?,
            self.left_parts_dict(),
            slice(self.right_parts(), start, stop)?,
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
    use vortex::compute::slice;
    use vortex::IntoArrayVariant;

    use crate::{ALPRDFloat, Encoder};

    #[rstest]
    #[case(0.1f32, 0.2f32, 3e25f32)]
    #[case(0.1f64, 0.2f64, 3e100f64)]
    fn test_slice<T: ALPRDFloat>(#[case] a: T, #[case] b: T, #[case] outlier: T) {
        let array = PrimitiveArray::from(vec![a, b, outlier]);
        let encoded = Encoder::new(&[a, b]).encode(&array);

        assert!(encoded.left_parts_exceptions().is_some());

        let decoded = slice(encoded.as_ref(), 1, 3)
            .unwrap()
            .into_primitive()
            .unwrap();

        assert_eq!(decoded.maybe_null_slice::<T>(), &[b, outlier]);
    }
}
