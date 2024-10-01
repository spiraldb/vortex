use vortex::compute::{slice, SliceFn};
use vortex::{Array, ArrayDType, IntoArray};
use vortex_error::VortexResult;

use crate::ALPRDArray;

impl SliceFn for ALPRDArray {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<Array> {
        let left_parts_exceptions = match self.left_parts_exceptions() {
            None => None,
            Some(exc) => Some(slice(&exc, start, stop)?),
        };

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
    use vortex::array::PrimitiveArray;
    use vortex::compute::slice;
    use vortex::IntoArrayVariant;

    use crate::{Encoder, RealDouble, RealFloat};

    macro_rules! test_slice_generic {
        ($typ:ty, $rd:ty) => {
            let a: $typ = (0.1 as $typ).next_up();
            let b: $typ = (0.2 as $typ).next_up();
            let outlier: $typ = (3e30 as $typ).next_up();

            let array = PrimitiveArray::from(vec![a, b, outlier]);
            let encoded = Encoder::<$rd>::new(&[a, b]).encode(&array);

            assert!(encoded.left_parts_exceptions().is_some());

            let decoded = slice(encoded.as_ref(), 1, 3)
                .unwrap()
                .into_primitive()
                .unwrap();

            assert_eq!(decoded.maybe_null_slice::<$typ>(), &[b, outlier]);
        };
    }

    #[test]
    fn test_slice() {
        test_slice_generic!(f32, RealFloat);
        test_slice_generic!(f64, RealDouble);
    }
}
