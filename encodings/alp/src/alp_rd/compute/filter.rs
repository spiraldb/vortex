use vortex::compute::{filter, FilterFn};
use vortex::{Array, ArrayDType, IntoArray};
use vortex_error::VortexResult;

use crate::ALPRDArray;

impl FilterFn for ALPRDArray {
    fn filter(&self, predicate: &Array) -> VortexResult<Array> {
        let left_parts_exceptions = match self.left_parts_exceptions() {
            None => None,
            Some(exc) => Some(filter(&exc, predicate)?),
        };

        Ok(ALPRDArray::try_new(
            self.dtype().clone(),
            filter(self.left_parts(), predicate)?,
            self.left_parts_dict(),
            filter(self.right_parts(), predicate)?,
            self.right_bit_width(),
            left_parts_exceptions,
        )?
        .into_array())
    }
}

#[cfg(test)]
mod test {
    use vortex::array::{BoolArray, PrimitiveArray};
    use vortex::compute::filter;
    use vortex::IntoArrayVariant;

    use crate::Encoder;

    macro_rules! test_filter_generic {
        ($typ:ty, $rd:ty) => {
            let a: $typ = (0.1 as $typ).next_up();
            let b: $typ = (0.2 as $typ).next_up();
            let outlier: $typ = (3e25 as $typ).next_up();

            let array = PrimitiveArray::from(vec![a, b, outlier]);
            let encoded = Encoder::new(&[a, b]).encode(&array);

            // Make sure that we're testing the exception pathway.
            assert!(encoded.left_parts_exceptions().is_some());

            // The first two values need no patching
            let filtered = filter(encoded.as_ref(), BoolArray::from(vec![true, false, true]))
                .unwrap()
                .into_primitive()
                .unwrap();
            assert_eq!(filtered.maybe_null_slice::<$typ>(), &[a, outlier]);
        };
    }

    #[test]
    fn test_filter() {
        test_filter_generic!(f32, RealFloat);
        test_filter_generic!(f64, RealDouble);
    }
}
