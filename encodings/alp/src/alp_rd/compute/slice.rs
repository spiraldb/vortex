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

    use crate::{Encoder, RealDouble};

    #[test]
    fn test_slice() {
        let a = 0.1f64.next_up();
        let b = 0.2f64.next_up();
        let outlier = 3e100f64.next_up();

        let array = PrimitiveArray::from(vec![a, b, outlier]);
        let encoded = Encoder::<RealDouble>::new(&[a, b]).encode(&array);

        assert!(encoded.left_parts_exceptions().is_some());

        let decoded = slice(encoded.as_ref(), 1, 3)
            .unwrap()
            .into_primitive()
            .unwrap();

        assert_eq!(decoded.maybe_null_slice::<f64>(), &[b, outlier]);
    }
}
