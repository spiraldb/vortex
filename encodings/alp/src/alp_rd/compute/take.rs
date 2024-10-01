use vortex::compute::{take, TakeFn};
use vortex::{Array, ArrayDType, IntoArray};
use vortex_error::VortexResult;

use crate::ALPRDArray;

impl TakeFn for ALPRDArray {
    fn take(&self, indices: &Array) -> VortexResult<Array> {
        let left_parts_exceptions = match self.left_parts_exceptions() {
            None => None,
            Some(exc) => Some(take(&exc, indices)?),
        };

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
    use vortex::array::PrimitiveArray;
    use vortex::compute::take;
    use vortex::IntoArrayVariant;

    use crate::{Encoder, RealDouble};

    #[test]
    fn test_take() {
        let a = 0.1f64.next_up();
        let b = 0.2f64.next_up();
        let outlier = 3e100f64.next_up();

        let array = PrimitiveArray::from(vec![a, b, outlier]);
        let encoded = Encoder::<RealDouble>::new(&[a, b]).encode(&array);

        assert!(encoded.left_parts_exceptions().is_some());

        let taken = take(encoded.as_ref(), PrimitiveArray::from(vec![0, 2]).as_ref())
            .unwrap()
            .into_primitive()
            .unwrap();

        assert_eq!(taken.maybe_null_slice::<f64>(), &[a, outlier]);
    }
}
