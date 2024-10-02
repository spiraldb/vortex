use vortex::compute::unary::{scalar_at, ScalarAtFn};
use vortex::ArrayDType;
use vortex_dtype::PType;
use vortex_error::{VortexResult, VortexUnwrap};
use vortex_scalar::Scalar;

use crate::alp_rd::array::ALPRDArray;

impl ScalarAtFn for ALPRDArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        // The left value can either be a direct value, or an exception.
        let left: u16 = match self.left_parts_exceptions() {
            Some(exceptions) if exceptions.with_dyn(|a| a.is_valid(index)) => {
                scalar_at(&exceptions, index)?.try_into()?
            }
            _ => {
                let left_code: u16 = scalar_at(&self.left_parts(), index)?.try_into()?;
                self.left_parts_dict()[left_code as usize]
            }
        };

        // combine left and right values
        if self.dtype().ptype() == Some(PType::F64) {
            let right: u64 = scalar_at(&self.right_parts(), index)?.try_into()?;
            let packed = f64::from_bits(((left as u64) << self.right_bit_width()) | right);
            Ok(packed.into())
        } else {
            let right: u32 = scalar_at(&self.right_parts(), index)?.try_into()?;
            let packed = f32::from_bits((left as u32) << self.right_bit_width() | right);
            Ok(packed.into())
        }
    }

    fn scalar_at_unchecked(&self, index: usize) -> Scalar {
        self.scalar_at(index).vortex_unwrap()
    }
}

#[cfg(test)]
mod test {
    use rstest::rstest;
    use vortex::array::PrimitiveArray;
    use vortex::compute::unary::scalar_at;
    use vortex_scalar::Scalar;

    use crate::{ALPRDFloat, Encoder};

    #[rstest]
    #[case(0.1f32, 0.2f32, 3e25f32)]
    #[case(0.1f64, 0.2f64, 3e100f64)]
    fn test_scalar_at<T: ALPRDFloat + Into<Scalar>>(
        #[case] a: T,
        #[case] b: T,
        #[case] outlier: T,
    ) {
        let array = PrimitiveArray::from(vec![a, b, outlier]);
        let encoded = Encoder::new(&[a, b]).encode(&array);

        // Make sure that we're testing the exception pathway.
        assert!(encoded.left_parts_exceptions().is_some());

        // The first two values need no patching
        assert_eq!(scalar_at(encoded.as_ref(), 0).unwrap(), a.into());
        assert_eq!(scalar_at(encoded.as_ref(), 1).unwrap(), b.into());

        // The right value hits the left_part_exceptions
        assert_eq!(scalar_at(encoded.as_ref(), 2).unwrap(), outlier.into());
    }
}
