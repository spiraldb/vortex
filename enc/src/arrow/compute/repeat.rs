use arrow2::array::{Array, PrimitiveArray};
use arrow2::datatypes::PhysicalType::Primitive;
use arrow2::scalar::PrimitiveScalar;
use arrow2::scalar::Scalar;
use arrow2::types::NativeType;
use arrow2::with_match_primitive_without_interval_type;

pub fn repeat(scalar: &dyn Scalar, n: usize) -> Box<dyn Array> {
    match scalar.data_type().to_physical_type() {
        Primitive(prim) => {
            with_match_primitive_without_interval_type!(prim, |$T| {
                let primitive_scalar: &PrimitiveScalar<$T> = scalar
                    .as_any()
                    .downcast_ref::<PrimitiveScalar<$T>>()
                    .unwrap();
                repeat_primitive(primitive_scalar.value(), n)
            })
        }
        _ => todo!("Not implemented yet"),
    }
}

fn repeat_primitive<T: NativeType>(value: &Option<T>, n: usize) -> Box<PrimitiveArray<T>> {
    let mut arr = arrow2::array::MutablePrimitiveArray::<T>::with_capacity(n);
    arr.extend_constant(n, *value);
    Box::new(arr.into())
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_repeat() {
        let scalar = PrimitiveScalar::from(Some::<u64>(47));
        let array = repeat(&scalar, 100);
        assert_eq!(array.len(), 100);
        assert_eq!(
            array
                .as_any()
                .downcast_ref::<PrimitiveArray<u64>>()
                .unwrap()
                .value(50),
            47
        );
    }
}
