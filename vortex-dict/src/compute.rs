use std::sync::Arc;

use vortex::array::primitive::PrimitiveArray;
use vortex::array::varbin::VarBinArray;
use vortex::compute::flatten::{flatten, flatten_primitive, FlattenFn, FlattenedArray};
use vortex::compute::scalar_at::{scalar_at, ScalarAtFn};
use vortex::compute::take::take;
use vortex::compute::ArrayCompute;
use vortex::scalar::Scalar;
use vortex_error::{VortexError, VortexResult};

use crate::DictArray;

impl ArrayCompute for DictArray {
    fn flatten(&self) -> Option<&dyn FlattenFn> {
        Some(self)
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
}

impl ScalarAtFn for DictArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        let dict_index: usize = scalar_at(self.codes(), index)?.try_into()?;
        scalar_at(self.values(), dict_index)
    }
}

impl FlattenFn for DictArray {
    fn flatten(&self) -> VortexResult<FlattenedArray> {
        let codes = flatten_primitive(self.codes())?;
        let values = flatten(self.values())?;

        match values {
            FlattenedArray::Primitive(v) => take(&v, &codes).map(|r| {
                FlattenedArray::Primitive(
                    Arc::try_unwrap(r.into_any().downcast::<PrimitiveArray>().unwrap())
                        .expect("Expected take on PrimitiveArray array to produce new array"),
                )
            }),
            FlattenedArray::VarBin(vb) => take(&vb, &codes).map(|r| {
                FlattenedArray::VarBin(
                    Arc::try_unwrap(r.into_any().downcast::<VarBinArray>().unwrap())
                        .expect("Expected take on VarBin array to produce new array"),
                )
            }),
            _ => Err(VortexError::InvalidArgument(
                "Only VarBin and Primitive values array are supported".into(),
            )),
        }
    }
}

#[cfg(test)]
mod test {
    use vortex::array::downcast::DowncastArrayBuiltin;
    use vortex::array::primitive::PrimitiveArray;
    use vortex::array::varbin::VarBinArray;
    use vortex::array::Array;
    use vortex::compute::flatten::{flatten_primitive, flatten_varbin};
    use vortex_schema::{DType, Nullability};

    use crate::{dict_encode_typed_primitive, dict_encode_varbin, DictArray};

    #[test]
    fn flatten_nullable_primitive() {
        let reference =
            PrimitiveArray::from_iter(vec![Some(42), Some(-9), None, Some(42), None, Some(-9)]);
        let (codes, values) = dict_encode_typed_primitive::<i32>(&reference);
        let dict = DictArray::new(codes.to_array(), values.to_array());
        let flattened_dict = flatten_primitive(&dict).unwrap();
        assert_eq!(flattened_dict.buffer(), reference.buffer());
    }

    #[test]
    fn flatten_nullable_varbin() {
        let reference = VarBinArray::from_iter(
            vec![Some("a"), Some("b"), None, Some("a"), None, Some("b")],
            DType::Utf8(Nullability::Nullable),
        );
        let (codes, values) = dict_encode_varbin(&reference);
        let dict = DictArray::new(codes.to_array(), values.to_array());
        let flattened_dict = flatten_varbin(&dict).unwrap();
        assert_eq!(
            flattened_dict.offsets().as_primitive().buffer(),
            reference.offsets().as_primitive().buffer()
        );
        assert_eq!(
            flattened_dict.bytes().as_primitive().buffer(),
            reference.bytes().as_primitive().buffer()
        );
    }
}
