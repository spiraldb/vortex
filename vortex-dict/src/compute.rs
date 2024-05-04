use vortex::compute::scalar_at::{scalar_at, ScalarAtFn};
use vortex::compute::slice::{slice, SliceFn};
use vortex::compute::take::{take, TakeFn};
use vortex::compute::ArrayCompute;
use vortex::{Array, IntoArray, OwnedArray};
use vortex_error::VortexResult;
use vortex_scalar::Scalar;

use crate::DictArray;

impl ArrayCompute for DictArray<'_> {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn slice(&self) -> Option<&dyn SliceFn> {
        Some(self)
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }
}

impl ScalarAtFn for DictArray<'_> {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        let dict_index: usize = scalar_at(&self.codes(), index)?.try_into()?;
        scalar_at(&self.values(), dict_index)
    }
}

impl TakeFn for DictArray<'_> {
    fn take(&self, indices: &Array) -> VortexResult<OwnedArray> {
        // Dict
        //   codes: 0 0 1
        //   dict: a b c d e f g h
        let codes = take(&self.codes(), indices)?;
        DictArray::try_new(codes, self.values()).map(|a| a.into_array())
    }
}

impl SliceFn for DictArray<'_> {
    // TODO(robert): Add function to trim the dictionary
    fn slice(&self, start: usize, stop: usize) -> VortexResult<OwnedArray> {
        DictArray::try_new(slice(&self.codes(), start, stop)?, self.values())
            .map(|a| a.into_array())
    }
}

#[cfg(test)]
mod test {
    use vortex::array::primitive::PrimitiveArray;
    use vortex::array::varbin::VarBinArray;
    use vortex::{IntoArray, ToArray};
    use vortex_dtype::DType;
    use vortex_dtype::Nullability;

    use crate::{dict_encode_typed_primitive, dict_encode_varbin, DictArray};

    #[test]
    fn flatten_nullable_primitive() {
        let reference = PrimitiveArray::from_nullable_vec(vec![
            Some(42),
            Some(-9),
            None,
            Some(42),
            None,
            Some(-9),
        ]);
        let (codes, values) = dict_encode_typed_primitive::<i32>(&reference);
        let dict = DictArray::try_new(codes.into_array(), values.into_array()).unwrap();
        let flattened_dict = dict.to_array().flatten_primitive().unwrap();
        assert_eq!(flattened_dict.buffer(), reference.buffer());
    }

    #[test]
    fn flatten_nullable_varbin() {
        let reference = VarBinArray::from_iter(
            vec![Some("a"), Some("b"), None, Some("a"), None, Some("b")],
            DType::Utf8(Nullability::Nullable),
        );
        let (codes, values) = dict_encode_varbin(&reference);
        let dict = DictArray::try_new(codes.into_array(), values.into_array()).unwrap();
        let flattened_dict = dict.to_array().flatten_varbin().unwrap();
        assert_eq!(
            flattened_dict
                .offsets()
                .flatten_primitive()
                .unwrap()
                .buffer(),
            reference.offsets().flatten_primitive().unwrap().buffer()
        );
        assert_eq!(
            flattened_dict.bytes().flatten_primitive().unwrap().buffer(),
            reference.bytes().flatten_primitive().unwrap().buffer()
        );
    }
}
