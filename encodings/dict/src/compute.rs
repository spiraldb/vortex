use vortex::compute::unary::{scalar_at, ScalarAtFn};
use vortex::compute::{slice, take, ArrayCompute, SliceFn, TakeFn};
use vortex::{Array, IntoArray};
use vortex_error::VortexResult;
use vortex_scalar::Scalar;

use crate::DictArray;

impl ArrayCompute for DictArray {
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

impl ScalarAtFn for DictArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        let dict_index: usize = scalar_at(&self.codes(), index)?.as_ref().try_into()?;
        scalar_at(&self.values(), dict_index)
    }
}

impl TakeFn for DictArray {
    fn take(&self, indices: &Array) -> VortexResult<Array> {
        // Dict
        //   codes: 0 0 1
        //   dict: a b c d e f g h
        let codes = take(&self.codes(), indices)?;
        Self::try_new(codes, self.values()).map(|a| a.into_array())
    }
}

impl SliceFn for DictArray {
    // TODO(robert): Add function to trim the dictionary
    fn slice(&self, start: usize, stop: usize) -> VortexResult<Array> {
        Self::try_new(slice(&self.codes(), start, stop)?, self.values()).map(|a| a.into_array())
    }
}

#[cfg(test)]
mod test {
    use vortex::accessor::ArrayAccessor;
    use vortex::array::primitive::PrimitiveArray;
    use vortex::array::varbinview::VarBinViewArray;
    use vortex::{IntoArray, IntoArrayVariant, ToArray};

    use crate::{dict_encode, dict_encode_typed_primitive, DictArray};

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
        let flattened_dict = dict.to_array().into_primitive().unwrap();
        assert_eq!(flattened_dict.buffer(), reference.buffer());
    }

    #[test]
    fn flatten_nullable_varbin() {
        let reference = VarBinViewArray::from_iter_nullable_str(vec![
            Some("a"),
            Some("b"),
            None,
            Some("c"),
            None,
            Some("d"),
        ]);
        let (codes, values) = dict_encode(&reference);
        let dict = DictArray::try_new(codes.into_array(), values.into_array()).unwrap();
        let flattened_dict = dict.to_array().into_varbinview().unwrap();

        assert_eq!(
            flattened_dict.views().into_primitive().unwrap().buffer(),
            reference.views().into_primitive().unwrap().buffer(),
        );

        // All values should be preserved here as well.
        let data: Vec<Option<String>> = flattened_dict
            .with_iterator(|iter| {
                iter.map(|s| s.map(|i| String::from_utf8_lossy(i).to_string()))
                    .collect()
            })
            .unwrap();
        assert_eq!(
            data,
            vec![
                Some("a".to_string()),
                Some("b".to_string()),
                None,
                Some("c".to_string()),
                None,
                Some("d".to_string()),
            ]
        );
    }
}
