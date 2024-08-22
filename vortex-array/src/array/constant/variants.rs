use std::iter;

use vortex_dtype::DType;
use vortex_scalar::StructScalar;

use crate::array::constant::ConstantArray;
use crate::iter::ArrayIter;
use crate::variants::{
    ArrayVariants, BinaryArrayTrait, BoolArrayTrait, ExtensionArrayTrait, ListArrayTrait,
    NullArrayTrait, PrimitiveArrayTrait, StructArrayTrait, Utf8ArrayTrait,
};
use crate::{Array, ArrayDType, IntoArray};

/// Constant arrays support all DTypes
impl ArrayVariants for ConstantArray {
    fn as_null_array(&self) -> Option<&dyn NullArrayTrait> {
        matches!(self.dtype(), DType::Null).then_some(self)
    }

    fn as_bool_array(&self) -> Option<&dyn BoolArrayTrait> {
        matches!(self.dtype(), DType::Bool(_)).then_some(self)
    }

    fn as_primitive_array(&self) -> Option<&dyn PrimitiveArrayTrait> {
        matches!(self.dtype(), DType::Primitive(..)).then_some(self)
    }

    fn as_utf8_array(&self) -> Option<&dyn Utf8ArrayTrait> {
        matches!(self.dtype(), DType::Utf8(_)).then_some(self)
    }

    fn as_binary_array(&self) -> Option<&dyn BinaryArrayTrait> {
        matches!(self.dtype(), DType::Binary(_)).then_some(self)
    }

    fn as_struct_array(&self) -> Option<&dyn StructArrayTrait> {
        matches!(self.dtype(), DType::Struct(..)).then_some(self)
    }

    fn as_list_array(&self) -> Option<&dyn ListArrayTrait> {
        matches!(self.dtype(), DType::List(..)).then_some(self)
    }

    fn as_extension_array(&self) -> Option<&dyn ExtensionArrayTrait> {
        matches!(self.dtype(), DType::Extension(..)).then_some(self)
    }
}

impl NullArrayTrait for ConstantArray {}

impl BoolArrayTrait for ConstantArray {
    fn maybe_null_indices_iter(&self) -> Box<dyn Iterator<Item = usize>> {
        let value = self.scalar().value().as_bool().unwrap_or_else(|err| {
            panic!("Failed to get bool value from constant array: {}", err);
        });
        if value.unwrap_or(false) {
            Box::new(0..self.len())
        } else {
            Box::new(iter::empty())
        }
    }

    fn maybe_null_slices_iter(&self) -> Box<dyn Iterator<Item = (usize, usize)>> {
        // Must be a boolean scalar
        let value = self.scalar().value().as_bool().unwrap_or_else(|err| {
            panic!("Failed to get bool value from constant array: {}", err);
        });

        if value.unwrap_or(false) {
            Box::new(iter::once((0, self.len())))
        } else {
            Box::new(iter::empty())
        }
    }
}

impl PrimitiveArrayTrait for ConstantArray {
    fn float32_iter(&self) -> Option<ArrayIter<f32>> {
        todo!()
    }

    fn float64_iter(&self) -> Option<ArrayIter<f64>> {
        todo!()
    }

    fn unsigned32_iter(&self) -> Option<ArrayIter<u32>> {
        todo!()
    }
}

impl Utf8ArrayTrait for ConstantArray {}

impl BinaryArrayTrait for ConstantArray {}

impl StructArrayTrait for ConstantArray {
    fn field(&self, idx: usize) -> Option<Array> {
        StructScalar::try_from(self.scalar())
            .ok()?
            .field_by_idx(idx)
            .map(|scalar| ConstantArray::new(scalar, self.len()).into_array())
    }
}

impl ListArrayTrait for ConstantArray {}

impl ExtensionArrayTrait for ConstantArray {}

#[cfg(test)]
mod test {
    use itertools::Itertools;
    use vortex_dtype::Nullability;
    use vortex_scalar::Scalar;

    use crate::array::constant::ConstantArray;
    use crate::variants::BoolArrayTrait;

    #[test]
    fn constant_iter_true_test() {
        let arr = ConstantArray::new(Scalar::bool(true, Nullability::NonNullable), 3);
        assert_eq!(vec![0, 1, 2], arr.maybe_null_indices_iter().collect_vec());
        assert_eq!(vec![(0, 3)], arr.maybe_null_slices_iter().collect_vec());
    }

    #[test]
    fn constant_iter_false_test() {
        let arr = ConstantArray::new(Scalar::bool(false, Nullability::NonNullable), 3);
        assert_eq!(0, arr.maybe_null_indices_iter().collect_vec().len());
        assert_eq!(0, arr.maybe_null_slices_iter().collect_vec().len());
    }
}
