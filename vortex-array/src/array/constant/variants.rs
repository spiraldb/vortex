use std::iter;

use vortex_dtype::DType;
use vortex_scalar::StructScalar;

use crate::array::constant::ConstantArray;
use crate::variants::{
    ArrayVariants, BinaryArrayTrait, BoolArrayTrait, ExtensionArrayTrait, ListArrayTrait,
    NullArrayTrait, PrimitiveArrayTrait, StructArrayTrait, Utf8ArrayTrait,
};
use crate::{Array, ArrayDType, IntoArray};

/// Constant arrays support all DTypes
impl ArrayVariants for ConstantArray {
    fn as_null_array(&self) -> Option<&dyn NullArrayTrait> {
        if matches!(self.dtype(), DType::Null) {
            Some(self)
        } else {
            None
        }
    }

    fn as_bool_array(&self) -> Option<&dyn BoolArrayTrait> {
        if matches!(self.dtype(), DType::Bool(_)) {
            Some(self)
        } else {
            None
        }
    }

    fn as_primitive_array(&self) -> Option<&dyn PrimitiveArrayTrait> {
        if matches!(self.dtype(), DType::Primitive(..)) {
            Some(self)
        } else {
            None
        }
    }

    fn as_utf8_array(&self) -> Option<&dyn Utf8ArrayTrait> {
        if matches!(self.dtype(), DType::Utf8(_)) {
            Some(self)
        } else {
            None
        }
    }

    fn as_binary_array(&self) -> Option<&dyn BinaryArrayTrait> {
        if matches!(self.dtype(), DType::Binary(_)) {
            Some(self)
        } else {
            None
        }
    }

    fn as_struct_array(&self) -> Option<&dyn StructArrayTrait> {
        if matches!(self.dtype(), DType::Struct(..)) {
            Some(self)
        } else {
            None
        }
    }

    fn as_list_array(&self) -> Option<&dyn ListArrayTrait> {
        if matches!(self.dtype(), DType::List(..)) {
            Some(self)
        } else {
            None
        }
    }

    fn as_extension_array(&self) -> Option<&dyn ExtensionArrayTrait> {
        if matches!(self.dtype(), DType::Extension(..)) {
            Some(self)
        } else {
            None
        }
    }
}

impl NullArrayTrait for ConstantArray {}

impl BoolArrayTrait for ConstantArray {
    fn maybe_null_indices_iter(&self) -> Box<dyn Iterator<Item = usize>> {
        let value = self.scalar().value().as_bool().unwrap();
        if value.unwrap_or(false) {
            Box::new(iter::successors(Some(0), |x| Some(*x + 1)).take(self.len()))
        } else {
            Box::new(iter::empty())
        }
    }

    fn maybe_null_slices_iter(&self) -> Box<dyn Iterator<Item = (usize, usize)>> {
        // Must be a boolean scalar
        let value = self.scalar().value().as_bool().unwrap();

        if value.unwrap_or(false) {
            Box::new(iter::once((0, self.len())))
        } else {
            Box::new(iter::empty())
        }
    }
}

impl PrimitiveArrayTrait for ConstantArray {}

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
