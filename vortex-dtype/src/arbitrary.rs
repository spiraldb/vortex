use arbitrary::{Arbitrary, Result, Unstructured};

use crate::{DType, FieldNames, Nullability, PType, StructDType};

impl<'a> Arbitrary<'a> for DType {
    fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
        Ok(match u.int_in_range(0..=4)? {
            0 => DType::Bool(u.arbitrary()?),
            1 => DType::Struct(u.arbitrary()?, u.arbitrary()?),
            2 => DType::Utf8(u.arbitrary()?),
            3 => DType::Binary(u.arbitrary()?),
            4 => DType::Primitive(u.arbitrary()?, u.arbitrary()?),
            // Null,
            // List(Arc<DType>, Nullability),
            // Extension(ExtDType, Nullability),
            _ => unreachable!("Number out of range"),
        })
    }
}

impl<'a> Arbitrary<'a> for Nullability {
    fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
        Ok(if u.arbitrary()? {
            Nullability::Nullable
        } else {
            Nullability::NonNullable
        })
    }
}

impl<'a> Arbitrary<'a> for PType {
    fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
        Ok(match u.int_in_range(0..=10)? {
            0 => PType::U8,
            1 => PType::U16,
            2 => PType::U32,
            3 => PType::U64,
            4 => PType::I8,
            5 => PType::I16,
            6 => PType::I32,
            7 => PType::I64,
            8 => PType::F16,
            9 => PType::F32,
            10 => PType::F64,
            _ => unreachable!("Number out of range"),
        })
    }
}

impl<'a> Arbitrary<'a> for StructDType {
    fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
        let names: FieldNames = u.arbitrary()?;
        let dtypes = (0..names.len())
            .map(|_| u.arbitrary())
            .collect::<Result<Vec<_>>>()?;
        Ok(StructDType::new(names, dtypes))
    }
}
