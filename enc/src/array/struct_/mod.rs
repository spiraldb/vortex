use std::borrow::Borrow;

use arrow2::array::Array as ArrowArray;
use arrow2::array::StructArray as ArrowStructArray;
use arrow2::datatypes::DataType;
use itertools::Itertools;

use crate::arrow::aligned_iter::AlignedArrowArrayIterator;
use crate::error::EncResult;
use crate::scalar::{Scalar, StructScalar};
use crate::types::DType;

use super::{Array, ArrayEncoding, ArrowIterator};

#[derive(Debug, Clone, PartialEq)]
pub struct StructArray {
    names: Vec<String>,
    fields: Vec<Array>,
}

impl StructArray {
    pub fn new(names: Vec<String>, fields: Vec<Array>) -> Self {
        assert!(
            fields.iter().map(|v| v.len()).all_equal(),
            "Fields didn't have the same length"
        );
        Self { names, fields }
    }
}

impl ArrayEncoding for StructArray {
    #[inline]
    fn len(&self) -> usize {
        self.fields.first().map_or(0, |a| a.len())
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    fn dtype(&self) -> DType {
        DType::Struct(
            self.names.clone(),
            self.fields.iter().map(|a| a.dtype().clone()).collect(),
        )
    }

    fn scalar_at(&self, index: usize) -> EncResult<Box<dyn Scalar>> {
        Ok(Box::new(StructScalar::new(
            self.names.clone(),
            self.fields
                .iter()
                .map(|field| field.scalar_at(index))
                .try_collect()?,
        )))
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        let datatype: DataType = self.dtype().borrow().into();
        Box::new(
            AlignedArrowArrayIterator::new(
                self.fields
                    .iter()
                    .map(|f| f.iter_arrow())
                    .collect::<Vec<_>>(),
            )
            .map(move |items| {
                Box::new(ArrowStructArray::new(datatype.clone(), items, None))
                    as Box<dyn ArrowArray>
            }),
        )
    }

    fn slice(&self, start: usize, stop: usize) -> EncResult<Array> {
        self.check_slice_bounds(start, stop)?;

        let fields = self
            .fields
            .iter()
            .map(|field| field.slice(start, stop))
            .try_collect()?;
        Ok(Array::Struct(StructArray::new(self.names.clone(), fields)))
    }
}

#[cfg(test)]
mod test {
    use arrow2::array::PrimitiveArray as ArrowPrimitiveArray;
    use arrow2::array::StructArray as ArrowStructArray;
    use arrow2::array::Utf8Array as ArrowUtf8Array;

    use crate::array::binary::VarBinArray;
    use crate::array::primitive::PrimitiveArray;
    use crate::array::struct_::StructArray;
    use crate::array::ArrayEncoding;

    #[test]
    pub fn iter() {
        let arrow_aas = ArrowPrimitiveArray::<i64>::from_vec(vec![1, 2, 3]);
        let aas: PrimitiveArray = arrow_aas.clone().into();
        let arrow_bbs = ArrowUtf8Array::<i32>::from_slice(["a", "b", "c"]);
        let bbs: VarBinArray = arrow_bbs.clone().into();
        let array = StructArray::new(vec!["a".into(), "b".into()], vec![aas.into(), bbs.into()]);
        let arrow_struct = ArrowStructArray::new(
            array.dtype().into(),
            vec![Box::new(arrow_aas), Box::new(arrow_bbs)],
            None,
        );
        assert_eq!(
            array
                .iter_arrow()
                .next()
                .unwrap()
                .as_any()
                .downcast_ref::<ArrowStructArray>()
                .unwrap(),
            &arrow_struct
        );
    }
}
