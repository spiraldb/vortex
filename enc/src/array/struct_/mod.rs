use std::sync::Arc;

use arrow::array::StructArray as ArrowStructArray;
use arrow::array::{Array as ArrowArray, ArrayRef};
use arrow::datatypes::Fields;
use itertools::Itertools;

use crate::arrow::aligned_iter::AlignedArrowArrayIterator;
use crate::error::EncResult;
use crate::scalar::{Scalar, StructScalar};
use crate::types::{DType, FieldNames};

use super::{Array, ArrayEncoding, ArrowIterator};

#[derive(Debug, Clone)]
pub struct StructArray {
    fields: Vec<Array>,
    dtype: DType,
}

impl StructArray {
    pub fn new(names: Vec<&str>, fields: Vec<Array>) -> Self {
        assert!(
            fields.iter().map(|v| v.len()).all_equal(),
            "Fields didn't have the same length"
        );
        let field_names: FieldNames = names.iter().map(|s| Arc::new((*s).to_owned())).collect();
        let dtype = DType::Struct(
            field_names,
            fields.iter().map(|a| a.dtype().clone()).collect(),
        );
        Self { fields, dtype }
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
    fn dtype(&self) -> &DType {
        &self.dtype
    }

    fn scalar_at(&self, index: usize) -> EncResult<Box<dyn Scalar>> {
        Ok(Box::new(StructScalar::new(
            self.dtype.clone(),
            self.fields
                .iter()
                .map(|field| field.scalar_at(index))
                .try_collect()?,
        )))
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        let fields: Fields = self.dtype().into();
        Box::new(
            AlignedArrowArrayIterator::new(
                self.fields
                    .iter()
                    .map(|f| f.iter_arrow())
                    .collect::<Vec<_>>(),
            )
            .map(move |items| {
                Arc::new(ArrowStructArray::new(
                    fields.clone(),
                    items.into_iter().map(ArrayRef::from).collect(),
                    None,
                )) as Arc<dyn ArrowArray>
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
        Ok(Array::Struct(Self {
            fields,
            dtype: self.dtype.clone(),
        }))
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::array::types::UInt64Type;
    use arrow::array::PrimitiveArray as ArrowPrimitiveArray;
    use arrow::array::StructArray as ArrowStructArray;
    use arrow::array::{Array, GenericStringArray as ArrowStringArray};

    use crate::array::struct_::StructArray;
    use crate::array::ArrayEncoding;

    #[test]
    pub fn iter() {
        let arrow_aas = ArrowPrimitiveArray::<UInt64Type>::from(vec![1, 2, 3]);
        let arrow_bbs = ArrowStringArray::<i32>::from(vec!["a", "b", "c"]);

        let array = StructArray::new(
            vec!["a", "b"],
            vec![(&arrow_aas).into(), (&arrow_bbs).into()],
        );
        let arrow_struct = ArrowStructArray::new(
            array.dtype().into(),
            vec![Arc::new(arrow_aas), Arc::new(arrow_bbs)],
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
