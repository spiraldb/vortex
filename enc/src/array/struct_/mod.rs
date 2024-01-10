use itertools::Itertools;

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
        // TODO(ngates): assert that all fields have the same length
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
        // We probably ought to implement the aligned iterator to zip up the chunks of each field.
        todo!("struct array iter arrow")
    }

    fn slice(&self, offset: usize, length: usize) -> EncResult<Array> {
        let fields = self
            .fields
            .iter()
            .map(|field| field.slice(offset, length))
            .try_collect()?;
        Ok(Array::Struct(StructArray::new(self.names.clone(), fields)))
    }
}
