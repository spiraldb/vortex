use std::marker::PhantomData;
use std::sync::Arc;

pub use adapter::*;
pub use ext::*;
use vortex_dtype::{DType, NativePType};
use vortex_error::VortexResult;

use crate::array::PrimitiveArray;
use crate::validity::ArrayValidity;
use crate::Array;

mod adapter;
mod ext;

/// A stream of array chunks along with a DType.
/// Analogous to Arrow's RecordBatchReader.
pub trait ArrayIterator: Iterator<Item = VortexResult<Array>> {
    fn dtype(&self) -> &DType;
}

pub struct ArrayIter<T> {
    accessor: Arc<dyn Accessor<T>>,
    current: usize,
}

impl<T> ArrayIter<T> {
    pub(crate) fn new(accessor: Arc<dyn Accessor<T>>) -> Self {
        Self {
            accessor,
            current: 0,
        }
    }
}

pub trait Accessor<O> {
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn is_valid(&self, index: usize) -> bool;
    fn value_unchecked(&self, index: usize) -> O;
}

pub struct PrimitiveAccessor<T> {
    array: PrimitiveArray,
    _marker: PhantomData<T>,
}

impl<T: NativePType> Accessor<T> for PrimitiveAccessor<T> {
    fn len(&self) -> usize {
        self.array.len()
    }

    fn is_valid(&self, index: usize) -> bool {
        self.array.is_valid(index)
    }

    fn value_unchecked(&self, index: usize) -> T {
        let start = index * std::mem::size_of::<T>();
        let end = (index + 1) * std::mem::size_of::<T>();
        T::try_from_le_bytes(&self.array.buffer()[start..end]).unwrap()
    }
}

impl<T: NativePType> PrimitiveAccessor<T> {
    pub fn new(array: PrimitiveArray) -> Self {
        Self {
            array,
            _marker: PhantomData,
        }
    }
}

impl<T> Iterator for ArrayIter<T> {
    type Item = Option<T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current == self.accessor.as_ref().len() {
            None
        } else if !self.accessor.is_valid(self.current) {
            self.current += 1;
            Some(None)
        } else {
            let v = self.accessor.value_unchecked(self.current);
            self.current += 1;
            Some(Some(v))
        }
    }
}
