pub use adapter::*;
pub use ext::*;
use vortex_dtype::{DType, PType};
use vortex_error::VortexResult;

use crate::compute::unary::scalar_at_unchecked;
use crate::{Array, ArrayDType};

mod adapter;
mod ext;

/// A stream of array chunks along with a DType.
/// Analogous to Arrow's RecordBatchReader.
pub trait ArrayIterator: Iterator<Item = VortexResult<Array>> {
    fn dtype(&self) -> &DType;
}

pub struct ArrayIter<T> {
    accessor: T,
    current: usize,
}

impl<T> ArrayIter<T> {
    pub(crate) fn new(accessor: T) -> Self {
        Self {
            accessor,
            current: 0,
        }
    }
}

pub trait Accessor {
    type O;

    fn len(&self) -> usize;
    fn is_valid(&self, index: usize) -> bool;
    fn value_unchecked(&self, index: usize) -> Self::O;
}

pub struct Float32Accessor {
    inner: Array,
}

impl Float32Accessor {
    pub(crate) fn new(inner: Array) -> Self {
        Self { inner }
    }
}

impl Accessor for Float32Accessor {
    type O = f32;

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn is_valid(&self, index: usize) -> bool {
        self.inner.with_dyn(|a| a.is_valid(index))
    }

    fn value_unchecked(&self, index: usize) -> Self::O {
        scalar_at_unchecked(&self.inner, index).try_into().unwrap()
    }
}

pub type F32Iter = ArrayIter<Float32Accessor>;

pub fn as_f32_array_opt(array: &Array) -> Option<F32Iter> {
    match array.dtype() {
        DType::Primitive(PType::F32, _) => {
            Some(ArrayIter::new(Float32Accessor::new(array.clone())))
        }
        _ => None,
    }
}

impl<A: Accessor> Iterator for ArrayIter<A> {
    type Item = Option<A::O>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current == self.accessor.len() {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::PrimitiveArray;
    use crate::validity::Validity;
    use crate::IntoArray;

    #[test]
    #[should_panic = "Panicking here because the types don't match"]
    fn iter_example() {
        let v = PrimitiveArray::from_vec(vec![1.0_f32, 1.0, 2.0, 3.0, 5.0], Validity::AllValid)
            .into_array();
        let f_iter = as_f32_array_opt(&v).unwrap();

        for f in f_iter {
            let f = f.unwrap();
            println!("{f}");
        }

        let v = PrimitiveArray::from_vec(vec![1.0_f64, 1.0, 2.0, 3.0, 5.0], Validity::AllValid)
            .into_array();
        as_f32_array_opt(&v).expect("Panicking here because the types don't match");
    }
}
