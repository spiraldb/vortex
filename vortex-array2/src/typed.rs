use std::marker::PhantomData;

use crate::{Array, ArrayDef};

#[derive(Debug)]
pub struct TypedArray<'a, D: ArrayDef> {
    array: Array<'a>,
    phantom: PhantomData<D>,
}

impl<D: ArrayDef> Clone for TypedArray<'_, D> {
    fn clone(&self) -> Self {
        Self {
            array: self.array.clone(),
            phantom: PhantomData,
        }
    }
}

impl<D: ArrayDef> TypedArray<'_, D> {}
