/// This module defines array traits for each Vortex DType.
///
/// When callers only want to make assumptions about the DType, and not about any specific
/// encoding, they can use these traits to write encoding-agnostic code.
use crate::{Array, ArrayTrait};

pub trait NullArrayTrait: ArrayTrait {}

pub trait BoolArrayTrait: ArrayTrait {}

pub trait PrimitiveArrayTrait: ArrayTrait {}

pub trait Utf8ArrayTrait: ArrayTrait {}

pub trait BinaryArrayTrait: ArrayTrait {}

pub trait StructArrayTrait: ArrayTrait {
    fn field(&self, idx: usize) -> Option<Array>;

    fn field_by_name(&self, name: &str) -> Option<Array>;
}

pub trait ListArrayTrait: ArrayTrait {}

pub trait ExtensionArrayTrait: ArrayTrait {}
