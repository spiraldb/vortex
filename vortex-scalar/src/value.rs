use std::sync::Arc;

use paste::paste;
use vortex_buffer::Buffer;
use vortex_dtype::half::f16;
use vortex_dtype::NativePType;

/// Represents the internal data of a scalar value. Can only be interpreted by wrapping
/// up with a DType to make a Scalar.
///
/// This is similar to serde_json::Value, but uses our own Buffer implementation for bytes,
/// an Arc<[]> for list elements, and structs are modelled as lists.
///
/// TODO(ngates): we could support reading structs from both structs and lists in the future since
///  storing sparse structs dense with null scalars may be inefficient.
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum ScalarValue {
    Null,
    Bool(bool),
    Buffer(Buffer),
    List(Arc<[ScalarValue]>),
}

impl ScalarValue {
    pub fn is_null(&self) -> bool {
        matches!(self, ScalarValue::Null)
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            ScalarValue::Bool(b) => Some(*b),
            _ => None,
        }
    }

    pub fn as_primitive<T: NativePType>(&self) -> Option<T> {
        match self {
            ScalarValue::Buffer(b) => T::try_from_le_bytes(b.as_ref()).ok(),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<Buffer> {
        match self {
            ScalarValue::Buffer(b) => Some(b.clone()),
            _ => None,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            ScalarValue::List(l) => l.len(),
            _ => 0,
        }
    }

    pub fn child(&self, idx: usize) -> Option<ScalarValue> {
        match self {
            ScalarValue::List(l) => l.get(idx).cloned(),
            _ => None,
        }
    }
}

macro_rules! primitive_from_scalar_view {
    ($T:ty) => {
        paste! {
            impl From<$T> for ScalarValue {
                fn from(value: $T) -> Self {
                    ScalarValue::Buffer(Buffer::from(value.to_le_bytes().as_ref().to_vec()))
                }
            }
        }
    };
}

primitive_from_scalar_view!(u8);
primitive_from_scalar_view!(u16);
primitive_from_scalar_view!(u32);
primitive_from_scalar_view!(u64);
primitive_from_scalar_view!(i8);
primitive_from_scalar_view!(i16);
primitive_from_scalar_view!(i32);
primitive_from_scalar_view!(i64);
primitive_from_scalar_view!(f32);
primitive_from_scalar_view!(f64);

impl From<f16> for ScalarValue {
    fn from(value: f16) -> Self {
        ScalarValue::Buffer(Buffer::from(value.to_le_bytes().as_ref().to_vec()))
    }
}
