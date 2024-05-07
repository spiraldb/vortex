use std::cmp::Ordering;
use std::sync::Arc;

use flexbuffers::{FlexBufferType, Reader};
use paste::paste;
use vortex_buffer::Buffer;
use vortex_dtype::half::f16;
use vortex_dtype::NativePType;
use vortex_error::VortexResult;
use ScalarValue::{Data, View};

// Internal enum to hide implementation from consumers.
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub(crate) enum ScalarValue {
    Data(ScalarData),
    // A lazily deserialized view over a flexbuffer.
    #[allow(dead_code)]
    View(ScalarView),
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum ScalarData {
    None,
    Bool(bool),
    #[allow(dead_code)]
    Buffer(Buffer),
    #[allow(dead_code)]
    List(Arc<[ScalarData]>),
}

#[derive(Debug, Clone)]
pub struct ScalarView(pub(crate) Reader<Buffer>);

impl ScalarView {
    pub fn try_new(buffer: Buffer) -> VortexResult<Self> {
        // Verify that the buffer contains valid flexbuffer data
        Ok(Self(Reader::get_root(buffer)?))
    }
}

impl PartialEq for ScalarView {
    fn eq(&self, other: &Self) -> bool {
        self.0.buffer() == other.0.buffer()
    }
}

impl PartialOrd for ScalarView {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.0.buffer().partial_cmp(&other.0.buffer())
    }
}

impl ScalarValue {
    pub fn is_null(&self) -> bool {
        match self {
            Data(data) => matches!(data, ScalarData::None),
            View(view) => view.0.flexbuffer_type() == FlexBufferType::Null,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Data(d) => match d {
                ScalarData::Bool(b) => Some(*b),
                _ => None,
            },
            View(v) => v.0.get_bool().ok(),
        }
    }

    pub fn as_primitive<T: NativePType + for<'a> From<&'a ScalarView>>(&self) -> Option<T> {
        match self {
            Data(d) => match d {
                ScalarData::Buffer(b) => T::try_from_le_bytes(b.as_ref()).ok(),
                _ => None,
            },
            View(v) => Some(v.into()),
        }
    }

    pub fn as_bytes(&self) -> Option<Buffer> {
        match self {
            Data(d) => match d {
                ScalarData::Buffer(b) => Some(b.clone()),
                _ => None,
            },
            View(v) => Some(v.0.as_blob().0),
        }
    }

    pub fn child(&self, idx: usize) -> Option<ScalarValue> {
        match self {
            Data(d) => match d {
                ScalarData::List(l) => l.get(idx).cloned().map(Data),
                _ => None,
            },
            View(v) => ScalarView::try_new(v.0.as_vector().idx(idx).buffer())
                .ok()
                .map(View),
        }
    }
}

macro_rules! primitive_from_scalar_view {
    ($T:ty) => {
        paste! {
            impl From<&ScalarView> for $T {
                fn from(value: &ScalarView) -> Self {
                    value.0.[<as_ $T>]().into()
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

impl<'a> From<&ScalarView> for f16 {
    fn from(value: &ScalarView) -> Self {
        f16::from_le_bytes(value.0.as_u16().to_le_bytes())
    }
}

impl<T: num_traits::ToBytes> From<T> for ScalarData {
    fn from(value: T) -> Self {
        ScalarData::Buffer(Buffer::from(value.to_le_bytes().as_ref().to_vec()))
    }
}
