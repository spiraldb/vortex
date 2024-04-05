mod compute;
mod context;
mod data;
mod encoding;
mod implementation;
mod metadata;
mod primitive;
mod ree;
mod validity;
mod view;

use std::fmt::Debug;

pub use compute::*;
pub use context::*;
pub use data::*;
pub use encoding::*;
pub use implementation::*;
pub use metadata::*;
pub use validity::*;
pub use view::*;
use vortex_schema::DType;

use crate::array2::ArrayData;
use crate::array2::ArrayEncoding;
use crate::array2::ArrayView;

// An array enum, similar to Cow.
#[derive(Debug, Clone)]
pub enum Array<'v> {
    Data(ArrayData),
    DataRef(&'v ArrayData),
    View(ArrayView<'v>),
}

pub trait ToArray {
    fn to_array(&self) -> Array;
}

pub trait IntoArray<'a> {
    fn into_array(self) -> Array<'a>;
}

pub trait ToArrayData {
    fn to_array_data(&self) -> ArrayData;
}

/// Collects together the behaviour of an array.
pub trait ArrayTrait: ArrayCompute + ArrayValidity + ToArrayData {
    fn len(&self) -> usize;
}

impl ToArrayData for Array<'_> {
    fn to_array_data(&self) -> ArrayData {
        match self {
            Array::Data(d) => d.encoding().with_data(d, |a| a.to_array_data()),
            Array::DataRef(d) => d.encoding().with_data(d, |a| a.to_array_data()),
            Array::View(v) => v.encoding().with_view(v, |a| a.to_array_data()),
        }
    }
}

impl WithArray for Array<'_> {
    fn with_array<R, F: Fn(&dyn ArrayTrait) -> R>(&self, f: F) -> R {
        match self {
            Array::Data(d) => d.encoding().with_data(d, f),
            Array::DataRef(d) => d.encoding().with_data(d, f),
            Array::View(v) => v.encoding().with_view(v, f),
        }
    }
}

#[cfg(test)]
mod test {
    use vortex_error::VortexResult;

    use crate::array2::compute::*;
    use crate::array2::primitive::PrimitiveData;
    use crate::array2::ToArray;

    #[test]
    fn test_primitive() -> VortexResult<()> {
        let array = PrimitiveData::from_vec(vec![1i32, 2, 3, 4, 5]);
        let scalar: i32 = scalar_at(&array.to_array(), 3)?.try_into()?;
        assert_eq!(scalar, 4);
        Ok(())
    }
}
