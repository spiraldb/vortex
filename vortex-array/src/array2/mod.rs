mod compute;
mod context;
mod data;
mod encoding;
mod implementation;
mod metadata;
mod primitive;
mod ree;
mod view;

use std::fmt::Debug;

pub use compute::*;
pub use context::*;
pub use data::*;
pub use encoding::*;
pub use implementation::*;
pub use metadata::*;
pub use view::*;
use vortex_error::VortexResult;

use crate::array2::ArrayData;
use crate::array2::ArrayEncoding;
use crate::array2::ArrayView;
use crate::array2::{ArrayCompute, WithCompute};

/// An array enum, similar to Cow.
#[derive(Debug, Clone)]
pub enum Array<'v> {
    Data(ArrayData),
    DataRef(&'v ArrayData),
    View(ArrayView<'v>),
}

impl WithCompute for Array<'_> {
    fn with_compute<R, F: Fn(&dyn ArrayCompute) -> VortexResult<R>>(
        &self,
        f: F,
    ) -> VortexResult<R> {
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

    #[test]
    fn test_primitive() -> VortexResult<()> {
        // let array = PrimitiveData::from_vec(vec![1i32, 2, 3, 4, 5]);
        // let scalar: i32 = array
        //     .as_ref()
        //     .scalar_at()
        //     .unwrap()
        //     .scalar_at(3)?
        //     .try_into()?;
        // assert_eq!(scalar, 4);
        // let parray: &dyn PrimitiveArray = &array;
        // assert!(parray.patch().is_none());
        Ok(())
    }
}
