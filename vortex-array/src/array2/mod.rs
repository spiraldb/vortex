mod data;
mod encoding;
mod primitive;
mod ree;
mod view;

use std::any::Any;
use std::sync::Arc;

use vortex_error::VortexResult;

use crate::array2::data::ArrayData;
use crate::compute::ArrayCompute;
use crate::encoding::EncodingId;
use crate::serde::ArrayView;

/// Dynamic trait representing an array type.
#[allow(dead_code)]
pub trait ArrayEncoding {
    fn id(&self) -> EncodingId;

    fn with_view_mut<'v>(
        &self,
        view: &'v ArrayView<'v>,
        f: &mut dyn FnMut(&dyn ArrayCompute) -> VortexResult<()>,
    ) -> VortexResult<()>;

    fn with_data_mut(
        &self,
        data: &ArrayData,
        f: &mut dyn FnMut(&dyn ArrayCompute) -> VortexResult<()>,
    ) -> VortexResult<()>;
}

/// Split out the generic functions into their own trait so that ArrayEncoding remains object-safe.
pub trait WithArray {
    fn with_view<'v, R, F: Fn(&dyn ArrayCompute) -> VortexResult<R>>(
        &self,
        view: &'v ArrayView<'v>,
        f: F,
    ) -> VortexResult<R>;

    fn with_data<R, F: Fn(&dyn ArrayCompute) -> VortexResult<R>>(
        &self,
        data: &ArrayData,
        f: F,
    ) -> VortexResult<R>;
}

impl<Encoding: ?Sized + ArrayEncoding> WithArray for Encoding {
    fn with_view<'v, R, F: Fn(&dyn ArrayCompute) -> VortexResult<R>>(
        &self,
        view: &'v ArrayView<'v>,
        f: F,
    ) -> VortexResult<R> {
        let mut result = None;
        self.with_view_mut(view, &mut |compute| {
            result = Some(f(compute));
            Ok(())
        })?;
        result.unwrap()
    }

    fn with_data<R, F: Fn(&dyn ArrayCompute) -> VortexResult<R>>(
        &self,
        data: &ArrayData,
        f: F,
    ) -> VortexResult<R> {
        let mut result = None;
        self.with_data_mut(data, &mut |compute| {
            result = Some(f(compute));
            Ok(())
        })?;
        result.unwrap()
    }
}

pub type EncodingRef = &'static dyn ArrayEncoding;

/// Dynamic trait used to represent opaque owned Array metadata
/// Note that this allows us to restrict the ('static + Send + Sync) requirement to just the
/// metadata trait, and not the entire array trait.
#[allow(dead_code)]
pub trait ArrayMetadata: 'static + Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync>;
    fn to_arc(&self) -> Arc<dyn ArrayMetadata>;
    fn into_arc(self) -> Arc<dyn ArrayMetadata>;
}

pub trait ParseArrayMetadata: Sized {
    fn try_from(metadata: Option<&[u8]>) -> VortexResult<Self>;
}

pub trait FromArrayData: Sized {
    fn try_from(data: ArrayData) -> VortexResult<Self>;
}

pub trait FromArrayView: Sized {
    fn try_from(view: ArrayView) -> VortexResult<Self>;
}

/// Trait to enable conversion into an owned ArrayData.
pub trait ToArrayData {
    fn to_data(&self) -> ArrayData;
}

/// An array enum, similar to Cow.
pub enum Array<'v> {
    Data(ArrayData),
    DataRef(&'v ArrayData),
    View(ArrayView<'v>),
}

/// Trait the defines the set of types relating to an array.
/// Because it has associated types it can't be used as a trait object.
pub trait ArrayDef {
    const ID: EncodingId;
    type Array<'a>: ?Sized + 'a;
    type Metadata: ArrayMetadata;
    type Encoding: ArrayEncoding;
}

#[cfg(test)]
mod test {
    use vortex_error::VortexResult;

    use crate::array2::primitive::{PrimitiveArray, PrimitiveData};
    use crate::compute::ArrayCompute;

    #[test]
    fn test_primitive() -> VortexResult<()> {
        let array = PrimitiveData::from_vec(vec![1i32, 2, 3, 4, 5]);
        let scalar: i32 = array
            .as_ref()
            .scalar_at()
            .unwrap()
            .scalar_at(3)?
            .try_into()?;
        assert_eq!(scalar, 4);
        let parray: &dyn PrimitiveArray = &array;
        assert!(parray.patch().is_none());
        Ok(())
    }
}
