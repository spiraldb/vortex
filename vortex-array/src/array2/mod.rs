mod data;
mod encoding;
mod primitive;
mod view;

use std::any::Any;
use std::sync::Arc;

use vortex_error::VortexResult;
use vortex_schema::DType;

use crate::array2::data::ArrayData;
use crate::compute::ArrayCompute;
use crate::encoding::EncodingId;
use crate::serde::ArrayView;

/// Dynamic trait representing an array type.
#[allow(dead_code)]
pub trait ArrayEncoding {
    fn id(&self) -> EncodingId;

    fn with_view<'v>(
        &self,
        view: ArrayView<'v>,
        f: &mut dyn FnMut(&dyn ArrayCompute) -> VortexResult<()>,
    ) -> VortexResult<()>;

    fn with_data(
        &self,
        data: ArrayData,
        f: &mut dyn FnMut(&dyn ArrayCompute) -> VortexResult<()>,
    ) -> VortexResult<()>;
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
}

pub trait ToArrayData {
    fn to_array_data(&self) -> ArrayData;
}

/// Trait capturing the behaviour of an array.
#[allow(dead_code)]
pub trait Array: ToArrayData {
    fn dtype(&self) -> &DType;
    fn len(&self) -> usize;
    fn children_array_data(&self) -> Vec<ArrayData>;
}

/// Trait the defines the set of types relating to an array.
/// Because it has associated types it can't be used as a trait object.
pub trait ArrayDef {
    const ID: EncodingId;
    type Array<'a>: ?Sized + 'a;
    // TODO(ngates): explore inverting this trait relationship
    // where &'a Self::Array<'a>: Array;
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
