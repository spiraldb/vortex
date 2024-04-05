mod data;
mod view;

use std::any::Any;
use std::sync::Arc;

use arrow_buffer::Buffer;
use vortex_error::{VortexError, VortexResult};
use vortex_schema::DType;

use crate::array2::data::{ArrayData, TypedArrayData};
use crate::array2::view::TypedArrayView;
use crate::compute::ArrayCompute;
use crate::encoding::EncodingId;
use crate::ptype::{NativePType, PType};
use crate::serde::ArrayView;

/// Dynamic trait representing an array type.
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
pub trait ArrayMetadata: 'static + Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync>;
}

macro_rules! impl_array_metadata {
    () => {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
            self
        }
    };
}

/// Trait capturing the behaviour of an array.
pub trait Array {
    fn dtype(&self) -> &DType;
    fn len(&self) -> usize;
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

//////////////////////////
// Primitive Array Data //
//////////////////////////

struct PrimitiveEncoding;
impl ArrayEncoding for PrimitiveEncoding {
    fn id(&self) -> EncodingId {
        PrimitiveDef::ID
    }

    fn with_view<'v>(
        &self,
        view: ArrayView<'v>,
        f: &mut dyn FnMut(&dyn ArrayCompute) -> VortexResult<()>,
    ) -> VortexResult<()> {
        // Convert ArrayView -> PrimitiveArray, then call compute.
        let typed_view: TypedArrayView<'v, PrimitiveDef> = TypedArrayView::try_from(view)?;
        f(&typed_view.as_array())
    }

    fn with_data(
        &self,
        data: ArrayData,
        f: &mut dyn FnMut(&dyn ArrayCompute) -> VortexResult<()>,
    ) -> VortexResult<()> {
        let data = TypedArrayData::<PrimitiveDef>::try_from(data)?;
        f(&data.as_array())
    }
}

struct PrimitiveDef;
impl ArrayDef for PrimitiveDef {
    const ID: EncodingId = EncodingId::new("vortex.primitive");
    type Array<'a> = dyn PrimitiveArray + 'a;
    type Metadata = PrimitiveMetadata;
    type Encoding = PrimitiveEncoding;
}

struct PrimitiveMetadata(PType);
impl PrimitiveMetadata {
    pub fn ptype(&self) -> PType {
        self.0
    }
}
impl ArrayMetadata for PrimitiveMetadata {
    impl_array_metadata!();
}

type PrimitiveData = TypedArrayData<PrimitiveDef>;
type PrimitiveView<'v> = TypedArrayView<'v, PrimitiveDef>;

trait PrimitiveArray {
    fn ptype(&self) -> PType;
    fn buffer(&self) -> &Buffer;
}

impl ArrayCompute for &dyn PrimitiveArray {}

impl PrimitiveData {
    pub fn from_vec<T: NativePType>(values: Vec<T>) -> Self {
        ArrayData::new(
            &PrimitiveEncoding,
            DType::from(T::PTYPE),
            Arc::new(PrimitiveMetadata(T::PTYPE)),
            vec![Buffer::from_vec(values)].into(),
            vec![].into(),
        )
        .as_typed()
    }
}

impl Array for PrimitiveData {
    fn dtype(&self) -> &DType {
        &self.data().dtype()
    }

    fn len(&self) -> usize {
        self.buffer().len() / self.ptype().byte_width()
    }
}

impl PrimitiveArray for PrimitiveData {
    fn ptype(&self) -> PType {
        self.metadata().ptype()
    }

    fn buffer(&self) -> &Buffer {
        self.data()
            .buffers()
            .get(0)
            // This assertion is made by construction.
            .expect("PrimitiveArray must have a single buffer")
    }
}
impl<'a> AsRef<dyn PrimitiveArray + 'a> for PrimitiveData {
    fn as_ref(&self) -> &(dyn PrimitiveArray + 'a) {
        self
    }
}

impl<'v> PrimitiveArray for PrimitiveView<'v>
where
    Self: 'v,
{
    fn ptype(&self) -> PType {
        // self.view().metadata()
        // Where is the metadata parsed?
        todo!()
    }

    fn buffer(&self) -> &Buffer {
        self.view()
            .buffers()
            .get(0)
            .expect("PrimitiveView must have a single buffer")
    }
}
impl<'v> Array for PrimitiveView<'v> {
    fn dtype(&self) -> &DType {
        self.view().dtype()
    }

    fn len(&self) -> usize {
        self.buffer().len() / self.ptype().byte_width()
    }
}

impl<'v> TryFrom<Option<&'v [u8]>> for PrimitiveMetadata {
    type Error = VortexError;

    fn try_from(value: Option<&'v [u8]>) -> Result<Self, Self::Error> {
        todo!()
    }
}

impl<'a> AsRef<dyn PrimitiveArray + 'a> for PrimitiveView<'a> {
    fn as_ref(&self) -> &(dyn PrimitiveArray + 'a) {
        self
    }
}

#[cfg(test)]
mod test {
    use vortex_error::VortexResult;

    use crate::array2::{PrimitiveArray, PrimitiveData};
    use crate::compute::ArrayCompute;

    #[test]
    fn test_primitive() -> VortexResult<()> {
        let array = PrimitiveData::from_vec(vec![1, 2, 3, 4, 5]);
        let parray: &dyn PrimitiveArray = &array;
        assert!(parray.patch().is_none());
        Ok(())
    }
}
