use std::any::Any;
use std::fmt::{Debug, Formatter};

use linkme::distributed_slice;
pub use vortex::encoding::EncodingId;
use vortex_error::{vortex_err, VortexResult};

use crate::{ArrayData, ArrayParts, ArrayTrait, TryDeserializeArrayMetadata, TryFromArrayParts};
use crate::{ArrayDef, ArrayView};

#[distributed_slice]
pub static VORTEX_ENCODINGS: [EncodingRef] = [..];

pub type EncodingRef = &'static dyn ArrayEncoding;

pub fn find_encoding(id: &str) -> Option<EncodingRef> {
    VORTEX_ENCODINGS
        .iter()
        .find(|&x| x.id().name() == id)
        .cloned()
}

/// Object-safe encoding trait for an array.
pub trait ArrayEncoding: 'static + Sync + Send {
    fn as_any(&self) -> &dyn Any;

    fn id(&self) -> EncodingId;

    fn with_view_mut<'v>(
        &self,
        view: &'v ArrayView<'v>,
        f: &mut dyn FnMut(&dyn ArrayTrait) -> VortexResult<()>,
    ) -> VortexResult<()>;

    fn with_data_mut(
        &self,
        data: &ArrayData,
        f: &mut dyn FnMut(&dyn ArrayTrait) -> VortexResult<()>,
    ) -> VortexResult<()>;
}

pub trait WithEncodedArray {
    type D: ArrayDef;

    fn with_view_mut<R, F: for<'a> FnMut(&<Self::D as ArrayDef>::Array<'a>) -> R>(
        &self,
        view: &ArrayView,
        mut f: F,
    ) -> R {
        let metadata = <Self::D as ArrayDef>::Metadata::try_deserialize_metadata(view.metadata())
            .map_err(|e| vortex_err!("Failed to deserialize metadata: {}", e))
            .unwrap();
        let array =
            <Self::D as ArrayDef>::Array::try_from_parts(view as &dyn ArrayParts, &metadata)
                .map_err(|e| vortex_err!("Failed to create array from parts: {}", e))
                .unwrap();
        f(&array)
    }

    fn with_data_mut<R, F: for<'a> FnMut(&<Self::D as ArrayDef>::Array<'a>) -> R>(
        &self,
        data: &ArrayData,
        mut f: F,
    ) -> R {
        let metadata = data
            .metadata()
            .as_any()
            .downcast_ref::<<Self::D as ArrayDef>::Metadata>()
            .ok_or_else(|| vortex_err!("Failed to downcast metadata"))
            .unwrap();
        let array =
            <Self::D as ArrayDef>::Array::try_from_parts(data as &dyn ArrayParts, &metadata)
                .map_err(|e| vortex_err!("Failed to create array from parts: {}", e))
                .unwrap();
        f(&array)
    }
}

impl Debug for dyn ArrayEncoding + '_ {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.id(), f)
    }
}

impl dyn ArrayEncoding {
    pub(crate) fn with_view<'v, R, F: FnMut(&dyn ArrayTrait) -> R>(
        &self,
        view: &'v ArrayView<'v>,
        mut f: F,
    ) -> R {
        let mut result = None;

        // Unwrap the result. This is safe since we validate that encoding against the
        // ArrayData during ArrayData::try_new.
        self.with_view_mut(view, &mut |array| {
            result = Some(f(array));
            Ok(())
        })
        .unwrap();

        // Now we unwrap the optional, which we know to be populated in the closure.
        result.unwrap()
    }

    pub(crate) fn with_data<R, F: FnMut(&dyn ArrayTrait) -> R>(
        &self,
        data: &ArrayData,
        mut f: F,
    ) -> R {
        let mut result = None;

        // Unwrap the result. This is safe since we validate that encoding against the
        // ArrayData during ArrayData::try_new.
        self.with_data_mut(data, &mut |array| {
            result = Some(f(array));
            Ok(())
        })
        .unwrap();

        // Now we unwrap the optional, which we know to be populated in the closure.
        result.unwrap()
    }
}
