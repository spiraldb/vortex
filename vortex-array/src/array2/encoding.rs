use std::fmt::{Debug, Formatter};

use vortex_error::VortexResult;

use crate::array2::ArrayCompute;
use crate::array2::ArrayData;
use crate::array2::ArrayView;
use crate::encoding::EncodingId;

pub type EncodingRef = &'static dyn ArrayEncoding;

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

impl Debug for dyn ArrayEncoding + '_ {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.id(), f)
    }
}

impl dyn ArrayEncoding {
    pub(crate) fn with_view<'v, R, F: Fn(&dyn ArrayCompute) -> R>(
        &self,
        view: &'v ArrayView<'v>,
        f: F,
    ) -> R {
        let mut result = None;

        // Unwrap the result. This is safe since we validate that encoding against the
        // ArrayData during ArrayData::try_new.
        self.with_view_mut(view, &mut |compute| {
            result = Some(f(compute));
            Ok(())
        })
        .unwrap();

        // Now we unwrap the optional, which we know to be populated in the closure.
        result.unwrap()
    }

    pub(crate) fn with_data<R, F: Fn(&dyn ArrayCompute) -> R>(&self, data: &ArrayData, f: F) -> R {
        let mut result = None;

        // Unwrap the result. This is safe since we validate that encoding against the
        // ArrayData during ArrayData::try_new.
        self.with_data_mut(data, &mut |compute| {
            result = Some(f(compute));
            Ok(())
        })
        .unwrap();

        // Now we unwrap the optional, which we know to be populated in the closure.
        result.unwrap()
    }
}
