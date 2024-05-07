#![cfg(feature = "flatbuffers")]

use itertools::Itertools;
use serde::Deserialize;
use vortex_dtype::DType;
use vortex_error::{vortex_err, VortexError};

use crate::flatbuffers::scalar as fb;
use crate::{Scalar, ScalarValue};

impl TryFrom<fb::Scalar<'_>> for Scalar {
    type Error = VortexError;

    fn try_from(value: fb::Scalar<'_>) -> Result<Self, Self::Error> {
        let dtype = value.dtype();
        let dtype = DType::try_from(dtype)?;

        let flex_value = value
            .value()
            .ok_or_else(|| vortex_err!("Missing scalar value"))?;

        // TODO(ngates): what's the point of all this if I have to copy the data into a Vec?
        let flex_value = flex_value.iter().collect_vec();
        let reader = flexbuffers::Reader::get_root(flex_value.as_slice())?;
        let value = ScalarValue::deserialize(reader)?;

        Ok(Scalar { dtype, value })
    }
}
