use vortex_dtype::{ExtDType, ExtMetadata};
use vortex_error::{vortex_bail, vortex_err, VortexError, VortexResult};

use crate::array::datetime::temporal::TemporalMetadata;
use crate::array::datetime::{TemporalArray, TimeUnit};
use crate::array::extension::ExtensionArray;
use crate::Array;

impl TryFrom<&ExtDType> for TemporalMetadata {
    type Error = VortexError;

    fn try_from(ext_dtype: &ExtDType) -> Result<Self, Self::Error> {
        match ext_dtype.id().as_ref() {
            "arrow.time32" => decode_time32_metadata(ext_dtype.metadata().unwrap()),
            "arrow.time64" => decode_time64_metadata(ext_dtype.metadata().unwrap()),
            "arrow.date32" => Ok(TemporalMetadata::Date32),
            "arrow.date64" => Ok(TemporalMetadata::Date64),
            "arrow.timestamp" => decode_timestamp_metadata(ext_dtype.metadata().unwrap()),
            _ => {
                vortex_bail!(InvalidArgument: "ExtDType must be known of the known temporal types")
            }
        }
    }
}

fn decode_time32_metadata(ext_meta: &ExtMetadata) -> VortexResult<TemporalMetadata> {
    let tag = ext_meta.as_ref()[0];
    let time_unit =
        TimeUnit::try_from(tag).map_err(|e| vortex_err!(ComputeError: "invalid unit tag: {e}"))?;
    Ok(TemporalMetadata::Time32(time_unit))
}

fn decode_time64_metadata(ext_meta: &ExtMetadata) -> VortexResult<TemporalMetadata> {
    let tag = ext_meta.as_ref()[0];
    let time_unit =
        TimeUnit::try_from(tag).map_err(|e| vortex_err!(ComputeError: "invalid unit tag: {e}"))?;
    Ok(TemporalMetadata::Time64(time_unit))
}

fn decode_timestamp_metadata(ext_meta: &ExtMetadata) -> VortexResult<TemporalMetadata> {
    let tag = ext_meta.as_ref()[0];
    let time_unit =
        TimeUnit::try_from(tag).map_err(|e| vortex_err!(ComputeError: "invalid unit tag: {e}"))?;
    let tz_len_bytes = &ext_meta.as_ref()[1..3];
    let tz_len = u16::from_le_bytes(tz_len_bytes.try_into().unwrap());
    if tz_len == 0 {
        return Ok(TemporalMetadata::Timestamp(time_unit, None));
    }

    // Attempt to load from len-prefixed bytes
    let tz_bytes = &ext_meta.as_ref()[3..(3 + (tz_len as usize))];
    let tz = String::from_utf8_lossy(tz_bytes).to_string();
    Ok(TemporalMetadata::Timestamp(time_unit, Some(tz)))
}

impl TryFrom<&Array> for TemporalArray {
    type Error = VortexError;

    /// Try to specialize a generic Vortex array as a TemporalArray.
    ///
    /// # Errors
    ///
    /// If the provided Array does not have `vortex.ext` encoding, an error will be returned.
    ///
    /// If the provided Array does not have recognized ExtMetadata corresponding to one of the known
    /// `TemporalMetadata` variants, an error is returned.
    fn try_from(value: &Array) -> Result<Self, Self::Error> {
        let ext = ExtensionArray::try_from(value)?;
        let temporal_metadata = TemporalMetadata::try_from(ext.ext_dtype())?;

        Ok(Self {
            ext,
            temporal_metadata,
        })
    }
}

impl TryFrom<Array> for TemporalArray {
    type Error = VortexError;

    /// Try to specialize a generic Vortex array as a TemporalArray.
    ///
    /// Delegates to `TryFrom<&Array>`.
    fn try_from(value: Array) -> Result<Self, Self::Error> {
        TemporalArray::try_from(&value)
    }
}

impl From<TemporalMetadata> for ExtMetadata {
    /// Infallibly serialize a `TemporalMetadata` as an `ExtMetadata` so it can be attached to
    /// an `ExtensionArray`.
    fn from(value: TemporalMetadata) -> Self {
        match value {
            // Time32/Time64 only need to encode the unit in their metadata
            TemporalMetadata::Time64(time_unit) | TemporalMetadata::Time32(time_unit) => {
                let mut meta = Vec::new();
                let unit_tag: u8 = time_unit.into();
                meta.push(unit_tag);

                ExtMetadata::from(meta.as_slice())
            }
            // Store both the time unit and zone in the metadata
            TemporalMetadata::Timestamp(time_unit, time_zone) => {
                let mut meta = Vec::new();
                let unit_tag: u8 = time_unit.into();

                meta.push(unit_tag);

                // Encode time_zone as u16 length followed by utf8 bytes.
                match time_zone {
                    None => meta.extend_from_slice(0u16.to_le_bytes().as_slice()),
                    Some(tz) => {
                        let tz_bytes = tz.as_bytes();
                        let tz_len = u16::try_from(tz_bytes.len()).expect("tz did not fit in u16");
                        meta.extend_from_slice(tz_len.to_le_bytes().as_slice());
                        meta.extend_from_slice(tz_bytes);
                    }
                };

                ExtMetadata::from(meta.as_slice())
            }
            // Date32/Date64 arrays are unambiguous in the semantic meaning of their values, so
            // the metadata can be an empty slice.
            TemporalMetadata::Date32 | TemporalMetadata::Date64 => {
                ExtMetadata::from(vec![].as_slice())
            }
        }
    }
}

// Conversions to/from ExtensionArray
impl From<&TemporalArray> for ExtensionArray {
    fn from(value: &TemporalArray) -> Self {
        value.ext.clone()
    }
}

impl From<TemporalArray> for ExtensionArray {
    fn from(value: TemporalArray) -> Self {
        value.ext
    }
}

impl TryFrom<ExtensionArray> for TemporalArray {
    type Error = VortexError;

    fn try_from(ext: ExtensionArray) -> Result<Self, Self::Error> {
        let temporal_metadata = TemporalMetadata::try_from(ext.ext_dtype())?;
        Ok(Self {
            ext,
            temporal_metadata,
        })
    }
}
