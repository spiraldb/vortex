use std::sync::Arc;

use lazy_static::lazy_static;
use vortex_dtype::{DType, ExtDType, ExtID, ExtMetadata, Nullability, PType};

use crate::array::datetime::TimeUnit;
use crate::array::extension::ExtensionArray;
use crate::{Array, ArrayDType, ArrayData, IntoArrayData};

mod from;

#[cfg(test)]
mod test;

lazy_static! {
    pub static ref DATE32_ID: ExtID = ExtID::from("arrow.date32");
    pub static ref DATE64_ID: ExtID = ExtID::from("arrow.date64");
    pub static ref TIME32_ID: ExtID = ExtID::from("arrow.time32");
    pub static ref TIME64_ID: ExtID = ExtID::from("arrow.time64");
    pub static ref TIMESTAMP_ID: ExtID = ExtID::from("arrow.timestamp");
    pub static ref DATE32_EXT_DTYPE: ExtDType =
        ExtDType::new(DATE32_ID.clone(), Some(ExtMetadata::new(Arc::new([]))));
    pub static ref DATE64_EXT_DTYPE: ExtDType =
        ExtDType::new(DATE64_ID.clone(), Some(ExtMetadata::new(Arc::new([]))));
}

pub fn is_temporal_ext_type(id: &ExtID) -> bool {
    match id.as_ref() {
        x if x == DATE32_ID.as_ref() => true,
        x if x == DATE64_ID.as_ref() => true,
        x if x == TIME32_ID.as_ref() => true,
        x if x == TIME64_ID.as_ref() => true,
        x if x == TIMESTAMP_ID.as_ref() => true,
        _ => false,
    }
}

/// Metadata for [TemporalArray].
///
/// There is one enum for each of the temporal array types we can load from Arrow.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TemporalMetadata {
    Time32(TimeUnit),
    Time64(TimeUnit),
    Timestamp(TimeUnit, Option<String>),
    Date32,
    Date64,
}

impl TemporalMetadata {
    /// Retrieve the time unit associated with the array.
    ///
    /// All temporal arrays have some sort of time unit. For some arrays, e.g. `arrow.date32`, the
    /// time unit is statically known based on the extension type. For others, such as
    /// `arrow.timestamp`, it is a parameter.
    pub fn time_unit(&self) -> TimeUnit {
        match self {
            TemporalMetadata::Time32(time_unit)
            | TemporalMetadata::Time64(time_unit)
            | TemporalMetadata::Timestamp(time_unit, _) => *time_unit,
            TemporalMetadata::Date32 => TimeUnit::D,
            TemporalMetadata::Date64 => TimeUnit::Ms,
        }
    }

    /// Access the optional time-zone component of the metadata.
    pub fn time_zone(&self) -> Option<&str> {
        if let TemporalMetadata::Timestamp(_, tz) = self {
            tz.as_ref().map(|s| s.as_str())
        } else {
            None
        }
    }
}

/// An array containing one of Arrow's temporal values.
///
/// TemporalArray can be created from Arrow arrays containing the following datatypes:
/// * `Time32`
/// * `Time64`
/// * `Timestamp`
/// * `Date32`
/// * `Date64`
/// * `Interval`: *TODO*
/// * `Duration`: *TODO*
#[derive(Clone, Debug)]
pub struct TemporalArray {
    /// The underlying Vortex array holding all of the data.
    ext: ExtensionArray,

    /// In-memory representation of the ExtMetadata that is held by the underlying extension array.
    ///
    /// We hold this directly to avoid needing to deserialize the metadata to access things like
    /// timezone and TimeUnit of the underlying array.
    temporal_metadata: TemporalMetadata,
}

impl TemporalArray {
    /// Create a new `TemporalArray` holding Arrow spec compliant Time32 data.
    ///
    /// # Panics
    ///
    /// If `array` does not hold Primitive i32 data, the function will panic.
    pub fn new_time32(array: Array, time_unit: TimeUnit) -> Self {
        assert!(
            array
                .dtype()
                .eq_ignore_nullability(&DType::Primitive(PType::I32, Nullability::NonNullable)),
            "time32 array must contain i32 data"
        );

        assert!(
            time_unit == TimeUnit::S || time_unit == TimeUnit::Ms,
            "time32 must have unit of seconds or milliseconds"
        );

        let temporal_metadata = TemporalMetadata::Time32(time_unit);
        Self {
            ext: ExtensionArray::new(
                ExtDType::new(TIME32_ID.clone(), Some(temporal_metadata.clone().into())),
                array,
            ),
            temporal_metadata,
        }
    }

    /// Create a new `TemporalArray` holding Arrow spec compliant Time64 data.
    ///
    /// # Panics
    ///
    /// If `array` does not hold Primitive i64 data, the function will panic.
    pub fn new_time64(array: Array, time_unit: TimeUnit) -> Self {
        assert!(
            array
                .dtype()
                .eq_ignore_nullability(&DType::Primitive(PType::I64, Nullability::NonNullable)),
            "time32 array must contain i64 data"
        );

        assert!(
            time_unit == TimeUnit::Us || time_unit == TimeUnit::Ns,
            "time32 must have unit of microseconds or nanoseconds"
        );

        let temporal_metadata = TemporalMetadata::Time64(time_unit);

        Self {
            ext: ExtensionArray::new(
                ExtDType::new(TIME64_ID.clone(), Some(temporal_metadata.clone().into())),
                array,
            ),
            temporal_metadata,
        }
    }

    /// Create a new `TemporalArray` holding Arrow spec compliant Timestamp data, with an
    /// optional timezone.
    ///
    /// # Panics
    ///
    /// If `array` does not hold Primitive i64 data, the function will panic.
    pub fn new_timestamp(array: Array, time_unit: TimeUnit, time_zone: Option<String>) -> Self {
        assert!(
            array
                .dtype()
                .eq_ignore_nullability(&DType::Primitive(PType::I64, Nullability::NonNullable)),
            "timestamp array must contain i64 data"
        );

        let temporal_metadata = TemporalMetadata::Timestamp(time_unit, time_zone);

        Self {
            ext: ExtensionArray::new(
                ExtDType::new(TIMESTAMP_ID.clone(), Some(temporal_metadata.clone().into())),
                array,
            ),
            temporal_metadata,
        }
    }

    /// Create a new `TemporalArray` holding Arrow spec compliant Date32 data.
    ///
    /// # Panics
    ///
    /// If `array` does not hold Primitive i32 data, the function will panic.
    pub fn new_date32(array: Array) -> Self {
        assert!(
            array
                .dtype()
                .eq_ignore_nullability(&DType::Primitive(PType::I32, Nullability::NonNullable)),
            "date32 array must contain i32 data"
        );

        Self {
            ext: ExtensionArray::new(DATE32_EXT_DTYPE.clone(), array),
            temporal_metadata: TemporalMetadata::Date32,
        }
    }

    /// Create a new `TemporalArray` holding Arrow spec compliant Date64 data.
    ///
    /// # Panics
    ///
    /// If `array` does not hold Primitive i64 data, the function will panic.
    pub fn new_date64(array: Array) -> Self {
        assert!(
            array
                .dtype()
                .eq_ignore_nullability(&DType::Primitive(PType::I64, Nullability::NonNullable)),
            "date64 array must contain i64 data"
        );

        Self {
            ext: ExtensionArray::new(DATE64_EXT_DTYPE.clone(), array),
            temporal_metadata: TemporalMetadata::Date64,
        }
    }
}

impl TemporalArray {
    /// Access the underlying temporal values in the underlying ExtensionArray storage.
    ///
    /// These values are to be interpreted based on the time unit and optional time-zone stored
    /// in the TemporalMetadata.
    pub fn temporal_values(&self) -> Array {
        self.ext.storage()
    }

    /// Retrieve the temporal metadata.
    ///
    /// The metadata is used to provide semantic meaning to the temporal values Array, for example
    /// to understand the granularity of the samples and if they have an associated timezone.
    pub fn temporal_metadata(&self) -> &TemporalMetadata {
        &self.temporal_metadata
    }

    /// Retrieve the extension DType associated with the underlying array.
    pub fn ext_dtype(&self) -> &ExtDType {
        self.ext.ext_dtype()
    }
}

impl IntoArrayData for TemporalArray {
    fn into_array_data(self) -> ArrayData {
        self.ext.into_array_data()
    }
}
