use lazy_static::lazy_static;
use vortex_dtype::{DType, ExtDType, ExtID};

use crate::array::datetime::TimeUnit;
use crate::array::extension::ExtensionArray;
use crate::{Array, ArrayDType, ArrayData, IntoArray};

mod from;

#[cfg(test)]
mod test;

lazy_static! {
    pub static ref DATE_ID: ExtID = ExtID::from("vortex.date");
    pub static ref TIME_ID: ExtID = ExtID::from("vortex.time");
    pub static ref TIMESTAMP_ID: ExtID = ExtID::from("vortex.timestamp");
}

pub fn is_temporal_ext_type(id: &ExtID) -> bool {
    match id.as_ref() {
        x if x == DATE_ID.as_ref() => true,
        x if x == TIME_ID.as_ref() => true,
        x if x == TIMESTAMP_ID.as_ref() => true,
        _ => false,
    }
}

/// Metadata for [TemporalArray].
///
/// There is one enum for each of the temporal array types we can load from Arrow.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TemporalMetadata {
    Time(TimeUnit),
    Date(TimeUnit),
    Timestamp(TimeUnit, Option<String>),
}

impl TemporalMetadata {
    /// Retrieve the time unit associated with the array.
    ///
    /// All temporal arrays have a single intrinsic time unit for all of its numeric values.
    pub fn time_unit(&self) -> TimeUnit {
        match self {
            TemporalMetadata::Time(time_unit)
            | TemporalMetadata::Date(time_unit)
            | TemporalMetadata::Timestamp(time_unit, _) => *time_unit,
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

/// An array wrapper for primitive values that have an associated temporal meaning.
///
/// This is a wrapper around ExtensionArrays containing numeric types, each of which corresponds to
/// either a timestamp or julian date (both referenced to UNIX epoch), OR a time since midnight.
///
/// ## Arrow compatibility
///
/// TemporalArray can be created from Arrow arrays containing the following datatypes:
/// * `Time32`
/// * `Time64`
/// * `Timestamp`
/// * `Date32`
/// * `Date64`
///
/// Anything that can be constructed and held in a `TemporalArray` can also be zero-copy converted
/// back to the relevant Arrow datatype.
#[derive(Clone, Debug)]
pub struct TemporalArray {
    /// The underlying Vortex extension array holding all the numeric values.
    ext: ExtensionArray,

    /// In-memory representation of the ExtMetadata that is held by the underlying extension array.
    ///
    /// We hold this directly to avoid needing to deserialize the metadata to access things like
    /// timezone and TimeUnit of the underlying array.
    temporal_metadata: TemporalMetadata,
}

macro_rules! assert_width {
    ($width:ty, $array:expr) => {{
        let DType::Primitive(ptype, _) = $array.dtype() else {
            panic!("array must have primitive type");
        };

        assert_eq!(
            <$width as vortex_dtype::NativePType>::PTYPE,
            *ptype,
            "invalid ptype {} for array, expected {}",
            <$width as vortex_dtype::NativePType>::PTYPE,
            *ptype
        );
    }};
}

impl TemporalArray {
    /// Create a new `TemporalArray` holding either i32 day offsets, or i64 millisecond offsets
    /// that are evenly divisible by the number of 86,400,000.
    ///
    /// This is equivalent to the data described by either of the `Date32` or `Date64` data types
    /// from Arrow.
    ///
    /// # Panics
    ///
    /// If the time unit is milliseconds, and the array is not of primitive I64 type, it panics.
    ///
    /// If the time unit is days, and the array is not of primitive I32 type, it panics.
    ///
    /// If any other time unit is provided, it panics.
    pub fn new_date(array: Array, time_unit: TimeUnit) -> Self {
        let ext_dtype = match time_unit {
            TimeUnit::D => {
                assert_width!(i32, array);

                ExtDType::new(
                    DATE_ID.clone(),
                    Some(TemporalMetadata::Date(time_unit).into()),
                )
            }
            TimeUnit::Ms => {
                assert_width!(i64, array);

                ExtDType::new(
                    DATE_ID.clone(),
                    Some(TemporalMetadata::Date(time_unit).into()),
                )
            }
            _ => panic!("invalid TimeUnit {time_unit} for vortex.date"),
        };

        Self {
            ext: ExtensionArray::new(ext_dtype, array),
            temporal_metadata: TemporalMetadata::Date(time_unit),
        }
    }

    /// Create a new `TemporalArray` holding one of the following values:
    ///
    /// * `i32` values representing seconds since midnight
    /// * `i32` values representing milliseconds since midnight
    /// * `i64` values representing microseconds since midnight
    /// * `i64` values representing nanoseconds since midnight
    ///
    /// Note, this is equivalent to the set of values represented by the Time32 or Time64 types
    /// from Arrow.
    ///
    /// # Panics
    ///
    /// If the time unit is seconds, and the array is not of primitive I32 type, it panics.
    ///
    /// If the time unit is milliseconds, and the array is not of primitive I32 type, it panics.
    ///
    /// If the time unit is microseconds, and the array is not of primitive I64 type, it panics.
    ///
    /// If the time unit is nanoseconds, and the array is not of primitive I64 type, it panics.
    pub fn new_time(array: Array, time_unit: TimeUnit) -> Self {
        match time_unit {
            TimeUnit::S | TimeUnit::Ms => assert_width!(i32, array),
            TimeUnit::Us | TimeUnit::Ns => assert_width!(i64, array),
            TimeUnit::D => panic!("invalid unit D for vortex.time data"),
        }

        let temporal_metadata = TemporalMetadata::Time(time_unit);
        Self {
            ext: ExtensionArray::new(
                ExtDType::new(TIME_ID.clone(), Some(temporal_metadata.clone().into())),
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
    ///
    /// If the time_unit is days, the function will panic.
    pub fn new_timestamp(array: Array, time_unit: TimeUnit, time_zone: Option<String>) -> Self {
        assert_width!(i64, array);

        let temporal_metadata = TemporalMetadata::Timestamp(time_unit, time_zone);

        Self {
            ext: ExtensionArray::new(
                ExtDType::new(TIMESTAMP_ID.clone(), Some(temporal_metadata.clone().into())),
                array,
            ),
            temporal_metadata,
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

impl From<TemporalArray> for ArrayData {
    fn from(value: TemporalArray) -> Self {
        value.ext.into()
    }
}

impl From<TemporalArray> for Array {
    fn from(value: TemporalArray) -> Self {
        value.ext.into_array()
    }
}
