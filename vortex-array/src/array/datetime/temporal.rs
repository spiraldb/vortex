use std::sync::Arc;

use lazy_static::lazy_static;
use vortex_dtype::{DType, ExtDType, ExtID, ExtMetadata, Nullability, PType};

use crate::array::datetime::TimeUnit;
use crate::array::extension::ExtensionArray;
use crate::{Array, ArrayDType, ArrayData, IntoArrayData};

mod arrow;
mod from;

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
            | TemporalMetadata::Timestamp(time_unit, _) => time_unit.clone(),
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
}

impl IntoArrayData for TemporalArray {
    fn into_array_data(self) -> ArrayData {
        self.ext.into_array_data()
    }
}

#[cfg(test)]
mod test {
    use vortex_dtype::{ExtDType, ExtMetadata};

    use crate::array::datetime::temporal::{TemporalMetadata, TIMESTAMP_ID};
    use crate::array::datetime::{TemporalArray, TimeUnit};
    use crate::array::primitive::PrimitiveArray;
    use crate::validity::Validity;
    use crate::{IntoArray, IntoArrayVariant};

    macro_rules! test_temporal_roundtrip {
        ($prim:ty, $constructor:expr, $unit:expr) => {{
            let array =
                PrimitiveArray::from_vec(vec![100 as $prim], Validity::NonNullable).into_array();
            let temporal: TemporalArray = $constructor(array, $unit);
            let prims = temporal.temporal_values().into_primitive().unwrap();

            assert_eq!(
                prims.maybe_null_slice::<$prim>(),
                vec![100 as $prim].as_slice(),
            );
            assert_eq!(temporal.temporal_metadata().time_unit(), $unit);
        }};

        ($prim:ty, $constructor:expr) => {{
            let array =
                PrimitiveArray::from_vec(vec![100 as $prim], Validity::NonNullable).into_array();
            let temporal: TemporalArray = $constructor(array);

            let prims = temporal.temporal_values().into_primitive().unwrap();
            assert_eq!(
                prims.maybe_null_slice::<$prim>(),
                vec![100 as $prim].as_slice(),
            );
        }};
    }

    macro_rules! test_success_case {
        ($name:ident, $prim:ty, $constructor:expr $(, $arg:expr)*) => {
            #[test]
            fn $name() {
                test_temporal_roundtrip!($prim, $constructor $(, $arg)*)
            }
        };
    }

    macro_rules! test_fail_case {
        ($name:ident, $prim:ty, $constructor:expr $(, $arg:expr)*) => {
            #[test]
            #[should_panic]
            fn $name() {
                test_temporal_roundtrip!($prim, $constructor $(, $arg)*)
            }
        };
    }

    #[test]
    fn test_roundtrip_metadata() {
        let meta: ExtMetadata =
            TemporalMetadata::Timestamp(TimeUnit::Ms, Some("UTC".to_string())).into();

        assert_eq!(
            meta.as_ref(),
            vec![
                2u8, // Tag for TimeUnit::Ms
                0x3u8, 0x0u8, // u16 length
                b'U', b'T', b'C',
            ]
            .as_slice()
        );

        let temporal_metadata =
            TemporalMetadata::try_from(&ExtDType::new(TIMESTAMP_ID.clone(), Some(meta))).unwrap();

        assert_eq!(
            temporal_metadata,
            TemporalMetadata::Timestamp(TimeUnit::Ms, Some("UTC".to_string()))
        );
    }

    // Time32 conformance tests
    test_success_case!(
        test_roundtrip_time32_second,
        i32,
        TemporalArray::new_time32,
        TimeUnit::S
    );
    test_success_case!(
        test_roundtrip_time32_millisecond,
        i32,
        TemporalArray::new_time32,
        TimeUnit::Ms
    );
    test_fail_case!(
        test_fail_time32_micro,
        i32,
        TemporalArray::new_time32,
        TimeUnit::Us
    );
    test_fail_case!(
        test_fail_time32_nano,
        i32,
        TemporalArray::new_time32,
        TimeUnit::Ns
    );

    // Time64 conformance tests
    test_success_case!(
        test_roundtrip_time64_us,
        i64,
        TemporalArray::new_time64,
        TimeUnit::Us
    );
    test_success_case!(
        test_roundtrip_time64_ns,
        i64,
        TemporalArray::new_time64,
        TimeUnit::Ns
    );
    test_fail_case!(
        test_fail_time64_ms,
        i64,
        TemporalArray::new_time64,
        TimeUnit::Ms
    );
    test_fail_case!(
        test_fail_time64_s,
        i64,
        TemporalArray::new_time64,
        TimeUnit::S
    );
    test_fail_case!(
        test_fail_time64_i32,
        i32,
        TemporalArray::new_time64,
        TimeUnit::Ns
    );

    // Date32 conformance tests
    test_success_case!(test_roundtrip_date32, i32, TemporalArray::new_date32);
    test_fail_case!(test_fail_date32, i64, TemporalArray::new_date32);

    // Date64 conformance tests
    test_success_case!(test_roundtrip_date64, i64, TemporalArray::new_date64);
    test_fail_case!(test_fail_date64, i32, TemporalArray::new_date64);

    // We test Timestamp explicitly to avoid the macro getting too complex.
    #[test]
    fn test_timestamp() {
        let ts = PrimitiveArray::from_vec(vec![100i64], Validity::NonNullable);
        let ts_array = ts.clone().into_array();

        for unit in vec![TimeUnit::S, TimeUnit::Ms, TimeUnit::Us, TimeUnit::Ns] {
            for tz in vec![Some("UTC".to_string()), None] {
                let temporal_array =
                    TemporalArray::new_timestamp(ts_array.clone(), unit, tz.clone());

                let values = temporal_array.temporal_values().into_primitive().unwrap();
                assert_eq!(values.maybe_null_slice::<i64>(), vec![100i64].as_slice());
                assert_eq!(
                    temporal_array.temporal_metadata(),
                    &TemporalMetadata::Timestamp(unit, tz)
                );
            }
        }
    }

    #[test]
    #[should_panic]
    fn test_timestamp_fails_i32() {
        let ts = PrimitiveArray::from_vec(vec![100i32], Validity::NonNullable);
        let ts_array = ts.clone().into_array();

        let _ = TemporalArray::new_timestamp(ts_array.clone(), TimeUnit::S, None);
    }
}
