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
            let temporal_array = TemporalArray::new_timestamp(ts_array.clone(), unit, tz.clone());

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
