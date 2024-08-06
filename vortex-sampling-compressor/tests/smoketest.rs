use std::collections::HashSet;
use std::ops::Add;

use chrono::TimeDelta;
use vortex::array::builder::VarBinBuilder;
use vortex::array::{BoolArray, PrimitiveArray, StructArray, TemporalArray, TimeUnit};
use vortex::validity::Validity;
use vortex::{Array, ArrayDType, ArrayData, IntoArray};
use vortex_dtype::{DType, FieldName, FieldNames, Nullability};
use vortex_sampling_compressor::compressors::alp::ALPCompressor;
use vortex_sampling_compressor::compressors::bitpacked::BitPackedCompressor;
use vortex_sampling_compressor::compressors::date_time_parts::DateTimePartsCompressor;
use vortex_sampling_compressor::compressors::dict::DictCompressor;
use vortex_sampling_compressor::compressors::r#for::FoRCompressor;
use vortex_sampling_compressor::compressors::roaring_bool::RoaringBoolCompressor;
use vortex_sampling_compressor::compressors::roaring_int::RoaringIntCompressor;
use vortex_sampling_compressor::compressors::runend::DEFAULT_RUN_END_COMPRESSOR;
use vortex_sampling_compressor::compressors::sparse::SparseCompressor;
use vortex_sampling_compressor::compressors::zigzag::ZigZagCompressor;
use vortex_sampling_compressor::compressors::CompressorRef;
use vortex_sampling_compressor::{CompressConfig, SamplingCompressor};

#[test]
#[cfg_attr(miri, ignore)] // This test is too slow on miri
pub fn smoketest_compressor() {
    let compressor = SamplingCompressor::new_with_options(
        HashSet::from([
            &ALPCompressor as CompressorRef,
            &BitPackedCompressor,
            // TODO(robert): Implement minimal compute for DeltaArrays - scalar_at and slice
            // &DeltaCompressor,
            &DictCompressor,
            &FoRCompressor,
            &DateTimePartsCompressor,
            &RoaringBoolCompressor,
            &RoaringIntCompressor,
            &DEFAULT_RUN_END_COMPRESSOR,
            &SparseCompressor,
            &ZigZagCompressor,
        ]),
        CompressConfig::default(),
    );

    let def: &[(&str, Array)] = &[
        ("prim_col", make_primitive_column(65536)),
        ("bool_col", make_bool_column(65536)),
        ("varbin_col", make_string_column(65536)),
        ("binary_col", make_binary_column(65536)),
        ("timestamp_col", make_timestamp_column(65536)),
    ];

    let fields: Vec<Array> = def.iter().map(|(_, arr)| arr.clone()).collect();
    let field_names: FieldNames = FieldNames::from(
        def.iter()
            .map(|(name, _)| FieldName::from(*name))
            .collect::<Vec<_>>(),
    );

    // Create new struct array
    let to_compress = StructArray::try_new(field_names, fields, 65536, Validity::NonNullable)
        .unwrap()
        .into_array();

    println!("uncompressed: {}", to_compress.tree_display());
    let compressed = compressor
        .compress(&to_compress, None)
        .unwrap()
        .into_array();

    println!("compressed: {}", compressed.tree_display());
    assert_eq!(compressed.dtype(), to_compress.dtype());
}

fn make_primitive_column(count: usize) -> Array {
    PrimitiveArray::from_vec(
        (0..count).map(|i| i as i64).collect::<Vec<i64>>(),
        Validity::NonNullable,
    )
    .into_array()
}

fn make_bool_column(count: usize) -> Array {
    let bools: Vec<bool> = (0..count).map(|_| rand::random::<bool>()).collect();
    BoolArray::from_vec(bools, Validity::NonNullable).into_array()
}

fn make_string_column(count: usize) -> Array {
    let values = ["zzzz", "bbbbbb", "cccccc", "ddddd"];
    let mut builder = VarBinBuilder::<i64>::with_capacity(count);
    for i in 0..count {
        builder.push_value(values[i % values.len()].as_bytes());
    }

    builder
        .finish(DType::Utf8(Nullability::NonNullable))
        .into_array()
}

fn make_binary_column(count: usize) -> Array {
    let mut builder = VarBinBuilder::<i64>::with_capacity(count);
    let random: Vec<u8> = (0..count).map(|_| rand::random::<u8>()).collect();
    for i in 1..=count {
        builder.push_value(&random[0..i]);
    }

    builder
        .finish(DType::Binary(Nullability::NonNullable))
        .into_array()
}

fn make_timestamp_column(count: usize) -> Array {
    // Make new timestamps in incrementing order from EPOCH.
    let t0 = chrono::NaiveDateTime::default().and_utc();

    let timestamps: Vec<i64> = (0..count)
        .map(|inc| t0.add(TimeDelta::seconds(inc as i64)).timestamp_millis())
        .collect();

    let storage_array = PrimitiveArray::from_vec(timestamps, Validity::NonNullable).into_array();

    ArrayData::from(TemporalArray::new_timestamp(
        storage_array,
        TimeUnit::Ms,
        None,
    ))
    .into()
}
