// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Compression round trips and regressions involving the built-in schemes.
//!
//! These tests cover scheme composition and configuration. Individual encoding kernels and the
//! generic selection algorithm are tested in their owning crates.

use std::iter;

use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;
use rstest::rstest;
#[cfg(feature = "zstd")]
use vortex_array::ArrayId;
#[cfg(feature = "zstd")]
use vortex_array::ArrayPlugin;
use vortex_array::ArrayRef;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::aggregate_fn::fns::sum::sum;
use vortex_array::arrays::DecimalArray;
use vortex_array::arrays::ListArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::StructArray;
use vortex_array::arrays::TemporalArray;
use vortex_array::arrays::VarBin;
use vortex_array::arrays::VarBinViewArray;
use vortex_array::arrays::varbin::VarBinArraySlotsExt;
use vortex_array::assert_arrays_eq;
use vortex_array::dtype::DType;
use vortex_array::dtype::DecimalDType;
use vortex_array::dtype::Nullability;
use vortex_array::extension::datetime::TimeUnit;
use vortex_array::validity::Validity;
#[cfg(feature = "zstd")]
use vortex_edition::EDITION_DECLARATIONS;
#[cfg(feature = "zstd")]
use vortex_edition::EDITION_FAMILIES;
#[cfg(feature = "zstd")]
use vortex_edition::EditionId;
#[cfg(feature = "zstd")]
use vortex_edition::EditionSessionExt;
#[cfg(feature = "zstd")]
use vortex_edition::declarations::core::CORE_2026_08_3;
use vortex_error::VortexResult;
use vortex_fastlanes::Delta;

use crate::BtrBlocksCompressor;
use crate::BtrBlocksCompressorBuilder;
use crate::SESSION;
use crate::SchemeExt;
use crate::schemes::binary;
use crate::schemes::integer;
use crate::schemes::string;

#[track_caller]
fn assert_roundtrip(compressor: &BtrBlocksCompressor, input: &ArrayRef) -> VortexResult<ArrayRef> {
    let mut ctx = SESSION.create_execution_ctx();
    let compressed = compressor.compress(input, &mut ctx)?;
    assert_arrays_eq!(&compressed, input, &mut ctx);

    Ok(compressed)
}

#[test]
fn test_default_compressor_roundtrip() -> VortexResult<()> {
    let integers = PrimitiveArray::from_option_iter(
        (0..2048i32).map(|i| (i % 7 != 0).then_some(1_000_000 + (i * 37) % 100)),
    );
    let floats = PrimitiveArray::from_iter((0..2048).map(|i| f64::from(i) / 100.0));
    let strings = VarBinViewArray::from_iter(
        (0..2048).map(|i| (i % 7 != 0).then(|| format!("https://example.com/products/{i}"))),
        DType::Utf8(Nullability::Nullable),
    );
    let binary = VarBinViewArray::from_iter(
        (0..2048).map(|i| (i % 7 != 0).then(|| format!("PREFIX_{i:09}").into_bytes())),
        DType::Binary(Nullability::Nullable),
    );
    let decimals = DecimalArray::new(
        (0..2048i64).map(|i| i * 123).collect(),
        DecimalDType::new(12, 2),
        Validity::NonNullable,
    );
    let timestamps = TemporalArray::new_timestamp(
        PrimitiveArray::from_iter((0..2048i64).map(|i| 1_700_000_000_000_000 + i * 123_456))
            .into_array(),
        TimeUnit::Microseconds,
        None,
    );
    let elements = StructArray::from_fields(&[
        ("integer", integers.into_array()),     //
        ("float", floats.into_array()),         //
        ("string", strings.into_array()),       //
        ("binary", binary.into_array()),        //
        ("decimal", decimals.into_array()),     //
        ("timestamp", timestamps.into_array()), //
    ])?;
    let offsets = PrimitiveArray::from_iter((0..=2048i32).step_by(4));
    let input = ListArray::try_new(
        elements.into_array(),
        offsets.into_array(),
        Validity::NonNullable,
    )?
    .into_array();

    let compressor = BtrBlocksCompressorBuilder::from_session(&SESSION)
        .unrestricted()
        .build();
    let compressed = assert_roundtrip(&compressor, &input)?;
    assert!(compressed.nbytes() < input.nbytes());

    Ok(())
}

#[rstest]
#[case::nonnullable(DType::Utf8(Nullability::NonNullable), false)]
#[case::nullable(DType::Utf8(Nullability::Nullable), true)]
fn test_utf8_varbin_fallback(#[case] dtype: DType, #[case] nullable: bool) -> VortexResult<()> {
    let mut values = vec![Some(""), Some("a"), Some("bb"), Some("é"), Some("🐬")];
    if nullable {
        values.push(None);
    }
    let input = VarBinViewArray::from_iter(values, dtype).into_array();
    let builder = BtrBlocksCompressorBuilder::from_session(&SESSION).unrestricted();
    let baseline = assert_roundtrip(
        &builder
            .clone()
            .exclude_schemes([string::VarBinScheme::UTF8.id()])
            .build(),
        &input,
    )?;
    let compressed = assert_roundtrip(&builder.build(), &input)?;

    assert!(compressed.is::<VarBin>());
    assert_eq!(compressed.dtype(), input.dtype());
    assert!(compressed.nbytes() < baseline.nbytes());
    Ok(())
}

#[rstest]
#[case::binary(DType::Binary(Nullability::NonNullable), &binary::VarBinScheme::BINARY)]
#[case::utf8(DType::Utf8(Nullability::NonNullable), &string::VarBinScheme::UTF8)]
#[case::nullable_utf8(DType::Utf8(Nullability::Nullable), &string::VarBinScheme::UTF8)]
fn test_varbin_compresses_offsets(
    #[case] dtype: DType,
    #[case] scheme: &'static dyn crate::Scheme,
) -> VortexResult<()> {
    let values = (0..4096).map(|i| {
        if dtype.is_nullable() && i % 7 == 0 {
            None
        } else {
            Some(format!("é🐬{i:08}"))
        }
    });
    let input = VarBinViewArray::from_iter(values, dtype.clone()).into_array();
    let compressor = BtrBlocksCompressorBuilder::empty()
        .with_new_scheme(scheme)
        .with_new_scheme(&integer::SequenceScheme)
        .with_new_scheme(&integer::FOR_V1)
        .with_new_scheme(&integer::BITPACKING_V1)
        .build();
    let compressed = assert_roundtrip(&compressor, &input)?;
    let varbin = compressed.as_::<VarBin>();

    assert_eq!(compressed.dtype(), &dtype);
    assert!(compressed.nbytes() < input.nbytes());
    assert!(varbin.offsets().nbytes() < ((input.len() + 1) * size_of::<u64>()) as u64);
    if !dtype.is_nullable() {
        assert!(varbin.offsets().is::<vortex_sequence::Sequence>());
    }
    Ok(())
}

#[rstest]
#[case::unaligned({
    let mut rng = StdRng::seed_from_u64(7);
    let mut value = 500_000i32;
    PrimitiveArray::from_iter((0..1025).map(|_| {
        value += 1 + (rng.next_u32() % 6) as i32;
        value
    }))
})]
#[case::nullable_unaligned(
    PrimitiveArray::from_option_iter(iter::once(None).chain((1i32..=100_000).map(Some)))
)]
fn test_delta_unaligned_roundtrip(#[case] input: PrimitiveArray) -> VortexResult<()> {
    // Zero-padding the final chunk used to inflate the delta span and reject this scheme.
    let compressor = BtrBlocksCompressorBuilder::from_session(&SESSION)
        .unrestricted()
        .build();
    let input = input.into_array();
    let compressed = assert_roundtrip(&compressor, &input)?;
    assert!(compressed.is::<Delta>());

    let mut ctx = SESSION.create_execution_ctx();
    assert_eq!(sum(&compressed, &mut ctx)?, sum(&input, &mut ctx)?);

    Ok(())
}

#[cfg(feature = "zstd")]
#[rstest]
#[case::array_level(CORE_2026_08_3, vortex_zstd::Zstd.id())]
#[case::buffer_level(vortex_zstd::editions::ZSTD_2026_02, vortex_zstd::ZstdBuffers.id())]
fn test_cuda_binary_zstd_follows_editions(
    #[case] edition: EditionId,
    #[case] expected: ArrayId,
) -> VortexResult<()> {
    let values: Vec<_> = (0..1024u32)
        .map(|i| {
            let mut value = Vec::from(&b"common binary payload prefix "[..]);
            value.extend_from_slice(&i.to_le_bytes());
            value.extend_from_slice(&[b'x'; 96]);
            value
        })
        .collect();
    let input = VarBinViewArray::from_iter_bin(values.iter().map(Vec::as_slice)).into_array();

    // The CUDA preset enables both Zstd schemes; the edition filter must choose between them.
    let session = vortex_array::array_session();
    vortex_zstd::initialize(&session);
    for family in EDITION_FAMILIES {
        session.editions().declare_family(family)?;
    }
    for declaration in EDITION_DECLARATIONS {
        session.register_edition(declaration)?;
    }
    session.enable_edition(edition)?;
    let compressor = BtrBlocksCompressorBuilder::from_session(&session)
        .only_cuda_compatible()
        .build();
    let compressed = assert_roundtrip(&compressor, &input)?;
    assert_eq!(compressed.encoding_id(), expected);

    Ok(())
}
