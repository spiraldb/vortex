// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::sync::LazyLock;

use rstest::rstest;
use vortex_array::ArrayContext;
use vortex_array::ArrayRef;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::PiecewiseSequenceArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::assert_arrays_eq;
use vortex_array::serde::SerializeOptions;
use vortex_array::session::ArraySessionExt;
use vortex_array::validity::Validity;
use vortex_error::VortexResult;
use vortex_session::VortexSession;

use crate::Pco;
use crate::PcoArray;
use crate::PcoArrayExt;
use crate::PcoData;
use crate::PcoLogicalPages;

static SESSION: LazyLock<VortexSession> = LazyLock::new(|| {
    let session = VortexSession::empty();
    crate::initialize(&session);
    session
});

fn legacy(array: &PcoArray, unknown: bool) -> VortexResult<PcoArray> {
    let mut metadata = array.metadata.as_ref().clone();
    metadata.logical_pages = unknown.then_some(PcoLogicalPages {
        version: 999,
        ends: vec![u64::MAX],
    });
    // SAFETY: The unchanged compressed buffers retain the historical metadata contract. The
    // optional unknown version is deliberately ignored and Pco::try_new validates the result.
    let data = unsafe {
        PcoData::new_unchecked(
            array.chunk_metas.to_vec(),
            array.pages.to_vec(),
            array.dtype().as_ptype(),
            metadata,
            array.len(),
        )
    };
    Pco::try_new(array.dtype().clone(), data, array.unsliced_validity())
}

#[rstest]
fn reordered_nullable_take(#[values(0, 1, 2)] format: usize) -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let input =
        PrimitiveArray::from_option_iter((0..4096i32).map(|i| (i % 7 != 0).then_some(i * 19)));
    let encoded = Pco::from_primitive(input.as_view(), 3, 31, &mut ctx)?;
    let encoded = match format {
        0 => encoded,
        1 => legacy(&encoded, false)?,
        _ => legacy(&encoded, true)?,
    }
    .into_array();
    let source = encoded.slice(17..4001)?.slice(13..3900)?;
    let input = input.slice(30..3917)?;
    let indices = PrimitiveArray::from_option_iter([
        Some(3000u32),
        Some(0),
        None,
        Some(127),
        Some(1),
        Some(3000),
        Some(128),
        None,
        Some(1000),
    ])
    .into_array();
    assert_arrays_eq!(
        source.take(indices.clone())?,
        input.take(indices)?,
        &mut ctx
    );
    Ok(())
}

#[rstest]
#[case(vec![0u64, 40, 0, 70], vec![15u64, 20, 3, 0], vec![1u64, 1, 0, 1])]
#[case(vec![100u64, 0, 20], vec![20u64, 20, 20], vec![1u64, 1, 2])]
#[case(vec![99u64, 0], vec![0u64, 0], vec![u64::MAX, 0])]
fn piecewise_ranges(
    #[case] starts: Vec<u64>,
    #[case] lengths: Vec<u64>,
    #[case] strides: Vec<u64>,
) -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let input =
        PrimitiveArray::from_option_iter((0..256i64).map(|i| (i % 5 != 0).then_some(i * i)));
    let encoded = Pco::from_primitive(input.as_view(), 3, 7, &mut ctx)?.into_array();
    let mut expected_indices = Vec::new();
    for ((&start, &length), &stride) in starts.iter().zip(&lengths).zip(&strides) {
        expected_indices.extend((0..length).map(|i| start + i * stride));
    }
    let indices = PiecewiseSequenceArray::try_new(
        PrimitiveArray::from_iter(starts).into_array(),
        PrimitiveArray::from_iter(lengths).into_array(),
        PrimitiveArray::from_iter(strides).into_array(),
        expected_indices.len(),
    )?
    .into_array();
    assert_arrays_eq!(
        encoded.take(indices)?,
        input.take(PrimitiveArray::from_iter(expected_indices).into_array())?,
        &mut ctx
    );
    Ok(())
}

#[test]
fn all_null_and_long_null_runs() -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    for input in [
        PrimitiveArray::new(vec![0i32; 1024], Validity::AllInvalid),
        PrimitiveArray::from_option_iter(
            (0..100_000).map(|i| [1, 90_000, 99_990].contains(&i).then_some(i)),
        ),
    ] {
        let encoded = Pco::from_primitive(input.as_view(), 3, 1, &mut ctx)?.into_array();
        let indices = PrimitiveArray::from_iter([0u32, 1, 100, 1000, 2, 1]).into_array();
        assert_arrays_eq!(
            encoded.take(indices.clone())?,
            input.take(indices)?,
            &mut ctx
        );
        assert_arrays_eq!(encoded.slice(1..1000)?, input.slice(1..1000)?, &mut ctx);
    }
    Ok(())
}

#[test]
fn rejects_invalid_indices_and_descriptors() -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let input = PrimitiveArray::from_iter(0..64i32);
    let encoded = Pco::from_primitive(input.as_view(), 3, 4, &mut ctx)?.into_array();
    for indices in [
        PrimitiveArray::from_iter([-1i64]).into_array(),
        PrimitiveArray::from_iter([64i64]).into_array(),
    ] {
        assert!(
            encoded
                .take(indices)?
                .execute::<PrimitiveArray>(&mut ctx)
                .is_err()
        );
    }
    for (start, length, stride, declared) in [
        (u64::MAX, 2u64, 1u64, 2usize),
        (1, 2, u64::MAX, 2),
        (1, 2, 1, 3),
    ] {
        let indices = PiecewiseSequenceArray::try_new(
            PrimitiveArray::from_iter([start]).into_array(),
            PrimitiveArray::from_iter([length]).into_array(),
            PrimitiveArray::from_iter([stride]).into_array(),
            declared,
        )?
        .into_array();
        assert!(
            encoded
                .take(indices)?
                .execute::<PrimitiveArray>(&mut ctx)
                .is_err()
        );
    }
    Ok(())
}

#[test]
fn raw_sliced_serde_requires_explicit_normalization() -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let input = PrimitiveArray::from_option_iter((0..128i32).map(|i| (i % 3 != 0).then_some(i)));
    let encoded = Pco::from_primitive(input.as_view(), 3, 7, &mut ctx)?.into_array();
    let sliced = encoded.slice(11..77)?;
    assert!(
        sliced
            .serialize(
                &ArrayContext::empty(),
                &SESSION,
                &SerializeOptions::default()
            )
            .is_err()
    );
    let normalized = sliced.execute::<PrimitiveArray>(&mut ctx)?;
    let recompressed = Pco::from_primitive(normalized.as_view(), 3, 7, &mut ctx)?.into_array();
    assert!(
        !recompressed
            .serialize(
                &ArrayContext::empty(),
                &SESSION,
                &SerializeOptions::default()
            )?
            .is_empty()
    );
    assert_arrays_eq!(recompressed, input.slice(11..77)?, &mut ctx);
    Ok(())
}

#[test]
fn registry_only_session_retains_legacy_take_fallback() -> VortexResult<()> {
    let session = VortexSession::empty();
    session.arrays().register(Pco);
    let mut ctx = session.create_execution_ctx();
    let input = PrimitiveArray::from_option_iter([Some(5i32), None, Some(9), Some(2)]);
    let encoded = Pco::from_primitive(input.as_view(), 3, 1, &mut ctx)?.into_array();
    let indices: ArrayRef = PrimitiveArray::from_iter([3u32, 1, 0, 3]).into_array();
    assert_arrays_eq!(
        encoded.take(indices.clone())?,
        input.take(indices)?,
        &mut ctx
    );
    Ok(())
}

#[test]
fn checked_metadata_and_touched_page_validity() -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let input = PrimitiveArray::from_option_iter((0..128i32).map(|i| (i % 2 == 0).then_some(i)));
    let encoded = Pco::from_primitive(input.as_view(), 3, 2, &mut ctx)?;
    for ends in [
        vec![128u64],
        vec![0; encoded.pages.len()],
        vec![u64::MAX; encoded.pages.len()],
    ] {
        let mut metadata = encoded.metadata.as_ref().clone();
        metadata.logical_pages = Some(PcoLogicalPages { version: 1, ends });
        // SAFETY: Constructor is followed immediately by fallible validation; malformed metadata
        // is never published as a valid array.
        let data = unsafe {
            PcoData::new_unchecked(
                encoded.chunk_metas.to_vec(),
                encoded.pages.to_vec(),
                encoded.dtype().as_ptype(),
                metadata,
                encoded.len(),
            )
        };
        assert!(Pco::try_new(encoded.dtype().clone(), data, encoded.unsliced_validity()).is_err());
    }
    let mut metadata = encoded.metadata.as_ref().clone();
    // Structurally valid but the first logical page contains three valid rows, not two.
    metadata
        .logical_pages
        .as_mut()
        .expect("writer emits logical ends")
        .ends[0] = 5;
    let data = unsafe {
        PcoData::new_unchecked(
            encoded.chunk_metas.to_vec(),
            encoded.pages.to_vec(),
            encoded.dtype().as_ptype(),
            metadata,
            encoded.len(),
        )
    };
    let malformed =
        Pco::try_new(encoded.dtype().clone(), data, encoded.unsliced_validity())?.into_array();
    assert!(
        malformed
            .take(PrimitiveArray::from_iter([0u32]).into_array())?
            .execute::<PrimitiveArray>(&mut ctx)
            .is_err()
    );
    Ok(())
}

#[test]
fn selections_cross_compression_chunks() -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let input =
        PrimitiveArray::from_option_iter((0..8192i64).map(|i| (i % 11 != 0).then_some(i * 37)));
    let data =
        PcoData::from_primitive_with_values_per_chunk(input.as_view(), 3, 257, 31, &mut ctx)?;
    let encoded = Pco::try_new(input.dtype().clone(), data, input.validity()?)?.into_array();
    let indices =
        PrimitiveArray::from_iter([0u32, 900, 260, 3000, 259, 8191, 4095, 900]).into_array();
    assert_arrays_eq!(
        encoded.take(indices.clone())?,
        input.take(indices)?,
        &mut ctx
    );
    assert_arrays_eq!(encoded.slice(233..7901)?, input.slice(233..7901)?, &mut ctx);
    Ok(())
}
