// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::sync::LazyLock;

use prost::Message;
use rstest::rstest;
use vortex_array::ArrayContext;
use vortex_array::ArrayDeserialization;
use vortex_array::ArrayPlugin;
use vortex_array::ArrayRef;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::PiecewiseSequenceArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::assert_arrays_eq;
use vortex_array::buffer::BufferHandle;
use vortex_array::serde::SerializeOptions;
use vortex_array::session::ArraySessionExt;
use vortex_array::validity::Validity;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_session::VortexSession;

use crate::Pco;
use crate::PcoLogicalPages;

static SESSION: LazyLock<VortexSession> = LazyLock::new(|| {
    let session = VortexSession::empty();
    crate::initialize(&session);
    session
});

/// Build a wire fixture through the public serializer and deserialize its mutated protobuf
/// metadata through the public plugin boundary. No private in-memory encoding fields are used.
fn with_wire_metadata(
    array: &ArrayRef,
    mutate: impl FnOnce(&mut crate::PcoMetadata),
) -> VortexResult<ArrayRef> {
    let mut wire = SESSION
        .array_serialize(array)?
        .vortex_expect("PCO is serializable");
    let mut metadata = crate::PcoMetadata::decode(wire.metadata.as_slice())?;
    mutate(&mut metadata);
    wire.metadata = metadata.encode_to_vec();
    let buffers = wire
        .buffers
        .into_iter()
        .map(BufferHandle::new_host)
        .collect::<Vec<_>>();
    ArrayPlugin::deserialize(
        &Pco,
        ArrayDeserialization::new(
            wire.serialized_id,
            array.dtype(),
            array.len(),
            &wire.metadata,
            &buffers,
            &wire.children,
        ),
        &SESSION,
    )
}

fn legacy(array: &ArrayRef, unknown: bool) -> VortexResult<ArrayRef> {
    with_wire_metadata(array, |metadata| {
        metadata.logical_pages = unknown.then_some(PcoLogicalPages {
            version: 999,
            ends: vec![u64::MAX],
        });
    })
}

#[rstest]
fn reordered_nullable_take(#[values(0, 1, 2)] format: usize) -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let input =
        PrimitiveArray::from_option_iter((0..4096i32).map(|i| (i % 7 != 0).then_some(i * 19)));
    let encoded = Pco::from_primitive(input.as_view(), 3, 31, &mut ctx)?.into_array();
    let encoded = match format {
        0 => encoded,
        1 => legacy(&encoded, false)?,
        _ => legacy(&encoded, true)?,
    };
    let source = encoded.slice(17..4001)?.slice(13..3900)?;
    let input = input.slice(30..3917)?;
    let mut probe = source.repeated_probe();
    for index in [3000, 0, 127, 1, 3000, 128, 1000, 0] {
        assert_eq!(
            probe.execute_scalar(index, &mut ctx)?,
            input.execute_scalar(index, &mut ctx)?
        );
    }
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
                .take(indices)
                .and_then(|array| array.execute::<PrimitiveArray>(&mut ctx))
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
                .take(indices)
                .and_then(|array| array.execute::<PrimitiveArray>(&mut ctx))
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
    let encoded = Pco::from_primitive(input.as_view(), 3, 2, &mut ctx)?.into_array();
    for ends in [vec![128u64], vec![0; 32], vec![u64::MAX; 32]] {
        assert!(
            with_wire_metadata(&encoded, |metadata| {
                metadata.logical_pages = Some(PcoLogicalPages { version: 1, ends });
            })
            .is_err()
        );
    }
    assert!(
        with_wire_metadata(&encoded, |metadata| {
            metadata.chunks[0].pages[0].n_values = u32::try_from(pco::DEFAULT_MAX_PAGE_N + 1)
                .vortex_expect("invalid fixture count fits u32");
        })
        .is_err()
    );
    let malformed = with_wire_metadata(&encoded, |metadata| {
        // Structurally valid but the first logical page has three valid rows, not two.
        metadata
            .logical_pages
            .as_mut()
            .vortex_expect("writer emits logical ends")
            .ends[0] = 5;
    })?;
    assert!(
        malformed
            .take(PrimitiveArray::from_iter([0u32]).into_array())?
            .execute::<PrimitiveArray>(&mut ctx)
            .is_err()
    );
    Ok(())
}

#[test]
fn large_public_compressor_selections() -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let len = pco::DEFAULT_MAX_PAGE_N + 257;
    let input = PrimitiveArray::from_iter(
        (0..len).map(|i| u32::try_from(i).vortex_expect("fixture fits u32")),
    );
    let encoded = Pco::from_primitive(input.as_view(), 0, 4096, &mut ctx)?.into_array();
    let indices = PrimitiveArray::from_iter([
        0u32,
        900,
        260,
        3000,
        u32::try_from(len - 1).vortex_expect("fixture fits u32"),
        4095,
        900,
    ])
    .into_array();
    assert_arrays_eq!(
        encoded.take(indices.clone())?,
        input.take(indices)?,
        &mut ctx
    );
    assert_arrays_eq!(
        encoded.slice(len - 1000..len - 1)?,
        input.slice(len - 1000..len - 1)?,
        &mut ctx
    );
    Ok(())
}

#[rstest]
fn repeated_scattered_requests_preserve_order_and_nulls(
    #[values(false, true)] legacy_format: bool,
) -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let values = (0..32768i64)
        .map(|row| (row % 7 != 0).then_some(row * 13))
        .collect::<Vec<_>>();
    let input = PrimitiveArray::from_option_iter(values.iter().copied());
    let encoded = Pco::from_primitive(input.as_view(), 3, 1024, &mut ctx)?.into_array();
    let encoded = if legacy_format {
        legacy(&encoded, false)?
    } else {
        encoded
    };
    let requests = (0..1024usize)
        .map(|i| (i % 11 != 0).then_some((i * 97) % 16))
        .collect::<Vec<_>>();
    let indices = PrimitiveArray::from_option_iter(
        requests
            .iter()
            .map(|row| row.map(|row| u64::try_from(row).vortex_expect("small index"))),
    )
    .into_array();
    let expected = PrimitiveArray::from_option_iter(
        requests.iter().map(|row| row.and_then(|row| values[row])),
    );
    assert_arrays_eq!(encoded.take(indices)?, expected, &mut ctx);
    let invalid = PrimitiveArray::from_iter([0i64, -1, 32768]).into_array();
    assert!(
        encoded
            .take(invalid)?
            .execute::<PrimitiveArray>(&mut ctx)
            .is_err()
    );
    Ok(())
}
