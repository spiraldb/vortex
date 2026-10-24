// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Public file harness also run, unchanged, against the pre-change reader by PCO compatibility CI.

#![expect(clippy::tests_outside_test_module)]

use std::sync::Arc;

use futures::TryStreamExt;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::BoolArray;
use vortex_array::arrays::ChunkedArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::StructArray;
use vortex_array::assert_arrays_eq;
use vortex_array::validity::Validity;
use vortex_buffer::ByteBuffer;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_file::OpenOptionsSessionExt;
use vortex_file::WriteOptionsSessionExt;
use vortex_io::session::RuntimeSession;
use vortex_layout::layouts::flat::writer::FlatLayoutStrategy;
use vortex_layout::session::LayoutSession;
use vortex_pco::Pco;
use vortex_runend::RunEnd;
use vortex_session::VortexSession;

mod common;

#[tokio::test]
async fn public_pco_compatibility() -> VortexResult<()> {
    let session = VortexSession::empty()
        .with::<LayoutSession>()
        .with::<RuntimeSession>();
    vortex_file::register_default_encodings(&session);
    common::enable_all_registered_array_encodings(&session);
    let mut ctx = session.create_execution_ctx();
    let original = PrimitiveArray::from_option_iter(
        (0..131_072i64).map(|i| ((i < 1024 || i > 120_000) && i % 7 != 0).then_some(i * 19)),
    );
    let mode = std::env::var("PCO_COMPAT_MODE").unwrap_or_else(|_| "roundtrip".to_owned());
    let directory = std::env::var_os("PCO_COMPAT_DIR").map(std::path::PathBuf::from);
    for (name, range) in [("unsliced", 0..131_072), ("normalized_slice", 777..125_000)] {
        let encoded = Pco::from_primitive(original.as_view(), 3, 127, &mut ctx)?.into_array();
        // Explicit writer-owned normalization, never a hidden serde fallback: execute selected
        // pages and recompress only the chosen logical rows using the caller's configuration.
        let selected = encoded
            .slice(range.clone())?
            .execute::<PrimitiveArray>(&mut ctx)?;
        let normalized = Pco::from_primitive(selected.as_view(), 3, 127, &mut ctx)?.into_array();
        let expected =
            StructArray::from_fields(&[("values", original.slice(range)?)])?.into_array();
        let data = StructArray::from_fields(&[("values", normalized)])?.into_array();
        let path = directory
            .as_ref()
            .map(|directory| directory.join(format!("{name}.vortex")));
        let bytes = if mode == "read" {
            std::fs::read(
                path.as_ref()
                    .vortex_expect("read mode requires PCO_COMPAT_DIR"),
            )?
        } else {
            let mut bytes = Vec::new();
            session
                .write_options()
                .with_strategy(Arc::new(FlatLayoutStrategy::default()))
                .write(&mut bytes, data.to_array_stream())
                .await?;
            if let Some(path) = &path {
                std::fs::create_dir_all(path.parent().vortex_expect("fixture path has a parent"))?;
                std::fs::write(path, &bytes)?;
            }
            bytes
        };
        let file = session
            .open_options()
            .open_buffer(ByteBuffer::from(bytes))?;
        let chunks = file.scan()?.into_stream()?.try_collect::<Vec<_>>().await?;
        let actual = ChunkedArray::try_new(chunks, expected.dtype().clone())?.into_array();
        assert_arrays_eq!(actual, expected, &mut ctx);
    }
    Ok(())
}

#[test]
fn compressed_validity_selection_oracle() -> VortexResult<()> {
    let session = VortexSession::empty();
    vortex_file::register_default_encodings(&session);
    let mut ctx = session.create_execution_ctx();
    let validity = Validity::Array(
        RunEnd::try_new(
            PrimitiveArray::from_iter([32768u64, 65536, 98304, 131072]).into_array(),
            BoolArray::from_iter([false, true, false, true]).into_array(),
            &mut ctx,
        )?
        .into_array(),
    );
    let input = PrimitiveArray::new((0..131072i64).collect::<Vec<_>>(), validity);
    let expected =
        PrimitiveArray::from_option_iter((0..131072i64).map(|i| (i / 32768 % 2 == 1).then_some(i)));
    let encoded = Pco::from_primitive(input.as_view(), 3, 127, &mut ctx)?.into_array();
    let indices =
        PrimitiveArray::from_iter([0u32, 32767, 32768, 65535, 65536, 98304, 131071, 32768])
            .into_array();
    assert_arrays_eq!(
        encoded.take(indices.clone())?,
        expected.take(indices)?,
        &mut ctx
    );
    assert_arrays_eq!(
        encoded.slice(30001..130001)?,
        expected.slice(30001..130001)?,
        &mut ctx
    );
    let mut probe = encoded.repeated_probe();
    for index in [0, 32767, 32768, 65535, 65536, 98304, 131071, 32768] {
        assert_eq!(
            probe.execute_scalar(index, &mut ctx)?,
            expected.execute_scalar(index, &mut ctx)?
        );
    }
    Ok(())
}

/// Public byte-boundary diagnostic for the exact deterministic compact stock fixture.
/// This reports format overhead; it is not a timing or topology assertion.
#[cfg(feature = "zstd")]
#[tokio::test]
async fn public_stock_size_attribution() -> VortexResult<()> {
    use rand::Rng;
    use rand::SeedableRng;
    use rand::rngs::StdRng;
    use vortex_array::stream::ArrayStreamExt;
    use vortex_btrblocks::BtrBlocksCompressorBuilder;
    use vortex_file::strategy::WriteStrategyBuilder;

    if std::env::var_os("PCO_SIZE_DIAGNOSTICS").is_none() {
        return Ok(());
    }
    let session = VortexSession::empty()
        .with::<LayoutSession>()
        .with::<RuntimeSession>();
    vortex_file::register_default_encodings(&session);
    common::enable_all_registered_array_encodings(&session);
    let mut rng = StdRng::seed_from_u64(0);
    let input = PrimitiveArray::from_iter((0..100000i64).map(|i| rng.random_range(i..=i + 10)))
        .into_array();
    let mut bytes = Vec::new();
    session
        .write_options()
        .with_strategy(
            WriteStrategyBuilder::from_session(&session)
                .with_btrblocks_builder(
                    BtrBlocksCompressorBuilder::from_session(&session).with_compact(),
                )
                .build(),
        )
        .write(&mut bytes, input.clone().to_array_stream())
        .await?;
    let len = bytes.len();
    let file = session
        .open_options()
        .open_buffer(ByteBuffer::from(bytes))?;
    let segments = file.footer().segment_map();
    let payload = segments
        .iter()
        .map(|segment| u64::from(segment.length))
        .sum::<u64>();
    let end = segments
        .iter()
        .map(|segment| segment.offset + u64::from(segment.length))
        .max()
        .unwrap_or(0);
    println!(
        "{{\"stock_file_bytes\":{len},\"segment_payload_bytes\":{payload},\"last_segment_end\":{end},\"trailing_format_bytes\":{},\"segments\":{:?}}}",
        u64::try_from(len).vortex_expect("file fits u64") - end,
        segments
            .iter()
            .map(|segment| vec![
                segment.offset,
                u64::from(segment.length),
                u64::try_from(segment.alignment.as_usize()).vortex_expect("alignment fits u64")
            ])
            .collect::<Vec<_>>()
    );
    let actual = file.scan()?.into_array_stream()?.read_all().await?;
    assert_arrays_eq!(actual, input, &mut session.create_execution_ctx());
    Ok(())
}
