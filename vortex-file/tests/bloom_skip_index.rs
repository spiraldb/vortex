// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! End-to-end coverage for the zoned Bloom skipping index.
//!
//! Bloom indexes are optional extensions rather than part of the default file layout. This test
//! exercises the complete opt-in lifecycle:
//!
//! 1. register the index with a write session (with editions disabled) and request it for one field;
//! 2. persist one Bloom filter per zone and reopen the file with a fresh registered session;
//! 3. prove that equality predicates prune zones while returning the same rows as a full scan; and
//! 4. reopen the indexed file with an unregistered, allow-unknown session to verify that the index
//!    is ignorable.
//!
//! The input is intentionally hostile to ordinary min/max pruning. Zone `z` contains values whose
//! remainder modulo [`NZONES`] is `z`, so both [`HIT`] and [`MISS`] lie inside every zone's
//! min/max range. `MISS` is then removed from its zone without changing that range. Consequently,
//! pruning either value requires the Bloom filter rather than the built-in range statistics.

#![expect(clippy::expect_used)]

use std::num::NonZeroU32;
use std::num::NonZeroUsize;
use std::sync::Arc;

use vortex_array::ArrayRef;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::aggregate_fn::AggregateFnVTableExt;
use vortex_array::aggregate_fn::NumericalAggregateOpts;
use vortex_array::aggregate_fn::fns::min::Min;
use vortex_array::arrays::ChunkedArray;
use vortex_array::arrays::DecimalArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::StructArray;
use vortex_array::assert_arrays_eq;
use vortex_array::dtype::DType;
use vortex_array::dtype::DecimalDType;
use vortex_array::dtype::Nullability;
use vortex_array::dtype::PType;
use vortex_array::expr::BoundExpression;
use vortex_array::expr::bound::eq;
use vortex_array::expr::bound::get_item;
use vortex_array::expr::bound::lit;
use vortex_array::expr::bound::lt;
use vortex_array::expr::bound::root;
use vortex_array::field_path;
use vortex_array::stream::ArrayStreamExt;
use vortex_array::validity::Validity;
use vortex_buffer::buffer;
use vortex_error::VortexResult;
use vortex_file::OpenOptionsSessionExt;
use vortex_file::WriteOptionsSessionExt;
use vortex_file::WriteStrategyBuilder;
use vortex_io::session::RuntimeSession;
use vortex_layout::LayoutStrategy;
use vortex_layout::layouts::chunked::writer::ChunkedLayoutStrategy;
use vortex_layout::layouts::flat::writer::FlatLayoutStrategy;
use vortex_layout::layouts::repartition::RepartitionStrategy;
use vortex_layout::layouts::repartition::RepartitionWriterOptions;
use vortex_layout::layouts::zoned::Zoned;
use vortex_layout::layouts::zoned::aggregates::bloom_filter::BloomOptions;
use vortex_layout::layouts::zoned::aggregates::bloom_filter::HashFn;
use vortex_layout::layouts::zoned::skip_index::SkipIndex;
use vortex_layout::layouts::zoned::skip_index::SkipIndexSessionExt;
use vortex_layout::layouts::zoned::skip_index::bloom::BloomSkipIndex;
use vortex_layout::layouts::zoned::writer::ZonedAggregates;
use vortex_layout::layouts::zoned::writer::ZonedLayoutOptions;
use vortex_layout::layouts::zoned::writer::ZonedStrategy;
use vortex_layout::session::LayoutSession;
use vortex_mask::Mask;
use vortex_session::VortexSession;

const ZONE_LEN: usize = 256;
const NZONES: usize = 4;
const HIT: i64 = 502;
const MISS: i64 = 503;

fn bloom() -> BloomSkipIndex {
    bloom_with_options(BloomOptions::default())
}

fn bloom_with_options(options: BloomOptions) -> BloomSkipIndex {
    BloomSkipIndex::new(options)
}

fn session(index: &BloomSkipIndex, register_index: bool) -> VortexSession {
    let session = vortex_array::array_session()
        .with::<LayoutSession>()
        .with::<RuntimeSession>();
    vortex_file::register_default_encodings(&session);

    if register_index {
        session.register_skip_index(index);
    }

    session
}

fn data() -> ArrayRef {
    data_with_shape(ZONE_LEN, NZONES, Some(MISS))
}

fn data_with_shape(zone_len: usize, nzones: usize, missing: Option<i64>) -> ArrayRef {
    let chunks = (0..nzones)
        .map(|zone| {
            let mut values = (0..zone_len)
                .map(|row| i64::try_from(row * nzones + zone).expect("test value fits i64"))
                .collect::<Vec<_>>();
            if let Some(missing) = missing
                && usize::try_from(missing).expect("missing value is non-negative") % nzones == zone
            {
                // Leave a hole inside every zone's min/max range so a MISS cannot be pruned by the
                // ordinary range stats. The bloom must provide the proof.
                values[usize::try_from(missing).expect("missing value is non-negative") / nzones] =
                    i64::try_from(zone_len * nzones + zone).expect("replacement fits i64");
            }
            StructArray::from_fields(&[("id", PrimitiveArray::from_iter(values).into_array())])
                .expect("valid test struct")
                .into_array()
        })
        .collect::<Vec<_>>();
    ChunkedArray::try_new(
        chunks,
        DType::struct_(
            [("id", DType::Primitive(PType::I64, Nullability::NonNullable))],
            Nullability::NonNullable,
        ),
    )
    .expect("valid chunked test data")
    .into_array()
}

fn filter(value: i64) -> BoundExpression {
    let input_dtype = DType::struct_(
        [("id", DType::Primitive(PType::I64, Nullability::NonNullable))],
        Nullability::NonNullable,
    );

    eq(get_item("id", root(input_dtype)), lit(value))
}

fn strategy<T: SkipIndex>(index: &T, zone_len: usize) -> VortexResult<Arc<dyn LayoutStrategy>> {
    Ok(WriteStrategyBuilder::default()
        .with_row_block_size(zone_len)
        .with_field_aggregates(field_path!(id), [index.aggregate_fn()])
        .build())
}

async fn scan(file: &vortex_file::VortexFile, value: i64) -> VortexResult<ArrayRef> {
    file.scan()?
        .with_filter(filter(value))
        .into_array_stream()?
        .read_all()
        .await
}

async fn write_file(
    session: &VortexSession,
    input: &ArrayRef,
    index: &BloomSkipIndex,
    zone_len: usize,
) -> VortexResult<Vec<u8>> {
    let mut bytes = Vec::new();
    session
        .write_options()
        // Bloom filters are not part of any edition.
        .disable_editions()
        .with_strategy(strategy(index, zone_len)?)
        .write(&mut bytes, input.to_array_stream())
        .await?;
    Ok(bytes)
}

#[expect(clippy::tests_outside_test_module)]
#[tokio::test]
async fn bloom_roundtrip_prunes_and_unknown_reader_matches_full_scan() -> VortexResult<()> {
    let index = bloom();
    let write_session = session(&index, true);
    let input = data();
    let bytes = write_file(&write_session, &input, &index, ZONE_LEN).await?;

    // Reconstruct every read-side extension from a fresh session rather than accidentally relying
    // on state retained by the writer.
    let read_session = session(&index, true);
    let file = read_session.open_options().open_buffer(bytes.clone())?;
    let reader = file.layout_reader()?;
    let row_count = file.row_count();

    // HIT is present only in zone 2. Since it falls within every zone's min/max range, the exact
    // one-zone mask proves that the Bloom falsifier participated in pruning.
    let hit_mask = reader
        .pruning_evaluation(
            &(0..row_count),
            &filter(HIT),
            Mask::new_true(usize::try_from(row_count)?),
        )?
        .await?;
    assert_eq!(hit_mask.true_count(), ZONE_LEN);
    assert!(hit_mask.iter().take(2 * ZONE_LEN).all(|keep| !keep));
    assert!(
        hit_mask
            .iter()
            .skip(2 * ZONE_LEN)
            .take(ZONE_LEN)
            .all(|keep| keep)
    );
    assert!(hit_mask.iter().skip(3 * ZONE_LEN).all(|keep| !keep));

    // MISS was removed while remaining inside every zone's min/max range. Only the Bloom filters
    // can prove that all four zones are absent.
    let miss_mask = reader
        .pruning_evaluation(
            &(0..row_count),
            &filter(MISS),
            Mask::new_true(usize::try_from(row_count)?),
        )?
        .await?;
    assert!(
        miss_mask.all_false(),
        "an absent value should prune every zone"
    );

    // An allow-unknown reader without Bloom registration bypasses the unavailable zone map and
    // scans the data child. This both supplies the reference result and verifies that an optional
    // index does not become a hard read-time dependency.
    let full_scan_session = session(&index, false);
    full_scan_session.allow_unknown();
    let full_scan_file = full_scan_session.open_options().open_buffer(bytes)?;

    let indexed_hit = scan(&file, HIT).await?;
    let full_scan_hit = scan(&full_scan_file, HIT).await?;
    // A Bloom filter may retain extra zones, but it must never change query results.
    assert_arrays_eq!(
        indexed_hit,
        full_scan_hit,
        &mut read_session.create_execution_ctx()
    );
    let expected_hit =
        StructArray::from_fields(&[("id", PrimitiveArray::from_iter([HIT]).into_array())])?
            .into_array();
    assert_arrays_eq!(
        full_scan_hit,
        expected_hit,
        &mut read_session.create_execution_ctx()
    );

    let indexed_miss = scan(&file, MISS).await?;
    let full_scan_miss = scan(&full_scan_file, MISS).await?;
    assert_arrays_eq!(
        indexed_miss,
        full_scan_miss,
        &mut read_session.create_execution_ctx()
    );
    assert_eq!(full_scan_miss.len(), 0);
    Ok(())
}

#[expect(clippy::tests_outside_test_module)]
#[tokio::test]
async fn reader_uses_bloom_options_serialized_in_file() -> VortexResult<()> {
    let read_session = session(&BloomSkipIndex::default(), true);
    let input = data();
    for blocks in [128, 512] {
        let index = bloom_with_options(BloomOptions::new(
            NonZeroU32::new(blocks).expect("block count is non-zero"),
            HashFn::XxHash3_64,
        ));
        let write_session = session(&index, true);
        let bytes = write_file(&write_session, &input, &index, ZONE_LEN).await?;
        let file = read_session.open_options().open_buffer(bytes)?;
        let reader = file.layout_reader()?;
        let row_count = file.row_count();
        let miss_mask = reader
            .pruning_evaluation(
                &(0..row_count),
                &filter(MISS),
                Mask::new_true(usize::try_from(row_count)?),
            )?
            .await?;
        assert!(
            miss_mask.all_false(),
            "an absent value should be pruned using the serialized Bloom options"
        );
        let hit = scan(&file, HIT).await?;
        let expected =
            StructArray::from_fields(&[("id", PrimitiveArray::from_iter([HIT]).into_array())])?
                .into_array();
        assert_arrays_eq!(hit, expected, &mut read_session.create_execution_ctx());
    }
    Ok(())
}

#[expect(clippy::tests_outside_test_module)]
#[tokio::test]
async fn unsupported_explicit_bloom_dtype_is_rejected() -> VortexResult<()> {
    let index = bloom();
    let write_session = session(&index, true);
    let decimals = DecimalArray::new(
        buffer![100i32, -250, 0, 375, 999, -1],
        DecimalDType::new(5, 2),
        Validity::NonNullable,
    );
    let input = StructArray::from_fields(&[("id", decimals.into_array())])?.into_array();
    let error = write_file(&write_session, &input, &index, 3)
        .await
        .expect_err("unsupported explicit index must fail");
    assert!(error.to_string().contains("supported input dtype"));
    Ok(())
}

#[expect(clippy::tests_outside_test_module)]
#[tokio::test]
async fn additive_api_preserves_defaults() -> VortexResult<()> {
    let index = bloom();
    let session = session(&index, true);
    let input = data();
    let aggregate = index.aggregate_fn();
    let additive = WriteStrategyBuilder::default()
        .with_row_block_size(ZONE_LEN)
        .with_field_aggregate_additions(field_path!(id), [aggregate.clone()])
        .with_field_aggregate_additions(field_path!(id), [aggregate.clone()])
        .build();
    {
        let strategy = additive;
        let mut bytes = Vec::new();
        session
            .write_options()
            .disable_editions()
            .with_strategy(strategy)
            .write(&mut bytes, input.to_array_stream())
            .await?;
        let file = session.open_options().open_buffer(bytes)?;
        let mut layouts = vec![Arc::clone(file.footer().layout())];
        let mut found = false;
        while let Some(layout) = layouts.pop() {
            let children = layout.children()?;
            if layout.is::<Zoned>() {
                let fields = children[1].dtype().as_struct_fields();
                assert!(
                    fields
                        .names()
                        .iter()
                        .any(|name| name.as_ref() == aggregate.to_string())
                );
                assert!(
                    fields
                        .names()
                        .iter()
                        .any(|name| name.as_ref().starts_with("vortex.min("))
                );
                assert_eq!(
                    fields
                        .names()
                        .iter()
                        .filter(|name| name.as_ref() == aggregate.to_string())
                        .count(),
                    1
                );
                found = true;
            }
            layouts.extend(children);
        }
        assert!(found);
    }
    Ok(())
}

#[expect(clippy::tests_outside_test_module)]
#[tokio::test]
async fn missing_explicit_field_is_rejected() -> VortexResult<()> {
    let index = bloom();
    let session = session(&index, true);
    let input = data();
    let strategy = WriteStrategyBuilder::default()
        .with_field_aggregate_additions(field_path!(typo), [index.aggregate_fn()])
        .build();
    let mut bytes = Vec::new();
    let error = session
        .write_options()
        .disable_editions()
        .with_strategy(strategy)
        .write(&mut bytes, input.to_array_stream())
        .await
        .err()
        .ok_or_else(|| vortex_error::vortex_err!("missing field must fail"))?;
    assert!(error.to_string().contains("$typo"));
    Ok(())
}

#[expect(clippy::tests_outside_test_module)]
#[tokio::test]
async fn unknown_aggregate_disables_known_pruning_in_same_zone_map() -> VortexResult<()> {
    let index = bloom();
    let write_session = session(&index, true);
    let zoned = ZonedStrategy::new(
        ChunkedLayoutStrategy::new(FlatLayoutStrategy::default()),
        FlatLayoutStrategy::default(),
        ZonedLayoutOptions {
            block_size: NonZeroUsize::new(ZONE_LEN).expect("positive test zone length"),
            aggregates: ZonedAggregates::Replace(
                vec![
                    Min.bind(NumericalAggregateOpts::skip_nans()),
                    index.aggregate_fn(),
                ]
                .into(),
            ),
            ..Default::default()
        },
    );
    let strategy = WriteStrategyBuilder::default()
        .with_field_writer(
            field_path!(id),
            Arc::new(RepartitionStrategy::new(
                zoned,
                RepartitionWriterOptions {
                    block_size_minimum: 0,
                    block_len_multiple: ZONE_LEN,
                    block_size_target: None,
                    canonicalize: false,
                },
            )),
        )
        .build();
    let input = data();
    let predicate = lt(get_item("id", root(input.dtype().clone())), lit(-1i64));
    let mut bytes = Vec::new();
    write_session
        .write_options()
        .disable_editions()
        .with_strategy(strategy)
        .write(&mut bytes, input.to_array_stream())
        .await?;
    for (register_bloom, expected_keep_count) in [(true, 0), (false, ZONE_LEN * NZONES)] {
        let read_session = session(&index, register_bloom);
        read_session.allow_unknown();
        let file = read_session.open_options().open_buffer(bytes.clone())?;
        let row_count = file.row_count();
        let reader = file.footer().layout().new_reader(
            "without-file-stats".into(),
            file.segment_source(),
            &read_session,
            &Default::default(),
        )?;
        let keep = reader
            .pruning_evaluation(
                &(0..row_count),
                &predicate,
                Mask::new_true(usize::try_from(row_count)?),
            )?
            .await?;
        assert_eq!(keep.true_count(), expected_keep_count);
        let result = file
            .scan()?
            .with_filter(predicate.clone())
            .into_array_stream()?
            .read_all()
            .await?;
        assert_eq!(result.len(), 0);
    }
    Ok(())
}

#[cfg(test)]
mod composition_tests {
    use std::num::NonZeroUsize;

    use rstest::rstest;
    use vortex_array::aggregate_fn::AggregateFnVTableExt;
    use vortex_array::aggregate_fn::EmptyOptions;
    use vortex_array::aggregate_fn::NumericalAggregateOpts;
    use vortex_array::aggregate_fn::fns::min::Min;
    use vortex_array::aggregate_fn::fns::null_count::NullCount;
    use vortex_array::arrays::ListArray;
    use vortex_array::dtype::Field;
    use vortex_array::dtype::FieldPath;
    use vortex_array::expr::bound::lt;
    use vortex_array::stats::rewrite::StatsRewriteRuleRef;
    use vortex_layout::layouts::chunked::writer::ChunkedLayoutStrategy;
    use vortex_layout::layouts::flat::writer::FlatLayoutStrategy;
    use vortex_layout::layouts::list::List;
    use vortex_layout::layouts::repartition::RepartitionStrategy;
    use vortex_layout::layouts::repartition::RepartitionWriterOptions;
    use vortex_layout::layouts::zoned::writer::ZonedAggregates;
    use vortex_layout::layouts::zoned::writer::ZonedLayoutOptions;
    use vortex_layout::layouts::zoned::writer::ZonedStrategy;

    use super::*;

    struct MinIndex;

    impl SkipIndex for MinIndex {
        type Aggregate = Min;

        fn aggregate_vtable(&self) -> Self::Aggregate {
            Min
        }

        fn options(&self) -> NumericalAggregateOpts {
            NumericalAggregateOpts::default()
        }

        fn rewrite_rules(&self) -> Vec<StatsRewriteRuleRef> {
            Vec::new()
        }
    }

    async fn write_with_strategy(
        session: &VortexSession,
        input: &ArrayRef,
        strategy: Arc<dyn LayoutStrategy>,
    ) -> VortexResult<vortex_file::VortexFile> {
        let mut bytes = Vec::new();
        session
            .write_options()
            .disable_editions()
            .with_file_statistics(Vec::new())
            .with_strategy(strategy)
            .write(&mut bytes, input.to_array_stream())
            .await?;
        session.open_options().open_buffer(bytes)
    }

    #[rstest]
    #[case::manual(false)]
    #[case::data_writer_hook(true)]
    #[tokio::test]
    async fn custom_data_writer_keeps_bloom_pruning(#[case] use_hook: bool) -> VortexResult<()> {
        let index = bloom();
        let session = session(&index, true);
        let input = data();
        let data_writer: Arc<dyn LayoutStrategy> =
            Arc::new(ChunkedLayoutStrategy::new(FlatLayoutStrategy::default()));
        let builder = WriteStrategyBuilder::default().with_row_block_size(ZONE_LEN);
        let strategy = if use_hook {
            builder
                .with_field_data_writer(field_path!(id), data_writer)
                .with_field_aggregates(field_path!(id), [index.aggregate_fn()])
                .try_build()?
        } else {
            let zoned = ZonedStrategy::new(
                data_writer,
                FlatLayoutStrategy::default(),
                ZonedLayoutOptions {
                    block_size: NonZeroUsize::new(ZONE_LEN).expect("positive test zone length"),
                    aggregates: ZonedAggregates::Replace(vec![index.aggregate_fn()].into()),
                    ..Default::default()
                },
            );
            let writer = RepartitionStrategy::new(
                zoned,
                RepartitionWriterOptions {
                    block_size_minimum: 0,
                    block_len_multiple: ZONE_LEN,
                    block_size_target: None,
                    canonicalize: false,
                },
            );
            builder
                .with_field_writer(field_path!(id), Arc::new(writer))
                .try_build()?
        };
        let file = write_with_strategy(&session, &input, strategy).await?;
        let count = file.row_count();
        let keep = file
            .layout_reader()?
            .pruning_evaluation(
                &(0..count),
                &filter(HIT),
                Mask::new_true(usize::try_from(count)?),
            )?
            .await?;
        assert_eq!(keep.true_count(), ZONE_LEN);
        let actual = file.scan()?.into_array_stream()?.read_all().await?;
        assert_arrays_eq!(actual, input, &mut session.create_execution_ctx());
        Ok(())
    }

    #[tokio::test]
    async fn index_without_custom_probe_uses_builtin_rewrites() -> VortexResult<()> {
        let session = session(&bloom(), false);
        session.register_skip_index(&MinIndex);
        let input = data();
        let strategy = WriteStrategyBuilder::default()
            .with_row_block_size(ZONE_LEN)
            .with_field_aggregates(field_path!(id), [MinIndex.aggregate_fn()])
            .try_build()?;
        let file = write_with_strategy(&session, &input, strategy).await?;
        let predicate = lt(get_item("id", root(input.dtype().clone())), lit(-1i64));
        let count = file.row_count();
        let keep = file
            .layout_reader()?
            .pruning_evaluation(
                &(0..count),
                &predicate,
                Mask::new_true(usize::try_from(count)?),
            )?
            .await?;
        assert!(keep.all_false());
        Ok(())
    }

    #[test]
    fn overlapping_complete_writer_returns_error() {
        let index = bloom();
        let result = WriteStrategyBuilder::default()
            .with_field_writer(field_path!(nested), Arc::new(FlatLayoutStrategy::default()))
            .with_field_aggregates(field_path!(nested.id), [index.aggregate_fn()])
            .try_build();
        assert!(result.is_err());
    }

    #[test]
    fn data_writer_and_complete_writer_return_error() {
        let result = WriteStrategyBuilder::default()
            .with_field_writer(field_path!(id), Arc::new(FlatLayoutStrategy::default()))
            .with_field_data_writer(field_path!(id), Arc::new(FlatLayoutStrategy::default()))
            .try_build();
        assert!(result.is_err());
    }
    #[tokio::test]
    async fn aggregate_override_preserves_list_decomposition() -> VortexResult<()> {
        let session = session(&bloom(), false);
        let items = ListArray::try_new(
            buffer![1i32, 2, 3, 4].into_array(),
            buffer![0u32, 2, 4].into_array(),
            Validity::NonNullable,
        )?
        .into_array();
        let input = StructArray::from_fields(&[("items", items)])?.into_array();
        for custom_aggregates in [false, true] {
            let mut builder = WriteStrategyBuilder::default().with_list_layout();
            if custom_aggregates {
                builder = builder
                    .with_field_aggregates(field_path!(items), [NullCount.bind(EmptyOptions)]);
            }
            let file = write_with_strategy(&session, &input, builder.try_build()?).await?;
            let mut pending = vec![Arc::clone(file.footer().layout())];
            let mut has_list_layout = false;
            while let Some(layout) = pending.pop() {
                has_list_layout |= layout.is::<List>();
                pending.extend(layout.children()?);
            }
            assert!(has_list_layout);
        }
        Ok(())
    }

    #[tokio::test]
    async fn list_element_aggregate_override_is_applied() -> VortexResult<()> {
        let index = bloom();
        let session = session(&index, true);
        let items = ListArray::try_new(
            buffer![1i64, 2, 3, 4].into_array(),
            buffer![0u32, 2, 4].into_array(),
            Validity::NonNullable,
        )?
        .into_array();
        let input = StructArray::from_fields(&[("items", items)])?.into_array();
        let path = FieldPath::from_iter([Field::from("items"), Field::ElementType]);
        let strategy = WriteStrategyBuilder::default()
            .with_list_layout()
            .with_field_aggregates(path, [index.aggregate_fn()])
            .try_build()?;
        let file = write_with_strategy(&session, &input, strategy).await?;
        let mut pending = vec![Arc::clone(file.footer().layout())];
        let mut has_bloom = false;
        while let Some(layout) = pending.pop() {
            if let Some(zoned) = layout.as_opt::<Zoned>() {
                has_bloom |= zoned
                    .present_aggregates()
                    .iter()
                    .any(|name| name.contains("bloom"));
            }
            pending.extend(layout.children()?);
        }
        assert!(has_bloom);
        Ok(())
    }

    #[rstest]
    #[case::bloom_inside(true)]
    #[case::bloom_outside(false)]
    #[tokio::test]
    async fn separate_zones_preserve_known_pruning(#[case] bloom_inside: bool) -> VortexResult<()> {
        let index = bloom();
        let write_session = session(&index, true);
        let input = data();
        let mut data_writer: Arc<dyn LayoutStrategy> =
            Arc::new(ChunkedLayoutStrategy::new(FlatLayoutStrategy::default()));
        let aggregates = if bloom_inside {
            [index.aggregate_fn(), MinIndex.aggregate_fn()]
        } else {
            [MinIndex.aggregate_fn(), index.aggregate_fn()]
        };
        for aggregate in aggregates {
            data_writer = Arc::new(ZonedStrategy::new(
                data_writer,
                FlatLayoutStrategy::default(),
                ZonedLayoutOptions {
                    block_size: NonZeroUsize::new(ZONE_LEN).expect("positive test zone length"),
                    aggregates: ZonedAggregates::Replace(vec![aggregate].into()),
                    ..Default::default()
                },
            ));
        }
        let strategy = WriteStrategyBuilder::default()
            .with_field_writer(
                field_path!(id),
                Arc::new(RepartitionStrategy::new(
                    data_writer,
                    RepartitionWriterOptions {
                        block_size_minimum: 0,
                        block_len_multiple: ZONE_LEN,
                        block_size_target: None,
                        canonicalize: false,
                    },
                )),
            )
            .try_build()?;
        let mut bytes = Vec::new();
        write_session
            .write_options()
            .disable_editions()
            .with_file_statistics(Vec::new())
            .with_strategy(strategy)
            .write(&mut bytes, input.to_array_stream())
            .await?;
        let read_session = session(&index, false);
        read_session.allow_unknown();
        let file = read_session.open_options().open_buffer(bytes)?;
        let predicate = lt(get_item("id", root(input.dtype().clone())), lit(-1i64));
        let count = file.row_count();
        let keep = file
            .layout_reader()?
            .pruning_evaluation(
                &(0..count),
                &predicate,
                Mask::new_true(usize::try_from(count)?),
            )?
            .await?;
        assert!(keep.all_false());
        let actual = file.scan()?.into_array_stream()?.read_all().await?;
        assert_arrays_eq!(actual, input, &mut read_session.create_execution_ctx());
        Ok(())
    }
    #[tokio::test]
    async fn parent_summary_and_nested_index_compose() -> VortexResult<()> {
        let index = bloom();
        let session = session(&index, true);
        let input = StructArray::from_fields(&[("nested", data())])?.into_array();
        let strategy = WriteStrategyBuilder::default()
            .with_row_block_size(ZONE_LEN)
            .with_field_aggregates(field_path!(nested), [NullCount.bind(EmptyOptions)])
            .with_field_aggregates(field_path!(nested.id), [index.aggregate_fn()])
            .try_build()?;
        let file = write_with_strategy(&session, &input, strategy).await?;
        let mut pending = vec![Arc::clone(file.footer().layout())];
        let mut has_bloom = false;
        while let Some(layout) = pending.pop() {
            if let Some(zoned) = layout.as_opt::<Zoned>() {
                has_bloom |= zoned
                    .present_aggregates()
                    .iter()
                    .any(|name| name.contains("bloom"));
            }
            pending.extend(layout.children()?);
        }
        assert!(has_bloom);
        let actual = file.scan()?.into_array_stream()?.read_all().await?;
        assert_arrays_eq!(actual, input, &mut session.create_execution_ctx());
        Ok(())
    }
    #[tokio::test]
    async fn missing_field_index_returns_error() -> VortexResult<()> {
        let index = bloom();
        let session = session(&index, true);
        let strategy = WriteStrategyBuilder::default()
            .with_field_aggregates(field_path!(missing), [index.aggregate_fn()])
            .try_build()?;
        let result = write_with_strategy(&session, &data(), strategy).await;
        assert!(result.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn element_index_requires_list_decomposition() -> VortexResult<()> {
        let index = bloom();
        let session = session(&index, true);
        let items = ListArray::try_new(
            buffer![1i64, 2].into_array(),
            buffer![0u32, 2].into_array(),
            Validity::NonNullable,
        )?
        .into_array();
        let input = StructArray::from_fields(&[("items", items)])?.into_array();
        let path = FieldPath::from_iter([Field::from("items"), Field::ElementType]);
        let strategy = WriteStrategyBuilder::default()
            .with_field_aggregates(path, [index.aggregate_fn()])
            .try_build()?;
        let result = write_with_strategy(&session, &input, strategy).await;
        assert!(result.is_err());
        Ok(())
    }
    #[test]
    fn opaque_parent_data_writer_rejects_nested_index() {
        let index = bloom();
        let result = WriteStrategyBuilder::default()
            .with_field_data_writer(field_path!(nested), Arc::new(FlatLayoutStrategy::default()))
            .with_field_aggregates(field_path!(nested.id), [index.aggregate_fn()])
            .try_build();
        assert!(result.is_err());
    }
}
