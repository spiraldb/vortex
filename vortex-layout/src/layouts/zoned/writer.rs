// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Write-time assembly for zoned layouts.

use std::num::NonZeroUsize;
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt as _;
use parking_lot::Mutex;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::aggregate_fn::AggregateFnRef;
use vortex_array::dtype::DType;
use vortex_error::VortexError;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_io::session::RuntimeSessionExt;
use vortex_session::VortexSession;
use vortex_utils::parallelism::get_available_parallelism;

use crate::LayoutRef;
use crate::LayoutStrategy;
use crate::LayoutWriterContext;
use crate::layouts::zoned::AggregateStatsAccumulator;
use crate::layouts::zoned::ZonedLayout;
use crate::layouts::zoned::aggregate_partials;
use crate::layouts::zoned::aggregates::default_zoned_aggregate_fns;
use crate::segments::SegmentSinkRef;
use crate::sequence::SendableSequentialStream;
use crate::sequence::SequencePointer;
use crate::sequence::SequentialArrayStreamExt;
use crate::sequence::SequentialStreamAdapter;
use crate::sequence::SequentialStreamExt;

/// Selects aggregate instances after the input dtype and session are available.
///
/// Explicit requests must support the input dtype. Exact duplicate requests are stored once;
/// different options describe different aggregates. Unsupported defaults are omitted.
#[derive(Clone, Default)]
pub enum ZonedAggregates {
    /// Use the supported default pruning aggregates.
    #[default]
    Defaults,
    /// Replace defaults with exactly these aggregates.
    Replace(Arc<[AggregateFnRef]>),
    /// Keep defaults and add these aggregates.
    Extend(Arc<[AggregateFnRef]>),
}

impl ZonedAggregates {
    fn resolve(&self, dtype: &DType, session: &VortexSession) -> VortexResult<Vec<AggregateFnRef>> {
        let defaults = || {
            default_zoned_aggregate_fns(dtype, session)
                .iter()
                .filter(|aggregate| {
                    aggregate.state_dtype(dtype).is_some()
                        && aggregate.return_dtype(dtype).is_some()
                })
                .cloned()
                .collect::<Vec<_>>()
        };

        let (mut selected, requested) = match self {
            Self::Defaults => (defaults(), Vec::new()),
            Self::Replace(aggregates) => (Vec::new(), aggregates.to_vec()),
            Self::Extend(aggregates) => (defaults(), aggregates.to_vec()),
        };

        for aggregate in requested {
            if aggregate.state_dtype(dtype).is_none() || aggregate.return_dtype(dtype).is_none() {
                vortex_bail!("Aggregate {aggregate} requires a supported input dtype, got {dtype}");
            }

            if !selected.contains(&aggregate) {
                selected.push(aggregate);
            }
        }

        Ok(selected)
    }
}

/// Configuration for building zoned layouts.
///
/// The input stream is assumed to already be partitioned into one chunk per zone, except
/// possibly the final partial zone.
#[derive(Clone)]
pub struct ZonedLayoutOptions {
    /// The size of a statistics block
    pub block_size: NonZeroUsize,
    /// Selects defaults, replacements, or additions after the input dtype is known.
    pub aggregates: ZonedAggregates,
    /// Number of chunks to compute aggregate partials in parallel.
    pub concurrency: NonZeroUsize,
}

impl Default for ZonedLayoutOptions {
    fn default() -> Self {
        Self {
            block_size: unsafe { NonZeroUsize::new_unchecked(8192) },
            aggregates: ZonedAggregates::Defaults,
            concurrency: unsafe {
                NonZeroUsize::new_unchecked(get_available_parallelism().unwrap_or(1))
            },
        }
    }
}

pub struct ZonedStrategy {
    child: Arc<dyn LayoutStrategy>,
    stats: Arc<dyn LayoutStrategy>,
    options: ZonedLayoutOptions,
}

impl ZonedStrategy {
    /// Create a writer that emits a data child plus an auxiliary per-zone stats child.
    pub fn new<Child: LayoutStrategy, Stats: LayoutStrategy>(
        child: Child,
        stats: Stats,
        options: ZonedLayoutOptions,
    ) -> Self {
        Self {
            child: Arc::new(child),
            stats: Arc::new(stats),
            options,
        }
    }
}

#[async_trait]
impl LayoutStrategy for ZonedStrategy {
    async fn write_stream(
        &self,
        ctx: LayoutWriterContext,
        segment_sink: SegmentSinkRef,
        stream: SendableSequentialStream,
        mut eof: SequencePointer,
        session: &VortexSession,
    ) -> VortexResult<LayoutRef> {
        let aggregate_fns = self.options.aggregates.resolve(stream.dtype(), session)?;

        let compute_session = session.clone();

        let stats_accumulator = Arc::new(Mutex::new(AggregateStatsAccumulator::new(
            stream.dtype(),
            &aggregate_fns,
        )));
        // Edition restrictions apply to the resolved selection, including supported defaults.
        let aggregate_fns = stats_accumulator.lock().aggregate_fns();
        for aggregate_fn in aggregate_fns.iter() {
            if !ctx.allows_aggregate(&aggregate_fn.id()) {
                vortex_bail!("Aggregate {} not permitted by ctx", aggregate_fn.id());
            }
        }

        let stream_dtype = stream.dtype().clone();
        let concurrency = self.options.concurrency.get();
        let stream = stream
            .map(move |item| {
                let aggregate_fns = Arc::clone(&aggregate_fns);
                let session = compute_session.clone();
                session.handle().spawn_cpu(move || {
                    let (sequence_id, chunk) = item?;
                    let partials = aggregate_partials(
                        &chunk,
                        &aggregate_fns,
                        &mut session.create_execution_ctx(),
                    )?;
                    Ok::<_, VortexError>((sequence_id, chunk, partials))
                })
            })
            .buffered(concurrency);

        // Accumulate zone stats in stream order so the auxiliary table stays aligned with the
        // data child.
        let stats_accumulator2 = Arc::clone(&stats_accumulator);
        let stream = SequentialStreamAdapter::new(
            stream_dtype,
            stream.map(move |item| {
                let (sequence_id, chunk, partials) = item?;
                stats_accumulator2.lock().push_partials(partials)?;
                Ok((sequence_id, chunk))
            }),
        )
        .sendable();

        let block_size = self.options.block_size;

        // The eof used for the data child should appear _before_ our own stats tables.
        let data_eof = eof.split_off();
        let data_layout = self
            .child
            .write_stream(
                ctx.clone(),
                Arc::clone(&segment_sink),
                stream,
                data_eof,
                session,
            )
            .await?;

        let mut exec_ctx = session.create_execution_ctx();
        let Some((stats_array, aggregate_fns)) =
            stats_accumulator.lock().as_array(&mut exec_ctx)?
        else {
            // If we have no stats (e.g. the DType doesn't support them), then we just return the
            // child layout.
            return Ok(data_layout);
        };

        // We must defer creating the stats table LayoutWriter until now, because the DType of
        // the table depends on which stats were successfully computed.
        let stats_stream = stats_array
            .into_array()
            .to_array_stream()
            .sequenced(eof.split_off());
        let zones_layout = self
            .stats
            .write_stream(ctx, Arc::clone(&segment_sink), stats_stream, eof, session)
            .await?;

        Ok(
            ZonedLayout::try_new(data_layout, zones_layout, block_size, aggregate_fns)?
                .into_layout(),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use rstest::rstest;
    use vortex_array::ArrayContext;
    use vortex_array::IntoArray;
    use vortex_array::aggregate_fn::AggregateFnVTable;
    use vortex_array::aggregate_fn::fns::bounded_max::BoundedMax;
    use vortex_array::aggregate_fn::fns::bounded_min::BoundedMin;
    use vortex_array::aggregate_fn::fns::max::Max;
    use vortex_array::aggregate_fn::fns::min::Min;
    use vortex_array::aggregate_fn::fns::nan_count::NanCount;
    use vortex_array::aggregate_fn::fns::null_count::NullCount;
    use vortex_array::aggregate_fn::fns::sum::Sum;
    use vortex_array::arrays::ChunkedArray;
    use vortex_array::dtype::DType;
    use vortex_array::dtype::DecimalDType;
    use vortex_array::dtype::Nullability;
    use vortex_array::dtype::PType;
    use vortex_array::extension::datetime::TimeUnit;
    use vortex_array::extension::datetime::Timestamp;
    use vortex_buffer::buffer;
    use vortex_error::VortexExpect;
    use vortex_io::runtime::Handle;
    use vortex_io::runtime::single::block_on;
    use vortex_io::session::RuntimeSession;
    use vortex_io::session::RuntimeSessionExt;
    use vortex_utils::aliases::hash_set::HashSet;

    use super::*;
    use crate::layouts::chunked::writer::ChunkedLayoutStrategy;
    use crate::layouts::flat::writer::FlatLayoutStrategy;
    use crate::layouts::zoned::Zoned;
    use crate::layouts::zoned::aggregates::bloom_filter::BloomFilter;
    use crate::layouts::zoned::aggregates::bloom_filter::BloomOptions;
    use crate::layouts::zoned::aggregates::bloom_filter::HashFn;
    use crate::layouts::zoned::schema::default_bounded_stat_max_bytes;
    use crate::layouts::zoned::skip_index::SkipIndex;
    use crate::layouts::zoned::skip_index::bloom::BloomSkipIndex;
    use crate::segments::TestSegments;
    use crate::sequence::SequenceId;
    use crate::sequence::SequentialArrayStreamExt;
    use crate::session::LayoutSession;

    fn write_zones_with_options_and_values(
        ctx: LayoutWriterContext,
        options: ZonedLayoutOptions,
        chunks: ChunkedArray,
    ) -> VortexResult<Vec<String>> {
        let strategy = ZonedStrategy::new(
            ChunkedLayoutStrategy::new(FlatLayoutStrategy::default()),
            FlatLayoutStrategy::default(),
            options,
        );
        let (ptr, eof) = SequenceId::root().split();
        let stream = chunks.into_array().to_array_stream().sequenced(ptr);

        let layout = block_on(|handle: Handle| async move {
            let session = vortex_array::array_session()
                .with::<LayoutSession>()
                .with::<RuntimeSession>()
                .with_handle(handle);
            strategy
                .write_stream(
                    ctx,
                    Arc::new(TestSegments::default()),
                    stream,
                    eof,
                    &session,
                )
                .await
        })?;

        Ok(layout
            .as_::<Zoned>()
            .aggregate_fns()
            .iter()
            .map(|aggregate_fn| aggregate_fn.id().to_string())
            .collect())
    }

    /// Write three zones of primitives using custom zone options through `ctx`,
    /// returning the aggregates the zoned layout recorded.
    fn write_zones_with_options(
        ctx: LayoutWriterContext,
        options: ZonedLayoutOptions,
    ) -> VortexResult<Vec<String>> {
        write_zones_with_options_and_values(ctx, options, zone_chunk_values())
    }

    /// Write three zones of primitives through `ctx`, returning the aggregates the zoned
    /// layout recorded.
    fn write_zones(ctx: LayoutWriterContext) -> VortexResult<Vec<String>> {
        write_zones_with_options_and_values(
            ctx,
            ZonedLayoutOptions {
                block_size: NonZeroUsize::new(3).vortex_expect("non zero"),
                ..Default::default()
            },
            zone_chunk_values(),
        )
    }

    #[inline]
    fn zone_chunk_values() -> ChunkedArray {
        ChunkedArray::from_iter([
            buffer![1, 2, 3].into_array(),
            buffer![4, 5, 6].into_array(),
            buffer![7, 8, 9].into_array(),
        ])
    }

    #[test]
    fn unrestricted_context_writes_the_default_aggregates() -> VortexResult<()> {
        let written = write_zones(LayoutWriterContext::new(ArrayContext::empty()))?;
        assert!(written.contains(&Min.id().to_string()));
        assert!(written.contains(&Max.id().to_string()));
        assert!(
            !written.contains(&Sum.id().to_string()),
            "wrote {written:?}"
        );
        Ok(())
    }

    #[test]
    fn a_permitted_set_covering_the_defaults_writes_them() -> VortexResult<()> {
        let ctx = LayoutWriterContext::new(ArrayContext::empty()).with_allowed_aggregates(
            HashSet::from_iter([Min.id(), Max.id(), NanCount.id(), NullCount.id()]),
        );
        assert!(write_zones(ctx)?.contains(&Max.id().to_string()));
        Ok(())
    }

    #[test]
    fn a_forbidden_aggregate_fails_the_write() {
        let ctx = LayoutWriterContext::new(ArrayContext::empty())
            .with_allowed_aggregates(HashSet::from_iter([Min.id()]));
        let error = write_zones(ctx).expect_err("the default aggregates are not all permitted");
        assert!(
            error.to_string().contains("not permitted by ctx"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn default_aggregates_bound_variable_length_min_max() {
        let aggregate_fns = default_zoned_aggregate_fns(
            &DType::Utf8(Nullability::NonNullable),
            &vortex_array::array_session(),
        );

        assert_eq!(
            aggregate_fns[0].as_::<BoundedMax>().max_bytes,
            default_bounded_stat_max_bytes()
        );
        assert_eq!(
            aggregate_fns[1].as_::<BoundedMin>().max_bytes,
            default_bounded_stat_max_bytes()
        );
    }

    #[test]
    fn default_aggregates_keep_fixed_width_min_max_exact() {
        let aggregate_fns =
            default_zoned_aggregate_fns(&PType::I32.into(), &vortex_array::array_session());

        assert!(aggregate_fns[0].is::<Max>());
        assert!(aggregate_fns[1].is::<Min>());
        assert!(aggregate_fns[2].is::<NanCount>());
    }

    /// Zone maps never carry a sum, whether or not the dtype could hold one.
    #[rstest]
    #[case::summable(PType::I32.into())]
    #[case::not_summable(DType::Extension(
        Timestamp::new(TimeUnit::Microseconds, Nullability::Nullable).erased(),
    ))]
    fn default_aggregates_never_record_sum(#[case] dtype: DType) {
        let aggregate_fns = default_zoned_aggregate_fns(&dtype, &vortex_array::array_session());

        assert!(
            aggregate_fns
                .iter()
                .all(|aggregate_fn| !aggregate_fn.is::<Sum>())
        );
    }

    #[test]
    fn writer_appends_skip_index_aggregate() -> VortexResult<()> {
        let mut options = ZonedLayoutOptions::default();
        let bloom_index = BloomSkipIndex::default();
        options.aggregates = ZonedAggregates::Replace(vec![bloom_index.aggregate_fn()].into());

        let written =
            write_zones_with_options(LayoutWriterContext::new(ArrayContext::empty()), options)?;

        // The aggregate list replaces the defaults with the Bloom filter.
        assert!(
            written == [BloomFilter {}.id().to_string()],
            "expected only the Bloom aggregate, wrote {written:?}"
        );
        Ok(())
    }
    #[test]
    fn additions_preserve_defaults_and_deduplicate_exact_instances() -> VortexResult<()> {
        let bloom = BloomSkipIndex::default().aggregate_fn();
        let selection = ZonedAggregates::Extend(vec![bloom.clone(), bloom.clone()].into());
        let selected = selection.resolve(&PType::I32.into(), &vortex_array::array_session())?;
        assert!(selected.iter().any(|aggregate| aggregate.is::<Min>()));
        assert!(selected.iter().any(|aggregate| aggregate.is::<Max>()));
        assert_eq!(
            selected
                .iter()
                .filter(|aggregate| *aggregate == &bloom)
                .count(),
            1
        );

        let written = write_zones_with_options(
            LayoutWriterContext::new(ArrayContext::empty()),
            ZonedLayoutOptions {
                aggregates: selection,
                ..Default::default()
            },
        )?;
        assert!(written.contains(&Min.id().to_string()));
        assert!(written.contains(&BloomFilter.id().to_string()));
        Ok(())
    }

    #[test]
    fn different_bloom_options_are_distinct_requests() -> VortexResult<()> {
        let small = BloomSkipIndex::new(BloomOptions::new(
            NonZeroU32::new(8).vortex_expect("nonzero test constant"),
            HashFn::XxHash3_64,
        ))
        .aggregate_fn();
        let large = BloomSkipIndex::default().aggregate_fn();
        let selected = ZonedAggregates::Replace(vec![small, large].into())
            .resolve(&PType::I32.into(), &vortex_array::array_session())?;
        assert_eq!(selected.len(), 2);
        assert_ne!(selected[0].to_string(), selected[1].to_string());
        Ok(())
    }

    #[test]
    fn unsupported_explicit_requests_fail() -> VortexResult<()> {
        let dtype = DType::Decimal(DecimalDType::new(5, 2), Nullability::NonNullable);
        let selection =
            ZonedAggregates::Extend(vec![BloomSkipIndex::default().aggregate_fn()].into());
        let session = vortex_array::array_session();
        assert!(selection.resolve(&dtype, &session).is_err());
        let defaults = ZonedAggregates::Defaults.resolve(&dtype, &session)?;
        assert!(defaults.iter().any(|aggregate| aggregate.is::<Min>()));
        Ok(())
    }

    #[test]
    fn empty_replacement_disables_zoning_selection() -> VortexResult<()> {
        let selected = ZonedAggregates::Replace(Arc::new([]))
            .resolve(&PType::I32.into(), &vortex_array::array_session())?;
        assert!(selected.is_empty());
        Ok(())
    }
}
