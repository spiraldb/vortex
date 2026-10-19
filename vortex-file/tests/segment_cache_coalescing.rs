// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Exercise cache lookup ordering with the file source's real physical-read coalescer.

#![expect(clippy::tests_outside_test_module)]

use std::sync::Arc;

use futures::FutureExt;
use futures::future::BoxFuture;
use parking_lot::Mutex;
use rstest::rstest;
use vortex_array::buffer::BufferHandle;
use vortex_buffer::Alignment;
use vortex_buffer::ByteBuffer;
use vortex_error::VortexResult;
use vortex_file::SegmentSpec;
use vortex_file::segments::FileSegmentSource;
use vortex_file::segments::RequestMetrics;
use vortex_io::CoalesceConfig;
use vortex_io::ReadAtRequest;
use vortex_io::VortexReadAt;
use vortex_io::runtime::tokio::TokioRuntime;
use vortex_layout::segments::MokaSegmentCache;
use vortex_layout::segments::SegmentCache;
use vortex_layout::segments::SegmentCacheSourceAdapter;
use vortex_layout::segments::SegmentEviction;
use vortex_layout::segments::SegmentId;
use vortex_layout::segments::SegmentSource;
use vortex_metrics::DefaultMetricsRegistry;

const SEGMENT_LENGTH: u32 = 4096;
const SEGMENT_BYTES: usize = SEGMENT_LENGTH as usize;
const SEGMENTS: u32 = 16;

#[derive(Clone)]
struct CountingReadAt {
    ranges: Arc<Mutex<Vec<ReadAtRequest>>>,
}

impl VortexReadAt for CountingReadAt {
    fn coalesce_config(&self) -> Option<CoalesceConfig> {
        Some(CoalesceConfig::object_storage())
    }

    fn concurrency(&self) -> usize {
        4
    }

    fn size(&self) -> BoxFuture<'static, VortexResult<u64>> {
        async { Ok(u64::from(SEGMENTS) * u64::from(SEGMENT_LENGTH)) }.boxed()
    }

    fn read_at(
        &self,
        offset: u64,
        length: usize,
        alignment: Alignment,
    ) -> BoxFuture<'static, VortexResult<BufferHandle>> {
        self.ranges
            .lock()
            .push(ReadAtRequest::new(offset, length, alignment));
        async move {
            Ok(BufferHandle::new_host(
                ByteBuffer::from(vec![0; length]).aligned(alignment),
            ))
        }
        .boxed()
    }
}

#[derive(Clone, Copy, Debug)]
enum Demand {
    Concurrent,
    Ordered,
}

#[rstest]
#[case::cold_concurrent(0, Demand::Concurrent)]
#[case::cold_ordered(0, Demand::Ordered)]
#[case::partial_concurrent(8, Demand::Concurrent)]
#[case::partial_ordered(8, Demand::Ordered)]
#[case::cached_concurrent(16, Demand::Concurrent)]
#[case::cached_ordered(16, Demand::Ordered)]
#[tokio::test]
async fn cache_misses_coalesce_without_reading_cached_suffix(
    #[case] cached_suffix: u32,
    #[case] demand: Demand,
) -> VortexResult<()> {
    let ranges = Arc::new(Mutex::new(Vec::new()));
    let registry = DefaultMetricsRegistry::default();
    let segments: Arc<[SegmentSpec]> = (0..SEGMENTS)
        .map(|id| SegmentSpec {
            offset: u64::from(id) * u64::from(SEGMENT_LENGTH),
            length: SEGMENT_LENGTH,
            alignment: Alignment::none(),
        })
        .collect();
    let source: Arc<dyn SegmentSource> = Arc::new(FileSegmentSource::open(
        segments,
        CountingReadAt {
            ranges: Arc::clone(&ranges),
        },
        TokioRuntime::current(),
        RequestMetrics::new(&registry, vec![]),
    ));
    let cache = MokaSegmentCache::new(1 << 20, SegmentEviction::Lru);
    let file_cache: Arc<dyn SegmentCache> = Arc::new(cache.for_file("file"));
    for id in SEGMENTS - cached_suffix..SEGMENTS {
        file_cache
            .put(
                SegmentId::from(id),
                ByteBuffer::from(vec![0; SEGMENT_BYTES]),
            )
            .await?;
    }
    cache.run_pending_tasks().await;
    let source = SegmentCacheSourceAdapter::new(file_cache, source);
    let requests = (0..SEGMENTS)
        .map(|id| source.request(SegmentId::from(id)))
        .collect::<Vec<_>>();
    match demand {
        Demand::Concurrent => {
            for result in futures::future::join_all(requests).await {
                assert_eq!(result?.len(), SEGMENT_BYTES);
            }
        }
        Demand::Ordered => {
            for request in requests {
                assert_eq!(request.await?.len(), SEGMENT_BYTES);
            }
        }
    }
    let ranges = ranges.lock();
    let uncached_segments = SEGMENTS - cached_suffix;
    if uncached_segments == 0 {
        assert!(ranges.is_empty());
    } else {
        // Unit tests count logical registrations; only the file coalescer can establish
        // that ordered demand combines misses without also fetching cached neighbors.
        assert_eq!(
            ranges.as_slice(),
            &[ReadAtRequest::new(
                0,
                uncached_segments as usize * SEGMENT_BYTES,
                Alignment::none(),
            )]
        );
    }
    Ok(())
}
