// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use async_trait::async_trait;
use futures::FutureExt;
use parking_lot::Mutex;
use vortex_array::buffer::BufferHandle;
use vortex_buffer::ByteBuffer;
use vortex_buffer::ByteBufferMut;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_ensure;
use vortex_error::vortex_err;
use vortex_io::segments::FetchTracker;
use vortex_io::segments::IoIntent;
use vortex_io::segments::IoOwnerId;
use vortex_io::segments::RegisteredSource;
use vortex_io::segments::SegmentHandle;
use vortex_io::segments::SegmentIoService;
use vortex_io::segments::SegmentIoSession;
use vortex_io::segments::SegmentRequest;
use vortex_io::segments::SourceId;

use crate::segments::SegmentFuture;
use crate::segments::SegmentId;
use crate::segments::SegmentSink;
use crate::segments::SegmentSource;
use crate::sequence::SequenceId;

/// A dummy in-memory implementation of a segment reader and writer.
#[derive(Default, Clone)]
pub struct TestSegments {
    segments: Arc<Mutex<Vec<ByteBuffer>>>,
}

impl SegmentSource for TestSegments {
    fn request(&self, id: SegmentId) -> SegmentFuture {
        let buffer = self.segments.lock().get(*id as usize).cloned();
        async move {
            buffer
                .map(BufferHandle::new_host)
                .ok_or_else(|| vortex_err!("Segment not found"))
        }
        .boxed()
    }
}

#[async_trait]
impl SegmentSink for TestSegments {
    async fn write(
        &self,
        _sequence_id: SequenceId,
        buffers: Vec<ByteBuffer>,
    ) -> VortexResult<SegmentId> {
        // Combine all the buffers since we're only a test implementation
        let mut buffer = ByteBufferMut::empty();
        for segment in buffers {
            buffer.extend_from_slice(segment.as_ref());
        }

        let mut segments = self.segments.lock();
        let segment_id =
            SegmentId::from(u32::try_from(segments.len()).vortex_expect("Too many segments"));
        segments.push(buffer.freeze());

        Ok(segment_id)
    }
}

/// Serves one segment source through the segment IO protocol, as source `0`.
///
/// A test stand-in for a file's IO service: announcements build the source's request without
/// polling it, so a source that coalesces on registration sees them, and prefetches and fetches
/// poll it once so it starts.
pub struct TestSegmentIo {
    source: Arc<dyn SegmentSource>,
}

impl TestSegmentIo {
    /// The registration of `source` with a service of its own.
    pub fn register(source: Arc<dyn SegmentSource>) -> RegisteredSource {
        RegisteredSource::new(Arc::new(Self { source }), SourceId(0), Arc::new(()))
    }
}

impl SegmentIoService for TestSegmentIo {
    fn session(&self) -> Arc<dyn SegmentIoSession> {
        Arc::new(TestSegmentSession {
            source: Arc::clone(&self.source),
            fetches: Mutex::default(),
        })
    }
}

struct TestSegmentSession {
    source: Arc<dyn SegmentSource>,
    fetches: Mutex<FetchTracker>,
}

impl SegmentIoSession for TestSegmentSession {
    fn submit(
        &self,
        owner: IoOwnerId,
        batch: &[SegmentRequest],
    ) -> VortexResult<Vec<SegmentHandle>> {
        let mut fetches = self.fetches.lock();
        batch
            .iter()
            .map(|request| {
                vortex_ensure!(
                    request.key.source == SourceId(0),
                    "unknown source {:?}",
                    request.key.source
                );
                let bytes = self
                    .source
                    .request(request.key.segment)
                    .map(|result| result.map_err(Arc::new))
                    .boxed()
                    .shared();
                if request.intent != IoIntent::Announce {
                    // Polling registers the read as wanted, so the source starts it now.
                    drop(bytes.clone().now_or_never());
                }
                let handle = SegmentHandle::new(bytes, Arc::new(()));
                if request.intent == IoIntent::Fetch {
                    fetches.track(owner, &handle);
                }
                Ok(handle)
            })
            .collect()
    }

    fn poll_ready(
        &self,
        cx: &mut Context<'_>,
        ready: &mut Vec<IoOwnerId>,
    ) -> Poll<VortexResult<()>> {
        self.fetches.lock().poll_ready(cx, ready)
    }
}
