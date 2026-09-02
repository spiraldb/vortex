// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Performs a scan's reads from outside plan execution.
//!
//! [`MorselScan`] never touches storage: it hands the reads it wants out as [`IoDemand`] and
//! waits for [`IoCompletions`]. [`SegmentSourceDriver`] is the standard way to answer that
//! demand, using any [`SegmentSource`]. It runs as one task on the caller's runtime, so the
//! worker threads that plan and execute morsels never poll a storage future.

use std::collections::VecDeque;
use std::sync::Arc;

use futures::FutureExt;
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use vortex_array::buffer::BufferHandle;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_io::runtime::Handle;
use vortex_layout::segments::SegmentFuture;
use vortex_layout::segments::SegmentId;
use vortex_layout::segments::SegmentSource;

use crate::MorselScan;
use crate::io::IoCompletions;
use crate::io::IoDemand;
use crate::io::IoDemandStream;
use crate::io::IoKey;
use crate::io::IoPriority;
use crate::io::NowaitProbe;

/// How many speculative reads are polled at once. Required and promoted reads are always polled.
const DEFAULT_BACKGROUND_WINDOW: usize = 16;

type TaggedRead = BoxFuture<'static, (IoKey, VortexResult<BufferHandle>)>;

/// Serves a scan's I/O demand from a [`SegmentSource`].
///
/// Reads arrive in the batches the scheduler planned them in and are registered with the source
/// in the same batches, so a coalescing source sees neighbours together. Required reads are
/// polled immediately; speculative reads are polled through a bounded window in demand order and
/// jump ahead when the scan promotes them. Polling is what a demand-driven source uses to order
/// its physical reads, so the window keeps that ordering meaningful.
#[derive(Clone)]
pub struct SegmentSourceDriver {
    source: Arc<dyn SegmentSource>,
    background_window: usize,
}

impl SegmentSourceDriver {
    /// Serve reads from `source`.
    pub fn new(source: Arc<dyn SegmentSource>) -> Self {
        Self {
            source,
            background_window: DEFAULT_BACKGROUND_WINDOW,
        }
    }

    /// Set how many speculative reads are polled concurrently.
    pub fn with_background_window(mut self, window: usize) -> Self {
        self.background_window = window.max(1);
        self
    }

    /// Whether the source wants planned reads started ahead of demand.
    pub fn prefers_background_reads(&self) -> bool {
        self.source.prefers_background_reads()
    }

    /// A probe over the source's non-blocking read path, for inline resolution during execution.
    pub fn nowait_probe(&self) -> NowaitProbe {
        let source = Arc::clone(&self.source);
        Arc::new(move |key| match key {
            IoKey::Segment(id) => source.request_nowait(id),
        })
    }

    /// Configure `scan` for this source and start serving its reads on `handle`.
    ///
    /// The driver task ends when the scan is dropped. Reads still in flight at that point are
    /// dropped with it, which cancels them at sources that support cancellation.
    pub fn connect(&self, scan: MorselScan, handle: &Handle) -> VortexResult<MorselScan> {
        let (demand, completions) = scan.take_io()?;
        handle.spawn(self.drive(demand, completions)).detach();
        Ok(scan
            .with_background_reads(self.prefers_background_reads())
            .with_nowait_probe(self.nowait_probe()))
    }

    /// Configure `scan` for this source and serve its reads from a dedicated thread.
    ///
    /// For callers without an async runtime, such as benchmarks and tests that run scans
    /// synchronously. The thread exits when the scan is dropped.
    pub fn connect_on_thread(&self, scan: MorselScan) -> VortexResult<MorselScan> {
        let (demand, completions) = scan.take_io()?;
        let drive = self.drive(demand, completions);
        std::thread::Builder::new()
            .name("vortex-morsel-io".into())
            .spawn(move || futures::executor::block_on(drive))
            .map_err(|err| vortex_err!("failed to spawn the segment source driver: {err}"))?;
        Ok(scan
            .with_background_reads(self.prefers_background_reads())
            .with_nowait_probe(self.nowait_probe()))
    }

    /// Serve `demand` until the scan drops its end of the stream.
    pub fn drive(
        &self,
        demand: IoDemandStream,
        completions: IoCompletions,
    ) -> impl Future<Output = ()> + Send + 'static {
        let source = Arc::clone(&self.source);
        let window = self.background_window;
        let background_reads = source.prefers_background_reads();
        async move {
            let mut demand = demand.fuse();
            let mut polled = FuturesUnordered::<TaggedRead>::new();
            let mut background = VecDeque::<(IoKey, SegmentFuture)>::new();
            loop {
                while polled.len() < window
                    && let Some((key, future)) = background.pop_front()
                {
                    polled.push(tag(key, future));
                }
                futures::select_biased! {
                    next = demand.next() => match next {
                        Some(IoDemand::Start(requests)) => {
                            let ids = requests
                                .iter()
                                .map(|request| match request.key {
                                    IoKey::Segment(id) => id,
                                })
                                .collect::<Vec<SegmentId>>();
                            let futures = if background_reads {
                                source.request_background_batch(&ids)
                            } else {
                                ids.iter().map(|&id| source.request(id)).collect()
                            };
                            for (request, future) in requests.into_iter().zip(futures) {
                                if request.priority == IoPriority::Required {
                                    polled.push(tag(request.key, future));
                                } else {
                                    background.push_back((request.key, future));
                                }
                            }
                        }
                        Some(IoDemand::Promote(key)) => {
                            if let Some(position) =
                                background.iter().position(|(queued, _)| *queued == key)
                                && let Some((key, future)) = background.remove(position)
                            {
                                polled.push(tag(key, future));
                            }
                        }
                        None => return,
                    },
                    completed = polled.select_next_some() => {
                        let (key, result) = completed;
                        if !completions.complete(key, result) {
                            return;
                        }
                    }
                }
            }
        }
    }
}

/// Attach the key to a segment future and settle device buffers on the host.
fn tag(key: IoKey, future: SegmentFuture) -> TaggedRead {
    async move {
        let result = match future.await {
            Ok(handle) if handle.is_on_device() => match handle.try_into_host() {
                Ok(copy) => copy.await.map(BufferHandle::new_host),
                Err(err) => Err(err),
            },
            result => result,
        };
        (key, result)
    }
    .boxed()
}
