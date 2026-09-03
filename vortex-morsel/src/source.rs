// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Performs a scan's reads from outside plan execution.
//!
//! [`MorselScan`] never touches storage: it hands the reads it wants out as [`IoDemand`] and
//! waits for [`IoCompletions`]. An [`IoAnswerer`] is whatever serves that demand;
//! [`SegmentSourceDriver`] is the standard one, over any [`SegmentSource`]. It runs as one task
//! on the caller's runtime, so the worker threads that plan and execute morsels never poll a
//! storage future.

use std::collections::VecDeque;
use std::sync::Arc;

use futures::FutureExt;
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use vortex_array::buffer::BufferHandle;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_layout::segments::SegmentFuture;
use vortex_layout::segments::SegmentId;
use vortex_layout::segments::SegmentSource;
use vortex_utils::aliases::hash_set::HashSet;

use crate::io::IoCompletions;
use crate::io::IoDemand;
use crate::io::IoDemandStream;
use crate::io::IoKey;
use crate::io::IoPriority;
use crate::io::NowaitProbe;

/// How many speculative reads are polled at once. Required and promoted reads are always polled.
const DEFAULT_BACKGROUND_WINDOW: usize = 16;

/// Answers a scan's I/O demand from outside plan execution.
///
/// The scan only ever sees the demand stream and the completions handle, so anything that can
/// consume one and drive the other can serve it: a segment source, an engine's own buffer
/// manager, a prefetcher, or a test double that scripts latency. An answerer is attached with
/// [`MorselScan::connect`](crate::MorselScan::connect).
pub trait IoAnswerer {
    /// Whether planned reads should be handed out ahead of demand.
    ///
    /// Storage that overlaps and coalesces I/O wants every planned read early. In-memory
    /// answerers keep this off so execution resolves cells inline through the probe instead.
    fn prefers_background_reads(&self) -> bool {
        false
    }

    /// A probe that resolves a read without waiting on storage, if this answerer has one.
    fn nowait_probe(&self) -> Option<NowaitProbe> {
        None
    }

    /// Serve `demand` until the stream ends, answering each read through `completions`.
    fn serve(&self, demand: IoDemandStream, completions: IoCompletions) -> BoxFuture<'static, ()>;
}

/// A read being polled: its key, whether it counts against the speculative window, its result.
type TaggedRead = BoxFuture<'static, (IoKey, bool, VortexResult<BufferHandle>)>;

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

    /// Serve `demand` until the scan drops its end of the stream.
    fn drive(
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
            // Keys currently being polled, and how many of those are speculative.
            let mut in_flight = HashSet::<IoKey>::default();
            let mut speculative_in_flight = 0usize;
            let mut background = VecDeque::<(IoKey, SegmentFuture)>::new();
            // A worker can block on a read between another worker starting it and that start
            // batch reaching this task, so a promotion may arrive before its read does.
            let mut early_promotions = HashSet::<IoKey>::default();
            loop {
                while speculative_in_flight < window
                    && let Some((key, future)) = background.pop_front()
                {
                    in_flight.insert(key);
                    speculative_in_flight += 1;
                    polled.push(tag(key, future, true));
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
                            let mut futures = if background_reads {
                                source.request_background_batch(&ids)
                            } else {
                                ids.iter().map(|&id| source.request(id)).collect()
                            };
                            for request in requests.iter().skip(futures.len()) {
                                let error = vortex_err!(
                                    "segment source returned no read for {:?}",
                                    request.key
                                );
                                if !completions.complete(request.key, Err(error)) {
                                    return;
                                }
                            }
                            futures.truncate(requests.len());
                            for (request, future) in requests.into_iter().zip(futures) {
                                let promoted = early_promotions.remove(&request.key);
                                if promoted || request.priority == IoPriority::Required {
                                    in_flight.insert(request.key);
                                    polled.push(tag(request.key, future, false));
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
                                in_flight.insert(key);
                                polled.push(tag(key, future, false));
                            } else if !in_flight.contains(&key) {
                                early_promotions.insert(key);
                            }
                        }
                        None => return,
                    },
                    completed = polled.select_next_some() => {
                        let (key, speculative, result) = completed;
                        in_flight.remove(&key);
                        if speculative {
                            speculative_in_flight -= 1;
                        }
                        if !completions.complete(key, result) {
                            return;
                        }
                    }
                }
            }
        }
    }
}

impl IoAnswerer for SegmentSourceDriver {
    fn prefers_background_reads(&self) -> bool {
        self.source.prefers_background_reads()
    }

    fn nowait_probe(&self) -> Option<NowaitProbe> {
        let source = Arc::clone(&self.source);
        Some(Arc::new(move |key| match key {
            IoKey::Segment(id) => source.request_nowait(id),
        }))
    }

    fn serve(&self, demand: IoDemandStream, completions: IoCompletions) -> BoxFuture<'static, ()> {
        self.drive(demand, completions).boxed()
    }
}

/// Attach the key to a segment future and settle device buffers on the host.
fn tag(key: IoKey, future: SegmentFuture, speculative: bool) -> TaggedRead {
    async move {
        let result = match future.await {
            Ok(handle) if handle.is_on_device() => match handle.try_into_host() {
                Ok(copy) => copy.await.map(BufferHandle::new_host),
                Err(err) => Err(err),
            },
            result => result,
        };
        (key, speculative, result)
    }
    .boxed()
}
