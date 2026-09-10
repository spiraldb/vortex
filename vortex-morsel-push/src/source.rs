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
use vortex_layout::segments::ReadAtNowait;
use vortex_layout::segments::SegmentFuture;
use vortex_layout::segments::SegmentId;
use vortex_layout::segments::SegmentSource;
use vortex_utils::aliases::hash_set::HashSet;

use crate::MorselScan;
use crate::io::IoCompletions;
use crate::io::IoDemand;
use crate::io::IoDemandStream;
use crate::io::IoKey;
use crate::io::IoPriority;
use crate::io::NowaitProbe;

/// How many speculative reads are polled at once. Required and promoted reads are always polled.
const DEFAULT_BACKGROUND_WINDOW: usize = 16;

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
    submission_nowait: bool,
}

impl SegmentSourceDriver {
    /// Serve reads from `source`.
    pub fn new(source: Arc<dyn SegmentSource>) -> Self {
        Self {
            source,
            background_window: DEFAULT_BACKGROUND_WINDOW,
            submission_nowait: false,
        }
    }

    /// Set how many speculative reads are polled concurrently.
    pub fn with_background_window(mut self, window: usize) -> Self {
        self.background_window = window.max(1);
        self
    }

    /// Probe reads inline as their start batch reaches the driver.
    ///
    /// A ready read bypasses construction and polling of its asynchronous source future. Misses
    /// remain in the original batch and follow the normal background/coalescing path.
    pub fn with_submission_nowait(mut self, enabled: bool) -> Self {
        self.submission_nowait = enabled;
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
        let submission_nowait = self.submission_nowait;
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
                        Some(IoDemand::Start(mut requests)) => {
                            if submission_nowait {
                                let mut pending = Vec::with_capacity(requests.len());
                                for request in requests {
                                    let IoKey::Segment(id) = request.key;
                                    match source.request_nowait(id) {
                                        Ok(ReadAtNowait::Ready(handle)) if !handle.is_on_device() => {
                                            early_promotions.remove(&request.key);
                                            if !completions.complete(request.key, Ok(handle)) {
                                                return;
                                            }
                                        }
                                        Ok(ReadAtNowait::Ready(handle)) => {
                                            early_promotions.remove(&request.key);
                                            in_flight.insert(request.key);
                                            polled.push(tag(
                                                request.key,
                                                futures::future::ready(Ok(handle)).boxed(),
                                                false,
                                            ));
                                        }
                                        Ok(ReadAtNowait::WouldBlock | ReadAtNowait::Unsupported) => {
                                            pending.push(request);
                                        }
                                        Err(error) => {
                                            early_promotions.remove(&request.key);
                                            if !completions.complete(request.key, Err(error)) {
                                                return;
                                            }
                                        }
                                    }
                                }
                                requests = pending;
                            }
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
