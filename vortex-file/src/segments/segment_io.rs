// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! The session's segment IO service for opened files.
//!
//! Every file opened in a session registers with the session's [`FileSegmentIo`], which serves
//! the segment reads of planner-driven scans. A request is answered, in order of preference:
//!
//! 1. from the file's segment cache, which also holds the segments the footer read covered;
//! 2. by slicing the file, when it was opened from a buffer;
//! 3. by the file's coalescing read driver, the same one its segment source reads through.
//!
//! Reads through the driver are shared: every handle of a segment's byte range, from any session,
//! shares one read while any of them is alive, and the last one dropped cancels it if it has not
//! finished. A batch registers all of its reads with the driver before marking any of them wanted,
//! so the driver can coalesce them.

use std::fmt;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;

use futures::FutureExt;
use futures::channel::mpsc;
use futures::channel::oneshot;
use parking_lot::Mutex;
use parking_lot::RwLock;
use vortex_array::buffer::BufferHandle;
use vortex_buffer::Alignment;
use vortex_buffer::ByteBuffer;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_err;
use vortex_io::segments::FetchTracker;
use vortex_io::segments::IoIntent;
use vortex_io::segments::IoOwnerId;
use vortex_io::segments::RegisteredSource;
use vortex_io::segments::SegmentHandle;
use vortex_io::segments::SegmentIoService;
use vortex_io::segments::SegmentIoSession;
use vortex_io::segments::SegmentRequest;
use vortex_io::segments::SharedBytes;
use vortex_io::segments::SourceId;
use vortex_layout::segments::SegmentCache;
use vortex_layout::segments::SegmentId;
use vortex_session::SessionExt;
use vortex_session::SessionVar;
use vortex_session::VortexSession;
use vortex_utils::aliases::hash_map::HashMap;

use crate::SegmentSpec;
use crate::read::ReadRequest;
use crate::read::RequestId;
use crate::segments::ReadEvent;
use crate::segments::source::SharedDriver;

/// Serves the segment reads of every file registered in one session.
#[derive(Clone, Default)]
pub struct FileSegmentIo(Arc<Registry>);

#[derive(Default)]
struct Registry {
    files: RwLock<HashMap<SourceId, Weak<FileIo>>>,
    next: AtomicU32,
}

impl Debug for FileSegmentIo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileSegmentIo")
            .field("files", &self.0.files.read().len())
            .finish()
    }
}

impl SessionVar for FileSegmentIo {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl FileSegmentIo {
    /// The service of `session`, created on first use.
    pub fn of(session: &VortexSession) -> Self {
        session.get::<Self>().clone()
    }

    /// Registers `file`. The service reads it while any clone of the registration is alive.
    pub(crate) fn register(&self, file: FileIo) -> RegisteredSource {
        let id = SourceId(self.0.next.fetch_add(1, Ordering::Relaxed));
        let file = Arc::new(file);
        self.0.files.write().insert(id, Arc::downgrade(&file));
        let registration = Arc::new(Registration {
            registry: Arc::downgrade(&self.0),
            id,
            _file: file,
        });
        RegisteredSource::new(Arc::new(self.clone()), id, registration)
    }
}

impl SegmentIoService for FileSegmentIo {
    fn session(&self) -> Arc<dyn SegmentIoSession> {
        Arc::new(FileSession {
            registry: Arc::clone(&self.0),
            files: Mutex::default(),
            fetches: Mutex::default(),
        })
    }
}

/// Keeps a file registered, and readable, while it lives.
struct Registration {
    registry: Weak<Registry>,
    id: SourceId,
    _file: Arc<FileIo>,
}

impl Drop for Registration {
    fn drop(&mut self) {
        if let Some(registry) = self.registry.upgrade() {
            registry.files.write().remove(&self.id);
        }
    }
}

/// How one registered file answers the segment reads its cache does not.
pub(crate) enum Backend {
    /// The file's coalescing read driver.
    Driver(DriverIo),
    /// The whole file, in memory.
    Buffer(ByteBuffer),
}

/// The parts of a file's segment source that its reads go through.
pub(crate) struct DriverIo {
    pub(crate) events: mpsc::UnboundedSender<ReadEvent>,
    /// Keeps the driver task running while the file is registered, even if the file's segment
    /// source is dropped.
    pub(crate) _driver: SharedDriver,
    pub(crate) next_id: Arc<AtomicUsize>,
}

/// A registered file's segments and how to read them.
pub(crate) struct FileIo {
    specs: Arc<[SegmentSpec]>,
    cache: Arc<dyn SegmentCache>,
    backend: Backend,
    /// Reads in flight or finished and still held, by byte range. Segments that share a range
    /// share its read.
    reads: Mutex<HashMap<(u64, u32, Alignment), Weak<Read>>>,
}

impl FileIo {
    pub(crate) fn new(
        specs: Arc<[SegmentSpec]>,
        cache: Arc<dyn SegmentCache>,
        backend: Backend,
    ) -> Self {
        Self {
            specs,
            cache,
            backend,
            reads: Mutex::default(),
        }
    }

    /// A handle for `segment`. A read that must be marked wanted once the whole batch is
    /// registered is pushed to `wants`.
    fn handle(
        &self,
        segment: SegmentId,
        want: bool,
        wants: &mut Vec<Arc<Read>>,
    ) -> VortexResult<SegmentHandle> {
        let spec = self
            .specs
            .get(*segment as usize)
            .ok_or_else(|| vortex_err!("segment {segment} is not in the file"))?;
        if let Some(bytes) = self.cache.get_if_ready(segment) {
            return Ok(SegmentHandle::ready(Ok(BufferHandle::new_host(
                bytes.aligned(spec.alignment),
            ))));
        }
        let driver = match &self.backend {
            Backend::Buffer(buffer) => {
                let start = usize::try_from(spec.offset)?;
                let end = start + spec.length as usize;
                vortex_ensure_in_file(end, buffer.len(), segment)?;
                return Ok(SegmentHandle::ready(Ok(BufferHandle::new_host(
                    buffer.slice(start..end).aligned(spec.alignment),
                ))));
            }
            Backend::Driver(driver) => driver,
        };
        let key = (spec.offset, spec.length, spec.alignment);
        let mut reads = self.reads.lock();
        let read = match reads.get(&key).and_then(Weak::upgrade) {
            Some(read) => read,
            None => {
                let read = Arc::new(Read::register(
                    driver,
                    spec,
                    segment,
                    Arc::clone(&self.cache),
                )?);
                reads.retain(|_, read| read.strong_count() > 0);
                reads.insert(key, Arc::downgrade(&read));
                read
            }
        };
        if want {
            wants.push(Arc::clone(&read));
        }
        Ok(SegmentHandle::new(read.bytes.clone(), read))
    }
}

fn vortex_ensure_in_file(end: usize, len: usize, segment: SegmentId) -> VortexResult<()> {
    if end > len {
        vortex_bail!(
            "segment {segment} ends at byte {end}, past the end of the file ({len} bytes)"
        );
    }
    Ok(())
}

/// One read registered with a file's driver, shared by every handle of its byte range.
struct Read {
    id: RequestId,
    /// Whether the driver was told the read is wanted.
    wanted: AtomicBool,
    bytes: SharedBytes,
    events: mpsc::UnboundedSender<ReadEvent>,
}

impl Read {
    /// Registers a read of `spec` with the driver, without marking it wanted.
    fn register(
        driver: &DriverIo,
        spec: &SegmentSpec,
        segment: SegmentId,
        cache: Arc<dyn SegmentCache>,
    ) -> VortexResult<Self> {
        let id = driver.next_id.fetch_add(1, Ordering::Relaxed);
        let (callback, receiver) = oneshot::channel();
        driver
            .events
            .unbounded_send(ReadEvent::Request(ReadRequest {
                id,
                offset: spec.offset,
                length: spec.length as usize,
                alignment: spec.alignment,
                callback,
            }))
            .map_err(|err| vortex_err!("the file's read driver has stopped: {err}"))?;
        let bytes = async move {
            let bytes = receiver
                .await
                .map_err(|_| Arc::new(vortex_err!("the file's read driver dropped the read")))?
                .map_err(Arc::new)?;
            if let Some(buffer) = bytes.as_host_opt()
                && let Err(err) = cache.put(segment, buffer.clone()).await
            {
                tracing::warn!("Failed to store segment {segment} in cache: {err}");
            }
            Ok(bytes)
        }
        .boxed()
        .shared();
        Ok(Self {
            id,
            wanted: AtomicBool::new(false),
            bytes,
            events: driver.events.clone(),
        })
    }

    /// Tells the driver the read is wanted, so it starts the read if it has not already.
    fn want(&self) -> VortexResult<()> {
        if !self.wanted.swap(true, Ordering::Relaxed) {
            self.events
                .unbounded_send(ReadEvent::Polled(self.id))
                .map_err(|err| vortex_err!("the file's read driver has stopped: {err}"))?;
        }
        Ok(())
    }
}

impl Drop for Read {
    fn drop(&mut self) {
        if self.bytes.peek().is_none() {
            // The last handle is gone before the read finished.
            drop(self.events.unbounded_send(ReadEvent::Dropped(self.id)));
        }
    }
}

/// One scope's reads through a [`FileSegmentIo`].
struct FileSession {
    registry: Arc<Registry>,
    /// The files this session has read, so later requests skip the registry.
    files: Mutex<HashMap<SourceId, Arc<FileIo>>>,
    fetches: Mutex<FetchTracker>,
}

impl FileSession {
    fn file(&self, source: SourceId) -> VortexResult<Arc<FileIo>> {
        let mut files = self.files.lock();
        if let Some(file) = files.get(&source) {
            return Ok(Arc::clone(file));
        }
        let file = self
            .registry
            .files
            .read()
            .get(&source)
            .and_then(Weak::upgrade)
            .ok_or_else(|| vortex_err!("{source:?} is not registered for segment reads"))?;
        files.insert(source, Arc::clone(&file));
        Ok(file)
    }
}

impl SegmentIoSession for FileSession {
    fn submit(
        &self,
        owner: IoOwnerId,
        batch: &[SegmentRequest],
    ) -> VortexResult<Vec<SegmentHandle>> {
        let mut wants = Vec::new();
        let handles = batch
            .iter()
            .map(|request| {
                if request.intent == IoIntent::Forget {
                    vortex_bail!("a Forget is withdrawn by dropping handles, not submitted");
                }
                self.file(request.key.source)?.handle(
                    request.key.segment,
                    request.intent != IoIntent::Announce,
                    &mut wants,
                )
            })
            .collect::<VortexResult<Vec<_>>>()?;
        // Every read of the batch is registered before any is wanted, so the driver can choose
        // physical reads with the whole batch in view.
        for read in wants {
            read.want()?;
        }
        let mut fetches = self.fetches.lock();
        for (request, handle) in batch.iter().zip(&handles) {
            if request.intent == IoIntent::Fetch && handle.try_get().is_none() {
                fetches.track(owner, handle);
            }
        }
        Ok(handles)
    }

    fn poll_ready(
        &self,
        cx: &mut Context<'_>,
        ready: &mut Vec<IoOwnerId>,
    ) -> Poll<VortexResult<()>> {
        self.fetches.lock().poll_ready(cx, ready)
    }
}

#[cfg(test)]
mod tests {
    use std::future::poll_fn;

    use futures::future::BoxFuture;
    use rstest::rstest;
    use vortex_buffer::ByteBufferMut;
    use vortex_error::VortexError;
    use vortex_io::CoalesceConfig;
    use vortex_io::VortexReadAt;
    use vortex_io::runtime::tokio::TokioRuntime;
    use vortex_io::segments::SegmentKey;
    use vortex_layout::segments::NoOpSegmentCache;
    use vortex_metrics::DefaultMetricsRegistry;

    use super::*;
    use crate::segments::FileSegmentSource;
    use crate::segments::InitialReadSegmentCache;
    use crate::segments::RequestMetrics;

    const FILE_LEN: usize = 256;
    /// Segments 0..3 sit next to each other; segment 3 is far from them.
    const OFFSETS: [u64; 4] = [0, 16, 32, 128];

    /// An in-memory file that records the physical reads made of it.
    #[derive(Clone)]
    struct RecordingRead {
        bytes: ByteBuffer,
        reads: Arc<Mutex<Vec<(u64, usize)>>>,
    }

    impl VortexReadAt for RecordingRead {
        fn coalesce_config(&self) -> Option<CoalesceConfig> {
            Some(CoalesceConfig {
                distance: 1024,
                max_size: 1024,
            })
        }

        fn concurrency(&self) -> usize {
            4
        }

        fn size(&self) -> BoxFuture<'static, VortexResult<u64>> {
            self.bytes.size()
        }

        fn read_at(
            &self,
            offset: u64,
            length: usize,
            alignment: Alignment,
        ) -> BoxFuture<'static, VortexResult<BufferHandle>> {
            self.reads.lock().push((offset, length));
            self.bytes.read_at(offset, length, alignment)
        }
    }

    fn bytes() -> ByteBuffer {
        let mut bytes = ByteBufferMut::with_capacity(FILE_LEN);
        for byte in 0..FILE_LEN {
            bytes.push(u8::try_from(byte % 251).unwrap_or_default());
        }
        bytes.freeze()
    }

    fn specs() -> Arc<[SegmentSpec]> {
        OFFSETS
            .iter()
            .map(|&offset| SegmentSpec {
                offset,
                length: 16,
                alignment: Alignment::none(),
            })
            .collect()
    }

    /// A service with one file registered over a recording reader.
    fn service(cache: Arc<dyn SegmentCache>) -> (FileSegmentIo, RegisteredSource, RecordingRead) {
        let reader = RecordingRead {
            bytes: bytes(),
            reads: Arc::default(),
        };
        let source = FileSegmentSource::open(
            specs(),
            reader.clone(),
            TokioRuntime::current(),
            RequestMetrics::new(&DefaultMetricsRegistry::default(), vec![]),
        );
        let service = FileSegmentIo::default();
        let registration = service.register(FileIo::new(
            specs(),
            cache,
            Backend::Driver(source.driver_io()),
        ));
        (service, registration, reader)
    }

    fn request(source: &RegisteredSource, segment: u32, intent: IoIntent) -> SegmentRequest {
        SegmentRequest::new(
            SegmentKey::new(source.id(), SegmentId::from(segment)),
            intent,
        )
    }

    /// The bytes of `segment`, as the file holds them.
    fn expected(segment: usize) -> ByteBuffer {
        let start = usize::try_from(OFFSETS[segment]).unwrap_or_default();
        bytes().slice(start..start + 16)
    }

    async fn read(handle: &SegmentHandle) -> VortexResult<ByteBuffer> {
        handle
            .bytes()
            .await
            .map_err(VortexError::from)?
            .try_into_host()?
            .await
    }

    #[tokio::test]
    async fn a_batch_coalesces_with_its_announcements() -> VortexResult<()> {
        let (service, file, reader) = service(Arc::new(NoOpSegmentCache));
        let session = service.session();
        let handles = session.submit(
            IoOwnerId(7),
            &[
                request(&file, 0, IoIntent::Fetch),
                request(&file, 1, IoIntent::Fetch),
                request(&file, 2, IoIntent::Announce),
            ],
        )?;
        assert_eq!(read(&handles[0]).await?, expected(0));
        assert_eq!(read(&handles[1]).await?, expected(1));
        // The announced segment rode along in the same physical read.
        assert_eq!(read(&handles[2]).await?, expected(2));
        assert_eq!(*reader.reads.lock(), vec![(0, 48)]);

        let mut ready = Vec::new();
        poll_fn(|cx| session.poll_ready(cx, &mut ready)).await?;
        assert!(ready.iter().all(|owner| *owner == IoOwnerId(7)));
        Ok(())
    }

    #[tokio::test]
    async fn sessions_share_one_read() -> VortexResult<()> {
        let (service, file, reader) = service(Arc::new(NoOpSegmentCache));
        let first = service
            .session()
            .submit(IoOwnerId(0), &[request(&file, 3, IoIntent::Fetch)])?;
        let second = service
            .session()
            .submit(IoOwnerId(0), &[request(&file, 3, IoIntent::Prefetch)])?;
        assert_eq!(read(&first[0]).await?, expected(3));
        assert_eq!(read(&second[0]).await?, expected(3));
        assert_eq!(*reader.reads.lock(), vec![(128, 16)]);
        Ok(())
    }

    #[rstest]
    #[case::held(true, vec![(0, 144)])]
    #[case::dropped(false, vec![(0, 16)])]
    #[tokio::test]
    async fn dropping_every_handle_withdraws_the_read(
        #[case] hold: bool,
        #[case] reads: Vec<(u64, usize)>,
    ) -> VortexResult<()> {
        let (service, file, reader) = service(Arc::new(NoOpSegmentCache));
        let session = service.session();
        let announced = session.submit(IoOwnerId(0), &[request(&file, 3, IoIntent::Announce)])?;
        if !hold {
            drop(announced);
        }
        let fetched = session.submit(IoOwnerId(0), &[request(&file, 0, IoIntent::Fetch)])?;
        assert_eq!(read(&fetched[0]).await?, expected(0));
        assert_eq!(*reader.reads.lock(), reads);
        Ok(())
    }

    #[tokio::test]
    async fn cached_segments_are_ready_without_a_read() -> VortexResult<()> {
        let cache = InitialReadSegmentCache {
            initial: HashMap::from_iter([(SegmentId::from(1), expected(1))]),
            fallback: Arc::new(NoOpSegmentCache),
        };
        let (service, file, reader) = service(Arc::new(cache));
        let handles = service
            .session()
            .submit(IoOwnerId(0), &[request(&file, 1, IoIntent::Fetch)])?;
        let bytes = handles[0]
            .try_get()
            .ok_or_else(|| vortex_err!("a cached segment is ready at submit"))??;
        assert_eq!(bytes.try_into_host_sync()?, expected(1));
        assert!(reader.reads.lock().is_empty());
        Ok(())
    }

    #[test]
    fn buffered_files_answer_at_submit() -> VortexResult<()> {
        let service = FileSegmentIo::default();
        let file = service.register(FileIo::new(
            specs(),
            Arc::new(NoOpSegmentCache),
            Backend::Buffer(bytes()),
        ));
        let handles = service
            .session()
            .submit(IoOwnerId(0), &[request(&file, 2, IoIntent::Fetch)])?;
        let bytes = handles[0]
            .try_get()
            .ok_or_else(|| vortex_err!("a buffered segment is ready at submit"))??;
        assert_eq!(bytes.try_into_host_sync()?, expected(2));
        Ok(())
    }

    #[test]
    fn a_dropped_registration_is_unreadable() -> VortexResult<()> {
        let service = FileSegmentIo::default();
        let file = service.register(FileIo::new(
            specs(),
            Arc::new(NoOpSegmentCache),
            Backend::Buffer(bytes()),
        ));
        let segment = request(&file, 0, IoIntent::Fetch);
        drop(file);
        assert!(service.session().submit(IoOwnerId(0), &[segment]).is_err());
        Ok(())
    }

    #[test]
    fn forget_is_never_submitted() {
        let service = FileSegmentIo::default();
        let file = service.register(FileIo::new(
            specs(),
            Arc::new(NoOpSegmentCache),
            Backend::Buffer(bytes()),
        ));
        assert!(
            service
                .session()
                .submit(IoOwnerId(0), &[request(&file, 0, IoIntent::Forget)])
                .is_err()
        );
    }
}
