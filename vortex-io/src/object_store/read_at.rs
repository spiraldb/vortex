// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

#[cfg(any(unix, windows))]
use std::fs::File;
use std::io;
use std::sync::Arc;
#[cfg(any(unix, windows))]
use std::sync::atomic::AtomicU64;
#[cfg(any(unix, windows))]
use std::sync::atomic::Ordering;

use futures::FutureExt;
use futures::SinkExt;
use futures::StreamExt;
use futures::channel::mpsc;
use futures::future::BoxFuture;
use futures::stream;
use object_store::GetOptions;
use object_store::GetRange;
use object_store::GetResultPayload;
#[cfg(any(unix, windows))]
use object_store::ObjectMeta;
use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::path::Path as ObjectPath;
#[cfg(any(unix, windows))]
use parking_lot::Mutex;
use vortex_array::buffer::BufferHandle;
use vortex_array::memory::DefaultHostAllocator;
use vortex_array::memory::HostAllocatorRef;
use vortex_buffer::Alignment;
use vortex_error::VortexError;
use vortex_error::VortexResult;
#[cfg(any(unix, windows))]
use vortex_error::vortex_bail;
use vortex_error::vortex_ensure;
use vortex_error::vortex_err;

use crate::CoalesceConfig;
use crate::ReadAtRequest;
use crate::ReadAtStream;
use crate::VortexReadAt;
use crate::runtime::Handle;
#[cfg(not(target_arch = "wasm32"))]
use crate::std_file::read_exact_at;

/// Default number of concurrent requests to allow.
pub const DEFAULT_CONCURRENCY: usize = 192;

/// An object store backed I/O source.
pub struct ObjectStoreReadAt {
    store: Arc<dyn ObjectStore>,
    path: ObjectPath,
    uri: Arc<str>,
    handle: Handle,
    allocator: HostAllocatorRef,
    concurrency: usize,
    coalesce_config: Option<CoalesceConfig>,
    #[cfg(any(unix, windows))]
    file_payload_promotion: Option<Arc<FilePayloadPromotion>>,
}

#[cfg(any(unix, windows))]
enum FilePayloadPromotionState {
    Eligible,
    File(Arc<File>),
    NonFile,
}

#[cfg(any(unix, windows))]
struct FilePayloadPromotion {
    expected_meta: ObjectMeta,
    state: Mutex<FilePayloadPromotionState>,
    diagnostics: bool,
    get_opts_calls: AtomicU64,
    file_payloads: AtomicU64,
    cache_hits: AtomicU64,
    installs: AtomicU64,
    install_races: AtomicU64,
    identity_mismatches: AtomicU64,
    blocking_read_submissions: AtomicU64,
    non_file_payloads: AtomicU64,
}

#[cfg(any(unix, windows))]
impl FilePayloadPromotion {
    fn new(expected_meta: ObjectMeta, diagnostics: bool) -> Self {
        Self {
            expected_meta,
            state: Mutex::new(FilePayloadPromotionState::Eligible),
            diagnostics,
            get_opts_calls: AtomicU64::new(0),
            file_payloads: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            installs: AtomicU64::new(0),
            install_races: AtomicU64::new(0),
            identity_mismatches: AtomicU64::new(0),
            blocking_read_submissions: AtomicU64::new(0),
            non_file_payloads: AtomicU64::new(0),
        }
    }

    fn cached_file(&self) -> Option<Arc<File>> {
        let file = match &*self.state.lock() {
            FilePayloadPromotionState::File(file) => Some(Arc::clone(file)),
            FilePayloadPromotionState::Eligible | FilePayloadPromotionState::NonFile => None,
        };
        if file.is_some() {
            self.record(&self.cache_hits, "cache_hit");
        }
        file
    }

    fn observe_file(&self, metadata: &ObjectMeta, file: File) -> VortexResult<Arc<File>> {
        self.record(&self.file_payloads, "file_payload");
        let candidate = Arc::new(file);
        let mut state = self.state.lock();
        match &*state {
            FilePayloadPromotionState::Eligible => {
                if metadata != &self.expected_meta {
                    self.record(&self.identity_mismatches, "identity_mismatch");
                    vortex_bail!(
                        "object identity changed while opening {} for a persistent positional reader",
                        self.expected_meta.location
                    );
                }
                *state = FilePayloadPromotionState::File(Arc::clone(&candidate));
                self.record(&self.installs, "install");
                Ok(candidate)
            }
            FilePayloadPromotionState::File(file) => {
                // Another exact-identity response already installed the authoritative descriptor.
                // Prefer it even if this concurrently opened response observed a replacement.
                self.record(&self.install_races, "install_race");
                Ok(Arc::clone(file))
            }
            FilePayloadPromotionState::NonFile => {
                if metadata != &self.expected_meta {
                    self.record(&self.identity_mismatches, "identity_mismatch");
                    vortex_bail!(
                        "object identity changed while opening {} for a persistent positional reader",
                        self.expected_meta.location
                    );
                }
                Ok(candidate)
            }
        }
    }

    fn observe_non_file(&self) {
        self.record(&self.non_file_payloads, "non_file_payload");
        let mut state = self.state.lock();
        if matches!(*state, FilePayloadPromotionState::Eligible) {
            *state = FilePayloadPromotionState::NonFile;
        }
    }

    fn record_identity_mismatch(&self) {
        self.record(&self.identity_mismatches, "identity_mismatch");
    }

    fn record(&self, counter: &AtomicU64, event: &'static str) {
        if self.diagnostics {
            counter.fetch_add(1, Ordering::Relaxed);
            tracing::trace!(
                target: "vortex_io::file_payload_promotion",
                event,
                path = %self.expected_meta.location,
                "persistent file-payload promotion event"
            );
        }
    }
}

#[cfg(any(unix, windows))]
impl Drop for FilePayloadPromotion {
    fn drop(&mut self) {
        if !self.diagnostics {
            return;
        }
        tracing::debug!(
            target: "vortex_io::file_payload_promotion",
            path = %self.expected_meta.location,
            get_opts_calls = self.get_opts_calls.load(Ordering::Relaxed),
            file_payloads = self.file_payloads.load(Ordering::Relaxed),
            cache_hits = self.cache_hits.load(Ordering::Relaxed),
            installs = self.installs.load(Ordering::Relaxed),
            install_races = self.install_races.load(Ordering::Relaxed),
            identity_mismatches = self.identity_mismatches.load(Ordering::Relaxed),
            blocking_read_submissions = self.blocking_read_submissions.load(Ordering::Relaxed),
            non_file_payloads = self.non_file_payloads.load(Ordering::Relaxed),
            "persistent file-payload promotion counters"
        );
    }
}

impl ObjectStoreReadAt {
    /// Create a new object store source.
    pub fn new(store: Arc<dyn ObjectStore>, path: ObjectPath, handle: Handle) -> Self {
        Self::new_with_allocator(store, path, handle, Arc::new(DefaultHostAllocator))
    }

    /// Create a new object store source with a custom writable buffer allocator.
    pub fn new_with_allocator(
        store: Arc<dyn ObjectStore>,
        path: ObjectPath,
        handle: Handle,
        allocator: HostAllocatorRef,
    ) -> Self {
        let uri = Arc::from(path.to_string());
        Self {
            store,
            path,
            uri,
            handle,
            allocator,
            concurrency: DEFAULT_CONCURRENCY,
            coalesce_config: Some(CoalesceConfig::object_storage()),
            #[cfg(any(unix, windows))]
            file_payload_promotion: None,
        }
    }

    /// Set the concurrency for this source.
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    /// Set the coalesce config for this source.
    pub fn with_coalesce_config(mut self, config: CoalesceConfig) -> Self {
        self.coalesce_config = Some(config);
        self
    }

    /// Reuse an exactly identified file payload as a persistent positional reader.
    #[cfg(any(unix, windows))]
    pub fn with_file_payload_promotion(
        mut self,
        expected_meta: ObjectMeta,
        diagnostics: bool,
    ) -> VortexResult<Self> {
        if self.path != expected_meta.location {
            vortex_bail!(
                "persistent file-payload promotion path {} does not match expected object {}",
                self.path,
                expected_meta.location
            );
        }
        self.file_payload_promotion = Some(Arc::new(FilePayloadPromotion::new(
            expected_meta,
            diagnostics,
        )));
        Ok(self)
    }
}

async fn read_object_store_range(
    store: Arc<dyn ObjectStore>,
    path: ObjectPath,
    io_handle: Handle,
    allocator: HostAllocatorRef,
    request: ReadAtRequest,
    #[cfg(any(unix, windows))] file_payload_promotion: Option<Arc<FilePayloadPromotion>>,
) -> VortexResult<BufferHandle> {
    let ReadAtRequest {
        offset,
        length,
        alignment,
    } = request;
    let end = offset
        .checked_add(u64::try_from(length)?)
        .ok_or_else(|| vortex_err!("positional read range overflow"))?;
    let range = offset..end;

    #[cfg(any(unix, windows))]
    if let Some((promotion, file)) = file_payload_promotion
        .as_ref()
        .and_then(|promotion| promotion.cached_file().map(|file| (promotion, file)))
    {
        promotion.record(
            &promotion.blocking_read_submissions,
            "blocking_read_submission",
        );
        let mut buffer = allocator.allocate(length, alignment)?;
        let buffer = io_handle
            .spawn_blocking(move || {
                read_exact_at(&file, buffer.as_mut_slice(), range.start)?;
                Ok::<_, io::Error>(buffer)
            })
            .await
            .map_err(io::Error::other)?;
        return Ok(BufferHandle::new_host(buffer.freeze()));
    }

    let mut buffer = allocator.allocate(length, alignment)?;

    #[cfg(any(unix, windows))]
    if let Some(promotion) = &file_payload_promotion {
        promotion.record(&promotion.get_opts_calls, "get_opts");
    }

    #[cfg(any(unix, windows))]
    let mut get_options = GetOptions {
        range: Some(GetRange::Bounded(range.clone())),
        ..Default::default()
    };
    #[cfg(not(any(unix, windows)))]
    let get_options = GetOptions {
        range: Some(GetRange::Bounded(range.clone())),
        ..Default::default()
    };
    #[cfg(any(unix, windows))]
    if let Some(promotion) = &file_payload_promotion {
        get_options.if_match = promotion.expected_meta.e_tag.clone();
        get_options.version = promotion.expected_meta.version.clone();
    }
    let response = store.get_opts(&path, get_options).await?;

    #[cfg(any(unix, windows))]
    if let Some(promotion) = file_payload_promotion
        .as_ref()
        .filter(|_| response.range != range)
    {
        promotion.record_identity_mismatch();
        vortex_bail!(
            "object store returned range {:?} for requested persistent file range {:?}",
            response.range,
            range
        );
    }
    #[cfg(any(unix, windows))]
    let response_meta = response.meta.clone();
    let buffer = match response.payload {
        #[cfg(any(unix, windows))]
        GetResultPayload::File(file, _) => {
            let file = match &file_payload_promotion {
                Some(promotion) => promotion.observe_file(&response_meta, file)?,
                None => Arc::new(file),
            };
            if let Some(promotion) = &file_payload_promotion {
                promotion.record(
                    &promotion.blocking_read_submissions,
                    "blocking_read_submission",
                );
            }
            io_handle
                .spawn_blocking(move || {
                    read_exact_at(&file, buffer.as_mut_slice(), range.start)?;
                    Ok::<_, io::Error>(buffer)
                })
                .await
                .map_err(io::Error::other)?
        }
        #[cfg(all(not(target_arch = "wasm32"), not(any(unix, windows))))]
        GetResultPayload::File(file, _) => io_handle
            .spawn_blocking(move || {
                read_exact_at(&file, buffer.as_mut_slice(), range.start)?;
                Ok::<_, io::Error>(buffer)
            })
            .await
            .map_err(io::Error::other)?,
        #[cfg(target_arch = "wasm32")]
        GetResultPayload::File(..) => {
            unreachable!("File payload not supported on wasm32")
        }
        GetResultPayload::Stream(mut byte_stream) => {
            #[cfg(any(unix, windows))]
            if let Some(promotion) = &file_payload_promotion {
                promotion.observe_non_file();
            }
            let mut written = 0usize;
            while let Some(bytes) = byte_stream.next().await {
                let bytes = bytes?;
                let end = written + bytes.len();
                vortex_ensure!(
                    end <= length,
                    "Object store stream returned too many bytes: {} > expected {} (range: {:?})",
                    end,
                    length,
                    range
                );
                buffer.as_mut_slice()[written..end].copy_from_slice(&bytes);
                written = end;
            }

            vortex_ensure!(
                written == length,
                "Object store stream returned {} bytes but expected {} bytes (range: {:?})",
                written,
                length,
                range
            );

            buffer
        }
    };

    Ok(BufferHandle::new_host(buffer.freeze()))
}

impl VortexReadAt for ObjectStoreReadAt {
    fn uri(&self) -> Option<&Arc<str>> {
        Some(&self.uri)
    }

    fn coalesce_config(&self) -> Option<CoalesceConfig> {
        self.coalesce_config
    }

    fn concurrency(&self) -> usize {
        self.concurrency
    }

    fn size(&self) -> BoxFuture<'static, VortexResult<u64>> {
        let store = Arc::clone(&self.store);
        let path = self.path.clone();
        async move {
            store
                .head(&path)
                .await
                .map(|h| h.size)
                .map_err(VortexError::from)
        }
        .boxed()
    }

    fn read_at(
        &self,
        offset: u64,
        length: usize,
        alignment: Alignment,
    ) -> BoxFuture<'static, VortexResult<BufferHandle>> {
        let store = Arc::clone(&self.store);
        let path = self.path.clone();
        let handle = self.handle.clone();
        let allocator = Arc::clone(&self.allocator);
        let io_handle = handle.clone();
        handle
            .spawn_io(read_object_store_range(
                store,
                path,
                io_handle,
                allocator,
                ReadAtRequest::new(offset, length, alignment),
                #[cfg(any(unix, windows))]
                self.file_payload_promotion.clone(),
            ))
            .boxed()
    }

    fn read_ranges(&self, requests: Arc<[ReadAtRequest]>) -> ReadAtStream {
        if requests.is_empty() {
            return stream::empty().boxed();
        }

        let store = Arc::clone(&self.store);
        let path = self.path.clone();
        let handle = self.handle.clone();
        let allocator = Arc::clone(&self.allocator);
        #[cfg(any(unix, windows))]
        let file_payload_promotion = self.file_payload_promotion.clone();
        let concurrency = self.concurrency.max(1);
        let (mut send, recv) = mpsc::channel(concurrency);
        let io_handle = handle.clone();

        // A single runtime task drives all GETs, avoiding one spawn per range. Do not use
        // ObjectStore::get_ranges here: it returns one Vec after every range completes, whereas
        // VortexReadAt::read_ranges must expose each result as soon as it is ready.
        let task = handle.spawn_io(async move {
            let reads = requests.iter().copied().map(|request| {
                let store = Arc::clone(&store);
                let path = path.clone();
                let io_handle = io_handle.clone();
                let allocator = Arc::clone(&allocator);
                #[cfg(any(unix, windows))]
                let file_payload_promotion = file_payload_promotion.clone();
                async move {
                    let result = read_object_store_range(
                        store,
                        path,
                        io_handle,
                        allocator,
                        request,
                        #[cfg(any(unix, windows))]
                        file_payload_promotion,
                    )
                    .await;
                    (request, result)
                }
            });

            let mut reads = stream::iter(reads).buffer_unordered(concurrency);
            while let Some(result) = reads.next().await {
                if send.send(result).await.is_err() {
                    break;
                }
            }
        });

        async_stream::stream! {
            let mut recv = recv;
            while let Some(result) = recv.next().await {
                yield result;
            }
            task.await;
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {

    use std::collections::HashMap;
    use std::fmt;
    use std::ops::Range;
    use std::sync::Weak;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::stream::BoxStream;
    use object_store::CopyOptions;
    use object_store::GetResult;
    use object_store::ListResult;
    use object_store::MultipartUpload;
    use object_store::PutMultipartOptions;
    use object_store::PutOptions;
    use object_store::PutPayload;
    use object_store::PutResult;
    use object_store::RenameOptions;
    use object_store::Result as ObjectStoreResult;
    use object_store::local::LocalFileSystem;
    use object_store::memory::InMemory;
    use tokio::sync::Barrier;

    use super::*;
    use crate::runtime::AbortHandle;
    use crate::runtime::AbortHandleRef;
    use crate::runtime::Executor;

    const TEST_DATA: &[u8] = b"object store test data";

    #[derive(Clone, Copy, Debug, Default)]
    enum ResponseMutation {
        #[default]
        None,
        Range,
        Metadata,
    }

    #[derive(Debug)]
    struct RecordingStore<T> {
        inner: T,
        get_opts_calls: AtomicUsize,
        get_ranges_calls: AtomicUsize,
        options: Mutex<Vec<GetOptions>>,
        barrier: Option<Arc<Barrier>>,
        delays: HashMap<u64, Duration>,
        error_offset: Option<u64>,
        mutation: ResponseMutation,
    }

    impl<T> RecordingStore<T> {
        fn new(inner: T) -> Self {
            Self {
                inner,
                get_opts_calls: AtomicUsize::new(0),
                get_ranges_calls: AtomicUsize::new(0),
                options: Mutex::new(Vec::new()),
                barrier: None,
                delays: HashMap::new(),
                error_offset: None,
                mutation: ResponseMutation::None,
            }
        }

        fn with_barrier(mut self, parties: usize) -> Self {
            self.barrier = Some(Arc::new(Barrier::new(parties)));
            self
        }

        fn with_delay(mut self, offset: u64, delay: Duration) -> Self {
            self.delays.insert(offset, delay);
            self
        }

        fn with_error(mut self, offset: u64) -> Self {
            self.error_offset = Some(offset);
            self
        }

        fn with_mutation(mut self, mutation: ResponseMutation) -> Self {
            self.mutation = mutation;
            self
        }

        fn requested_offset(options: &GetOptions) -> Option<u64> {
            match &options.range {
                Some(GetRange::Bounded(range)) => Some(range.start),
                Some(GetRange::Offset(offset)) => Some(*offset),
                Some(GetRange::Suffix(_)) | None => None,
            }
        }
    }

    impl<T: fmt::Display> fmt::Display for RecordingStore<T> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "RecordingStore({})", self.inner)
        }
    }

    #[async_trait]
    impl<T: ObjectStore> ObjectStore for RecordingStore<T> {
        async fn put_opts(
            &self,
            location: &ObjectPath,
            payload: PutPayload,
            options: PutOptions,
        ) -> ObjectStoreResult<PutResult> {
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &ObjectPath,
            options: PutMultipartOptions,
        ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &ObjectPath,
            options: GetOptions,
        ) -> ObjectStoreResult<GetResult> {
            self.get_opts_calls.fetch_add(1, Ordering::SeqCst);
            let offset = Self::requested_offset(&options);
            self.options.lock().push(options.clone());
            if let Some(barrier) = &self.barrier {
                barrier.wait().await;
            }
            if let Some(delay) = offset.and_then(|offset| self.delays.get(&offset)) {
                tokio::time::sleep(*delay).await;
            }
            if offset == self.error_offset {
                return Err(object_store::Error::Generic {
                    store: "recording test store",
                    source: Box::new(io::Error::other("injected get failure")),
                });
            }

            let mut response = self.inner.get_opts(location, options).await?;
            match self.mutation {
                ResponseMutation::None => {}
                ResponseMutation::Range => {
                    response.range.end = response.range.end.saturating_add(1)
                }
                ResponseMutation::Metadata => {
                    response.meta.e_tag = Some("mismatched-etag".to_owned());
                }
            }
            Ok(response)
        }

        async fn get_ranges(
            &self,
            location: &ObjectPath,
            ranges: &[Range<u64>],
        ) -> ObjectStoreResult<Vec<Bytes>> {
            self.get_ranges_calls.fetch_add(1, Ordering::SeqCst);
            self.inner.get_ranges(location, ranges).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, ObjectStoreResult<ObjectPath>>,
        ) -> BoxStream<'static, ObjectStoreResult<ObjectPath>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
            self.inner.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&ObjectPath>,
            offset: &ObjectPath,
        ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
            self.inner.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> ObjectStoreResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &ObjectPath,
            to: &ObjectPath,
            options: CopyOptions,
        ) -> ObjectStoreResult<()> {
            self.inner.copy_opts(from, to, options).await
        }

        async fn rename_opts(
            &self,
            from: &ObjectPath,
            to: &ObjectPath,
            options: RenameOptions,
        ) -> ObjectStoreResult<()> {
            self.inner.rename_opts(from, to, options).await
        }
    }

    #[derive(Default)]
    struct CountingExecutor {
        spawn_count: AtomicUsize,
        spawn_io_count: AtomicUsize,
    }

    impl Executor for CountingExecutor {
        fn spawn(&self, fut: BoxFuture<'static, ()>) -> AbortHandleRef {
            self.spawn_count.fetch_add(1, Ordering::SeqCst);
            TokioAbortHandle::new_handle(tokio::spawn(fut).abort_handle())
        }

        fn spawn_io(&self, fut: BoxFuture<'static, ()>) -> AbortHandleRef {
            self.spawn_io_count.fetch_add(1, Ordering::SeqCst);
            TokioAbortHandle::new_handle(tokio::spawn(fut).abort_handle())
        }

        fn spawn_cpu(&self, task: Box<dyn FnOnce() + Send + 'static>) -> AbortHandleRef {
            TokioAbortHandle::new_handle(tokio::spawn(async move { task() }).abort_handle())
        }

        fn spawn_blocking_io(&self, task: Box<dyn FnOnce() + Send + 'static>) -> AbortHandleRef {
            TokioAbortHandle::new_handle(tokio::task::spawn_blocking(task).abort_handle())
        }
    }

    struct TokioAbortHandle(tokio::task::AbortHandle);

    impl TokioAbortHandle {
        fn new_handle(handle: tokio::task::AbortHandle) -> AbortHandleRef {
            Box::new(Self(handle))
        }
    }

    impl AbortHandle for TokioAbortHandle {
        fn abort(self: Box<Self>) {
            self.0.abort();
        }
    }

    #[tokio::test]
    async fn read_at_uses_spawn_io() -> anyhow::Result<()> {
        let executor = Arc::new(CountingExecutor::default());
        let runtime = Arc::clone(&executor) as Arc<dyn Executor>;
        let handle = Handle::new(Arc::downgrade(&runtime));

        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let path = ObjectPath::from("test.bin");
        store.put(&path, PutPayload::from_static(TEST_DATA)).await?;

        let reader = ObjectStoreReadAt::new(store, path, handle);
        let buffer = reader.read_at(7, 5, Alignment::new(1)).await?;

        assert_eq!(buffer.to_host().await.as_slice(), b"store");
        assert_eq!(executor.spawn_io_count.load(Ordering::SeqCst), 1);
        assert_eq!(executor.spawn_count.load(Ordering::SeqCst), 0);

        Ok(())
    }

    #[tokio::test]
    async fn read_ranges_uses_one_io_task() -> anyhow::Result<()> {
        let executor = Arc::new(CountingExecutor::default());
        let runtime = Arc::clone(&executor) as Arc<dyn Executor>;
        let handle = Handle::new(Arc::downgrade(&runtime));

        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let path = ObjectPath::from("test.bin");
        store.put(&path, PutPayload::from_static(TEST_DATA)).await?;

        let reader = ObjectStoreReadAt::new(store, path, handle);
        let requests: Arc<[ReadAtRequest]> = Arc::from([
            ReadAtRequest::new(0, 6, Alignment::new(1)),
            ReadAtRequest::new(7, 5, Alignment::new(1)),
            ReadAtRequest::new(18, 4, Alignment::new(1)),
        ]);
        let results = reader.read_ranges(requests).collect::<Vec<_>>().await;

        assert_eq!(results.len(), 3);
        for (request, result) in results {
            let buffer = result?;
            let offset = usize::try_from(request.offset)?;
            assert_eq!(buffer.len(), request.length);
            assert_eq!(
                buffer.to_host().await.as_slice(),
                &TEST_DATA[offset..offset + request.length]
            );
        }
        assert_eq!(executor.spawn_io_count.load(Ordering::SeqCst), 1);
        assert_eq!(executor.spawn_count.load(Ordering::SeqCst), 0);

        Ok(())
    }
}
