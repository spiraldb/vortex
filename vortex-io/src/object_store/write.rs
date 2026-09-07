// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::io;
use std::sync::Arc;

use bytes::BytesMut;
use futures::TryStreamExt;
use futures::stream::FuturesUnordered;
use object_store::MultipartUpload;
use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::PutPayload;
use object_store::PutResult;
use object_store::path::Path;
use vortex_error::VortexResult;

use crate::IoBuf;
use crate::VortexWrite;

/// Adapter type to write data through a [`ObjectStore`] instance.
///
/// After writing, the caller must make sure to call `shutdown`, in order to ensure the data is actually persisted.
///
/// A write that fails aborts the upload before returning, because S3 and GCS keep the parts that
/// already landed when a [`MultipartUpload`] is dropped without one.
pub struct ObjectStoreWrite {
    upload: Box<dyn MultipartUpload>,
    buffer: BytesMut,
    put_result: Option<PutResult>,
    /// Set once the upload has been aborted. Aborting an already-aborted upload is
    /// implementation-defined behaviour, so no path may do it twice.
    aborted: bool,
}

const CHUNK_SIZE: usize = 16 * 1024 * 1024;
const BUFFER_SIZE: usize = 128 * 1024 * 1024;

impl ObjectStoreWrite {
    pub async fn new(object_store: Arc<dyn ObjectStore>, location: &Path) -> VortexResult<Self> {
        let upload = object_store.put_multipart(location).await?;
        Ok(Self {
            upload,
            buffer: BytesMut::with_capacity(CHUNK_SIZE),
            put_result: None,
            aborted: false,
        })
    }

    pub fn put_result(&self) -> Option<&PutResult> {
        self.put_result.as_ref()
    }

    /// Abort the upload and hand back `error`, which is the one the caller needs to see.
    ///
    /// A failing abort is only logged: it cannot be recovered from here, and returning it instead
    /// would hide why the write failed.
    async fn abort_with(&mut self, error: io::Error) -> io::Error {
        if !self.aborted {
            self.aborted = true;
            if let Err(abort_error) = self.upload.abort().await {
                tracing::warn!("failed to abort multipart upload: {abort_error}");
            }
        }
        error
    }
}

impl VortexWrite for ObjectStoreWrite {
    async fn write_all<B: IoBuf>(&mut self, buffer: B) -> io::Result<B> {
        self.buffer.extend_from_slice(buffer.as_slice());
        let parts = FuturesUnordered::new();

        // If the buffer is full
        if self.buffer.len() > BUFFER_SIZE {
            // Split off chunks while buffer is larger than CHUNKS_SIZE
            while self.buffer.len() > CHUNK_SIZE {
                let payload = self.buffer.split_to(CHUNK_SIZE).freeze();
                let part_fut = self.upload.put_part(PutPayload::from_bytes(payload));

                parts.push(part_fut);
            }
        }

        if let Err(error) = parts.try_collect::<Vec<_>>().await {
            return Err(self.abort_with(error.into()).await);
        }

        Ok(buffer)
    }

    async fn flush(&mut self) -> io::Result<()> {
        let parts = FuturesUnordered::new();

        while self.buffer.len() > CHUNK_SIZE {
            let payload = self.buffer.split_to(CHUNK_SIZE).freeze();
            let part_fut = self.upload.put_part(PutPayload::from_bytes(payload));

            parts.push(part_fut);
        }

        if let Err(error) = parts.try_collect::<Vec<_>>().await {
            return Err(self.abort_with(error.into()).await);
        }

        Ok(())
    }

    async fn shutdown(&mut self) -> io::Result<()> {
        // `flush` aborts the upload itself when it fails.
        self.flush().await?;

        if !self.buffer.is_empty() {
            let payload = std::mem::take(&mut self.buffer).freeze();
            let part = self.upload.put_part(PutPayload::from_bytes(payload));
            if let Err(error) = part.await {
                return Err(self.abort_with(error.into()).await);
            }
        }

        let completed = self.upload.complete().await;
        match completed {
            Ok(put_result) => {
                self.put_result = Some(put_result);
                Ok(())
            }
            Err(error) => Err(self.abort_with(error.into()).await),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use async_trait::async_trait;
    use object_store::ObjectStore;
    use object_store::UploadPart;
    use object_store::local::LocalFileSystem;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use rstest::rstest;
    use tempfile::tempdir;

    use super::*;

    /// A [`MultipartUpload`] that refuses every part and every completion, counting how often it
    /// is aborted. S3 and GCS keep already-uploaded parts when an upload is dropped without an
    /// abort, and neither `InMemory` nor `LocalFileSystem` exposes staged parts, so the count is
    /// the only observable for that cleanup.
    #[derive(Debug)]
    struct RefusingUpload {
        aborts: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl MultipartUpload for RefusingUpload {
        fn put_part(&mut self, _data: PutPayload) -> UploadPart {
            Box::pin(std::future::ready(Err(object_store::Error::Generic {
                store: "refusing",
                source: "put_part refused".into(),
            })))
        }

        async fn complete(&mut self) -> object_store::Result<PutResult> {
            Err(object_store::Error::Generic {
                store: "refusing",
                source: "complete refused".into(),
            })
        }

        async fn abort(&mut self) -> object_store::Result<()> {
            self.aborts.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn refusing_writer(aborts: &Arc<AtomicUsize>) -> ObjectStoreWrite {
        ObjectStoreWrite {
            upload: Box::new(RefusingUpload {
                aborts: Arc::clone(aborts),
            }),
            buffer: BytesMut::with_capacity(CHUNK_SIZE),
            put_result: None,
            aborted: false,
        }
    }

    /// The trailing part `shutdown` uploads is the one a small file goes out as, so its failure
    /// must still abort.
    #[tokio::test]
    async fn shutdown_aborts_when_the_trailing_part_fails() -> anyhow::Result<()> {
        let aborts = Arc::new(AtomicUsize::new(0));
        let mut writer = refusing_writer(&aborts);

        writer.write_all(vec![0u8; 8]).await?;
        let error = writer.shutdown().await.expect_err("the part is refused");

        assert!(error.to_string().contains("put_part refused"));
        assert_eq!(aborts.load(Ordering::SeqCst), 1);
        assert!(writer.put_result().is_none());
        Ok(())
    }

    /// Nothing has been staged when only `complete` fails, but the upload is still open, so it
    /// has to be aborted too.
    #[tokio::test]
    async fn shutdown_aborts_when_complete_fails() {
        let aborts = Arc::new(AtomicUsize::new(0));
        let mut writer = refusing_writer(&aborts);

        let error = writer.shutdown().await.expect_err("completion is refused");

        assert!(error.to_string().contains("complete refused"));
        assert_eq!(aborts.load(Ordering::SeqCst), 1);
        assert!(writer.put_result().is_none());
    }

    /// `flush` aborts on its own failure, and a caller that goes on to `shutdown` must not abort a
    /// second time — `object_store` leaves that implementation-defined.
    #[tokio::test]
    async fn a_second_failure_does_not_abort_twice() -> anyhow::Result<()> {
        let aborts = Arc::new(AtomicUsize::new(0));
        let mut writer = refusing_writer(&aborts);

        writer.write_all(vec![0u8; CHUNK_SIZE + 1]).await?;
        writer.flush().await.expect_err("the part is refused");
        assert_eq!(aborts.load(Ordering::SeqCst), 1);

        writer.shutdown().await.expect_err("the part is refused");
        assert_eq!(aborts.load(Ordering::SeqCst), 1);
        Ok(())
    }

    // Note: Concurrent writes test removed because &mut self in write_all already ensures
    // exclusive access. Multiple writers would need to be created with separate buffers,
    // which is not the intended use case.

    #[tokio::test]
    #[rstest]
    #[case(100)]
    #[case(8 * 1024 * 1024)]
    #[case(25 * 1024 * 1024)]
    #[case(26 * 1024 * 1024)]
    async fn test_object_store_writer_multiple_flushes(
        #[case] chunk_size: usize,
    ) -> anyhow::Result<()> {
        let temp_dir = tempdir()?;
        let local_store =
            Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path())?) as Arc<dyn ObjectStore>;
        let memory_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let location = Path::from("test.bin");

        for test_store in [memory_store, local_store] {
            let mut writer = ObjectStoreWrite::new(Arc::clone(&test_store), &location).await?;

            #[expect(clippy::cast_possible_truncation)]
            let data = (0..3)
                .map(|i| vec![i as u8; chunk_size])
                .collect::<Vec<_>>();

            // Write and flush multiple times
            for i in 0..3 {
                let data = data[i].clone();
                writer.write_all(data).await?;
                writer.flush().await?;
            }

            // Shutdown the writer to make sure data actually gets persisted.
            writer.shutdown().await?;

            // Verify all data was written
            let result = test_store.get(&location).await?;
            let bytes = result.bytes().await?;

            let expected_data = itertools::concat(data.into_iter());
            assert_eq!(bytes, expected_data);
        }

        Ok(())
    }
}
