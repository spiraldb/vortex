// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Factory for creating [`VortexReadAt`] instances for [`PartitionedFile`]s.

use std::fmt::Debug;
use std::sync::Arc;

use datafusion_common::Result as DFResult;
use datafusion_datasource::PartitionedFile;
use futures::FutureExt;
use futures::future::BoxFuture;
use memmap2::Mmap;
use object_store::GetOptions;
use object_store::GetResultPayload;
use object_store::ObjectStore;
use vortex::array::memory::MemorySessionExt;
use vortex::buffer::ByteBuffer;
use vortex::io::VortexReadAt;
use vortex::io::object_store::ObjectStoreReadAt;
use vortex::io::session::RuntimeSessionExt;
use vortex::session::VortexSession;

/// Factory to create [`VortexReadAt`] instances for a `PartitionedFile`.
///
/// Plug a custom implementation into [`VortexSource::with_vortex_reader_factory`]
/// when the default object-store reader is not sufficient, for example to:
///
/// - reuse an application-level metadata cache,
/// - wrap reads with custom authentication or routing,
/// - coalesce I/O in a remote storage layer.
///
/// [`VortexSource::with_vortex_reader_factory`]: crate::VortexSource::with_vortex_reader_factory
pub trait VortexReaderFactory: Debug + Send + Sync + 'static {
    /// Create a reader for a target object.
    fn create_reader(
        &self,
        file: &PartitionedFile,
        session: &VortexSession,
    ) -> DFResult<Arc<dyn VortexReadAt>>;

    /// Maps the whole of a local file into memory, so the V2 scan reads its segments without
    /// I/O. `None` when the object is not a local file, and by default.
    fn map_local_file(
        &self,
        _file: &PartitionedFile,
    ) -> BoxFuture<'static, DFResult<Option<ByteBuffer>>> {
        futures::future::ready(Ok(None)).boxed()
    }
}

/// Default [`VortexReaderFactory`] backed by DataFusion's [`ObjectStore`].
///
/// This is the reader used by [`crate::VortexSource`] and
/// [`crate::VortexFormat`] unless a
/// custom factory is supplied. It works with any object store that DataFusion
/// has registered for the scan.
#[derive(Debug)]
pub struct DefaultVortexReaderFactory {
    object_store: Arc<dyn ObjectStore>,
}

impl DefaultVortexReaderFactory {
    /// Creates a new factory from an [`ObjectStore`].
    ///
    /// # Example
    ///
    /// ```rust
    /// # use std::sync::Arc;
    /// # use object_store::memory::InMemory;
    /// use vortex_datafusion::reader::DefaultVortexReaderFactory;
    ///
    /// let factory = DefaultVortexReaderFactory::new(Arc::new(InMemory::new()));
    /// # let _ = factory;
    /// ```
    pub fn new(object_store: Arc<dyn ObjectStore>) -> Self {
        Self { object_store }
    }
}

impl VortexReaderFactory for DefaultVortexReaderFactory {
    fn create_reader(
        &self,
        file: &PartitionedFile,
        session: &VortexSession,
    ) -> DFResult<Arc<dyn VortexReadAt>> {
        Ok(Arc::new(ObjectStoreReadAt::new_with_allocator(
            Arc::clone(&self.object_store),
            file.path().clone(),
            session.handle(),
            session.allocator(),
        )) as _)
    }

    fn map_local_file(
        &self,
        file: &PartitionedFile,
    ) -> BoxFuture<'static, DFResult<Option<ByteBuffer>>> {
        let store = Arc::clone(&self.object_store);
        let path = file.path().clone();
        let size = file.object_meta.size;
        async move {
            // A local store answers with the open file; a remote one only with its metadata.
            let options = GetOptions {
                head: true,
                ..Default::default()
            };
            let GetResultPayload::File(file, _) = store.get_opts(&path, options).await?.payload
            else {
                return Ok(None);
            };
            // SAFETY: Vortex files are immutable once written. A file truncated while mapped
            // would fault on access, as it would for any reader of a memory-mapped file.
            let map = unsafe { Mmap::map(&file) }?;
            Ok((map.len() as u64 == size).then(|| ByteBuffer::from(map)))
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion_datasource::PartitionedFile;
    use object_store::ObjectStoreExt;
    use object_store::local::LocalFileSystem;
    use object_store::memory::InMemory;
    use object_store::path::Path;

    use super::DefaultVortexReaderFactory;
    use super::VortexReaderFactory;

    const BYTES: &[u8] = b"vortex file bytes";

    #[tokio::test]
    async fn maps_only_whole_local_files() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        std::fs::write(dir.path().join("file.vortex"), BYTES)?;
        let local = DefaultVortexReaderFactory::new(Arc::new(LocalFileSystem::new_with_prefix(
            dir.path(),
        )?));
        let file = PartitionedFile::new("file.vortex", BYTES.len() as u64);
        let mapped = local
            .map_local_file(&file)
            .await?
            .ok_or_else(|| anyhow::anyhow!("a local file is mapped"))?;
        assert_eq!(mapped.as_slice(), BYTES);

        // A file whose size differs from what was planned is read through the object store.
        let grown = PartitionedFile::new("file.vortex", BYTES.len() as u64 - 1);
        assert!(local.map_local_file(&grown).await?.is_none());

        let memory = InMemory::new();
        memory.put(&Path::from("file.vortex"), BYTES.into()).await?;
        let memory = DefaultVortexReaderFactory::new(Arc::new(memory));
        assert!(memory.map_local_file(&file).await?.is_none());
        Ok(())
    }
}
