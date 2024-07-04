#![cfg(feature = "object_store")]

use std::future::Future;
use std::io::Cursor;
use std::ops::Range;

use bytes::{Bytes, BytesMut};
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};
use vortex_buffer::io_buf::IoBuf;
use vortex_buffer::Buffer;
use vortex_error::VortexResult;

use crate::io::{VortexRead, VortexReadAt, VortexWrite};

pub trait ObjectStoreExt {
    fn vortex_read(
        &self,
        location: &Path,
        range: Range<usize>,
    ) -> impl Future<Output = VortexResult<impl VortexRead>>;

    fn vortex_reader(&self, location: &Path) -> impl VortexReadAt;

    fn vortex_writer(&self, location: &Path) -> impl VortexWrite;
}

impl<O: ObjectStore> ObjectStoreExt for O {
    async fn vortex_read(
        &self,
        location: &Path,
        range: Range<usize>,
    ) -> VortexResult<impl VortexRead> {
        let bytes = self.get_range(location, range).await?;
        Ok(Cursor::new(Buffer::Bytes(bytes)))
    }

    fn vortex_reader(&self, location: &Path) -> impl VortexReadAt {
        ObjectStoreReadAt::new(self, location)
    }

    fn vortex_writer(&self, location: &Path) -> impl VortexWrite {
        ObjectStoreWriter::new(self, location)
    }
}

pub struct ObjectStoreReadAt<'a, 'b, O: ObjectStore> {
    object_store: &'a O,
    location: &'b Path,
}

impl<'a, 'b, O: ObjectStore> ObjectStoreReadAt<'a, 'b, O> {
    pub fn new(object_store: &'a O, location: &'b Path) -> Self {
        Self {
            object_store,
            location,
        }
    }
}

impl<'a, 'b, O: ObjectStore> VortexReadAt for ObjectStoreReadAt<'a, 'b, O> {
    async fn read_at_into(&self, pos: u64, mut buffer: BytesMut) -> std::io::Result<BytesMut> {
        let start_range = pos as usize;
        let bytes = self
            .object_store
            .get_range(self.location, start_range..(start_range + buffer.len()))
            .await?;
        buffer.as_mut().copy_from_slice(bytes.as_ref());
        Ok(buffer)
    }
}

pub struct ObjectStoreWriter<'a, 'b, O: ObjectStore> {
    object_store: &'a O,
    location: &'b Path,
}

impl<'a, 'b, O: ObjectStore> ObjectStoreWriter<'a, 'b, O> {
    pub fn new(object_store: &'a O, location: &'b Path) -> Self {
        Self {
            object_store,
            location,
        }
    }
}

impl<'a, 'b, O: ObjectStore> VortexWrite for ObjectStoreWriter<'a, 'b, O> {
    async fn write_all<B: IoBuf>(&mut self, buffer: B) -> std::io::Result<B> {
        self.object_store
            .put(
                self.location,
                PutPayload::from_bytes(Bytes::copy_from_slice(buffer.as_slice())),
            )
            .await?;
        Ok(buffer)
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }

    async fn shutdown(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
