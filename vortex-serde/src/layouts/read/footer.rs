use bytes::{Bytes, BytesMut};
use flatbuffers::{root, root_unchecked};
use vortex_dtype::field::Field;
use vortex_dtype::flatbuffers::deserialize_and_project;
use vortex_dtype::DType;
use vortex_error::{vortex_bail, vortex_err, VortexResult};
use vortex_flatbuffers::{footer, message as fb, ReadFlatBuffer};
use vortex_schema::Schema;

use crate::io::VortexReadAt;
use crate::layouts::read::cache::RelativeLayoutCache;
use crate::layouts::read::context::LayoutDeserializer;
use crate::layouts::read::{LayoutReader, Scan, FILE_POSTSCRIPT_SIZE, INITIAL_READ_SIZE};
use crate::layouts::MAGIC_BYTES;
use crate::messages::IPCDType;
use crate::FLATBUFFER_SIZE_LENGTH;

/// Wrapper around serialized file footer. Provides handle on file schema and
/// layout metadata to read the contents.
///
/// # Footer format
/// ┌────────────────────────────┐
/// │                            │
///              ...
/// ├────────────────────────────┤
/// │                            │
/// │          Schema            │
/// │                            │
/// ├────────────────────────────┤
/// │                            │
/// │         Layouts            │
/// │                            │
/// ├────────────────────────────┤
/// │   Schema Offset (8 bytes)  │
/// ├────────────────────────────┤
/// │   Layout Offset (8 bytes)  │
/// ├────────────────────────────┤
/// │    Magic bytes (4 bytes)   │
/// └────────────────────────────┘
///
pub struct Footer {
    pub(crate) schema_offset: u64,
    pub(crate) layout_offset: u64,
    pub(crate) leftovers: Bytes,
    pub(crate) leftovers_offset: u64,
    pub(crate) layout_serde: LayoutDeserializer,
}

impl Footer {
    fn leftovers_layout_offset(&self) -> usize {
        (self.layout_offset - self.leftovers_offset) as usize
    }

    fn leftovers_schema_offset(&self) -> usize {
        (self.schema_offset - self.leftovers_offset) as usize
    }

    pub fn row_count(&self) -> VortexResult<u64> {
        Ok(self.fb_footer()?.row_count())
    }

    pub fn layout(
        &self,
        scan: Scan,
        message_cache: RelativeLayoutCache,
    ) -> VortexResult<Box<dyn LayoutReader>> {
        let start_offset = self.leftovers_layout_offset();
        let end_offset = self.leftovers.len() - FILE_POSTSCRIPT_SIZE;
        let footer_bytes = self
            .leftovers
            .slice(start_offset + FLATBUFFER_SIZE_LENGTH..end_offset);
        let fb_footer = unsafe { root_unchecked::<footer::Footer>(&footer_bytes) };

        let fb_layout = fb_footer
            .layout()
            .ok_or_else(|| vortex_err!("Footer must contain a layout"))?;
        let loc = fb_layout._tab.loc();
        self.layout_serde
            .read_layout(footer_bytes, loc, self.row_count()?, scan, message_cache)
    }

    pub fn dtype_bytes(&self) -> Bytes {
        let start_offset = self.leftovers_schema_offset();
        let end_offset = self.leftovers_layout_offset();
        self.leftovers
            .slice(start_offset + FLATBUFFER_SIZE_LENGTH..end_offset)
    }

    pub fn dtype(&self) -> VortexResult<DType> {
        Ok(IPCDType::read_flatbuffer(&self.fb_schema()?)?.0)
    }

    pub fn schema(&self) -> VortexResult<Schema> {
        self.dtype().map(Schema::new)
    }

    pub fn projected_dtype(&self, projection: &[Field]) -> VortexResult<DType> {
        let fb_dtype = self
            .fb_schema()?
            .dtype()
            .ok_or_else(|| vortex_err!(InvalidSerde: "Schema missing DType"))?;
        deserialize_and_project(fb_dtype, projection)
    }

    fn fb_footer(&self) -> VortexResult<footer::Footer> {
        let start_offset = self.leftovers_layout_offset();
        let end_offset = self.leftovers.len() - FILE_POSTSCRIPT_SIZE;
        let footer_bytes = &self.leftovers[start_offset + FLATBUFFER_SIZE_LENGTH..end_offset];
        Ok(root::<footer::Footer>(footer_bytes)?)
    }

    fn fb_schema(&self) -> VortexResult<fb::Schema> {
        let start_offset = self.leftovers_schema_offset();
        let end_offset = self.leftovers_layout_offset();
        let dtype_bytes = &self.leftovers[start_offset + FLATBUFFER_SIZE_LENGTH..end_offset];

        root::<fb::Message>(dtype_bytes)
            .map_err(|e| e.into())
            .and_then(|m| {
                m.header_as_schema()
                    .ok_or_else(|| vortex_err!("Message was not a schema"))
            })
    }
}

pub struct FooterReader {
    layout_serde: LayoutDeserializer,
}

impl FooterReader {
    pub fn new(layout_serde: LayoutDeserializer) -> Self {
        Self { layout_serde }
    }

    pub async fn read_footer<R: VortexReadAt>(
        &self,
        read: &R,
        file_size: u64,
    ) -> VortexResult<Footer> {
        if file_size < FILE_POSTSCRIPT_SIZE as u64 {
            vortex_bail!(
                "Malformed vortex file, size {} must be at least {}",
                file_size,
                FILE_POSTSCRIPT_SIZE,
            )
        }

        let read_size = INITIAL_READ_SIZE.min(file_size as usize);
        let mut buf = BytesMut::with_capacity(read_size);
        unsafe { buf.set_len(read_size) }

        let read_offset = file_size - read_size as u64;
        buf = read.read_at_into(read_offset, buf).await?;

        let magic_bytes_loc = read_size - MAGIC_BYTES.len();

        let magic_number = &buf[magic_bytes_loc..];
        if magic_number != MAGIC_BYTES {
            vortex_bail!("Malformed file, invalid magic bytes, got {magic_number:?}")
        }

        let layout_offset =
            u64::from_le_bytes(buf[magic_bytes_loc - 8..magic_bytes_loc].try_into()?);
        let schema_offset =
            u64::from_le_bytes(buf[magic_bytes_loc - 16..magic_bytes_loc - 8].try_into()?);

        Ok(Footer {
            schema_offset,
            layout_offset,
            leftovers: buf.freeze(),
            leftovers_offset: read_offset,
            layout_serde: self.layout_serde.clone(),
        })
    }
}
