use bytes::{Bytes, BytesMut};
use flatbuffers::root;
use vortex_dtype::field::Field;
use vortex_dtype::flatbuffers::deserialize_and_project;
use vortex_dtype::DType;
use vortex_error::{vortex_bail, vortex_err, VortexResult};
use vortex_flatbuffers::message as fb;

use crate::io::VortexReadAt;
use crate::layouts::read::cache::RelativeLayoutCache;
use crate::layouts::read::context::LayoutDeserializer;
use crate::layouts::read::{LayoutReader, Scan, FILE_POSTSCRIPT_SIZE, INITIAL_READ_SIZE};
use crate::layouts::MAGIC_BYTES;
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
    pub(crate) footer_offset: u64,
    pub(crate) leftovers: Bytes,
    pub(crate) leftovers_offset: u64,
    pub(crate) layout_serde: LayoutDeserializer,
}

impl Footer {
    fn leftovers_layout_offset(&self) -> usize {
        (self.footer_offset - self.leftovers_offset) as usize
    }

    fn leftovers_schema_offset(&self) -> usize {
        (self.schema_offset - self.leftovers_offset) as usize
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
        let fb_footer = root::<vortex_flatbuffers::footer::Footer>(&footer_bytes)?;

        let fb_layout = fb_footer
            .layout()
            .ok_or_else(|| vortex_err!("Footer must contain a layout"))?;
        let loc = fb_layout._tab.loc();
        self.layout_serde
            .read_layout(footer_bytes, loc, scan, message_cache)
    }

    pub fn dtype_bytes(&self) -> VortexResult<Bytes> {
        let start_offset = self.leftovers_schema_offset();
        let end_offset = self.leftovers_layout_offset();
        let bytes = self
            .leftovers
            .slice(start_offset + FLATBUFFER_SIZE_LENGTH..end_offset);
        // Run validation on dtype bytes
        self.fb_schema()?;
        Ok(bytes)
    }

    pub fn dtype(&self) -> VortexResult<DType> {
        DType::try_from(
            self.fb_schema()?
                .dtype()
                .ok_or_else(|| vortex_err!(InvalidSerde: "Schema missing DType"))?,
        )
        .map_err(|e| vortex_err!(InvalidSerde: "Failed to parse DType: {}", e))
    }

    pub fn projected_dtype(&self, projection: &[Field]) -> VortexResult<DType> {
        let fb_dtype = self
            .fb_schema()?
            .dtype()
            .ok_or_else(|| vortex_err!(InvalidSerde: "Schema missing DType"))?;
        deserialize_and_project(fb_dtype, projection)
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

        let ps = root::<vortex_flatbuffers::footer::Postscript>(
            &buf[magic_bytes_loc - 32..magic_bytes_loc],
        )?;

        Ok(Footer {
            schema_offset: ps.schema_offset(),
            footer_offset: ps.footer_offset(),
            leftovers: buf.freeze(),
            leftovers_offset: read_offset,
            layout_serde: self.layout_serde.clone(),
        })
    }
}
