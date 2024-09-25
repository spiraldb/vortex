use bytes::Bytes;
use flatbuffers::root;
use vortex_dtype::field::Field;
use vortex_dtype::flatbuffers::{deserialize_and_project, resolve_field_references};
use vortex_dtype::DType;
use vortex_error::{vortex_err, VortexResult};
use vortex_flatbuffers::{message as fb, ReadFlatBuffer};
use vortex_schema::Schema;

use crate::layouts::read::cache::RelativeLayoutCache;
use crate::layouts::read::context::LayoutDeserializer;
use crate::layouts::read::{LayoutReader, Scan, FILE_POSTSCRIPT_SIZE};
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

    /// Convert all name based references to index based for sake of augmenting read projection
    pub(crate) fn resolve_references(&self, projection: &[Field]) -> VortexResult<Vec<Field>> {
        let dtype = self
            .fb_schema()?
            .dtype()
            .ok_or_else(|| vortex_err!(InvalidSerde: "Schema missing DType"))?;
        let fb_struct = dtype
            .type__as_struct_()
            .ok_or_else(|| vortex_err!("The top-level type should be a struct"))?;
        resolve_field_references(fb_struct, projection)
            .map(|idx| idx.map(Field::from))
            .collect::<VortexResult<Vec<_>>>()
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
