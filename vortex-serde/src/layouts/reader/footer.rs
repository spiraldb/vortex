use bytes::Bytes;
use flatbuffers::root;
use vortex_dtype::field::Field;
use vortex_dtype::{deserialize_and_project, DType};
use vortex_error::{vortex_err, VortexResult};
use vortex_flatbuffers::ReadFlatBuffer;

use crate::layouts::reader::cache::RelativeLayoutCache;
use crate::layouts::reader::context::LayoutDeserializer;
use crate::layouts::reader::{Layout, Scan, FILE_POSTSCRIPT_SIZE};
use crate::messages::IPCDType;

pub struct Footer {
    pub(crate) schema_offset: u64,
    /// This is actually layouts
    pub(crate) footer_offset: u64,
    pub(crate) leftovers: Bytes,
    pub(crate) leftovers_offset: u64,
    pub(crate) layout_serde: LayoutDeserializer,
}

impl Footer {
    pub fn leftovers_footer_offset(&self) -> usize {
        (self.footer_offset - self.leftovers_offset) as usize
    }

    pub fn leftovers_schema_offset(&self) -> usize {
        (self.schema_offset - self.leftovers_offset) as usize
    }

    pub fn layout(
        &self,
        scan: Scan,
        message_cache: RelativeLayoutCache,
    ) -> VortexResult<Box<dyn Layout>> {
        let start_offset = self.leftovers_footer_offset();
        let end_offset = self.leftovers.len() - FILE_POSTSCRIPT_SIZE;
        let footer_bytes = self.leftovers.slice(start_offset..end_offset);
        let fb_footer = root::<vortex_flatbuffers::footer::Footer>(&footer_bytes)?;

        let fb_layout = fb_footer
            .layout()
            .ok_or_else(|| vortex_err!("Footer must contain a layout"))?;
        let loc = fb_layout._tab.loc();
        self.layout_serde
            .read_layout(footer_bytes, loc, scan, message_cache)
    }

    pub fn dtype(&self) -> VortexResult<DType> {
        let start_offset = self.leftovers_schema_offset();
        let end_offset = self.leftovers_footer_offset();
        let dtype_bytes = &self.leftovers[start_offset..end_offset];

        Ok(
            IPCDType::read_flatbuffer(&root::<vortex_flatbuffers::message::Schema>(dtype_bytes)?)?
                .0,
        )
    }

    pub fn projected_dtype(&self, projection: &[Field]) -> VortexResult<DType> {
        let start_offset = self.leftovers_schema_offset();
        let end_offset = self.leftovers_footer_offset();
        let dtype_bytes = &self.leftovers[start_offset..end_offset];

        let fb_schema = root::<vortex_flatbuffers::message::Schema>(dtype_bytes)?;
        let fb_dtype = fb_schema
            .dtype()
            .ok_or_else(|| vortex_err!(InvalidSerde: "Schema missing DType"))?;
        deserialize_and_project(fb_dtype, projection)
    }
}
