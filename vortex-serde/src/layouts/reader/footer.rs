use bytes::Bytes;
use flatbuffers::root;
use vortex_dtype::DType;
use vortex_error::VortexResult;
use vortex_flatbuffers::ReadFlatBuffer;

use crate::layouts::reader::context::LayoutReader;
use crate::layouts::reader::{Layout, RelativeLayoutCache, Scan};
use crate::messages::IPCDType;

pub const FULL_FOOTER_SIZE: usize = 20;

pub struct Footer {
    pub(crate) schema_offset: u64,
    /// This is actually layouts
    pub(crate) footer_offset: u64,
    pub(crate) leftovers: Bytes,
    pub(crate) leftovers_offset: u64,
    pub(crate) layout_serde: LayoutReader,
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
        let end_offset = self.leftovers.len() - FULL_FOOTER_SIZE;
        let footer_bytes = self.leftovers.slice(start_offset..end_offset);
        let fb_footer = root::<crate::flatbuffers::footer::Footer>(&footer_bytes)?;

        let fb_layout = fb_footer.layout().expect("Footer must contain a layout");
        let loc = fb_layout._tab.loc();
        self.layout_serde
            .read(footer_bytes, loc, scan, message_cache)
    }

    pub fn dtype(&self) -> VortexResult<DType> {
        let start_offset = self.leftovers_schema_offset();
        let end_offset = self.leftovers_footer_offset();
        let dtype_bytes = &self.leftovers[start_offset..end_offset];

        Ok(IPCDType::read_flatbuffer(&root::<crate::flatbuffers::serde::Schema>(dtype_bytes)?)?.0)
    }
}
