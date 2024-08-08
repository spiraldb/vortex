use bytes::Bytes;
use flatbuffers::root;
use vortex_dtype::DType;
use vortex_error::VortexResult;
use vortex_flatbuffers::ReadFlatBuffer;

use crate::file::layouts::Layout;
use crate::file::FULL_FOOTER_SIZE;
use crate::messages::IPCDType;

pub struct Footer {
    pub(crate) schema_offset: u64,
    /// This is actually layouts
    pub(crate) footer_offset: u64,
    pub(crate) leftovers: Bytes,
    pub(crate) leftovers_offset: u64,
}

impl Footer {
    pub fn leftovers_footer_offset(&self) -> usize {
        (self.footer_offset - self.leftovers_offset) as usize
    }

    pub fn leftovers_schema_offset(&self) -> usize {
        (self.schema_offset - self.leftovers_offset) as usize
    }

    pub fn layout(&self) -> VortexResult<Layout> {
        let start_offset = self.leftovers_footer_offset();
        let end_offset = self.leftovers.len() - FULL_FOOTER_SIZE;
        let layout_bytes = &self.leftovers[start_offset..end_offset];
        let fb_footer = root::<vortex_flatbuffers::footer::Footer>(layout_bytes)?;
        let fb_layout = fb_footer.layout().expect("Footer must contain a layout");

        Layout::try_from(fb_layout)
    }

    pub fn dtype(&self) -> VortexResult<DType> {
        let start_offset = self.leftovers_schema_offset();
        let end_offset = self.leftovers_footer_offset();
        let dtype_bytes = &self.leftovers[start_offset..end_offset];

        Ok(IPCDType::read_flatbuffer(&root::<vortex_flatbuffers::message::Schema>(dtype_bytes)?)?.0)
    }
}
