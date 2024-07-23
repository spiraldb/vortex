use bytes::Bytes;

pub(crate) struct Footer {
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
}
