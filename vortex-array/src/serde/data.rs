use crate::array::EncodingId;
use arrow_buffer::Buffer;

pub struct ArrayData {
    columns: Vec<ColumnData>,
}

impl ArrayData {
    pub fn new(columns: Vec<ColumnData>) -> Self {
        Self { columns }
    }

    pub fn columns(&self) -> &[ColumnData] {
        &self.columns
    }
}

#[derive(Debug)]
pub struct ColumnData {
    encoding: EncodingId,
    metadata: Option<Buffer>,
    children: Vec<ChildColumnData>,
    buffers: Vec<Buffer>,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct ChildColumnData {
    data: ColumnData,
    num_buffers: u16,
}

impl ColumnData {
    pub fn new(
        encoding: EncodingId,
        metadata: Option<Buffer>,
        children: Vec<ChildColumnData>,
        buffers: Vec<Buffer>,
    ) -> Self {
        Self {
            encoding,
            metadata,
            children,
            buffers,
        }
    }

    pub fn encoding(&self) -> EncodingId {
        self.encoding
    }

    pub fn metadata(&self) -> Option<&Buffer> {
        self.metadata.as_ref()
    }

    pub fn children(&self) -> &[ChildColumnData] {
        &self.children
    }

    pub fn buffers(&self) -> &[Buffer] {
        &self.buffers
    }

    /// Return the buffer offsets and the total length of all buffers, assuming the given alignment.
    pub fn buffer_offsets(&self, alignment: usize) -> Vec<usize> {
        let mut offsets = Vec::with_capacity(self.buffers.len() + 1);
        let mut offset = 0;
        for buffer in &self.buffers {
            offsets.push(offset);

            let buffer_size = buffer.len();
            let aligned_size = (buffer_size + (alignment - 1)) & !(alignment - 1);
            offset += aligned_size;
        }
        offsets.push(offset);
        offsets
    }
}

#[allow(dead_code)]
pub struct ColumnDataBuilder {
    buffers: Vec<Buffer>,
}

#[allow(dead_code)]
impl ColumnDataBuilder {
    pub fn new() -> Self {
        Self {
            buffers: Vec::new(),
        }
    }
}
