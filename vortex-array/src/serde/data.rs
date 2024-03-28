use arrow_buffer::Buffer;

#[allow(dead_code)]
#[derive(Debug)]
pub struct SomeData {
    metadata: Buffer,
    buffers: Vec<Buffer>,
}

impl SomeData {
    pub fn new(metadata: Buffer, buffers: Vec<Buffer>) -> Self {
        Self { metadata, buffers }
    }
}

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
    metadata: Buffer,
    buffers: Vec<Buffer>,
}

impl ColumnData {
    pub fn new(metadata: Buffer, buffers: Vec<Buffer>) -> Self {
        Self { metadata, buffers }
    }

    pub fn metadata(&self) -> &Buffer {
        &self.metadata
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
