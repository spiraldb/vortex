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

pub struct ColumnData {
    buffers: Vec<Buffer>,
}

impl ColumnData {
    pub fn new(buffers: Vec<Buffer>) -> Self {
        Self { buffers }
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
