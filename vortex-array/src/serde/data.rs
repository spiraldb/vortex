use crate::array::Array;
use crate::encoding::EncodingId;
use crate::walk::ArrayWalker;
use arrow_buffer::Buffer;
use vortex_error::{VortexError, VortexResult};

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
    children: Vec<ColumnData>,
    buffers: Vec<Buffer>,
}

impl ColumnData {
    pub fn try_from_array(array: &dyn Array) -> VortexResult<Self> {
        let mut data = ColumnData {
            encoding: array.encoding().id(),
            metadata: array
                .serde()
                .ok_or_else(|| {
                    VortexError::InvalidSerde(
                        format!("Array {} does not support serde", array.encoding()).into(),
                    )
                })?
                .metadata()?
                .map(Buffer::from_vec),
            children: Vec::new(),
            buffers: Vec::new(),
        };
        array.walk(&mut data)?;
        Ok(data)
    }

    pub fn new(
        encoding: EncodingId,
        metadata: Option<Buffer>,
        children: Vec<ColumnData>,
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

    pub fn children(&self) -> &[ColumnData] {
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

impl ArrayWalker for ColumnData {
    fn visit_child(&mut self, array: &dyn Array) -> VortexResult<()> {
        self.children.push(ColumnData::try_from_array(array)?);
        Ok(())
    }

    fn visit_buffer(&mut self, buffer: &Buffer) -> VortexResult<()> {
        self.buffers.push(buffer.clone());
        Ok(())
    }
}
