use crate::array2::{ArrayData, ArrayMetadata};

impl ArrayData {
    pub fn metadata(&self) -> Option<&[u8]> {
        self.metadata.as_deref()
    }
}

#[allow(dead_code)]
pub struct TypedArrayData<M: ArrayMetadata> {
    data: ArrayData,
    metadata: M,
}
