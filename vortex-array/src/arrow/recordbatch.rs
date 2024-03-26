use crate::array::struct_::StructArray;
use crate::array::{Array, ArrayRef, IntoArray};
use crate::encode::FromArrowArray;
use arrow_array::RecordBatch;
use std::sync::Arc;

impl IntoArray for &RecordBatch {
    fn into_array(self) -> ArrayRef {
        StructArray::new(
            self.schema()
                .fields()
                .iter()
                .map(|f| f.name())
                .map(|s| s.to_owned())
                .map(Arc::new)
                .collect(),
            self.columns()
                .iter()
                .zip(self.schema().fields())
                .map(|(array, field)| ArrayRef::from_arrow(array.clone(), field.is_nullable()))
                .collect(),
        )
        .into_array()
    }
}
