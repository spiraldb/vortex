use std::sync::Arc;

use arrow_array::RecordBatch;

use crate::array::struct_::StructArray;
use crate::array::{ArrayRef, IntoArray, OwnedArray};
use crate::encode::FromArrowArray;

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
            self.num_rows(),
        )
        .into_array()
    }
}
