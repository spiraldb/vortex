use std::sync::Arc;

use arrow_array::RecordBatch;

use crate::array::struct_::StructArray;
use crate::array::{Array, ArrayRef, IntoArray};
use crate::encode::FromArrowArray;

impl IntoArray for &RecordBatch {
    fn to_array_data(self) -> ArrayRef {
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
        .to_array_data()
    }
}
