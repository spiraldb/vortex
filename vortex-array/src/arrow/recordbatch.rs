use std::sync::Arc;

use arrow_array::RecordBatch;

use crate::array::r#struct::StructArray;
use crate::arrow::FromArrowArray;
use crate::{ArrayData, IntoArray, IntoArrayData, ToArrayData};

impl ToArrayData for RecordBatch {
    fn to_array_data(&self) -> ArrayData {
        StructArray::try_new(
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
                .map(|(array, field)| {
                    ArrayData::from_arrow(array.clone(), field.is_nullable()).into_array()
                })
                .collect(),
            self.num_rows(),
        )
        .unwrap()
        .into_array_data()
    }
}
