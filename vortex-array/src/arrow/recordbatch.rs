use arrow_array::RecordBatch;
use itertools::Itertools;

use crate::array::r#struct::StructArray;
use crate::arrow::FromArrowArray;
use crate::validity::Validity;
use crate::{ArrayData, IntoArray, IntoArrayData, ToArrayData};

impl ToArrayData for RecordBatch {
    fn to_array_data(&self) -> ArrayData {
        StructArray::try_new(
            self.schema()
                .fields()
                .iter()
                .map(|f| f.name().as_str().into())
                .collect_vec()
                .into(),
            self.columns()
                .iter()
                .zip(self.schema().fields())
                .map(|(array, field)| {
                    ArrayData::from_arrow(array.clone(), field.is_nullable()).into_array()
                })
                .collect(),
            self.num_rows(),
            Validity::AllValid,
        )
        .unwrap()
        .into_array_data()
    }
}
