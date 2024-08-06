use arrow_array::RecordBatch;
use itertools::Itertools;

use crate::array::struct_::StructArray;
use crate::arrow::FromArrowArray;
use crate::validity::Validity;
use crate::{Array, ArrayData};

impl From<RecordBatch> for ArrayData {
    fn from(value: RecordBatch) -> Self {
        StructArray::try_new(
            value
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().as_str().into())
                .collect_vec()
                .into(),
            value
                .columns()
                .iter()
                .zip(value.schema().fields())
                .map(|(array, field)| {
                    ArrayData::from_arrow(array.clone(), field.is_nullable()).into()
                })
                .collect(),
            value.num_rows(),
            Validity::AllValid,
        )
        .unwrap()
        .into()
    }
}

impl From<RecordBatch> for Array {
    fn from(value: RecordBatch) -> Self {
        let data = ArrayData::from(value);
        data.into()
    }
}
