use arrow_array::RecordBatch;
use itertools::Itertools;

use crate::array::StructArray;
use crate::arrow::FromArrowArray;
use crate::validity::Validity;
use crate::Array;

impl From<RecordBatch> for Array {
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
                .map(|(array, field)| Array::from_arrow(array.clone(), field.is_nullable()))
                .collect(),
            value.num_rows(),
            Validity::AllValid,
        )
        .unwrap()
        .into()
    }
}
