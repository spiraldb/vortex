use arrow_array::cast::as_struct_array;
use arrow_array::RecordBatch;
use itertools::Itertools;

use crate::array::StructArray;
use crate::arrow::FromArrowArray;
use crate::validity::Validity;
use crate::{Array, IntoArrayVariant, IntoCanonical};

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
            Validity::NonNullable, // Must match FromArrowType<SchemaRef> for DType
        )
        .unwrap()
        .into()
    }
}

impl From<Array> for RecordBatch {
    fn from(value: Array) -> Self {
        let struct_arr = value
            .into_struct()
            .expect("RecordBatch can only be constructed from a Vortex StructArray");
        Self::from(struct_arr)
    }
}

impl From<StructArray> for RecordBatch {
    fn from(value: StructArray) -> Self {
        let array_ref = value
            .into_canonical()
            .expect("Struct arrays must canonicalize")
            .into_arrow();
        let struct_array = as_struct_array(array_ref.as_ref());
        Self::from(struct_array)
    }
}
