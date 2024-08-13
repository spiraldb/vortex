use arrow_array::cast::as_struct_array;
use arrow_array::RecordBatch;
use itertools::Itertools;
use vortex_error::{VortexError, VortexResult};

use crate::array::StructArray;
use crate::arrow::FromArrowArray;
use crate::validity::Validity;
use crate::{Array, IntoArray, IntoCanonical};

impl TryFrom<RecordBatch> for Array {
    type Error = VortexError;

    fn try_from(value: RecordBatch) -> VortexResult<Self> {
        Ok(StructArray::try_new(
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
        )?
        .into())
    }
}

impl TryFrom<Array> for RecordBatch {
    type Error = VortexError;

    fn try_from(value: Array) -> VortexResult<Self> {
        let array_ref = value.into_canonical()?.into_arrow()?;
        let struct_array = as_struct_array(array_ref.as_ref());
        Ok(RecordBatch::from(struct_array))
    }
}

impl TryFrom<StructArray> for RecordBatch {
    type Error = VortexError;

    fn try_from(value: StructArray) -> VortexResult<Self> {
        RecordBatch::try_from(value.into_array())
    }
}
