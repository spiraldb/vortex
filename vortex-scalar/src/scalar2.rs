use vortex_dtype::DType;

use crate::value::ScalarValue;

pub struct Scalar<'a> {
    dtype: DType,
    value: ScalarValue<'a>,
}
