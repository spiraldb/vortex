use datafusion_common::ScalarValue;
use vortex::array::make_temporal_ext_dtype;
use vortex_dtype::{DType, Nullability};
use vortex_scalar::{PValue, Scalar};

pub fn dfvalue_to_scalar(value: ScalarValue) -> Scalar {
    match value {
        ScalarValue::Null => Some(Scalar::null(DType::Null)),
        ScalarValue::Boolean(b) => b.map(Scalar::from),
        ScalarValue::Float16(f) => f.map(Scalar::from),
        ScalarValue::Float32(f) => f.map(Scalar::from),
        ScalarValue::Float64(f) => f.map(Scalar::from),
        ScalarValue::Int8(i) => i.map(Scalar::from),
        ScalarValue::Int16(i) => i.map(Scalar::from),
        ScalarValue::Int32(i) => i.map(Scalar::from),
        ScalarValue::Int64(i) => i.map(Scalar::from),
        ScalarValue::UInt8(i) => i.map(Scalar::from),
        ScalarValue::UInt16(i) => i.map(Scalar::from),
        ScalarValue::UInt32(i) => i.map(Scalar::from),
        ScalarValue::UInt64(i) => i.map(Scalar::from),
        ScalarValue::Utf8(s) => s.as_ref().map(|s| Scalar::from(s.as_str())),
        ScalarValue::Utf8View(s) => s.as_ref().map(|s| Scalar::from(s.as_str())),
        ScalarValue::LargeUtf8(s) => s.as_ref().map(|s| Scalar::from(s.as_str())),
        ScalarValue::Binary(b) => b.as_ref().map(|b| Scalar::from(b.clone())),
        ScalarValue::BinaryView(b) => b.as_ref().map(|b| Scalar::from(b.clone())),
        ScalarValue::LargeBinary(b) => b.as_ref().map(|b| Scalar::from(b.clone())),
        ScalarValue::FixedSizeBinary(_, b) => b.map(|b| Scalar::from(b.clone())),
        ScalarValue::Date32(v) => v.map(|i| {
            let ext_dtype = make_temporal_ext_dtype(&value.data_type());
            Scalar::new(
                DType::Extension(ext_dtype, Nullability::Nullable),
                vortex_scalar::ScalarValue::Primitive(PValue::I32(i)),
            )
        }),
        _ => unimplemented!("Can't convert {value:?} value to a Vortex scalar"),
    }
    .unwrap_or(Scalar::null(DType::Null))
}
