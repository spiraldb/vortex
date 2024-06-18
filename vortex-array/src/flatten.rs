use std::sync::Arc;

use arrow_array::types::{
    Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type,
    UInt32Type, UInt64Type, UInt8Type,
};
use arrow_array::{
    ArrayRef, ArrowPrimitiveType, BinaryArray, BooleanArray as ArrowBoolArray, LargeBinaryArray,
    LargeStringArray, NullArray as ArrowNullArray, PrimitiveArray as ArrowPrimitiveArray,
    StringArray, StructArray as ArrowStructArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use arrow_buffer::ScalarBuffer;
use arrow_schema::{Field, Fields};
use vortex_dtype::{DType, PType};
use vortex_error::VortexResult;

use crate::array::bool::BoolArray;
use crate::array::datetime::{LocalDateTimeArray, TimeUnit};
use crate::array::extension::ExtensionArray;
use crate::array::null::NullArray;
use crate::array::primitive::PrimitiveArray;
use crate::array::r#struct::StructArray;
use crate::array::varbin::VarBinArray;
use crate::arrow::wrappers::as_offset_buffer;
use crate::compute::cast::cast;
use crate::encoding::ArrayEncoding;
use crate::validity::ArrayValidity;
use crate::{Array, ArrayDType, ArrayTrait, IntoArray, ToArray};

/// The set of encodings that can be converted to Arrow with zero-copy.
pub enum Flattened {
    Null(NullArray),
    Bool(BoolArray),
    Primitive(PrimitiveArray),
    Struct(StructArray),
    VarBin(VarBinArray),
    // TODO(aduffy): VarBinView is being disabled until execution engines improve their
    //  support for them, or we build better execution kernels of our own.
    //  Should re-enable once DataFusion completes support for them, tracked in
    //  https://github.com/apache/datafusion/issues/10918
    // VarBinView(VarBinViewArray),
    Extension(ExtensionArray),
}

impl Flattened {
    /// Convert a flat array into its equivalent [ArrayRef](Arrow array).
    ///
    /// Scalar arrays such as Bool and Primitive flattened arrays though should convert with
    /// zero copies, while more complex variants such as Struct may require allocations if its child
    /// arrays require decompression.
    pub fn into_arrow(self) -> ArrayRef {
        match self {
            Flattened::Null(a) => null_to_arrow(a),
            Flattened::Bool(a) => bool_to_arrow(a),
            Flattened::Primitive(a) => primitive_to_arrow(a),
            Flattened::Struct(a) => struct_to_arrow(a),
            Flattened::VarBin(a) => varbin_to_arrow(a),
            Flattened::Extension(a) => match a.id().as_ref() {
                "vortex.localdatetime" => local_date_time_to_arrow(
                    LocalDateTimeArray::try_from(&a.into_array()).expect("localdatetime"),
                ),
                _ => panic!("unsupported extension dtype with ID {}", a.id().as_ref()),
            },
        }
    }
}

fn null_to_arrow(null_array: NullArray) -> ArrayRef {
    Arc::new(ArrowNullArray::new(null_array.len()))
}

fn bool_to_arrow(bool_array: BoolArray) -> ArrayRef {
    Arc::new(ArrowBoolArray::new(
        bool_array.boolean_buffer(),
        bool_array
            .logical_validity()
            .to_null_buffer()
            .expect("null buffer"),
    ))
}

fn primitive_to_arrow(primitive_array: PrimitiveArray) -> ArrayRef {
    fn as_arrow_array_primitive<T: ArrowPrimitiveType>(
        array: &PrimitiveArray,
    ) -> ArrowPrimitiveArray<T> {
        ArrowPrimitiveArray::new(
            ScalarBuffer::<T::Native>::new(array.buffer().clone().into(), 0, array.len()),
            array
                .logical_validity()
                .to_null_buffer()
                .expect("null buffer"),
        )
    }

    match primitive_array.ptype() {
        PType::U8 => Arc::new(as_arrow_array_primitive::<UInt8Type>(&primitive_array)),
        PType::U16 => Arc::new(as_arrow_array_primitive::<UInt16Type>(&primitive_array)),
        PType::U32 => Arc::new(as_arrow_array_primitive::<UInt32Type>(&primitive_array)),
        PType::U64 => Arc::new(as_arrow_array_primitive::<UInt64Type>(&primitive_array)),
        PType::I8 => Arc::new(as_arrow_array_primitive::<Int8Type>(&primitive_array)),
        PType::I16 => Arc::new(as_arrow_array_primitive::<Int16Type>(&primitive_array)),
        PType::I32 => Arc::new(as_arrow_array_primitive::<Int32Type>(&primitive_array)),
        PType::I64 => Arc::new(as_arrow_array_primitive::<Int64Type>(&primitive_array)),
        PType::F16 => Arc::new(as_arrow_array_primitive::<Float16Type>(&primitive_array)),
        PType::F32 => Arc::new(as_arrow_array_primitive::<Float32Type>(&primitive_array)),
        PType::F64 => Arc::new(as_arrow_array_primitive::<Float64Type>(&primitive_array)),
    }
}

fn struct_to_arrow(struct_array: StructArray) -> ArrayRef {
    let field_arrays: Vec<ArrayRef> = struct_array
        .children()
        .map(|f| f.flatten().unwrap().into_arrow())
        .collect();

    let arrow_fields: Fields = struct_array
        .names()
        .iter()
        .zip(field_arrays.iter())
        .zip(struct_array.dtypes().iter())
        .map(|((name, arrow_field), vortex_field)| {
            Field::new(
                &**name,
                arrow_field.data_type().clone(),
                vortex_field.is_nullable(),
            )
        })
        .map(Arc::new)
        .collect();

    Arc::new(ArrowStructArray::new(arrow_fields, field_arrays, None))
}

fn varbin_to_arrow(varbin_array: VarBinArray) -> ArrayRef {
    let offsets = varbin_array
        .offsets()
        .flatten_primitive()
        .expect("flatten_primitive");
    let offsets = match offsets.ptype() {
        PType::I32 | PType::I64 => offsets,
        // Unless it's u64, everything else can be converted into an i32.
        // FIXME(ngates): do not copy offsets again
        PType::U64 => cast(&offsets.to_array(), PType::I64.into())
            .expect("cast to i64")
            .flatten_primitive()
            .expect("flatten_primitive"),
        _ => cast(&offsets.to_array(), PType::I32.into())
            .expect("cast to i32")
            .flatten_primitive()
            .expect("flatten_primitive"),
    };
    let nulls = varbin_array
        .logical_validity()
        .to_null_buffer()
        .expect("null buffer");

    let data = varbin_array
        .bytes()
        .flatten_primitive()
        .expect("flatten_primitive");
    assert_eq!(data.ptype(), PType::U8);
    let data = data.buffer();

    // Switch on Arrow DType.
    match varbin_array.dtype() {
        DType::Binary(_) => match offsets.ptype() {
            PType::I32 => Arc::new(BinaryArray::new(
                as_offset_buffer::<i32>(offsets),
                data.into(),
                nulls,
            )),
            PType::I64 => Arc::new(LargeBinaryArray::new(
                as_offset_buffer::<i64>(offsets),
                data.into(),
                nulls,
            )),
            _ => panic!("Invalid offsets type"),
        },
        DType::Utf8(_) => match offsets.ptype() {
            PType::I32 => Arc::new(StringArray::new(
                as_offset_buffer::<i32>(offsets),
                data.into(),
                nulls,
            )),
            PType::I64 => Arc::new(LargeStringArray::new(
                as_offset_buffer::<i64>(offsets),
                data.into(),
                nulls,
            )),
            _ => panic!("Invalid offsets type"),
        },
        _ => panic!(
            "expected utf8 or binary instead of {}",
            varbin_array.dtype()
        ),
    }
}

fn local_date_time_to_arrow(local_date_time_array: LocalDateTimeArray) -> ArrayRef {
    // A LocalDateTime maps to an Arrow Timestamp array with no timezone.
    let timestamps = cast(&local_date_time_array.timestamps(), PType::I64.into())
        .expect("timestamps must cast to i64")
        .flatten_primitive()
        .expect("must be i64 array");
    let validity = timestamps
        .logical_validity()
        .to_null_buffer()
        .expect("null buffer");
    let timestamps_len = timestamps.len();
    let buffer = ScalarBuffer::<i64>::new(timestamps.into_buffer().into(), 0, timestamps_len);

    match local_date_time_array.time_unit() {
        TimeUnit::Ns => Arc::new(TimestampNanosecondArray::new(buffer, validity)),
        TimeUnit::Us => Arc::new(TimestampMicrosecondArray::new(buffer, validity)),
        TimeUnit::Ms => Arc::new(TimestampMillisecondArray::new(buffer, validity)),
        TimeUnit::S => Arc::new(TimestampSecondArray::new(buffer, validity)),
    }
}

/// Support trait for transmuting an array into its [vortex_dtype::DType]'s canonical encoding.
///
/// Flattening an Array ensures that the array's encoding matches one of the builtin canonical
/// encodings, each of which has a corresponding [Flattened] variant.
///
/// **Important**: DType remains the same before and after a flatten operation.
pub trait ArrayFlatten {
    fn flatten(self) -> VortexResult<Flattened>;
}

impl Array {
    pub fn flatten(self) -> VortexResult<Flattened> {
        ArrayEncoding::flatten(self.encoding(), self)
    }

    pub fn flatten_extension(self) -> VortexResult<ExtensionArray> {
        ExtensionArray::try_from(self.flatten()?.into_array())
    }

    pub fn flatten_bool(self) -> VortexResult<BoolArray> {
        BoolArray::try_from(self.flatten()?.into_array())
    }

    pub fn flatten_primitive(self) -> VortexResult<PrimitiveArray> {
        PrimitiveArray::try_from(self.flatten()?.into_array())
    }

    pub fn flatten_varbin(self) -> VortexResult<VarBinArray> {
        VarBinArray::try_from(self.flatten()?.into_array())
    }
}

impl IntoArray for Flattened {
    fn into_array(self) -> Array {
        match self {
            Self::Null(a) => a.into_array(),
            Self::Bool(a) => a.into_array(),
            Self::Primitive(a) => a.into_array(),
            Self::Struct(a) => a.into_array(),
            Self::VarBin(a) => a.into_array(),
            Self::Extension(a) => a.into_array(),
        }
    }
}
