use std::sync::Arc;

use arrow_array::types::{
    Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type,
    UInt32Type, UInt64Type, UInt8Type,
};
use arrow_array::{
    ArrayRef, ArrowPrimitiveType, BinaryArray, BooleanArray as ArrowBoolArray, Date32Array,
    Date64Array, LargeBinaryArray, LargeStringArray, NullArray as ArrowNullArray,
    PrimitiveArray as ArrowPrimitiveArray, StringArray, StructArray as ArrowStructArray,
    Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow_buffer::ScalarBuffer;
use arrow_schema::{Field, Fields};
use vortex_dtype::{DType, NativePType, PType};
use vortex_error::{vortex_bail, VortexResult};

use crate::array::bool::BoolArray;
use crate::array::datetime::temporal::{is_temporal_ext_type, TemporalMetadata};
use crate::array::datetime::{TemporalArray, TimeUnit};
use crate::array::extension::ExtensionArray;
use crate::array::null::NullArray;
use crate::array::primitive::PrimitiveArray;
use crate::array::struct_::StructArray;
use crate::array::varbin::VarBinArray;
use crate::arrow::wrappers::as_offset_buffer;
use crate::compute::unary::try_cast;
use crate::encoding::ArrayEncoding;
use crate::validity::ArrayValidity;
use crate::variants::StructArrayTrait;
use crate::{Array, ArrayDType, IntoArray, ToArray};

/// The set of canonical array encodings, also the set of encodings that can be transferred to
/// Arrow with zero-copy.
///
/// Note that a canonical form is not recursive, i.e. a StructArray may contain non-canonical
/// child arrays, which may themselves need to be [canonicalized](IntoCanonical).
///
/// # Logical vs. Physical encodings
///
/// Vortex separates logical and physical types, however this creates ambiguity with Arrow, there is
/// no separation. Thus, if you receive an Arrow array, compress it using Vortex, and then
/// decompress it later to pass to a compute kernel, there are multiple suitable Arrow array
/// variants to hold the data.
///
/// To disambiguate, we choose a canonical physical encoding for every Vortex [`DType`], which
/// will correspond to an arrow-rs [`arrow_schema::DataType`].
///
/// # Views support
///
/// Binary and String views are a new, better encoding format for nearly all use-cases. For now,
/// because DataFusion does not include pervasive support for compute over StringView, we opt to use
/// the [`VarBinArray`] as the canonical encoding (which corresponds to the Arrow `BinaryViewArray`).
///
/// We expect to change this soon once DataFusion is able to finish up some initial support, which
/// is tracked in <https://github.com/apache/datafusion/issues/10918>.
#[derive(Debug, Clone)]
pub enum Canonical {
    Null(NullArray),
    Bool(BoolArray),
    Primitive(PrimitiveArray),
    Struct(StructArray),
    VarBin(VarBinArray),
    // TODO(aduffy): switch to useing VarBinView instead of VarBin
    // VarBinView(VarBinViewArray),
    Extension(ExtensionArray),
}

impl Canonical {
    /// Convert a canonical array into its equivalent [ArrayRef](Arrow array).
    ///
    /// Scalar arrays such as Bool and Primitive canonical arrays should convert with
    /// zero copies, while more complex variants such as Struct may require allocations if its child
    /// arrays require decompression.
    pub fn into_arrow(self) -> ArrayRef {
        match self {
            Canonical::Null(a) => null_to_arrow(a),
            Canonical::Bool(a) => bool_to_arrow(a),
            Canonical::Primitive(a) => primitive_to_arrow(a),
            Canonical::Struct(a) => struct_to_arrow(a),
            Canonical::VarBin(a) => varbin_to_arrow(a),
            Canonical::Extension(a) => {
                if !is_temporal_ext_type(a.id()) {
                    panic!("unsupported extension dtype with ID {}", a.id().as_ref())
                }

                temporal_to_arrow(
                    TemporalArray::try_from(&a.into_array())
                        .expect("array must be known temporal array ext type"),
                )
            }
        }
    }
}

// Unwrap canonical type back down to specialized type.
impl Canonical {
    pub fn into_null(self) -> VortexResult<NullArray> {
        match self {
            Canonical::Null(a) => Ok(a),
            _ => vortex_bail!(InvalidArgument: "cannot unwrap NullArray from {:?}", &self),
        }
    }

    pub fn into_bool(self) -> VortexResult<BoolArray> {
        match self {
            Canonical::Bool(a) => Ok(a),
            _ => vortex_bail!(InvalidArgument: "cannot unwrap BoolArray from {:?}", &self),
        }
    }

    pub fn into_primitive(self) -> VortexResult<PrimitiveArray> {
        match self {
            Canonical::Primitive(a) => Ok(a),
            _ => vortex_bail!(InvalidArgument: "cannot unwrap PrimitiveArray from {:?}", &self),
        }
    }

    pub fn into_struct(self) -> VortexResult<StructArray> {
        match self {
            Canonical::Struct(a) => Ok(a),
            _ => vortex_bail!(InvalidArgument: "cannot unwrap StructArray from {:?}", &self),
        }
    }

    pub fn into_varbin(self) -> VortexResult<VarBinArray> {
        match self {
            Canonical::VarBin(a) => Ok(a),
            _ => vortex_bail!(InvalidArgument: "cannot unwrap VarBinArray from {:?}", &self),
        }
    }

    pub fn into_extension(self) -> VortexResult<ExtensionArray> {
        match self {
            Canonical::Extension(a) => Ok(a),
            _ => vortex_bail!(InvalidArgument: "cannot unwrap ExtensionArray from {:?}", &self),
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
            ScalarBuffer::<T::Native>::new(array.buffer().clone().into_arrow(), 0, array.len()),
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
        .map(|f| {
            let canonical = f.into_canonical().unwrap();
            match canonical {
                // visit nested structs recursively
                Canonical::Struct(a) => struct_to_arrow(a),
                _ => canonical.into_arrow(),
            }
        })
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
        .into_primitive()
        .expect("flatten_primitive");
    let offsets = match offsets.ptype() {
        PType::I32 | PType::I64 => offsets,
        // Unless it's u64, everything else can be converted into an i32.
        // FIXME(ngates): do not copy offsets again
        PType::U64 => try_cast(&offsets.to_array(), PType::I64.into())
            .expect("cast to i64")
            .into_primitive()
            .expect("flatten_primitive"),
        _ => try_cast(&offsets.to_array(), PType::I32.into())
            .expect("cast to i32")
            .into_primitive()
            .expect("flatten_primitive"),
    };
    let nulls = varbin_array
        .logical_validity()
        .to_null_buffer()
        .expect("null buffer");

    let data = varbin_array
        .bytes()
        .into_primitive()
        .expect("flatten_primitive");
    assert_eq!(data.ptype(), PType::U8);
    let data = data.buffer();

    // Switch on Arrow DType.
    match varbin_array.dtype() {
        DType::Binary(_) => match offsets.ptype() {
            PType::I32 => Arc::new(unsafe {
                BinaryArray::new_unchecked(
                    as_offset_buffer::<i32>(offsets),
                    data.clone().into_arrow(),
                    nulls,
                )
            }),
            PType::I64 => Arc::new(unsafe {
                LargeBinaryArray::new_unchecked(
                    as_offset_buffer::<i64>(offsets),
                    data.clone().into_arrow(),
                    nulls,
                )
            }),
            _ => panic!("Invalid offsets type"),
        },
        DType::Utf8(_) => match offsets.ptype() {
            PType::I32 => Arc::new(unsafe {
                StringArray::new_unchecked(
                    as_offset_buffer::<i32>(offsets),
                    data.clone().into_arrow(),
                    nulls,
                )
            }),
            PType::I64 => Arc::new(unsafe {
                LargeStringArray::new_unchecked(
                    as_offset_buffer::<i64>(offsets),
                    data.clone().into_arrow(),
                    nulls,
                )
            }),
            _ => panic!("Invalid offsets type"),
        },
        _ => panic!(
            "expected utf8 or binary instead of {}",
            varbin_array.dtype()
        ),
    }
}

fn temporal_to_arrow(temporal_array: TemporalArray) -> ArrayRef {
    macro_rules! extract_temporal_values {
        ($values:expr, $prim:ty) => {{
            let temporal_values = try_cast(
                &temporal_array.temporal_values(),
                <$prim as NativePType>::PTYPE.into(),
            )
            .expect("values must cast to primitive type")
            .into_primitive()
            .expect("must be primitive array");
            let len = temporal_values.len();
            let nulls = temporal_values
                .logical_validity()
                .to_null_buffer()
                .expect("null buffer");
            let scalars =
                ScalarBuffer::<$prim>::new(temporal_values.into_buffer().into_arrow(), 0, len);

            (scalars, nulls)
        }};
    }

    match temporal_array.temporal_metadata() {
        TemporalMetadata::Date(time_unit) => match time_unit {
            TimeUnit::D => {
                let (scalars, nulls) =
                    extract_temporal_values!(temporal_array.temporal_values(), i32);
                Arc::new(Date32Array::new(scalars, nulls))
            }
            TimeUnit::Ms => {
                let (scalars, nulls) =
                    extract_temporal_values!(temporal_array.temporal_values(), i64);
                Arc::new(Date64Array::new(scalars, nulls))
            }
            _ => panic!("invalid time_unit {time_unit} for vortex.date"),
        },
        TemporalMetadata::Time(time_unit) => match time_unit {
            TimeUnit::S => {
                let (scalars, nulls) =
                    extract_temporal_values!(temporal_array.temporal_values(), i32);
                Arc::new(Time32SecondArray::new(scalars, nulls))
            }
            TimeUnit::Ms => {
                let (scalars, nulls) =
                    extract_temporal_values!(temporal_array.temporal_values(), i32);
                Arc::new(Time32MillisecondArray::new(scalars, nulls))
            }
            TimeUnit::Us => {
                let (scalars, nulls) =
                    extract_temporal_values!(temporal_array.temporal_values(), i64);
                Arc::new(Time64MicrosecondArray::new(scalars, nulls))
            }
            TimeUnit::Ns => {
                let (scalars, nulls) =
                    extract_temporal_values!(temporal_array.temporal_values(), i64);
                Arc::new(Time64NanosecondArray::new(scalars, nulls))
            }
            _ => panic!("invalid TimeUnit for Time32 array {time_unit}"),
        },
        TemporalMetadata::Timestamp(time_unit, _) => {
            let (scalars, nulls) = extract_temporal_values!(temporal_array.temporal_values(), i64);
            match time_unit {
                TimeUnit::Ns => Arc::new(TimestampNanosecondArray::new(scalars, nulls)),
                TimeUnit::Us => Arc::new(TimestampMicrosecondArray::new(scalars, nulls)),
                TimeUnit::Ms => Arc::new(TimestampMillisecondArray::new(scalars, nulls)),
                TimeUnit::S => Arc::new(TimestampSecondArray::new(scalars, nulls)),
                _ => panic!("invalid TimeUnit for Time32 array {time_unit}"),
            }
        }
    }
}

/// Support trait for transmuting an array into its [vortex_dtype::DType]'s canonical encoding.
///
/// This conversion ensures that the array's encoding matches one of the builtin canonical
/// encodings, each of which has a corresponding [Canonical] variant.
///
/// # Invariants
///
/// The DType of the array will be unchanged by canonicalization.
pub trait IntoCanonical {
    fn into_canonical(self) -> VortexResult<Canonical>;
}

/// Trait for types that can be converted from an owned type into an owned array variant.
///
/// # Canonicalization
///
/// This trait has a blanket implementation for all types implementing [IntoCanonical].
pub trait IntoArrayVariant {
    fn into_null(self) -> VortexResult<NullArray>;

    fn into_bool(self) -> VortexResult<BoolArray>;

    fn into_primitive(self) -> VortexResult<PrimitiveArray>;

    fn into_struct(self) -> VortexResult<StructArray>;

    fn into_varbin(self) -> VortexResult<VarBinArray>;

    fn into_extension(self) -> VortexResult<ExtensionArray>;
}

impl<T> IntoArrayVariant for T
where
    T: IntoCanonical,
{
    fn into_null(self) -> VortexResult<NullArray> {
        self.into_canonical()?.into_null()
    }

    fn into_bool(self) -> VortexResult<BoolArray> {
        self.into_canonical()?.into_bool()
    }

    fn into_primitive(self) -> VortexResult<PrimitiveArray> {
        self.into_canonical()?.into_primitive()
    }

    fn into_struct(self) -> VortexResult<StructArray> {
        self.into_canonical()?.into_struct()
    }

    fn into_varbin(self) -> VortexResult<VarBinArray> {
        self.into_canonical()?.into_varbin()
    }

    fn into_extension(self) -> VortexResult<ExtensionArray> {
        self.into_canonical()?.into_extension()
    }
}

/// IntoCanonical implementation for Array.
///
/// Canonicalizing an array requires potentially decompressing, so this requires a roundtrip through
/// the array's internal codec.
impl IntoCanonical for Array {
    fn into_canonical(self) -> VortexResult<Canonical> {
        ArrayEncoding::canonicalize(self.encoding(), self)
    }
}

/// Implement the IntoArray for the [Canonical] type.
///
/// This conversion is always "free" and should not touch underlying data. All it does is create an
/// owned pointer to the underlying concrete array type.
///
/// This combined with the above [IntoCanonical] impl for [Array] allows simple two-way conversions
/// between arbitrary Vortex encodings and canonical Arrow-compatible encodings.
impl IntoArray for Canonical {
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

#[cfg(test)]
mod test {
    use arrow_array::types::{Int64Type, UInt64Type};
    use arrow_array::{
        Array, PrimitiveArray as ArrowPrimitiveArray, StructArray as ArrowStructArray,
    };
    use vortex_dtype::Nullability;
    use vortex_scalar::Scalar;

    use crate::array::primitive::PrimitiveArray;
    use crate::array::sparse::SparseArray;
    use crate::array::struct_::StructArray;
    use crate::validity::Validity;
    use crate::{IntoArray, IntoCanonical};

    #[test]
    fn test_canonicalize_nested_struct() {
        // Create a struct array with multiple internal components.
        let nested_struct_array = StructArray::from_fields(&[
            (
                "a",
                PrimitiveArray::from_vec(vec![1u64], Validity::NonNullable).into_array(),
            ),
            (
                "b",
                StructArray::from_fields(&[(
                    "inner_a",
                    // The nested struct contains a SparseArray representing the primitive array
                    //   [100i64, 100i64, 100i64]
                    // SparseArray is not a canonical type, so converting `into_arrow()` should map
                    // this to the nearest canonical type (PrimitiveArray).
                    SparseArray::try_new(
                        PrimitiveArray::from_vec(vec![0u64; 1], Validity::NonNullable).into_array(),
                        PrimitiveArray::from_vec(vec![100i64], Validity::NonNullable).into_array(),
                        1,
                        Scalar::primitive(0i64, Nullability::NonNullable),
                    )
                    .unwrap()
                    .into_array(),
                )])
                .into_array(),
            ),
        ]);

        let arrow_struct = nested_struct_array
            .into_canonical()
            .unwrap()
            .into_arrow()
            .as_any()
            .downcast_ref::<ArrowStructArray>()
            .cloned()
            .unwrap();

        assert!(arrow_struct
            .column(0)
            .as_any()
            .downcast_ref::<ArrowPrimitiveArray<UInt64Type>>()
            .is_some());

        let inner_struct = arrow_struct
            .column(1)
            .clone()
            .as_any()
            .downcast_ref::<ArrowStructArray>()
            .cloned()
            .unwrap()
            .clone();

        let inner_a = inner_struct
            .column(0)
            .as_any()
            .downcast_ref::<ArrowPrimitiveArray<Int64Type>>();
        assert!(inner_a.is_some());

        assert_eq!(
            inner_a.cloned().unwrap(),
            ArrowPrimitiveArray::from(vec![100i64]),
        );
    }
}
