// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! A plain Rust value model used as the oracle for reference fuzzing.
//!
//! Values are decoded from canonical arrays by reading their buffers directly, so the comparison
//! does not depend on the scalar or compute paths being fuzzed.

use std::cmp::Ordering;
use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;

use vortex_array::ArrayRef;
use vortex_array::Canonical;
use vortex_array::ExecutionCtx;
use vortex_array::IntoArray;
use vortex_array::arrays::BoolArray;
use vortex_array::arrays::DecimalArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::VarBinViewArray;
use vortex_array::arrays::bool::BoolArrayExt;
use vortex_array::arrays::decimal::DecimalArrayExt;
use vortex_array::arrays::extension::ExtensionArrayExt;
use vortex_array::arrays::fixed_size_list::FixedSizeListArrayExt;
use vortex_array::arrays::listview::ListViewArrayExt;
use vortex_array::arrays::map::MapArrayExt;
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::arrays::union::UnionArrayExt;
use vortex_array::arrays::union::UnionArraySlotsExt;
use vortex_array::builders::builder_with_capacity_in;
use vortex_array::dtype::DType;
use vortex_array::dtype::DecimalType;
use vortex_array::dtype::PType;
use vortex_array::dtype::half::f16;
use vortex_array::match_each_decimal_value_type;
use vortex_array::match_each_native_ptype;
use vortex_array::scalar::DecimalValue;
use vortex_array::scalar::PValue;
use vortex_array::scalar::Scalar;
use vortex_array::scalar::ScalarValue;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_err;

/// A single reference value. `Null` stands for a null at any nesting level.
#[derive(Clone, Debug)]
pub enum Value {
    Null,
    Bool(bool),
    /// Every integer ptype, widened losslessly.
    Int(i128),
    /// Every float ptype, widened losslessly.
    Float(f64),
    /// The unscaled decimal value; the precision and scale live in the dtype.
    Decimal(i128),
    /// Both `Utf8` and `Binary`.
    Bytes(Vec<u8>),
    /// Lists, fixed-size lists, maps (as a list of key/value structs) and struct fields.
    Tuple(Vec<Value>),
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    pub fn as_int(&self) -> Option<i128> {
        match self {
            Value::Int(v) | Value::Decimal(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_float(&self) -> Option<f64> {
        match self {
            Value::Float(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Bytes(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_tuple(&self) -> Option<&[Value]> {
        match self {
            Value::Tuple(v) => Some(v),
            _ => None,
        }
    }

    /// Total order matching Vortex's scalar ordering: nulls first, floats by `total_cmp`.
    pub fn total_cmp(&self, other: &Value) -> Ordering {
        match (self, other) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Less,
            (_, Value::Null) => Ordering::Greater,
            (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
            (Value::Int(a), Value::Int(b)) | (Value::Decimal(a), Value::Decimal(b)) => a.cmp(b),
            (Value::Float(a), Value::Float(b)) => a.total_cmp(b),
            (Value::Bytes(a), Value::Bytes(b)) => a.cmp(b),
            (Value::Tuple(a), Value::Tuple(b)) => a
                .iter()
                .zip(b.iter())
                .map(|(a, b)| a.total_cmp(b))
                .find(|o| o.is_ne())
                .unwrap_or_else(|| a.len().cmp(&b.len())),
            (a, b) => discriminant(a).cmp(&discriminant(b)),
        }
    }
}

fn discriminant(v: &Value) -> u8 {
    match v {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Int(_) => 2,
        Value::Float(_) => 3,
        Value::Decimal(_) => 4,
        Value::Bytes(_) => 5,
        Value::Tuple(_) => 6,
    }
}

/// Bitwise equality: `NaN == NaN` and `-0.0 != 0.0`, since encodings must round-trip exactly.
impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Float(a), Value::Float(b)) => a.to_bits() == b.to_bits(),
            _ => self.total_cmp(other).is_eq(),
        }
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "null"),
            Value::Bool(v) => write!(f, "{v}"),
            Value::Int(v) => write!(f, "{v}"),
            Value::Float(v) => write!(f, "{v:?}"),
            Value::Decimal(v) => write!(f, "{v}d"),
            Value::Bytes(bytes) => match std::str::from_utf8(bytes) {
                Ok(text) => write!(f, "{text:?}"),
                Err(_) => write!(f, "{bytes:?}"),
            },
            Value::Tuple(vs) => {
                write!(f, "[")?;
                for (i, v) in vs.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{v}")?;
                }
                write!(f, "]")
            }
        }
    }
}

/// Decode an array of any encoding into reference values by canonicalizing it and reading the
/// canonical buffers.
pub fn decode(array: &ArrayRef, ctx: &mut ExecutionCtx) -> VortexResult<Vec<Value>> {
    let len = array.len();
    let canonical = array.clone().execute::<Canonical>(ctx)?;
    let validity = canonical
        .clone()
        .into_array()
        .validity()?
        .execute_mask(len, ctx)?;
    let valid = |i: usize| validity.value(i);

    let values = match canonical {
        Canonical::Null(_) => vec![Value::Null; len],
        Canonical::Bool(a) => decode_bool(&a)
            .into_iter()
            .enumerate()
            .map(|(i, v)| {
                if valid(i) {
                    Value::Bool(v)
                } else {
                    Value::Null
                }
            })
            .collect(),
        Canonical::Primitive(a) => decode_primitive(&a)
            .into_iter()
            .enumerate()
            .map(|(i, v)| if valid(i) { v } else { Value::Null })
            .collect(),
        Canonical::Decimal(a) => decode_decimal(&a)?
            .into_iter()
            .enumerate()
            .map(|(i, v)| {
                if valid(i) {
                    Value::Decimal(v)
                } else {
                    Value::Null
                }
            })
            .collect(),
        Canonical::VarBinView(a) => decode_bytes(&a)
            .into_iter()
            .enumerate()
            .map(|(i, v)| {
                if valid(i) {
                    Value::Bytes(v)
                } else {
                    Value::Null
                }
            })
            .collect(),
        Canonical::List(a) => (0..len)
            .map(|i| {
                if valid(i) {
                    Ok(Value::Tuple(decode(&a.list_elements_at(i)?, ctx)?))
                } else {
                    Ok(Value::Null)
                }
            })
            .collect::<VortexResult<_>>()?,
        Canonical::FixedSizeList(a) => (0..len)
            .map(|i| {
                if valid(i) {
                    Ok(Value::Tuple(decode(
                        &a.fixed_size_list_elements_at(i)?,
                        ctx,
                    )?))
                } else {
                    Ok(Value::Null)
                }
            })
            .collect::<VortexResult<_>>()?,
        Canonical::Map(a) => (0..len)
            .map(|i| {
                if valid(i) {
                    Ok(Value::Tuple(decode(&a.entries_at(i)?, ctx)?))
                } else {
                    Ok(Value::Null)
                }
            })
            .collect::<VortexResult<_>>()?,
        Canonical::Struct(a) => {
            let fields = a
                .iter_unmasked_fields()
                .map(|f| decode(f, ctx))
                .collect::<VortexResult<Vec<_>>>()?;
            (0..len)
                .map(|i| {
                    if valid(i) {
                        Value::Tuple(fields.iter().map(|f| f[i].clone()).collect())
                    } else {
                        Value::Null
                    }
                })
                .collect()
        }
        Canonical::Extension(a) => decode(a.storage_array(), ctx)?
            .into_iter()
            .enumerate()
            .map(|(i, v)| if valid(i) { v } else { Value::Null })
            .collect(),
        Canonical::Union(a) => {
            let tags = decode(a.type_ids(), ctx)?;
            let children = a
                .iter_children()
                .map(|c| decode(c, ctx))
                .collect::<VortexResult<Vec<_>>>()?;
            (0..len)
                .map(|i| {
                    let Some(tag) = tags[i].as_int().filter(|_| valid(i)) else {
                        return Ok(Value::Null);
                    };
                    let tag = u8::try_from(tag)?;
                    let child = a
                        .variants()
                        .tag_to_child_index(tag)
                        .ok_or_else(|| vortex_err!("unknown union tag {tag}"))?;
                    Ok(Value::Tuple(vec![
                        Value::Int(i128::from(tag)),
                        children[child][i].clone(),
                    ]))
                })
                .collect::<VortexResult<_>>()?
        }
        // Variant rows are dynamically typed scalars with no fixed buffer layout to read.
        Canonical::Variant(a) => {
            let a = a.into_array();
            (0..len)
                .map(|i| Ok(from_scalar(&a.execute_scalar(i, ctx)?)))
                .collect::<VortexResult<_>>()?
        }
    };
    Ok(values)
}

fn decode_bool(array: &BoolArray) -> Vec<bool> {
    array.to_bit_buffer().iter().collect()
}

fn decode_primitive(array: &PrimitiveArray) -> Vec<Value> {
    match array.ptype() {
        PType::F16 => array
            .as_slice::<f16>()
            .iter()
            .map(|v| Value::Float(f64::from(*v)))
            .collect(),
        PType::F32 => array
            .as_slice::<f32>()
            .iter()
            .map(|v| Value::Float(f64::from(*v)))
            .collect(),
        PType::F64 => array
            .as_slice::<f64>()
            .iter()
            .map(|v| Value::Float(*v))
            .collect(),
        p => match_each_native_ptype!(p, |P| {
            array
                .as_slice::<P>()
                .iter()
                .map(|v| Value::Int(int_to_i128(*v)))
                .collect()
        }),
    }
}

/// Widen any native ptype to `i128`. Only called with integer ptypes.
fn int_to_i128<T: num_traits::ToPrimitive>(v: T) -> i128 {
    v.to_i128().unwrap_or_default()
}

fn decode_decimal(array: &DecimalArray) -> VortexResult<Vec<i128>> {
    match_each_decimal_value_type!(array.values_type(), |D| {
        array
            .buffer::<D>()
            .iter()
            .map(|v| {
                <i128 as vortex_array::dtype::BigCast>::from(*v)
                    .ok_or_else(|| vortex_err!("decimal value does not fit in i128"))
            })
            .collect()
    })
}

fn decode_bytes(array: &VarBinViewArray) -> Vec<Vec<u8>> {
    (0..array.len())
        .map(|i| array.bytes_at(i).to_vec())
        .collect()
}

/// Convert a Vortex scalar into a reference value.
pub fn from_scalar(scalar: &Scalar) -> Value {
    from_scalar_value(scalar.value())
}

fn from_scalar_value(value: Option<&ScalarValue>) -> Value {
    match value {
        None => Value::Null,
        Some(ScalarValue::Bool(b)) => Value::Bool(*b),
        Some(ScalarValue::Primitive(p)) => match p {
            PValue::F16(v) => Value::Float(f64::from(*v)),
            PValue::F32(v) => Value::Float(f64::from(*v)),
            PValue::F64(v) => Value::Float(*v),
            PValue::U8(v) => Value::Int(i128::from(*v)),
            PValue::U16(v) => Value::Int(i128::from(*v)),
            PValue::U32(v) => Value::Int(i128::from(*v)),
            PValue::U64(v) => Value::Int(i128::from(*v)),
            PValue::I8(v) => Value::Int(i128::from(*v)),
            PValue::I16(v) => Value::Int(i128::from(*v)),
            PValue::I32(v) => Value::Int(i128::from(*v)),
            PValue::I64(v) => Value::Int(i128::from(*v)),
        },
        Some(ScalarValue::Decimal(d)) => Value::Decimal(decimal_to_i128(d)),
        Some(ScalarValue::Utf8(s)) => Value::Bytes(s.as_bytes().to_vec()),
        Some(ScalarValue::Binary(b)) => Value::Bytes(b.as_slice().to_vec()),
        Some(ScalarValue::Tuple(vs)) => {
            Value::Tuple(vs.iter().map(|v| from_scalar_value(v.as_ref())).collect())
        }
        Some(ScalarValue::Union(u)) => Value::Tuple(vec![
            Value::Int(i128::from(u.type_id())),
            from_scalar_value(u.child_value()),
        ]),
        Some(ScalarValue::Variant(inner)) => from_scalar(inner),
    }
}

fn decimal_to_i128(d: &DecimalValue) -> i128 {
    match d {
        DecimalValue::I8(v) => i128::from(*v),
        DecimalValue::I16(v) => i128::from(*v),
        DecimalValue::I32(v) => i128::from(*v),
        DecimalValue::I64(v) => i128::from(*v),
        DecimalValue::I128(v) => *v,
        DecimalValue::I256(v) => v.maybe_i128().unwrap_or(i128::MAX),
    }
}

/// Convert a reference value into a Vortex scalar of the given dtype.
pub fn to_scalar(dtype: &DType, value: &Value) -> VortexResult<Scalar> {
    Scalar::try_new(dtype.clone(), to_scalar_value(dtype, value)?)
}

fn to_scalar_value(dtype: &DType, value: &Value) -> VortexResult<Option<ScalarValue>> {
    let dtype = match dtype {
        DType::Extension(ext) => ext.storage_dtype(),
        d => d,
    };
    Ok(Some(match (dtype, value) {
        (_, Value::Null) => return Ok(None),
        (DType::Bool(_), Value::Bool(b)) => ScalarValue::Bool(*b),
        (DType::Primitive(ptype, _), Value::Int(v)) => {
            ScalarValue::Primitive(int_pvalue(*ptype, *v)?)
        }
        (DType::Primitive(ptype, _), Value::Float(v)) => ScalarValue::Primitive(match ptype {
            PType::F16 => PValue::F16(f16::from_f64(*v)),
            #[expect(clippy::cast_possible_truncation, reason = "values originate as f32")]
            PType::F32 => PValue::F32(*v as f32),
            PType::F64 => PValue::F64(*v),
            p => vortex_bail!("float value for {p}"),
        }),
        (DType::Decimal(..), Value::Decimal(v)) => ScalarValue::Decimal(DecimalValue::I128(*v)),
        (DType::Utf8(_), Value::Bytes(b)) => ScalarValue::Utf8(
            String::from_utf8(b.clone())
                .map_err(|e| vortex_err!("invalid utf8: {e}"))?
                .into(),
        ),
        (DType::Binary(_), Value::Bytes(b)) => ScalarValue::Binary(b.clone().into()),
        (DType::List(elem, _) | DType::FixedSizeList(elem, ..), Value::Tuple(vs)) => {
            ScalarValue::Tuple(
                vs.iter()
                    .map(|v| to_scalar_value(elem, v))
                    .collect::<VortexResult<_>>()?,
            )
        }
        (DType::Struct(fields, _), Value::Tuple(vs)) => ScalarValue::Tuple(
            fields
                .fields()
                .zip(vs.iter())
                .map(|(f, v)| to_scalar_value(&f, v))
                .collect::<VortexResult<_>>()?,
        ),
        (d, v) => vortex_bail!("cannot convert reference value {v} to {d}"),
    }))
}

fn int_pvalue(ptype: PType, v: i128) -> VortexResult<PValue> {
    let err = || vortex_err!("{v} does not fit in {ptype}");
    Ok(match ptype {
        PType::U8 => PValue::U8(u8::try_from(v).map_err(|_| err())?),
        PType::U16 => PValue::U16(u16::try_from(v).map_err(|_| err())?),
        PType::U32 => PValue::U32(u32::try_from(v).map_err(|_| err())?),
        PType::U64 => PValue::U64(u64::try_from(v).map_err(|_| err())?),
        PType::I8 => PValue::I8(i8::try_from(v).map_err(|_| err())?),
        PType::I16 => PValue::I16(i16::try_from(v).map_err(|_| err())?),
        PType::I32 => PValue::I32(i32::try_from(v).map_err(|_| err())?),
        PType::I64 => PValue::I64(i64::try_from(v).map_err(|_| err())?),
        p => vortex_bail!("integer value for {p}"),
    })
}

/// Build a canonical Vortex array holding the given reference values.
pub fn to_array(dtype: &DType, values: &[Value], ctx: &mut ExecutionCtx) -> VortexResult<ArrayRef> {
    let mut builder = builder_with_capacity_in(dtype, values.len(), ctx.allocator());
    for v in values {
        builder.append_scalar(&to_scalar(dtype, v)?)?;
    }
    Ok(builder.finish())
}

/// The smallest physical decimal type that can hold the given precision.
pub fn decimal_type_for(precision: u8) -> DecimalType {
    DecimalType::smallest_decimal_value_type(&vortex_array::dtype::DecimalDType::new(precision, 0))
}
