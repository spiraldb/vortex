// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! One generator per array encoding, grouped behind a single [`EncodingKind`] enum.
//!
//! Every generator returns the reference values alongside the encoded array, so any encoding can
//! be checked against the plain Rust oracle. Container encodings (lists, structs, chunks,
//! dictionaries, ...) build their children with another randomly chosen leaf encoding, one level
//! deep, so encodings are also exercised as children without the inputs growing unwieldy.
//!
//! The `tests` module asserts that every encoding registered in the fuzz session has a generator
//! here, and that every generator actually produces its encoding.

use std::sync::Arc;

use arbitrary::Error::IncorrectFormat;
use arbitrary::Unstructured;
use strum::EnumIter;
use strum::IntoEnumIterator;
use strum::IntoStaticStr;
use vortex::encodings::alp::ALPRDFloat;
use vortex::encodings::alp::RDEncoder;
use vortex::encodings::alp::RDEncoderExt;
use vortex::encodings::alp::alp_encode;
use vortex::encodings::bytebool::ByteBool as ByteBoolEnc;
use vortex::encodings::datetime_parts::DateTimeParts as DateTimePartsEnc;
use vortex::encodings::decimal_byte_parts::DecimalByteParts as DecimalBytePartsEnc;
use vortex::encodings::fastlanes::BitPacked as BitPackedEnc;
use vortex::encodings::fastlanes::Delta as DeltaEnc;
use vortex::encodings::fastlanes::FoR as FoREnc;
use vortex::encodings::fastlanes::RLE;
use vortex::encodings::pco::Pco as PcoEnc;
use vortex::encodings::runend::RunEnd as RunEndEnc;
use vortex::encodings::sequence::Sequence as SequenceEnc;
use vortex::encodings::sparse::Sparse as SparseEnc;
use vortex::encodings::zigzag::zigzag_encode;
use vortex::encodings::zstd::Zstd as ZstdEnc;
use vortex::encodings::zstd::ZstdBuffers as ZstdBuffersEnc;
use vortex_array::ArrayRef;
use vortex_array::ExecutionCtx;
use vortex_array::IntoArray;
use vortex_array::arrays::ChunkedArray;
use vortex_array::arrays::ConstantArray;
use vortex_array::arrays::DecimalArray;
use vortex_array::arrays::DictArray;
use vortex_array::arrays::ExtensionArray;
use vortex_array::arrays::FilterArray;
use vortex_array::arrays::InterleaveArray;
use vortex_array::arrays::ListArray;
use vortex_array::arrays::ListViewArray;
use vortex_array::arrays::MapArray;
use vortex_array::arrays::MaskedArray;
use vortex_array::arrays::NullArray;
use vortex_array::arrays::Patched as PatchedEnc;
use vortex_array::arrays::PiecewiseSequenceArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::ScalarFnArray;
use vortex_array::arrays::SliceArray;
use vortex_array::arrays::UnionArray;
use vortex_array::arrays::VarBinArray;
use vortex_array::arrays::VarBinViewArray;
use vortex_array::arrays::VariantArray;
use vortex_array::arrays::datetime::TemporalData;
use vortex_array::builtins::ArrayBuiltins;
use vortex_array::dtype::DType;
use vortex_array::dtype::DecimalDType;
use vortex_array::dtype::FieldNames;
use vortex_array::dtype::MapDType;
use vortex_array::dtype::Nullability;
use vortex_array::dtype::PType;
use vortex_array::dtype::UnionVariants;
use vortex_array::extension::datetime::TimeUnit;
use vortex_array::patches::Patches;
use vortex_array::scalar::PValue;
use vortex_array::scalar::Scalar;
use vortex_array::scalar_fn::EmptyOptions;
use vortex_array::scalar_fn::ScalarFnVTableExt;
use vortex_array::scalar_fn::fns::not::Not;
use vortex_array::session::ArraySessionExt;
use vortex_array::validity::Validity;
use vortex_error::VortexResult;
use vortex_mask::Mask;

use super::generate::GenOptions;
use super::generate::Shape;
use super::generate::gen_values;
use super::value::Value;
use super::value::to_array;
use super::value::to_scalar;
use crate::SESSION;

/// Generation fails either because the fuzz input ran out or because Vortex rejected it.
#[derive(Debug)]
enum GenError {
    Input,
    Vortex,
}

impl From<arbitrary::Error> for GenError {
    fn from(_: arbitrary::Error) -> Self {
        GenError::Input
    }
}

impl From<vortex_error::VortexError> for GenError {
    fn from(_: vortex_error::VortexError) -> Self {
        GenError::Vortex
    }
}

type GenResult<T> = Result<T, GenError>;

/// Every array encoding the reference fuzzer can generate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EnumIter, IntoStaticStr)]
pub enum EncodingKind {
    // Canonical encodings.
    Null,
    Bool,
    Primitive,
    Decimal,
    VarBinView,
    ListView,
    FixedSizeList,
    Map,
    Struct,
    Union,
    Variant,
    Extension,
    // Utility encodings from vortex-array.
    VarBin,
    List,
    Chunked,
    Constant,
    Dict,
    Masked,
    PiecewiseSequence,
    Patched,
    Slice,
    Filter,
    Interleave,
    ScalarFn,
    // Compressed encodings.
    Alp,
    AlpRd,
    ByteBool,
    DateTimeParts,
    DecimalByteParts,
    BitPacked,
    Delta,
    FoR,
    Rle,
    Fsst,
    OnPair,
    Pco,
    RunEnd,
    Sequence,
    Sparse,
    ZigZag,
    Zstd,
    ZstdBuffers,
}

/// An encoded array together with its reference values.
#[derive(Clone)]
pub struct Case {
    pub kind: EncodingKind,
    pub dtype: DType,
    pub values: Vec<Value>,
    pub array: ArrayRef,
}

impl std::fmt::Debug for Case {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}<{}; {}>", self.kind, self.dtype, self.values.len())
    }
}

/// Bounds on generated arrays.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub max_len: usize,
    /// Remaining nesting depth for encodings that build children with other encodings.
    pub depth: u8,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_len: super::generate::MAX_LEN,
            depth: 1,
        }
    }
}

impl EncodingKind {
    /// The in-memory encoding ids this generator produces as its top-level array.
    pub fn ids(self) -> &'static [&'static str] {
        use EncodingKind::*;
        match self {
            Null => &["vortex.null"],
            Bool => &["vortex.bool"],
            Primitive => &["vortex.primitive"],
            Decimal => &["vortex.decimal"],
            VarBinView => &["vortex.varbinview"],
            ListView => &["vortex.listview"],
            FixedSizeList => &["vortex.fixed_size_list"],
            Map => &["vortex.map"],
            Struct => &["vortex.struct"],
            Union => &["vortex.union"],
            Variant => &["vortex.variant"],
            Extension => &["vortex.ext"],
            VarBin => &["vortex.varbin"],
            List => &["vortex.list"],
            Chunked => &["vortex.chunked"],
            Constant => &["vortex.constant"],
            Dict => &["vortex.dict"],
            Masked => &["vortex.masked"],
            PiecewiseSequence => &["vortex.piecewise-sequence"],
            Patched => &["vortex.patched"],
            Slice => &["vortex.slice"],
            Filter => &["vortex.filter"],
            Interleave => &["vortex.interleave"],
            ScalarFn => &["vortex.not"],
            Alp => &["vortex.alp"],
            AlpRd => &["vortex.alprd"],
            ByteBool => &["vortex.bytebool"],
            DateTimeParts => &["vortex.datetimeparts"],
            DecimalByteParts => &["vortex.decimal_byte_parts.v2"],
            BitPacked => &["fastlanes.bitpacked.v2"],
            Delta => &["fastlanes.delta"],
            FoR => &["fastlanes.for.v2"],
            Rle => &["fastlanes.rle"],
            Fsst => &["vortex.fsst"],
            OnPair => &["vortex.onpair"],
            Pco => &["vortex.pco"],
            RunEnd => &["vortex.runend"],
            Sequence => &["vortex.sequence"],
            Sparse => &["vortex.sparse"],
            ZigZag => &["vortex.zigzag"],
            Zstd => &["vortex.zstd"],
            ZstdBuffers => &["vortex.zstd_buffers"],
        }
    }

    /// Whether this encoding can hold values of `dtype`.
    pub fn supports(self, dtype: &DType) -> bool {
        use EncodingKind::*;
        let int = matches!(dtype, DType::Primitive(p, _) if p.is_int());
        let float = matches!(dtype, DType::Primitive(p, _) if p.is_float());
        let float32_64 = matches!(dtype, DType::Primitive(PType::F32 | PType::F64, _));
        let unsigned = matches!(dtype, DType::Primitive(p, _) if p.is_unsigned_int());
        let signed = matches!(dtype, DType::Primitive(p, _) if p.is_signed_int());
        let string = matches!(dtype, DType::Utf8(_) | DType::Binary(_));
        let leaf = is_leaf(dtype);
        match self {
            Null => matches!(dtype, DType::Null),
            Bool | ByteBool => matches!(dtype, DType::Bool(_)),
            Primitive => int || float,
            Decimal | DecimalByteParts => matches!(dtype, DType::Decimal(..)),
            VarBinView | VarBin | Fsst | OnPair => string,
            ListView | List => matches!(dtype, DType::List(..)),
            FixedSizeList => matches!(dtype, DType::FixedSizeList(..)),
            Map => matches!(dtype, DType::Map(..)),
            Struct => matches!(dtype, DType::Struct(..)),
            Union => matches!(dtype, DType::Union(..)),
            Variant => matches!(dtype, DType::Variant(_)),
            Extension | DateTimeParts => *dtype == timestamp_dtype(dtype.nullability()),
            Chunked | Constant | Dict | Slice | Filter => leaf,
            // Interleave execution only implements bool and primitive values.
            Interleave => matches!(dtype, DType::Bool(_) | DType::Primitive(..)),
            Masked => leaf && dtype.is_nullable(),
            PiecewiseSequence => dtype == &DType::Primitive(PType::U64, Nullability::NonNullable),
            ScalarFn => matches!(dtype, DType::Bool(_)),
            Alp | AlpRd => float32_64,
            Patched | BitPacked | FoR | Delta => unsigned,
            Rle | RunEnd => int || float,
            Pco => (int || float) && !matches!(dtype, DType::Primitive(PType::U8 | PType::I8, _)),
            Zstd => int || float || string,
            Sequence => int && !dtype.is_nullable(),
            Sparse => leaf,
            ZigZag => signed,
            ZstdBuffers => leaf,
        }
    }

    /// Generate an encoded array of this kind. Returns `IncorrectFormat` when the input does not
    /// satisfy the encoding's preconditions.
    pub fn generate(
        self,
        u: &mut Unstructured<'_>,
        dtype: Option<&DType>,
        limits: Limits,
        ctx: &mut ExecutionCtx,
    ) -> arbitrary::Result<Case> {
        let dtype = match dtype {
            Some(d) if self.supports(d) => d.clone(),
            Some(_) => return Err(IncorrectFormat),
            None => self.pick_dtype(u)?,
        };
        let (values, array) = self
            .build(u, &dtype, limits, ctx)
            .map_err(|_| IncorrectFormat)?
            .ok_or(IncorrectFormat)?;
        debug_assert_eq!(array.len(), values.len());
        Ok(Case {
            kind: self,
            dtype: array.dtype().clone(),
            values,
            array,
        })
    }

    pub(crate) fn pick_dtype(self, u: &mut Unstructured<'_>) -> arbitrary::Result<DType> {
        use EncodingKind::*;
        let n: Nullability = u.arbitrary()?;
        let ints = [
            PType::U8,
            PType::U16,
            PType::U32,
            PType::U64,
            PType::I8,
            PType::I16,
            PType::I32,
            PType::I64,
        ];
        let unsigned = [PType::U8, PType::U16, PType::U32, PType::U64];
        let signed = [PType::I8, PType::I16, PType::I32, PType::I64];
        let numeric = [
            PType::U8,
            PType::U16,
            PType::U32,
            PType::U64,
            PType::I8,
            PType::I16,
            PType::I32,
            PType::I64,
            PType::F16,
            PType::F32,
            PType::F64,
        ];
        Ok(match self {
            Null => DType::Null,
            Bool | ByteBool | ScalarFn => DType::Bool(n),
            Primitive | Rle | RunEnd => DType::Primitive(*u.choose(&numeric)?, n),
            Pco => {
                let ptypes: Vec<_> = numeric
                    .iter()
                    .copied()
                    .filter(|p| !matches!(p, PType::U8 | PType::I8))
                    .collect();
                DType::Primitive(*u.choose(&ptypes)?, n)
            }
            Zstd => {
                if u.arbitrary()? {
                    DType::Utf8(n)
                } else {
                    DType::Primitive(*u.choose(&numeric)?, n)
                }
            }
            Decimal | DecimalByteParts => arb_decimal(u, n)?,
            VarBinView | VarBin | Fsst | OnPair => {
                if u.arbitrary()? {
                    DType::Utf8(n)
                } else {
                    DType::Binary(n)
                }
            }
            ListView | List => DType::List(Arc::new(arb_leaf_dtype(u)?), n),
            FixedSizeList => {
                DType::FixedSizeList(Arc::new(arb_leaf_dtype(u)?), u.int_in_range(0..=4)?, n)
            }
            Map => DType::Map(
                MapDType::try_new(
                    DType::Primitive(PType::I32, Nullability::NonNullable),
                    arb_leaf_dtype(u)?,
                    false,
                )
                .map_err(|_| IncorrectFormat)?,
                n,
            ),
            Struct => {
                let n_fields = u.int_in_range(1..=3)?;
                let fields = (0..n_fields)
                    .map(|_| arb_leaf_dtype(u))
                    .collect::<arbitrary::Result<Vec<_>>>()?;
                let names: FieldNames = (0..n_fields).map(|i| format!("f{i}")).collect();
                DType::Struct(vortex_array::dtype::StructFields::new(names, fields), n)
            }
            Union => DType::Union(
                UnionVariants::new(
                    ["a", "b"].into(),
                    vec![arb_leaf_dtype(u)?, arb_leaf_dtype(u)?],
                )
                .map_err(|_| IncorrectFormat)?,
                Nullability::Nullable,
            ),
            Variant => DType::Variant(Nullability::NonNullable),
            Extension | DateTimeParts => timestamp_dtype(n),
            Interleave => {
                if u.arbitrary()? {
                    DType::Bool(n)
                } else {
                    DType::Primitive(*u.choose(&numeric)?, n)
                }
            }
            Chunked | Constant | Dict | Slice | Filter | Sparse | ZstdBuffers => arb_leaf_dtype(u)?,
            Masked => arb_leaf_dtype(u)?.as_nullable(),
            PiecewiseSequence => DType::Primitive(PType::U64, Nullability::NonNullable),
            Alp | AlpRd => DType::Primitive(*u.choose(&[PType::F32, PType::F64])?, n),
            Patched | BitPacked | FoR | Delta => DType::Primitive(*u.choose(&unsigned)?, n),
            Sequence => DType::Primitive(*u.choose(&ints)?, Nullability::NonNullable),
            ZigZag => DType::Primitive(*u.choose(&signed)?, n),
        })
    }

    #[expect(clippy::too_many_lines, reason = "one arm per encoding")]
    fn build(
        self,
        u: &mut Unstructured<'_>,
        dtype: &DType,
        limits: Limits,
        ctx: &mut ExecutionCtx,
    ) -> GenResult<Option<(Vec<Value>, ArrayRef)>> {
        use EncodingKind::*;
        let opts = GenOptions::arbitrary(u, dtype.nullability(), limits.max_len)?;
        let leaf = |u: &mut Unstructured<'_>, _: &mut ExecutionCtx| gen_values(u, dtype, opts);
        Ok(Some(match self {
            Null => (
                vec![Value::Null; opts.len],
                NullArray::new(opts.len).into_array(),
            ),
            Bool | Primitive | Decimal | VarBinView | FixedSizeList | Struct => {
                let values = match self {
                    FixedSizeList | Struct => gen_nested(u, dtype, opts.len, limits, ctx)?,
                    _ => leaf(u, ctx)?,
                };
                let array = to_array(dtype, &values, ctx)?;
                (values, array)
            }
            ListView | List | Map => {
                let elem_dtype = match dtype {
                    DType::Map(m, _) => m.entries_dtype(),
                    DType::List(elem, _) => elem.as_ref().clone(),
                    _ => return Ok(None),
                };
                let (elements, sizes) = gen_lists(u, &elem_dtype, opts, limits, ctx)?;
                let validity = gen_validity(u, dtype.nullability(), sizes.len())?;
                let offsets = sizes
                    .iter()
                    .scan(0u64, |acc, s| {
                        let o = *acc;
                        *acc += *s as u64;
                        Some(o)
                    })
                    .collect::<Vec<_>>();
                let values = list_values(&elements.values, &sizes, &validity);
                let validity_arr = to_validity(dtype.nullability(), &validity);
                let array = match self {
                    ListView => ListViewArray::try_new(
                        elements.array,
                        PrimitiveArray::from_iter(offsets).into_array(),
                        PrimitiveArray::from_iter(sizes.iter().map(|s| *s as u64)).into_array(),
                        validity_arr,
                    )?
                    .into_array(),
                    List => {
                        let mut ends = offsets;
                        ends.push(sizes.iter().map(|s| *s as u64).sum());
                        ListArray::try_new(
                            elements.array,
                            PrimitiveArray::from_iter(ends).into_array(),
                            validity_arr,
                        )?
                        .into_array()
                    }
                    _ => {
                        let DType::Map(map_dtype, _) = dtype else {
                            unreachable!()
                        };
                        let entries = ListViewArray::try_new(
                            elements.array,
                            PrimitiveArray::from_iter(offsets).into_array(),
                            PrimitiveArray::from_iter(sizes.iter().map(|s| *s as u64)).into_array(),
                            validity_arr,
                        )?;
                        MapArray::try_new(map_dtype.clone(), entries)?.into_array()
                    }
                };
                (values, array)
            }
            Union => {
                let DType::Union(variants, _) = dtype else {
                    return Ok(None);
                };
                let type_ids = (0..opts.len)
                    .map(|_| u.int_in_range(0u8..=1))
                    .collect::<arbitrary::Result<Vec<_>>>()?;
                let children = variants
                    .variants()
                    .collect::<Vec<_>>()
                    .iter()
                    .map(|d| {
                        let o = GenOptions {
                            len: opts.len,
                            ..opts
                        };
                        gen_values(u, d, o).map(|v| (d.clone(), v))
                    })
                    .collect::<arbitrary::Result<Vec<_>>>()?;
                let values = type_ids
                    .iter()
                    .enumerate()
                    .map(|(i, t)| {
                        Value::Tuple(vec![
                            Value::Int(i128::from(*t)),
                            children[usize::from(*t)].1[i].clone(),
                        ])
                    })
                    .collect();
                let children = children
                    .iter()
                    .map(|(d, v)| to_array(d, v, ctx))
                    .collect::<VortexResult<Vec<_>>>()?;
                let array = UnionArray::try_new(
                    PrimitiveArray::from_iter(type_ids).into_array(),
                    variants.clone(),
                    children,
                )?
                .into_array();
                (values, array)
            }
            Variant => {
                let v = i32::from(u.arbitrary::<i16>()?);
                let scalar = Scalar::variant(Scalar::primitive(v, Nullability::NonNullable));
                let storage = ConstantArray::new(scalar, opts.len).into_array();
                (
                    vec![Value::Int(i128::from(v)); opts.len],
                    VariantArray::try_new(storage, None)?.into_array(),
                )
            }
            Extension | DateTimeParts => {
                let storage_dtype = DType::Primitive(PType::I64, dtype.nullability());
                // Timestamps must stay within the range representable by the Timestamp type.
                let values = gen_values(u, &storage_dtype, opts)?
                    .into_iter()
                    .map(|v| match v {
                        Value::Int(i) => Value::Int(i.clamp(
                            super::generate::MIN_TIMESTAMP_MICROS,
                            super::generate::MAX_TIMESTAMP_MICROS,
                        )),
                        v => v,
                    })
                    .collect::<Vec<_>>();
                let storage = to_array(&storage_dtype, &values, ctx)?;
                let temporal = TemporalData::new_timestamp(storage, TimeUnit::Microseconds, None);
                let array = if self == DateTimeParts {
                    DateTimePartsEnc::try_from_temporal(temporal, ctx)?.into_array()
                } else {
                    ExtensionArray::from(temporal).into_array()
                };
                (values, array)
            }
            VarBin => {
                let values = leaf(u, ctx)?;
                let array = VarBinArray::from_iter(
                    values.iter().map(|v| v.as_bytes().map(<[u8]>::to_vec)),
                    dtype.clone(),
                )
                .into_array();
                (values, array)
            }
            Chunked => {
                let n_chunks = u.int_in_range(0..=3)?;
                let mut values = Vec::new();
                let mut chunks = Vec::new();
                for _ in 0..n_chunks {
                    let child = child_case(u, dtype, chunk_limits(limits, n_chunks), ctx)?;
                    values.extend(child.values);
                    chunks.push(child.array);
                }
                (
                    values,
                    ChunkedArray::try_new(chunks, dtype.clone())?.into_array(),
                )
            }
            Constant => {
                let v = if dtype.is_nullable() && u.ratio(1, 4)? {
                    Value::Null
                } else {
                    super::generate::gen_one(u, dtype, Shape::Random, 0)?
                };
                let array = ConstantArray::new(to_scalar(dtype, &v)?, opts.len).into_array();
                (vec![v; opts.len], array)
            }
            Dict => {
                let dict = child_case(u, dtype, small_limits(limits, 16), ctx)?;
                if dict.values.is_empty() {
                    return Ok(None);
                }
                let codes = (0..opts.len)
                    .map(|_| u.choose_index(dict.values.len()))
                    .collect::<arbitrary::Result<Vec<_>>>()?;
                let values = codes.iter().map(|c| dict.values[*c].clone()).collect();
                let codes = PrimitiveArray::from_iter(codes.iter().map(|c| *c as u64)).into_array();
                (values, DictArray::try_new(codes, dict.array)?.into_array())
            }
            Masked => {
                let child = child_case(u, &dtype.as_nonnullable(), limits, ctx)?;
                let validity = gen_validity(u, Nullability::Nullable, child.values.len())?;
                let values = child
                    .values
                    .into_iter()
                    .zip(&validity)
                    .map(|(v, ok)| if *ok { v } else { Value::Null })
                    .collect();
                let array = MaskedArray::try_new(child.array, Validity::from_iter(validity))?;
                (values, array.into_array())
            }
            PiecewiseSequence => {
                let n = u.int_in_range(0..=4)?;
                let mut values = Vec::new();
                let (mut starts, mut lengths, mut mults) = (vec![], vec![], vec![]);
                for _ in 0..n {
                    let start = u64::from(u.arbitrary::<u16>()?);
                    let length = u.int_in_range(0u64..=32)?;
                    let mult = u.int_in_range(0u64..=3)?;
                    values.extend((0..length).map(|i| Value::Int(i128::from(start + i * mult))));
                    starts.push(start);
                    lengths.push(length);
                    mults.push(mult);
                }
                let array = PiecewiseSequenceArray::try_new(
                    PrimitiveArray::from_iter(starts).into_array(),
                    PrimitiveArray::from_iter(lengths).into_array(),
                    PrimitiveArray::from_iter(mults).into_array(),
                    values.len(),
                )?
                .into_array();
                (values, array)
            }
            Patched => {
                let mut values = leaf(u, ctx)?;
                let inner = to_array(dtype, &values, ctx)?;
                let mut idx = (0..values.len())
                    .filter(|_| u.ratio(1, 8).unwrap_or(false))
                    .collect::<Vec<_>>();
                idx.dedup();
                let patch_values = idx
                    .iter()
                    .map(|_| super::generate::gen_one(u, dtype, Shape::Random, 0))
                    .collect::<arbitrary::Result<Vec<_>>>()?;
                // Patches replace values but keep the inner array's validity.
                for (i, v) in idx.iter().zip(&patch_values) {
                    if !values[*i].is_null() {
                        values[*i] = v.clone();
                    }
                }
                let patches = Patches::new(
                    values.len(),
                    0,
                    PrimitiveArray::from_iter(idx.iter().map(|i| *i as u64)).into_array(),
                    to_array(&dtype.as_nonnullable(), &patch_values, ctx)?.cast(dtype.clone())?,
                    None,
                )?;
                let array = PatchedEnc::from_array_and_patches(inner, &patches, ctx)?.into_array();
                (values, array)
            }
            Slice | Filter => {
                let child = child_case(u, dtype, limits, ctx)?;
                let len = child.values.len();
                if self == Slice {
                    let start = u.int_in_range(0..=len)?;
                    let end = u.int_in_range(start..=len)?;
                    (
                        child.values[start..end].to_vec(),
                        SliceArray::try_new(child.array, start..end)?.into_array(),
                    )
                } else {
                    let mask = (0..len)
                        .map(|_| u.arbitrary())
                        .collect::<arbitrary::Result<Vec<bool>>>()?;
                    let values = child
                        .values
                        .into_iter()
                        .zip(&mask)
                        .filter_map(|(v, keep)| keep.then_some(v))
                        .collect();
                    (
                        values,
                        FilterArray::try_new(child.array, Mask::from_iter(mask))?.into_array(),
                    )
                }
            }
            Interleave => {
                let first = child_case(u, dtype, small_limits(limits, 32), ctx)?;
                let second = child_case(u, dtype, small_limits(limits, 32), ctx)?;
                let sources = [&first.values, &second.values];
                let mut array_idx = Vec::with_capacity(opts.len);
                let mut row_idx = Vec::with_capacity(opts.len);
                let mut values = Vec::with_capacity(opts.len);
                for _ in 0..opts.len {
                    let candidates: Vec<u8> = (0u8..2)
                        .filter(|s| !sources[usize::from(*s)].is_empty())
                        .collect();
                    let Ok(source) = u.choose(&candidates) else {
                        break;
                    };
                    let row = u.choose_index(sources[usize::from(*source)].len())?;
                    values.push(sources[usize::from(*source)][row].clone());
                    array_idx.push(*source);
                    row_idx.push(row as u64);
                }
                let array = InterleaveArray::try_new(
                    vec![first.array, second.array],
                    PrimitiveArray::from_iter(array_idx).into_array(),
                    PrimitiveArray::from_iter(row_idx).into_array(),
                )?
                .into_array();
                (values, array)
            }
            ScalarFn => {
                let child = child_case(u, dtype, limits, ctx)?;
                let values = child
                    .values
                    .iter()
                    .map(|v| v.as_bool().map_or(Value::Null, |b| Value::Bool(!b)))
                    .collect();
                let array =
                    ScalarFnArray::try_new(Not.bind(EmptyOptions), [child.array])?.into_array();
                (values, array)
            }
            Alp | AlpRd | Rle | Pco | Zstd | RunEnd | FoR | Delta | BitPacked | ZigZag => {
                let values = leaf(u, ctx)?;
                let canonical = to_array(dtype, &values, ctx)?;
                let array = encode_primitive_like(self, u, dtype, &values, canonical, ctx)?;
                let Some(array) = array else { return Ok(None) };
                (values, array)
            }
            ByteBool => {
                let values = leaf(u, ctx)?;
                let array = if dtype.is_nullable() {
                    ByteBoolEnc::from_option_vec(values.iter().map(Value::as_bool).collect())
                } else {
                    ByteBoolEnc::from_vec(
                        values
                            .iter()
                            .map(|v| v.as_bool().unwrap_or(false))
                            .collect(),
                        Validity::NonNullable,
                    )
                };
                (values, array.into_array())
            }
            DecimalByteParts => {
                let values = leaf(u, ctx)?;
                let decimal = to_array(dtype, &values, ctx)?.execute::<DecimalArray>(ctx)?;
                (
                    values,
                    DecimalBytePartsEnc::encode(&decimal, ctx)?.into_array(),
                )
            }
            Fsst => {
                let values = leaf(u, ctx)?;
                let canonical = to_array(dtype, &values, ctx)?;
                let compressor = vortex_fsst::fsst_train_compressor(&canonical, ctx)?;
                let array = vortex_fsst::fsst_compress(&canonical, &compressor, ctx)?.into_array();
                (values, array)
            }
            OnPair => {
                let values = leaf(u, ctx)?;
                let canonical = to_array(dtype, &values, ctx)?;
                let array =
                    vortex_onpair::onpair_compress(&canonical, vortex_onpair::DEFAULT_CONFIG, ctx)?;
                (values, array)
            }
            Sequence => {
                let DType::Primitive(ptype, n) = dtype else {
                    return Ok(None);
                };
                let base = i128::from(u.arbitrary::<i8>()?);
                let mult = i128::from(u.int_in_range(-3i8..=3)?);
                let values = (0..opts.len)
                    .map(|i| Value::Int(base + mult * i as i128))
                    .collect::<Vec<_>>();
                let pvalue = |v: i128| -> VortexResult<PValue> {
                    to_scalar(&DType::Primitive(*ptype, *n), &Value::Int(v))?
                        .as_primitive()
                        .pvalue()
                        .ok_or_else(|| vortex_error::vortex_err!("null"))
                };
                let array =
                    SequenceEnc::try_new(pvalue(base)?, pvalue(mult)?, *ptype, *n, opts.len)?
                        .into_array();
                (values, array)
            }
            Sparse => {
                let fill = if dtype.is_nullable() && u.arbitrary()? {
                    Value::Null
                } else {
                    super::generate::gen_one(u, dtype, Shape::Random, 0)?
                };
                let patches = child_case(u, dtype, small_limits(limits, 32), ctx)?;
                let len = opts.len.max(patches.values.len());
                let mut positions = (0..len).collect::<Vec<_>>();
                for i in 0..patches.values.len() {
                    let j = u.int_in_range(i..=len - 1)?;
                    positions.swap(i, j);
                }
                let mut indices = positions[..patches.values.len()].to_vec();
                let mut order = (0..indices.len()).collect::<Vec<_>>();
                order.sort_by_key(|i| indices[*i]);
                indices.sort_unstable();
                let mut values = vec![fill.clone(); len];
                for (sorted_pos, orig) in order.iter().enumerate() {
                    values[indices[sorted_pos]] = patches.values[*orig].clone();
                }
                let patch_values = patches.array.take(
                    PrimitiveArray::from_iter(order.iter().map(|i| *i as u64)).into_array(),
                )?;
                let array = SparseEnc::try_new(
                    PrimitiveArray::from_iter(indices.iter().map(|i| *i as u64)).into_array(),
                    patch_values,
                    len,
                    to_scalar(dtype, &fill)?,
                )?
                .into_array();
                (values, array)
            }
            ZstdBuffers => {
                let mut child = child_case(u, dtype, limits, ctx)?;
                // TODO: nullable ZstdBuffers resolve their inner encoding against the legacy
                // session when computing validity, so they cannot wrap non-core encodings yet.
                #[expect(
                    clippy::disallowed_methods,
                    reason = "mirrors the session ZstdBuffers uses for validity"
                )]
                let core = vortex_array::legacy_session()
                    .arrays()
                    .registry()
                    .contains_key(&child.array.encoding_id());
                if dtype.is_nullable() && !core {
                    child.array = to_array(dtype, &child.values, ctx)?;
                }
                let level = u.int_in_range(-3..=3)?;
                let array = ZstdBuffersEnc::compress(&child.array, level, &SESSION)?.into_array();
                (child.values, array)
            }
        }))
    }
}

fn encode_primitive_like(
    kind: EncodingKind,
    u: &mut Unstructured<'_>,
    dtype: &DType,
    values: &[Value],
    canonical: ArrayRef,
    ctx: &mut ExecutionCtx,
) -> GenResult<Option<ArrayRef>> {
    use EncodingKind::*;
    if dtype.is_utf8() || dtype.is_binary() {
        let vbv = canonical.execute::<VarBinViewArray>(ctx)?;
        let frame = u.int_in_range(0..=64usize)?;
        return Ok(Some(
            ZstdEnc::from_var_bin_view(&vbv, 3, frame, ctx)?.into_array(),
        ));
    }
    let prim = canonical.execute::<PrimitiveArray>(ctx)?;
    Ok(Some(match kind {
        Alp => alp_encode(prim.as_view(), None, ctx)?.into_array(),
        AlpRd => {
            let valid = |v: &Value| v.as_float();
            let encoded = match prim.ptype() {
                PType::F32 => rd_encode::<f32>(&prim, values.iter().filter_map(valid))?,
                _ => rd_encode::<f64>(&prim, values.iter().filter_map(valid))?,
            };
            let Some(encoded) = encoded else {
                return Ok(None);
            };
            encoded
        }
        Rle => RLE::encode(prim.as_view(), ctx)?.into_array(),
        Pco => {
            let level = u.int_in_range(0..=8)?;
            let per_page = u.int_in_range(0..=128usize)?;
            PcoEnc::from_primitive(prim.as_view(), level, per_page, ctx)?.into_array()
        }
        Zstd => {
            let frame = u.int_in_range(0..=64usize)?;
            ZstdEnc::from_primitive(&prim, 3, frame, ctx)?.into_array()
        }
        RunEnd => RunEndEnc::encode(prim.into_array(), ctx)?.into_array(),
        FoR => FoREnc::encode(prim, ctx)?.into_array(),
        Delta => DeltaEnc::try_from_primitive_array(&prim, ctx)?.into_array(),
        BitPacked => {
            let max_width = u8::try_from(prim.ptype().bit_width()).unwrap_or(64);
            let width = u.int_in_range(0..=max_width - 1)?;
            BitPackedEnc::encode(&prim.into_array(), width, ctx)?.into_array()
        }
        ZigZag => zigzag_encode(prim.as_view())?.into_array(),
        _ => return Ok(None),
    }))
}

fn rd_encode<T: ALPRDFloat + vortex_array::dtype::NativePType>(
    prim: &PrimitiveArray,
    sample: impl Iterator<Item = f64>,
) -> VortexResult<Option<ArrayRef>> {
    let sample = sample
        .filter_map(|v| T::from_f64(v))
        .take(32)
        .collect::<Vec<T>>();
    if sample.is_empty() {
        return Ok(None);
    }
    Ok(Some(
        RDEncoder::new(&sample).encode(prim.as_view()).into_array(),
    ))
}

/// Generate a child array with a random encoding compatible with `dtype`, falling back to the
/// canonical encoding once the nesting depth is exhausted.
pub fn child_case(
    u: &mut Unstructured<'_>,
    dtype: &DType,
    limits: Limits,
    ctx: &mut ExecutionCtx,
) -> arbitrary::Result<Case> {
    if limits.depth > 0 && u.arbitrary()? {
        let candidates = EncodingKind::iter()
            .filter(|k| k.supports(dtype))
            .collect::<Vec<_>>();
        if let Ok(kind) = u.choose(&candidates) {
            let child_limits = Limits {
                depth: limits.depth - 1,
                ..limits
            };
            if let Ok(case) = kind.generate(u, Some(dtype), child_limits, ctx) {
                return Ok(case);
            }
        }
    }
    let opts = GenOptions::arbitrary(u, dtype.nullability(), limits.max_len)?;
    let values = if is_leaf(dtype) {
        gen_values(u, dtype, opts)?
    } else {
        gen_nested(u, dtype, opts.len, limits, ctx)?
    };
    let array = to_array(dtype, &values, ctx).map_err(|_| IncorrectFormat)?;
    Ok(Case {
        kind: canonical_kind(dtype),
        dtype: dtype.clone(),
        values,
        array,
    })
}

/// Generate an operand of exactly `len` rows: either a constant or an array in a random encoding.
pub fn operand(
    u: &mut Unstructured<'_>,
    dtype: &DType,
    len: usize,
    ctx: &mut ExecutionCtx,
) -> arbitrary::Result<Case> {
    if !buildable(dtype) {
        return Err(IncorrectFormat);
    }
    if u.ratio(1, 3)? {
        let value = if dtype.is_nullable() && u.ratio(1, 8)? {
            Value::Null
        } else {
            super::generate::gen_one(u, dtype, Shape::Random, 0)?
        };
        let scalar = to_scalar(dtype, &value).map_err(|_| IncorrectFormat)?;
        return Ok(Case {
            kind: EncodingKind::Constant,
            dtype: dtype.clone(),
            values: vec![value; len],
            array: ConstantArray::new(scalar, len).into_array(),
        });
    }
    let limits = Limits {
        max_len: len,
        depth: 1,
    };
    let mut case = child_case(u, dtype, limits, ctx)?;
    if case.values.len() < len {
        let mut opts = GenOptions::arbitrary(u, dtype.nullability(), 0)?;
        opts.len = len;
        let values = if is_leaf(dtype) {
            gen_values(u, dtype, opts)?
        } else {
            gen_nested(u, dtype, len, limits, ctx)?
        };
        case.array = to_array(dtype, &values, ctx).map_err(|_| IncorrectFormat)?;
        case.kind = canonical_kind(dtype);
        case.values = values;
    } else if case.values.len() > len {
        case.array = case.array.slice(0..len).map_err(|_| IncorrectFormat)?;
        case.values.truncate(len);
    }
    Ok(case)
}

fn canonical_kind(dtype: &DType) -> EncodingKind {
    match dtype {
        DType::Null => EncodingKind::Null,
        DType::Bool(_) => EncodingKind::Bool,
        DType::Primitive(..) => EncodingKind::Primitive,
        DType::Decimal(..) => EncodingKind::Decimal,
        DType::Utf8(_) | DType::Binary(_) => EncodingKind::VarBinView,
        DType::List(..) => EncodingKind::ListView,
        DType::FixedSizeList(..) => EncodingKind::FixedSizeList,
        DType::Struct(..) => EncodingKind::Struct,
        DType::Map(..) => EncodingKind::Map,
        DType::Union(..) => EncodingKind::Union,
        DType::Variant(_) => EncodingKind::Variant,
        DType::Extension(_) => EncodingKind::Extension,
    }
}

/// Values for fixed-size lists and structs, with leaf children.
fn gen_nested(
    u: &mut Unstructured<'_>,
    dtype: &DType,
    len: usize,
    limits: Limits,
    ctx: &mut ExecutionCtx,
) -> arbitrary::Result<Vec<Value>> {
    let validity = gen_validity(u, dtype.nullability(), len)?;
    let rows: Vec<Value> = match dtype {
        DType::FixedSizeList(elem, size, _) => {
            let size = *size as usize;
            let opts = GenOptions::arbitrary(u, elem.nullability(), 0)?;
            let elems = gen_values(
                u,
                elem,
                GenOptions {
                    len: len * size,
                    ..opts
                },
            )?;
            elems
                .chunks(size.max(1))
                .take(len)
                .map(|c| Value::Tuple(if size == 0 { vec![] } else { c.to_vec() }))
                .chain(std::iter::repeat_with(|| Value::Tuple(vec![])))
                .take(len)
                .collect()
        }
        DType::Struct(fields, _) => {
            let columns = fields
                .fields()
                .map(|f| {
                    let opts = GenOptions::arbitrary(u, f.nullability(), 0)?;
                    gen_values(u, &f, GenOptions { len, ..opts })
                })
                .collect::<arbitrary::Result<Vec<_>>>()?;
            (0..len)
                .map(|i| Value::Tuple(columns.iter().map(|c| c[i].clone()).collect()))
                .collect()
        }
        _ => return Err(IncorrectFormat),
    };
    let _ = (limits, ctx);
    Ok(rows
        .into_iter()
        .zip(validity)
        .map(|(v, ok)| if ok { v } else { Value::Null })
        .collect())
}

/// Generate list sizes and an encoded elements child that holds their concatenation.
fn gen_lists(
    u: &mut Unstructured<'_>,
    elem_dtype: &DType,
    opts: GenOptions,
    limits: Limits,
    ctx: &mut ExecutionCtx,
) -> arbitrary::Result<(Case, Vec<usize>)> {
    let n_lists = opts.len.min(32);
    let elements = child_case(u, elem_dtype, small_limits(limits, 128), ctx)?;
    let total = elements.values.len();
    let mut sizes = Vec::with_capacity(n_lists);
    let mut remaining = total;
    for i in 0..n_lists {
        let s = if i + 1 == n_lists {
            remaining
        } else {
            u.int_in_range(0..=remaining.min(8))?
        };
        remaining -= s;
        sizes.push(s);
    }
    if n_lists == 0 && total != 0 {
        return Err(IncorrectFormat);
    }
    Ok((elements, sizes))
}

fn list_values(elements: &[Value], sizes: &[usize], validity: &[bool]) -> Vec<Value> {
    let mut offset = 0;
    sizes
        .iter()
        .zip(validity)
        .map(|(s, ok)| {
            let v = Value::Tuple(elements[offset..offset + s].to_vec());
            offset += s;
            if *ok { v } else { Value::Null }
        })
        .collect()
}

fn gen_validity(
    u: &mut Unstructured<'_>,
    nullability: Nullability,
    len: usize,
) -> arbitrary::Result<Vec<bool>> {
    match nullability {
        Nullability::NonNullable => Ok(vec![true; len]),
        Nullability::Nullable => (0..len).map(|_| Ok(!u.ratio(1, 4)?)).collect(),
    }
}

fn to_validity(nullability: Nullability, validity: &[bool]) -> Validity {
    match nullability {
        Nullability::NonNullable => Validity::NonNullable,
        Nullability::Nullable => Validity::from_iter(validity.iter().copied()),
    }
}

fn chunk_limits(limits: Limits, n: usize) -> Limits {
    small_limits(limits, limits.max_len / n.max(1))
}

fn small_limits(limits: Limits, max_len: usize) -> Limits {
    Limits {
        max_len: limits.max_len.min(max_len),
        ..limits
    }
}

/// Whether canonical arrays of `dtype` can be built from scalars. Array builders do not support
/// union and variant values.
pub fn buildable(dtype: &DType) -> bool {
    match dtype {
        DType::Union(..) | DType::Variant(_) => false,
        DType::List(e, _) | DType::FixedSizeList(e, ..) => buildable(e),
        DType::Struct(fields, _) => fields.fields().all(|f| buildable(&f)),
        DType::Map(m, _) => buildable(&m.key_dtype()) && buildable(&m.value_dtype()),
        _ => true,
    }
}

/// Leaf dtypes: the ones generated directly from scalars.
pub fn is_leaf(dtype: &DType) -> bool {
    matches!(
        dtype,
        DType::Bool(_)
            | DType::Primitive(..)
            | DType::Decimal(..)
            | DType::Utf8(_)
            | DType::Binary(_)
    )
}

pub fn arb_leaf_dtype(u: &mut Unstructured<'_>) -> arbitrary::Result<DType> {
    let n: Nullability = u.arbitrary()?;
    Ok(match u.int_in_range(0..=4)? {
        0 => DType::Bool(n),
        1 => DType::Primitive(u.arbitrary()?, n),
        2 => arb_decimal(u, n)?,
        3 => DType::Utf8(n),
        _ => DType::Binary(n),
    })
}

fn arb_decimal(u: &mut Unstructured<'_>, n: Nullability) -> arbitrary::Result<DType> {
    let precision = u.int_in_range(1..=38)?;
    let scale = u.int_in_range(0..=i8::try_from(precision).unwrap_or(i8::MAX))?;
    Ok(DType::Decimal(DecimalDType::new(precision, scale), n))
}

fn timestamp_dtype(n: Nullability) -> DType {
    TemporalData::new_timestamp(
        PrimitiveArray::new(vortex_buffer::Buffer::<i64>::empty(), Validity::from(n)).into_array(),
        TimeUnit::Microseconds,
        None,
    )
    .into_array()
    .dtype()
    .clone()
}

#[cfg(test)]
mod tests {
    use vortex_array::VortexSessionExecute;
    use vortex_array::session::ArraySessionExt;
    use vortex_utils::aliases::hash_set::HashSet;

    use super::*;

    /// Every registered array encoding must be produced by some generator.
    #[test]
    fn every_registered_encoding_has_a_generator() {
        let covered: HashSet<&str> = EncodingKind::iter()
            .flat_map(|k| k.ids().iter().copied())
            .collect();
        let registered: Vec<String> = SESSION
            .arrays()
            .registry()
            .read(|map| map.values().map(|p| p.id().to_string()).collect());
        let missing: Vec<_> = registered
            .iter()
            .filter(|id| !covered.contains(id.as_str()))
            .collect();
        assert!(
            missing.is_empty(),
            "encodings without a reference generator: {missing:?}"
        );
    }

    fn panic_message(e: &Box<dyn std::any::Any + Send>) -> String {
        e.downcast_ref::<String>()
            .cloned()
            .or_else(|| e.downcast_ref::<&str>().map(|s| (*s).to_string()))
            .unwrap_or_default()
            .lines()
            .next()
            .unwrap_or_default()
            .to_string()
    }

    /// Every generator must produce its encoding and agree with its own reference values.
    #[test]
    fn every_generator_produces_its_encoding() {
        let mut ctx = SESSION.create_execution_ctx();
        let mut failures = Vec::new();
        for kind in EncodingKind::iter() {
            let mut produced = 0;
            for seed in 0..500u64 {
                let data = super::super::tests::test_bytes(seed);
                let generated = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let mut u = Unstructured::new(&data);
                    kind.generate(&mut u, None, Limits::default(), &mut ctx)
                }));
                let case = match generated {
                    Ok(Ok(case)) => case,
                    Ok(Err(_)) => continue,
                    Err(e) => {
                        failures.push(format!(
                            "{kind:?} seed {seed}: generate panicked: {}",
                            panic_message(&e)
                        ));
                        break;
                    }
                };
                if !kind.ids().contains(&case.array.encoding_id().as_ref()) {
                    continue;
                }
                produced += 1;
                let decoded = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    super::super::decode(&case.array, &mut ctx)
                }))
                .unwrap_or_else(|e| {
                    Err(vortex_error::vortex_err!("panicked: {}", panic_message(&e)))
                });
                match decoded {
                    Ok(decoded) if decoded == case.values => {}
                    Ok(decoded) => {
                        failures.push(format!(
                            "{kind:?} seed {seed}: decoded {} values != reference {}",
                            decoded.len(),
                            case.values.len()
                        ));
                        break;
                    }
                    Err(e) => {
                        failures.push(format!("{kind:?} seed {seed}: decode failed: {e}"));
                        break;
                    }
                }
                if produced >= 20 {
                    break;
                }
            }
            if produced == 0 {
                failures.push(format!("{kind:?} never produced {:?}", kind.ids()));
            }
        }
        assert!(failures.is_empty(), "{}", failures.join("\n"));
    }
}
