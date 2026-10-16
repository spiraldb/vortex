// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Scalar functions and their plain Rust reference implementations.
//!
//! Every registered scalar function has a [`ScalarOpKind`]. Each kind generates a concrete
//! [`ScalarOp`] for the current array (if the array's dtype supports it), and every op is
//! evaluated both through a `ScalarFnArray` and through a reference implementation over
//! [`Value`]s. Operands of binary functions are generated with random encodings, so encoding
//! specific kernels are compared against the reference as well.

use std::cmp::Ordering;
use std::sync::Arc;

use arbitrary::Error::IncorrectFormat;
use arbitrary::Unstructured;
use strum::EnumIter;
use vortex_array::ArrayRef;
use vortex_array::ExecutionCtx;
use vortex_array::IntoArray;
use vortex_array::aggregate_fn::AggregateFn;
use vortex_array::aggregate_fn::EmptyOptions as AggEmptyOptions;
use vortex_array::aggregate_fn::NumericalAggregateOpts;
use vortex_array::aggregate_fn::fns::max::Max;
use vortex_array::aggregate_fn::fns::min::Min;
use vortex_array::aggregate_fn::fns::null_count::NullCount;
use vortex_array::arrays::ConstantArray;
use vortex_array::arrays::ScalarFnArray;
use vortex_array::dtype::DType;
use vortex_array::dtype::FieldName;
use vortex_array::dtype::FieldNames;
use vortex_array::dtype::Nullability;
use vortex_array::dtype::PType;
use vortex_array::dtype::StructFields;
use vortex_array::dtype::half::f16;
use vortex_array::extension::datetime::Timestamp;
use vortex_array::scalar_fn::EmptyOptions;
use vortex_array::scalar_fn::ScalarFnRef;
use vortex_array::scalar_fn::ScalarFnVTableExt;
use vortex_array::scalar_fn::fns::between::Between;
use vortex_array::scalar_fn::fns::between::BetweenOptions;
use vortex_array::scalar_fn::fns::between::StrictComparison;
use vortex_array::scalar_fn::fns::binary::Binary;
use vortex_array::scalar_fn::fns::byte_length::ByteLength;
use vortex_array::scalar_fn::fns::cast::Cast;
use vortex_array::scalar_fn::fns::ext_storage::ExtStorage;
use vortex_array::scalar_fn::fns::fill_null::FillNull;
use vortex_array::scalar_fn::fns::get_item::GetItem;
use vortex_array::scalar_fn::fns::is_nan::IsNan;
use vortex_array::scalar_fn::fns::is_not_null::IsNotNull;
use vortex_array::scalar_fn::fns::is_null::IsNull;
use vortex_array::scalar_fn::fns::like::Like;
use vortex_array::scalar_fn::fns::like::LikeOptions;
use vortex_array::scalar_fn::fns::list_contains::ListContains;
use vortex_array::scalar_fn::fns::list_contains::ListContainsOptions;
use vortex_array::scalar_fn::fns::list_contains::PreparedSet;
use vortex_array::scalar_fn::fns::list_length::ListLength;
use vortex_array::scalar_fn::fns::list_sum::ListSum;
use vortex_array::scalar_fn::fns::literal::Literal;
use vortex_array::scalar_fn::fns::mask::Mask;
use vortex_array::scalar_fn::fns::merge::DuplicateHandling;
use vortex_array::scalar_fn::fns::merge::Merge;
use vortex_array::scalar_fn::fns::not::Not;
use vortex_array::scalar_fn::fns::operators::Operator;
use vortex_array::scalar_fn::fns::pack::Pack;
use vortex_array::scalar_fn::fns::pack::PackOptions;
use vortex_array::scalar_fn::fns::replace_time_zone::ReplaceTimeZone;
use vortex_array::scalar_fn::fns::replace_time_zone::ReplaceTimeZoneOptions;
use vortex_array::scalar_fn::fns::select::FieldSelection;
use vortex_array::scalar_fn::fns::select::Select;
use vortex_array::scalar_fn::fns::stat::StatFn;
use vortex_array::scalar_fn::fns::stat::StatOptions;
use vortex_array::scalar_fn::fns::variant_get::VariantGet;
use vortex_array::scalar_fn::fns::variant_get::VariantGetOptions;
use vortex_array::scalar_fn::fns::variant_get::VariantPath;
use vortex_array::scalar_fn::fns::zip::Zip;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;

use super::encodings::Case;
use super::encodings::is_leaf;
use super::encodings::operand;
use super::value::Value;
use super::value::to_scalar;

/// Every scalar function the reference fuzzer exercises, one variant per registered function
/// (binary operators are split by family).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EnumIter)]
pub enum ScalarOpKind {
    Compare,
    Arithmetic,
    Logical,
    Between,
    Not,
    IsNull,
    IsNotNull,
    IsNan,
    FillNull,
    Mask,
    Zip,
    Like,
    ByteLength,
    Cast,
    ListLength,
    ListContains,
    ListSum,
    GetItem,
    Select,
    Pack,
    Merge,
    ExtStorage,
    Literal,
    PreparedSet,
    ReplaceTimeZone,
    Stat,
    VariantGet,
}

impl ScalarOpKind {
    /// The scalar function ids this kind exercises.
    pub fn ids(self) -> &'static [&'static str] {
        use ScalarOpKind::*;
        match self {
            Compare | Arithmetic | Logical => &["vortex.binary"],
            Between => &["vortex.between"],
            Not => &["vortex.not"],
            IsNull => &["vortex.is_null"],
            IsNotNull => &["vortex.is_not_null"],
            IsNan => &["vortex.is_nan"],
            FillNull => &["vortex.fill_null"],
            Mask => &["vortex.mask"],
            Zip => &["vortex.zip"],
            Like => &["vortex.like"],
            ByteLength => &["vortex.byte_length"],
            Cast => &["vortex.cast"],
            ListLength => &["vortex.list.length"],
            ListContains => &["vortex.list.contains"],
            ListSum => &["vortex.list.sum"],
            GetItem => &["vortex.get_item"],
            Select => &["vortex.select"],
            Pack => &["vortex.pack"],
            Merge => &["vortex.merge"],
            ExtStorage => &["vortex.ext.storage"],
            Literal => &["vortex.literal"],
            PreparedSet => &["vortex.list.prepared_set", "vortex.list.contains"],
            ReplaceTimeZone => &["vortex.replace_time_zone"],
            Stat => &["vortex.stat"],
            VariantGet => &["vortex.variant_get"],
        }
    }

    /// Generate an op of this kind applicable to `input`, or `IncorrectFormat` if the input's
    /// dtype does not support it.
    #[expect(clippy::cognitive_complexity, reason = "one arm per op kind")]
    pub fn generate(
        self,
        u: &mut Unstructured<'_>,
        input: &Case,
        ctx: &mut ExecutionCtx,
    ) -> arbitrary::Result<ScalarOp> {
        use ScalarOpKind::*;
        let dtype = &input.dtype;
        let len = input.values.len();
        let same_type = |u: &mut Unstructured<'_>, ctx: &mut ExecutionCtx| {
            let n: Nullability = u.arbitrary()?;
            operand(u, &dtype.with_nullability(n), len, ctx)
        };
        Ok(match self {
            Compare if comparable(dtype) => {
                ScalarOp::Compare(*u.choose(&COMPARE_OPS)?, same_type(u, ctx)?)
            }
            Arithmetic if arithmetic(dtype) => {
                ScalarOp::Arithmetic(*u.choose(&ARITH_OPS)?, same_type(u, ctx)?)
            }
            Cast => ScalarOp::Cast(cast_target(u, dtype)?),
            ListSum => {
                let DType::List(elem, _) = dtype else {
                    return Err(IncorrectFormat);
                };
                let summable = match elem.as_ref() {
                    DType::Bool(_) | DType::Primitive(..) => true,
                    DType::Decimal(d, _) => d.precision() <= 30,
                    _ => false,
                };
                if !summable {
                    return Err(IncorrectFormat);
                }
                ScalarOp::ListSum {
                    skip_nans: u.arbitrary()?,
                }
            }
            PreparedSet if comparable(dtype) && !matches!(dtype, DType::Extension(_)) => {
                let n = u.int_in_range(0..=4)?;
                let elem_dtype = dtype.as_nullable();
                let elems = (0..n)
                    .map(|_| {
                        if u.ratio(1, 6)? {
                            Ok(Value::Null)
                        } else {
                            super::generate::gen_one(
                                u,
                                &elem_dtype,
                                super::generate::Shape::Random,
                                0,
                            )
                        }
                    })
                    .collect::<arbitrary::Result<Vec<_>>>()?;
                // Reuse some of the input's values so that matches are likely.
                let mut elems = elems;
                if let Ok(i) = u.choose_index(len) {
                    elems.push(input.values[i].clone());
                }
                ScalarOp::PreparedSet {
                    elems,
                    sql_null_semantics: u.arbitrary()?,
                }
            }
            ReplaceTimeZone if is_naive_timestamp(dtype) => ScalarOp::ReplaceTimeZone(
                (*u.choose(&[None, Some("UTC"), Some("+01:00"), Some("-05:30")])?)
                    .map(Arc::<str>::from),
            ),
            Stat if matches!(
                dtype,
                DType::Primitive(..) | DType::Bool(_) | DType::Utf8(_)
            ) =>
            {
                ScalarOp::Stat(*u.choose(&[StatKind::NullCount, StatKind::Min, StatKind::Max])?)
            }
            VariantGet if matches!(dtype, DType::Variant(_)) => ScalarOp::VariantGet {
                field: u.arbitrary()?,
                target: *u.choose(&[None, Some(PType::I8), Some(PType::I32), Some(PType::I64)])?,
            },
            Logical if matches!(dtype, DType::Bool(_)) => ScalarOp::Logical(
                *u.choose(&[Operator::And, Operator::Or])?,
                same_type(u, ctx)?,
            ),
            Between if comparable(dtype) => ScalarOp::Between {
                lower: same_type(u, ctx)?,
                upper: same_type(u, ctx)?,
                lower_strict: u.arbitrary()?,
                upper_strict: u.arbitrary()?,
            },
            Not if matches!(dtype, DType::Bool(_)) => ScalarOp::Not,
            IsNull => ScalarOp::IsNull,
            IsNotNull => ScalarOp::IsNotNull,
            IsNan if dtype.is_float() => ScalarOp::IsNan,
            FillNull if is_leaf(dtype) => {
                let n: Nullability = u.arbitrary()?;
                let fill_dtype = dtype.with_nullability(n);
                let fill =
                    super::generate::gen_one(u, &fill_dtype, super::generate::Shape::Random, 0)?;
                ScalarOp::FillNull(fill_dtype, fill)
            }
            // TODO: masking needs a nullable cast, which union and variant arrays do not support.
            Mask if !matches!(dtype, DType::Union(..) | DType::Variant(_)) => ScalarOp::Mask(
                operand(u, &DType::Bool(Nullability::NonNullable), len, ctx)?,
            ),
            Zip if is_leaf(dtype) => ScalarOp::Zip {
                if_false: same_type(u, ctx)?,
                mask: {
                    let n: Nullability = u.arbitrary()?;
                    operand(u, &DType::Bool(n), len, ctx)?
                },
            },
            Like if matches!(dtype, DType::Utf8(_)) => {
                let nullability: Nullability = u.arbitrary()?;
                let pattern = if u.arbitrary()? {
                    let text = gen_pattern(u)?;
                    let value = Value::Bytes(text.into_bytes());
                    let scalar = to_scalar(&DType::Utf8(nullability), &value)
                        .map_err(|_| IncorrectFormat)?;
                    Case {
                        kind: super::encodings::EncodingKind::Constant,
                        dtype: DType::Utf8(nullability),
                        values: vec![value; len],
                        array: ConstantArray::new(scalar, len).into_array(),
                    }
                } else {
                    operand(u, &DType::Utf8(nullability), len, ctx)?
                };
                ScalarOp::Like {
                    pattern,
                    negated: u.arbitrary()?,
                    case_insensitive: u.arbitrary()?,
                }
            }
            ByteLength if matches!(dtype, DType::Utf8(_) | DType::Binary(_)) => {
                ScalarOp::ByteLength
            }
            ListLength if matches!(dtype, DType::List(..) | DType::FixedSizeList(..)) => {
                ScalarOp::ListLength
            }
            ListContains => {
                let DType::List(elem, _) = dtype else {
                    return Err(IncorrectFormat);
                };
                if !comparable(elem) {
                    return Err(IncorrectFormat);
                }
                let n: Nullability = u.arbitrary()?;
                ScalarOp::ListContains {
                    needle: operand(u, &elem.with_nullability(n), len, ctx)?,
                    sql_null_semantics: u.arbitrary()?,
                }
            }
            GetItem => {
                let DType::Struct(fields, _) = dtype else {
                    return Err(IncorrectFormat);
                };
                ScalarOp::GetItem(u.choose_index(fields.nfields())?)
            }
            Select => {
                let DType::Struct(fields, _) = dtype else {
                    return Err(IncorrectFormat);
                };
                let keep = (0..fields.nfields())
                    .map(|_| u.arbitrary())
                    .collect::<arbitrary::Result<Vec<bool>>>()?;
                ScalarOp::Select {
                    fields: keep,
                    include: u.arbitrary()?,
                }
            }
            // TODO: struct fields of union or variant type need casts those arrays lack.
            Pack if !matches!(dtype, DType::Union(..) | DType::Variant(_)) => ScalarOp::Pack {
                copies: u.int_in_range(1..=2)?,
                nullability: u.arbitrary()?,
            },
            Merge => {
                let DType::Struct(fields, Nullability::NonNullable) = dtype else {
                    return Err(IncorrectFormat);
                };
                // Merge with a struct that overrides one field and adds another.
                let overridden = u.choose_index(fields.nfields())?;
                let new_field = super::encodings::arb_leaf_dtype(u)?;
                let other_fields = StructFields::new(
                    [
                        fields
                            .field_name(overridden)
                            .cloned()
                            .ok_or(IncorrectFormat)?,
                        FieldName::from(
                            (0..)
                                .map(|i| format!("merged{i}"))
                                .find(|n| fields.find(n.as_str()).is_none())
                                .unwrap_or_default(),
                        ),
                    ]
                    .into(),
                    vec![
                        fields.field_by_index(overridden).ok_or(IncorrectFormat)?,
                        new_field,
                    ],
                );
                ScalarOp::Merge(operand(
                    u,
                    &DType::Struct(other_fields, Nullability::NonNullable),
                    len,
                    ctx,
                )?)
            }
            ExtStorage if matches!(dtype, DType::Extension(_)) => ScalarOp::ExtStorage,
            Literal => {
                let lit_dtype = super::encodings::arb_leaf_dtype(u)?;
                let v = super::generate::gen_one(u, &lit_dtype, super::generate::Shape::Random, 0)?;
                ScalarOp::Literal(lit_dtype, v)
            }
            _ => return Err(IncorrectFormat),
        })
    }
}

const COMPARE_OPS: [Operator; 6] = [
    Operator::Eq,
    Operator::NotEq,
    Operator::Gt,
    Operator::Gte,
    Operator::Lt,
    Operator::Lte,
];

const ARITH_OPS: [Operator; 4] = [Operator::Add, Operator::Sub, Operator::Mul, Operator::Div];

fn comparable(dtype: &DType) -> bool {
    match dtype {
        DType::Extension(ext) => comparable(ext.storage_dtype()),
        d => is_leaf(d),
    }
}

fn arithmetic(dtype: &DType) -> bool {
    match dtype {
        DType::Primitive(..) => true,
        // Keep decimal intermediates within i128.
        DType::Decimal(d, _) => d.precision() <= 16,
        _ => false,
    }
}

fn is_naive_timestamp(dtype: &DType) -> bool {
    let DType::Extension(ext) = dtype else {
        return false;
    };
    ext.metadata_opt::<Timestamp>()
        .is_some_and(|m| m.tz.is_none())
}

/// The stats readable through `vortex.stat`.
#[derive(Debug, Clone, Copy)]
pub enum StatKind {
    NullCount,
    Min,
    Max,
}

fn cast_target(u: &mut Unstructured<'_>, dtype: &DType) -> arbitrary::Result<DType> {
    let nullability: Nullability = u.arbitrary()?;
    Ok(match dtype {
        DType::Bool(_) | DType::Primitive(..) => {
            if dtype.is_boolean() && u.ratio(1, 4)? {
                DType::Bool(nullability)
            } else {
                DType::Primitive(u.arbitrary()?, nullability)
            }
        }
        DType::Decimal(..) => match u.int_in_range(0..=2)? {
            0 => DType::Primitive(
                *u.choose(&[PType::I8, PType::I32, PType::I64, PType::U16])?,
                nullability,
            ),
            1 => DType::Primitive(PType::F64, nullability),
            _ => {
                let precision = u.int_in_range(1..=38)?;
                let scale = u.int_in_range(0..=i8::try_from(precision).unwrap_or(38))?;
                DType::Decimal(
                    vortex_array::dtype::DecimalDType::new(precision, scale),
                    nullability,
                )
            }
        },
        DType::Utf8(_) | DType::Binary(_) => dtype.with_nullability(nullability),
        _ => return Err(IncorrectFormat),
    })
}

/// LIKE patterns over the generator's string alphabet, with wildcards and escapes.
fn gen_pattern(u: &mut Unstructured<'_>) -> arbitrary::Result<String> {
    const PIECES: &[&str] = &[
        "%", "_", "a", "b", "c", "A", "é", "É", "🦀", " ", "\\%", "\\_", "\\\\",
    ];
    let n = u.int_in_range(0..=6)?;
    (0..n).map(|_| Ok(*u.choose(PIECES)?)).collect()
}

/// A concrete scalar function application.
#[derive(Debug, Clone)]
pub enum ScalarOp {
    Compare(Operator, Case),
    Arithmetic(Operator, Case),
    Logical(Operator, Case),
    Between {
        lower: Case,
        upper: Case,
        lower_strict: bool,
        upper_strict: bool,
    },
    Not,
    IsNull,
    IsNotNull,
    IsNan,
    FillNull(DType, Value),
    Mask(Case),
    Zip {
        if_false: Case,
        mask: Case,
    },
    Like {
        pattern: Case,
        negated: bool,
        case_insensitive: bool,
    },
    ByteLength,
    ListLength,
    ListContains {
        needle: Case,
        sql_null_semantics: bool,
    },
    GetItem(usize),
    Select {
        fields: Vec<bool>,
        include: bool,
    },
    Pack {
        copies: usize,
        nullability: Nullability,
    },
    Merge(Case),
    ExtStorage,
    Literal(DType, Value),
    Cast(DType),
    ListSum {
        skip_nans: bool,
    },
    PreparedSet {
        elems: Vec<Value>,
        sql_null_semantics: bool,
    },
    ReplaceTimeZone(Option<Arc<str>>),
    Stat(StatKind),
    VariantGet {
        field: bool,
        target: Option<PType>,
    },
}

/// The reference result of a scalar op: either the expected output or an expected error.
pub enum Expected {
    Values(Vec<Value>),
    /// Any of these outputs is acceptable (e.g. a stat that may or may not be cached).
    AnyOf(Vec<Vec<Value>>),
    Error(String),
}

impl ScalarOp {
    /// Build the Vortex expression for this op over `input`.
    pub fn vortex(&self, input: &ArrayRef, dtype: &DType) -> VortexResult<ArrayRef> {
        let len = input.len();
        let apply = |f: ScalarFnRef, children: Vec<ArrayRef>| -> VortexResult<ArrayRef> {
            Ok(ScalarFnArray::try_new(f, children)?.into_array())
        };
        let input = input.clone();
        match self {
            ScalarOp::Compare(op, rhs)
            | ScalarOp::Arithmetic(op, rhs)
            | ScalarOp::Logical(op, rhs) => apply(Binary.bind(*op), vec![input, rhs.array.clone()]),
            ScalarOp::Between {
                lower,
                upper,
                lower_strict,
                upper_strict,
            } => apply(
                Between.bind(BetweenOptions {
                    lower_strict: strictness(*lower_strict),
                    upper_strict: strictness(*upper_strict),
                }),
                vec![input, lower.array.clone(), upper.array.clone()],
            ),
            ScalarOp::Not => apply(Not.bind(EmptyOptions), vec![input]),
            ScalarOp::IsNull => apply(IsNull.bind(EmptyOptions), vec![input]),
            ScalarOp::IsNotNull => apply(IsNotNull.bind(EmptyOptions), vec![input]),
            ScalarOp::IsNan => apply(IsNan.bind(EmptyOptions), vec![input]),
            ScalarOp::FillNull(fill_dtype, fill) => apply(
                FillNull.bind(EmptyOptions),
                vec![
                    input,
                    ConstantArray::new(to_scalar(fill_dtype, fill)?, len).into_array(),
                ],
            ),
            ScalarOp::Mask(mask) => apply(Mask.bind(EmptyOptions), vec![input, mask.array.clone()]),
            ScalarOp::Zip { if_false, mask } => apply(
                Zip.bind(EmptyOptions),
                vec![input, if_false.array.clone(), mask.array.clone()],
            ),
            ScalarOp::Like {
                pattern,
                negated,
                case_insensitive,
            } => apply(
                Like.bind(LikeOptions {
                    negated: *negated,
                    case_insensitive: *case_insensitive,
                }),
                vec![input, pattern.array.clone()],
            ),
            ScalarOp::ByteLength => apply(ByteLength.bind(EmptyOptions), vec![input]),
            ScalarOp::ListLength => apply(ListLength.bind(EmptyOptions), vec![input]),
            ScalarOp::ListContains {
                needle,
                sql_null_semantics,
            } => apply(
                ListContains.bind(ListContainsOptions {
                    sql_null_semantics: *sql_null_semantics,
                }),
                vec![input, needle.array.clone()],
            ),
            ScalarOp::GetItem(idx) => {
                let DType::Struct(fields, _) = dtype else {
                    vortex_bail!("get_item on {dtype}");
                };
                let Some(name) = fields.field_name(*idx).cloned() else {
                    vortex_bail!("no field {idx} in {dtype}");
                };
                apply(GetItem.bind(name), vec![input])
            }
            ScalarOp::Select { fields, include } => {
                let DType::Struct(struct_fields, _) = dtype else {
                    vortex_bail!("select on {dtype}");
                };
                let names: FieldNames = struct_fields
                    .names()
                    .iter()
                    .zip(fields)
                    .filter(|(_, keep)| **keep)
                    .map(|(n, _)| n.clone())
                    .collect();
                let selection = if *include {
                    FieldSelection::Include(names)
                } else {
                    FieldSelection::Exclude(names)
                };
                apply(Select.bind(selection), vec![input])
            }
            ScalarOp::Pack {
                copies,
                nullability,
            } => apply(
                Pack.bind(PackOptions {
                    names: (0..*copies).map(|i| format!("p{i}")).collect(),
                    nullability: *nullability,
                }),
                vec![input; *copies],
            ),
            ScalarOp::Merge(other) => apply(
                Merge.bind(DuplicateHandling::RightMost),
                vec![input, other.array.clone()],
            ),
            ScalarOp::ExtStorage => apply(ExtStorage.bind(EmptyOptions), vec![input]),
            ScalarOp::Cast(target) => apply(Cast.bind(target.clone()), vec![input]),
            ScalarOp::ListSum { skip_nans } => apply(
                ListSum.bind(NumericalAggregateOpts {
                    skip_nans: *skip_nans,
                }),
                vec![input],
            ),
            ScalarOp::PreparedSet {
                elems,
                sql_null_semantics,
            } => {
                let elem_dtype = dtype.as_nullable();
                let list = vortex_array::scalar::Scalar::list(
                    Arc::new(elem_dtype.clone()),
                    elems
                        .iter()
                        .map(|v| to_scalar(&elem_dtype, v))
                        .collect::<VortexResult<Vec<_>>>()?,
                    Nullability::NonNullable,
                );
                apply(
                    ListContains.bind(ListContainsOptions {
                        sql_null_semantics: *sql_null_semantics,
                    }),
                    vec![PreparedSet::try_new_array(list, len)?, input],
                )
            }
            ScalarOp::ReplaceTimeZone(tz) => apply(
                ReplaceTimeZone.bind(ReplaceTimeZoneOptions {
                    time_zone: tz.clone(),
                    null_on_non_existent: false,
                }),
                vec![input, ConstantArray::new("raise", len).into_array()],
            ),
            ScalarOp::Stat(stat) => {
                let opts = NumericalAggregateOpts { skip_nans: true };
                let f = match stat {
                    StatKind::NullCount => AggregateFn::new(NullCount, AggEmptyOptions).erased(),
                    StatKind::Min => AggregateFn::new(Min, opts).erased(),
                    StatKind::Max => AggregateFn::new(Max, opts).erased(),
                };
                apply(StatFn.bind(StatOptions::new(f)), vec![input])
            }
            ScalarOp::VariantGet { field, target } => {
                let path = if *field {
                    VariantPath::field("a")
                } else {
                    VariantPath::root()
                };
                let target = target.map(|p| DType::Primitive(p, Nullability::Nullable));
                apply(
                    VariantGet.bind(VariantGetOptions::new(path, target)),
                    vec![input],
                )
            }
            ScalarOp::Literal(dtype, v) => {
                Ok(
                    ScalarFnArray::try_new_with_len(Literal.bind(to_scalar(dtype, v)?), [], len)?
                        .into_array(),
                )
            }
        }
    }

    /// Evaluate this op with the plain Rust reference.
    pub fn reference(&self, input: &[Value], dtype: &DType) -> Expected {
        let len = input.len();
        let col = |i: usize| -> &Value { &input[i] };
        let values: Vec<Value> = match self {
            ScalarOp::Compare(op, rhs) => (0..len)
                .map(|i| match (col(i), &rhs.values[i]) {
                    (Value::Null, _) | (_, Value::Null) => Value::Null,
                    (a, b) => Value::Bool(compare(*op, a.total_cmp(b))),
                })
                .collect(),
            ScalarOp::Arithmetic(op, rhs) => {
                return arithmetic_reference(*op, dtype, input, &rhs.values);
            }
            ScalarOp::Logical(op, rhs) => (0..len)
                .map(|i| kleene(*op, col(i).as_bool(), rhs.values[i].as_bool()))
                .collect(),
            ScalarOp::Between {
                lower,
                upper,
                lower_strict,
                upper_strict,
            } => (0..len)
                .map(|i| {
                    let lo = cmp_bound(&lower.values[i], col(i), *lower_strict);
                    let hi = cmp_bound(col(i), &upper.values[i], *upper_strict);
                    kleene(Operator::And, lo, hi)
                })
                .collect(),
            ScalarOp::Not => input
                .iter()
                .map(|v| v.as_bool().map_or(Value::Null, |b| Value::Bool(!b)))
                .collect(),
            ScalarOp::IsNull => input.iter().map(|v| Value::Bool(v.is_null())).collect(),
            ScalarOp::IsNotNull => input.iter().map(|v| Value::Bool(!v.is_null())).collect(),
            ScalarOp::IsNan => input
                .iter()
                .map(|v| {
                    v.as_float()
                        .map_or(Value::Null, |f| Value::Bool(f.is_nan()))
                })
                .collect(),
            ScalarOp::FillNull(_, fill) => input
                .iter()
                .map(|v| if v.is_null() { fill.clone() } else { v.clone() })
                .collect(),
            ScalarOp::Mask(mask) => input
                .iter()
                .zip(&mask.values)
                .map(|(v, m)| {
                    if m.as_bool() == Some(true) {
                        v.clone()
                    } else {
                        Value::Null
                    }
                })
                .collect(),
            ScalarOp::Zip { if_false, mask } => (0..len)
                .map(|i| {
                    if mask.values[i].as_bool() == Some(true) {
                        col(i).clone()
                    } else {
                        if_false.values[i].clone()
                    }
                })
                .collect(),
            ScalarOp::Like {
                pattern,
                negated,
                case_insensitive,
            } => (0..len)
                .map(
                    |i| match (col(i).as_bytes(), pattern.values[i].as_bytes()) {
                        (Some(s), Some(p)) => {
                            let matched = like(
                                &String::from_utf8_lossy(s),
                                &String::from_utf8_lossy(p),
                                *case_insensitive,
                            );
                            Value::Bool(matched != *negated)
                        }
                        _ => Value::Null,
                    },
                )
                .collect(),
            ScalarOp::ByteLength => input
                .iter()
                .map(|v| {
                    v.as_bytes()
                        .map_or(Value::Null, |b| Value::Int(b.len() as i128))
                })
                .collect(),
            ScalarOp::ListLength => input
                .iter()
                .map(|v| {
                    v.as_tuple()
                        .map_or(Value::Null, |t| Value::Int(t.len() as i128))
                })
                .collect(),
            ScalarOp::ListContains {
                needle,
                sql_null_semantics,
            } => (0..len)
                .map(|i| list_contains(col(i), &needle.values[i], *sql_null_semantics))
                .collect(),
            ScalarOp::GetItem(idx) => input
                .iter()
                .map(|v| v.as_tuple().map_or(Value::Null, |t| t[*idx].clone()))
                .collect(),
            ScalarOp::Select { fields, include } => input
                .iter()
                .map(|v| {
                    v.as_tuple().map_or(Value::Null, |t| {
                        Value::Tuple(
                            t.iter()
                                .zip(fields)
                                .filter(|(_, keep)| **keep == *include)
                                .map(|(v, _)| v.clone())
                                .collect(),
                        )
                    })
                })
                .collect(),
            ScalarOp::Pack { copies, .. } => input
                .iter()
                .map(|v| Value::Tuple(vec![v.clone(); *copies]))
                .collect(),
            ScalarOp::Merge(other) => {
                let DType::Struct(fields, _) = dtype else {
                    return Expected::Error("merge on non-struct".into());
                };
                let DType::Struct(other_fields, _) = &other.dtype else {
                    return Expected::Error("merge with non-struct".into());
                };
                let overridden = other_fields
                    .names()
                    .iter()
                    .filter_map(|n| fields.find(n))
                    .collect::<Vec<_>>();
                (0..len)
                    .map(|i| {
                        let (Some(l), Some(r)) = (col(i).as_tuple(), other.values[i].as_tuple())
                        else {
                            return Value::Null;
                        };
                        let mut out = l.to_vec();
                        let mut appended = Vec::new();
                        for (name, v) in other_fields.names().iter().zip(r) {
                            match fields.find(name) {
                                Some(pos) => out[pos] = v.clone(),
                                None => appended.push(v.clone()),
                            }
                        }
                        let _ = &overridden;
                        out.extend(appended);
                        Value::Tuple(out)
                    })
                    .collect()
            }
            ScalarOp::ExtStorage => input.to_vec(),
            ScalarOp::Literal(_, v) => vec![v.clone(); len],
            ScalarOp::Cast(target) => return cast_reference(dtype, target, input),
            ScalarOp::ListSum { skip_nans } => {
                let DType::List(elem, _) = dtype else {
                    return Expected::Error("list_sum on non-list".into());
                };
                input
                    .iter()
                    .map(|v| list_sum(elem, v, *skip_nans))
                    .collect()
            }
            ScalarOp::PreparedSet {
                elems,
                sql_null_semantics,
            } => {
                let list = Value::Tuple(elems.clone());
                input
                    .iter()
                    .map(|needle| list_contains(&list, needle, *sql_null_semantics))
                    .collect()
            }
            ScalarOp::ReplaceTimeZone(tz) => {
                let offset_micros: i128 = match tz.as_deref() {
                    Some("+01:00") => 3_600_000_000,
                    Some("-05:30") => -19_800_000_000,
                    _ => 0,
                };
                let mut out = Vec::with_capacity(len);
                for v in input {
                    out.push(match v.as_int() {
                        None => Value::Null,
                        Some(micros) => {
                            let shifted = micros - offset_micros;
                            if !(super::generate::MIN_TIMESTAMP_MICROS
                                ..=super::generate::MAX_TIMESTAMP_MICROS)
                                .contains(&shifted)
                            {
                                return Expected::Error("timestamp out of range".into());
                            }
                            Value::Int(shifted)
                        }
                    });
                }
                out
            }
            ScalarOp::Stat(stat) => {
                let exact = match stat {
                    StatKind::NullCount => {
                        Value::Int(input.iter().filter(|v| v.is_null()).count() as i128)
                    }
                    StatKind::Min | StatKind::Max => {
                        let non_null = input
                            .iter()
                            .filter(|v| !v.is_null() && !v.as_float().is_some_and(f64::is_nan));
                        let best = match stat {
                            StatKind::Min => non_null.min_by(|a, b| a.total_cmp(b)),
                            _ => non_null.max_by(|a, b| a.total_cmp(b)),
                        };
                        best.cloned().unwrap_or(Value::Null)
                    }
                };
                return Expected::AnyOf(vec![vec![Value::Null; len], vec![exact; len]]);
            }
            ScalarOp::VariantGet { field, target } => input
                .iter()
                .map(|v| match (v, field, target) {
                    (_, true, _) | (Value::Null, ..) => Value::Null,
                    (v, false, None) => v.clone(),
                    (Value::Int(i), false, Some(p)) => {
                        let (lo, hi) = super::generate::int_bounds(*p);
                        if (lo..=hi).contains(i) {
                            Value::Int(*i)
                        } else {
                            Value::Null
                        }
                    }
                    _ => Value::Null,
                })
                .collect(),
        };
        Expected::Values(values)
    }
}

fn strictness(strict: bool) -> StrictComparison {
    if strict {
        StrictComparison::Strict
    } else {
        StrictComparison::NonStrict
    }
}

fn compare(op: Operator, ord: Ordering) -> bool {
    match op {
        Operator::Eq => ord.is_eq(),
        Operator::NotEq => ord.is_ne(),
        Operator::Gt => ord.is_gt(),
        Operator::Gte => ord.is_ge(),
        Operator::Lt => ord.is_lt(),
        Operator::Lte => ord.is_le(),
        _ => false,
    }
}

/// `a < b` (strict) or `a <= b`, null if either side is null.
fn cmp_bound(a: &Value, b: &Value, strict: bool) -> Option<bool> {
    if a.is_null() || b.is_null() {
        return None;
    }
    let ord = a.total_cmp(b);
    Some(if strict { ord.is_lt() } else { ord.is_le() })
}

/// Three-valued AND/OR.
fn kleene(op: Operator, lhs: Option<bool>, rhs: Option<bool>) -> Value {
    let result = match op {
        Operator::And => match (lhs, rhs) {
            (Some(false), _) | (_, Some(false)) => Some(false),
            (Some(true), Some(true)) => Some(true),
            _ => None,
        },
        _ => match (lhs, rhs) {
            (Some(true), _) | (_, Some(true)) => Some(true),
            (Some(false), Some(false)) => Some(false),
            _ => None,
        },
    };
    result.map_or(Value::Null, Value::Bool)
}

fn list_contains(list: &Value, needle: &Value, sql: bool) -> Value {
    let Some(elems) = list.as_tuple() else {
        return Value::Null;
    };
    if needle.is_null() {
        return if sql || !elems.is_empty() {
            Value::Null
        } else {
            Value::Bool(false)
        };
    }
    if elems
        .iter()
        .any(|e| !e.is_null() && e.total_cmp(needle).is_eq())
    {
        Value::Bool(true)
    } else if sql && elems.iter().any(Value::is_null) {
        Value::Null
    } else {
        Value::Bool(false)
    }
}

/// SQL LIKE: `%` matches any sequence, `_` exactly one character and `\` escapes the next one.
pub fn like(s: &str, pattern: &str, case_insensitive: bool) -> bool {
    #[derive(Clone, Copy)]
    enum Tok {
        Any,
        One,
        Char(char),
    }
    let fold = |c: char| -> char {
        if case_insensitive {
            c.to_lowercase().next().unwrap_or(c)
        } else {
            c
        }
    };
    let mut toks = Vec::new();
    let mut chars = pattern.chars();
    while let Some(c) = chars.next() {
        toks.push(match c {
            '%' => Tok::Any,
            '_' => Tok::One,
            '\\' => Tok::Char(fold(chars.next().unwrap_or('\\'))),
            c => Tok::Char(fold(c)),
        });
    }
    let text: Vec<char> = s.chars().map(fold).collect();
    // Classic DP over (pattern position, text position).
    let mut dp = vec![false; text.len() + 1];
    dp[0] = true;
    for tok in &toks {
        let mut next = vec![false; text.len() + 1];
        match tok {
            Tok::Any => {
                let mut seen = false;
                for j in 0..=text.len() {
                    seen |= dp[j];
                    next[j] = seen;
                }
            }
            Tok::One => next[1..].copy_from_slice(&dp[..text.len()]),
            Tok::Char(c) => {
                for j in 1..=text.len() {
                    next[j] = dp[j - 1] && text[j - 1] == *c;
                }
            }
        }
        dp = next;
    }
    dp[text.len()]
}

fn arithmetic_reference(op: Operator, dtype: &DType, lhs: &[Value], rhs: &[Value]) -> Expected {
    let mut out = Vec::with_capacity(lhs.len());
    for (l, r) in lhs.iter().zip(rhs) {
        let v = match (dtype, l, r) {
            (_, Value::Null, _) | (_, _, Value::Null) => Value::Null,
            (DType::Primitive(ptype, _), Value::Int(lhs), Value::Int(rhs)) => {
                let (lo, hi) = super::generate::int_bounds(*ptype);
                let result = match op {
                    Operator::Add => lhs.checked_add(*rhs),
                    Operator::Sub => lhs.checked_sub(*rhs),
                    Operator::Mul => lhs.checked_mul(*rhs),
                    _ => lhs.checked_div(*rhs),
                };
                match result.filter(|r| (lo..=hi).contains(r)) {
                    Some(result) => Value::Int(result),
                    None => {
                        return Expected::Error(format!("{lhs} {op} {rhs} overflows {ptype}"));
                    }
                }
            }
            (DType::Primitive(ptype, _), Value::Float(lhs), Value::Float(rhs)) => {
                Value::Float(float_op(*ptype, op, *lhs, *rhs))
            }
            (DType::Decimal(decimal, _), Value::Decimal(lhs), Value::Decimal(rhs)) => {
                let (precision, value) = match op {
                    Operator::Add => (decimal.precision() + 1, lhs.checked_add(*rhs)),
                    Operator::Sub => (decimal.precision() + 1, lhs.checked_sub(*rhs)),
                    Operator::Mul => (2 * decimal.precision() + 1, lhs.checked_mul(*rhs)),
                    _ => {
                        let scale = u32::try_from(decimal.scale()).unwrap_or(0) + 4;
                        let scaled = lhs.checked_mul(10i128.pow(scale));
                        (
                            decimal.precision() + u8::try_from(scale).unwrap_or(u8::MAX),
                            scaled.and_then(|s| s.checked_div(*rhs)),
                        )
                    }
                };
                match value {
                    Some(v) if v.unsigned_abs() < 10u128.pow(u32::from(precision.min(38))) => {
                        Value::Decimal(v)
                    }
                    _ => return Expected::Error(format!("{lhs} {op} {rhs} overflows {decimal}")),
                }
            }
            _ => return Expected::Error(format!("unsupported arithmetic on {dtype}")),
        };
        out.push(v);
    }
    Expected::Values(out)
}

/// IEEE arithmetic in the storage precision of `ptype`.
#[expect(
    clippy::cast_possible_truncation,
    reason = "values are stored at this precision"
)]
fn float_op(ptype: PType, op: Operator, a: f64, b: f64) -> f64 {
    fn apply<
        T: std::ops::Add<Output = T>
            + std::ops::Sub<Output = T>
            + std::ops::Mul<Output = T>
            + std::ops::Div<Output = T>,
    >(
        op: Operator,
        a: T,
        b: T,
    ) -> T {
        match op {
            Operator::Add => a + b,
            Operator::Sub => a - b,
            Operator::Mul => a * b,
            _ => a / b,
        }
    }
    match ptype {
        PType::F16 => f64::from(apply(op, f16::from_f64(a), f16::from_f64(b))),
        PType::F32 => f64::from(apply(op, a as f32, b as f32)),
        _ => apply(op, a, b),
    }
}

fn list_sum(elem: &DType, list: &Value, skip_nans: bool) -> Value {
    let Some(elems) = list.as_tuple() else {
        return Value::Null;
    };
    let valid: Vec<&Value> = elems.iter().filter(|v| !v.is_null()).collect();
    if valid.is_empty() {
        return Value::Null;
    }
    match elem {
        DType::Bool(_) => {
            Value::Int(valid.iter().filter(|v| v.as_bool() == Some(true)).count() as i128)
        }
        DType::Primitive(p, _) if p.is_float() => {
            let mut acc = 0.0f64;
            for v in valid {
                let f = v.as_float().unwrap_or_default();
                if f.is_nan() && skip_nans {
                    continue;
                }
                acc += f;
            }
            Value::Float(acc)
        }
        DType::Primitive(p, _) => {
            let (lo, hi) = if p.is_unsigned_int() {
                (0, i128::from(u64::MAX))
            } else {
                (i128::from(i64::MIN), i128::from(i64::MAX))
            };
            let mut acc: i128 = 0;
            for v in valid {
                acc += v.as_int().unwrap_or_default();
                if !(lo..=hi).contains(&acc) {
                    return Value::Null;
                }
            }
            Value::Int(acc)
        }
        DType::Decimal(..) => Value::Decimal(valid.iter().filter_map(|v| v.as_int()).sum()),
        _ => Value::Null,
    }
}

fn cast_reference(from: &DType, to: &DType, input: &[Value]) -> Expected {
    if !to.is_nullable() && input.iter().any(Value::is_null) {
        return Expected::Error("cannot cast nulls to a non-nullable type".into());
    }
    let mut out = Vec::with_capacity(input.len());
    for v in input {
        if v.is_null() {
            out.push(Value::Null);
            continue;
        }
        match cast_value(from, to, v) {
            Ok(v) => out.push(v),
            Err(e) => return Expected::Error(e),
        }
    }
    Expected::Values(out)
}

#[expect(clippy::cast_possible_truncation, reason = "mirrors `as` casts")]
#[expect(clippy::cast_precision_loss, reason = "mirrors `as` casts")]
fn cast_value(from: &DType, to: &DType, value: &Value) -> Result<Value, String> {
    let out_of_range = || format!("{value} does not fit in {to}");
    Ok(match (from, to, value) {
        (DType::Bool(_), DType::Bool(_), value) => value.clone(),
        (DType::Bool(_), DType::Primitive(ptype, _), Value::Bool(flag)) => {
            if ptype.is_float() {
                Value::Float(if *flag { 1.0 } else { 0.0 })
            } else {
                Value::Int(i128::from(*flag))
            }
        }
        (DType::Primitive(..), DType::Bool(_), _) => return Err("cannot cast to bool".into()),
        (DType::Primitive(..), DType::Primitive(to_p, _), Value::Int(int)) => {
            if to_p.is_float() {
                let float = match to_p {
                    PType::F16 => f64::from(f16::from_f32(*int as f32)),
                    PType::F32 => f64::from(*int as f32),
                    _ => *int as f64,
                };
                Value::Float(float)
            } else {
                let (lo, hi) = super::generate::int_bounds(*to_p);
                if !(lo..=hi).contains(int) {
                    return Err(out_of_range());
                }
                Value::Int(*int)
            }
        }
        (DType::Primitive(from_p, _), DType::Primitive(to_p, _), Value::Float(float)) => {
            if to_p.is_float() {
                Value::Float(match (from_p, to_p) {
                    (_, PType::F64) => *float,
                    (_, PType::F32) => f64::from(*float as f32),
                    (PType::F16, PType::F16) => *float,
                    _ => f64::from(f16::from_f32(*float as f32)),
                })
            } else {
                let truncated = float.trunc();
                let (lo, hi) = super::generate::int_bounds(*to_p);
                if !truncated.is_finite() || truncated < lo as f64 || truncated > hi as f64 {
                    return Err(out_of_range());
                }
                Value::Int(truncated as i128)
            }
        }
        (
            DType::Decimal(from_decimal, _),
            DType::Decimal(to_decimal, _),
            Value::Decimal(unscaled),
        ) => {
            let diff = i32::from(to_decimal.scale()) - i32::from(from_decimal.scale());
            let factor = 10i128.pow(diff.unsigned_abs());
            let scaled = if diff >= 0 {
                unscaled.checked_mul(factor).ok_or_else(out_of_range)?
            } else if unscaled % factor == 0 {
                unscaled / factor
            } else {
                return Err(format!("{value} loses precision when rescaled to {to}"));
            };
            if scaled.unsigned_abs() >= 10u128.pow(u32::from(to_decimal.precision())) {
                return Err(out_of_range());
            }
            Value::Decimal(scaled)
        }
        (
            DType::Decimal(from_decimal, _),
            DType::Primitive(PType::F64, _),
            Value::Decimal(unscaled),
        ) => Value::Float(*unscaled as f64 / 10f64.powi(i32::from(from_decimal.scale()))),
        (DType::Decimal(from_decimal, _), DType::Primitive(to_p, _), Value::Decimal(unscaled)) => {
            // Truncates toward zero, but the exact value must already be within range.
            let factor = 10i128.pow(u32::try_from(from_decimal.scale()).unwrap_or(0));
            let int = unscaled / factor;
            let (lo, hi) = super::generate::int_bounds(*to_p);
            let fractional = unscaled % factor != 0;
            if !(lo..=hi).contains(&int)
                || (fractional && ((*unscaled < 0 && int == lo) || (*unscaled > 0 && int == hi)))
            {
                return Err(out_of_range());
            }
            Value::Int(int)
        }
        (DType::Utf8(_), DType::Utf8(_), value) | (DType::Binary(_), DType::Binary(_), value) => {
            value.clone()
        }
        _ => return Err(format!("unsupported cast from {from} to {to}")),
    })
}

#[cfg(test)]
mod tests {
    use arbitrary::Unstructured;
    use strum::IntoEnumIterator;
    use vortex_array::VortexSessionExecute;
    use vortex_array::scalar_fn::session::ScalarFnSessionExt;
    use vortex_utils::aliases::hash_set::HashSet;

    use super::*;
    use crate::SESSION;
    use crate::reference::FuzzArrayReference;
    use crate::reference::encodings::EncodingKind;
    use crate::reference::encodings::Limits;
    use crate::reference::ops::RefAction;
    use crate::reference::run_array_reference;
    use crate::reference::tests::test_bytes;

    /// Every scalar function registered in the fuzz session must have a reference.
    #[test]
    fn every_registered_scalar_fn_has_a_reference() {
        let covered: HashSet<&str> = ScalarOpKind::iter()
            .flat_map(|k| k.ids().iter().copied())
            .collect();
        let registered: Vec<String> = SESSION
            .scalar_fns()
            .registry()
            .read(|map| map.keys().map(ToString::to_string).collect());
        let missing: Vec<_> = registered
            .iter()
            .filter(|id| !covered.contains(id.as_str()))
            .collect();
        assert!(
            missing.is_empty(),
            "scalar functions without a reference: {missing:?}"
        );
    }

    /// Every op kind must be generated for some input and agree with its reference.
    #[test]
    fn every_scalar_op_kind_is_exercised() {
        let mut ctx = SESSION.create_execution_ctx();
        let encodings = EncodingKind::iter().collect::<Vec<_>>();
        let mut failures = Vec::new();
        for kind in ScalarOpKind::iter() {
            let mut exercised = 0;
            for seed in 0..3000u64 {
                let data = test_bytes(seed);
                let mut u = Unstructured::new(&data);
                let Ok(encoding) = u.choose(&encodings).copied() else {
                    continue;
                };
                let Ok(case) = encoding.generate(&mut u, None, Limits::default(), &mut ctx) else {
                    continue;
                };
                let Ok(op) = kind.generate(&mut u, &case, &mut ctx) else {
                    continue;
                };
                let input = FuzzArrayReference {
                    case,
                    actions: vec![RefAction::Scalar(op)],
                };
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    run_array_reference(input).map_err(|e| e.to_string())
                }));
                match result {
                    Ok(Ok(_)) => exercised += 1,
                    Ok(Err(e)) => {
                        failures.push(format!("{kind:?} seed {seed}: {e}"));
                        break;
                    }
                    Err(e) => {
                        let msg = e
                            .downcast_ref::<String>()
                            .map(String::as_str)
                            .or_else(|| e.downcast_ref::<&str>().copied())
                            .unwrap_or_default()
                            .lines()
                            .next()
                            .unwrap_or_default()
                            .to_string();
                        failures.push(format!("{kind:?} seed {seed}: panicked: {msg}"));
                        break;
                    }
                }
                if exercised >= 20 {
                    break;
                }
            }
            if exercised == 0 {
                failures.push(format!("{kind:?} was never exercised"));
            }
        }
        assert!(failures.is_empty(), "{}", failures.join("\n\n"));
    }
}
