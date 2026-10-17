// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Compare one portable function on Arrow and Vortex with matched logical inputs.
//!
//! Input construction and output planning stay outside the timed calls. Each fixture checks
//! Arrow's kernel, the Arrow binding, and the Vortex binding before measurement.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef as ArrowArrayRef, BooleanArray, Int32Array, Int64Array,
    Scalar, StringViewArray};
use arrow_schema::{DataType, Field};
use divan::{Bencher, black_box};
use rowfn::InputBinding;
use rowfn_arrow::ArrowOperand;
use rowfn_examples::{ByteLength, Like, MultiplyWrapping, Negate, NoOptions, StringEqual,
    StringLessThan, StringPredicate};
use vortex_array::{ArrayRef as VortexArrayRef, ExecutionCtx, IntoArray, VortexSessionExecute,
    array_session, assert_arrays_eq};
use vortex_array::arrays::{BoolArray, ConstantArray, PrimitiveArray, VarBinViewArray};
use vortex_array::dtype::DType;
use vortex_array::scalar_fn::{EmptyOptions, ScalarFnId, VecExecutionArgs};
use vortex_array::scalar_fn::unstable::rowfn::{self as vortex_rowfn, VortexHost};
use vortex_array::scalar_fn::unstable::row::{self as legacy, RowFn as LegacyRowFn, Utf8Column};
use vortex_error::VortexResult;
use vortex_session::registry::CachedId;

const SIZES: &[usize] = &[64, 1024, 16_384];

#[derive(Clone, Copy, Debug)]
enum Case {
    WrappingNegate,
    WrappingMultiply,
    ByteLengthView,
    StringEqualViewShort,
    StringLessThanViewScalar,
    LikeLiteralScalar,
    StartsWithScalar,
}

const CASES: &[Case] = &[
    Case::WrappingNegate,
    Case::WrappingMultiply,
    Case::ByteLengthView,
    Case::StringEqualViewShort,
    Case::StringLessThanViewScalar,
    Case::LikeLiteralScalar,
    Case::StartsWithScalar,
];

const STRING_CASES: &[Case] = &[
    Case::ByteLengthView,
    Case::StringEqualViewShort,
    Case::StringLessThanViewScalar,
    Case::LikeLiteralScalar,
    Case::StartsWithScalar,
];

fn main() {
    divan::main();
}

fn arrow_operand(column: ArrowArrayRef, scalar: bool) -> ArrowOperand {
    ArrowOperand {
        dtype: Field::new("input", column.data_type().clone(), column.logical_null_count() != 0),
        column,
        scalar,
    }
}

fn string_column(values: &[Option<&str>], rows: usize) -> (ArrowOperand, VortexArrayRef) {
    let arrow: ArrowArrayRef = Arc::new(StringViewArray::from(values.to_vec()));
    let vortex = VarBinViewArray::from_iter_nullable_str(values.iter().copied())
        .into_array()
        .slice(3..rows + 3)
        .expect("fixture slice");
    (arrow_operand(arrow.slice(3, rows), false), vortex)
}

struct Fixture {
    case: Case,
    arrow: Vec<ArrowOperand>,
    vortex: Vec<VortexArrayRef>,
    arrow_output: Field,
    vortex_output: DType,
    rows: usize,
}

impl Fixture {
    fn new(case: Case, rows: usize) -> Self {
        let (arrow, vortex) = match case {
            Case::WrappingNegate | Case::WrappingMultiply => {
                let values: Vec<_> = (0..rows + 3).map(|index| (index % 1024) as i64 + 1)
                    .collect();
                let left: ArrowArrayRef = Arc::new(Int64Array::from(values.clone()));
                let left_vortex = PrimitiveArray::from_iter(values).into_array()
                    .slice(3..rows + 3).expect("fixture slice");
                let mut inputs = (vec![arrow_operand(left.slice(3, rows), false)],
                    vec![left_vortex]);
                if matches!(case, Case::WrappingMultiply) {
                    let right: Vec<_> = (0..rows + 3)
                        .map(|index| (index % 7) as i64 + 1).collect();
                    let array: ArrowArrayRef = Arc::new(Int64Array::from(right.clone()));
                    inputs.0.push(arrow_operand(array.slice(3, rows), false));
                    inputs.1.push(PrimitiveArray::from_iter(right).into_array()
                        .slice(3..rows + 3).expect("fixture slice"));
                }
                inputs
            }
            _ => {
                let normal = ["arrow and Rust", "ARROW and rust", "a longer arrow payload",
                    "other values", "λ arrow", ""];
                let short = ["a", "b", "aa", "ab", "é", ""];
                let source = if matches!(case, Case::StringEqualViewShort) { &short } else { &normal };
                let text: Vec<_> = (0..rows + 3).map(|index| {
                    (!index.is_multiple_of(9)).then_some(source[index % source.len()])
                }).collect();
                let (left_arrow, left_vortex) = string_column(&text, rows);
                let mut inputs = (vec![left_arrow], vec![left_vortex]);
                match case {
                    Case::StringEqualViewShort => {
                        let right: Vec<_> = (0..rows + 3).map(|index| {
                            (!index.is_multiple_of(11)).then_some(
                                short[(index + index % 3) % short.len()])
                        }).collect();
                        let (right_arrow, right_vortex) = string_column(&right, rows);
                        inputs.0.push(right_arrow);
                        inputs.1.push(right_vortex);
                    }
                    Case::StringLessThanViewScalar | Case::LikeLiteralScalar
                        | Case::StartsWithScalar => {
                        let value = match case {
                            Case::StringLessThanViewScalar => "arrow and Rust",
                            Case::LikeLiteralScalar => "arrow%",
                            Case::StartsWithScalar => "arrow",
                            _ => unreachable!(),
                        };
                        inputs.0.push(arrow_operand(
                            Arc::new(StringViewArray::from(vec![Some(value)])), true));
                        inputs.1.push(ConstantArray::new(value, rows).into_array());
                    }
                    Case::ByteLengthView => {}
                    _ => unreachable!(),
                }
                inputs
            }
        };

        let arrow_fields: Vec<_> = arrow.iter().map(|input| input.dtype.clone()).collect();
        let vortex_types: Vec<_> = vortex.iter().map(|input| input.dtype().clone()).collect();
        macro_rules! plan {
            ($function:expr) => {{
                let function = $function;
                let arrow = rowfn_arrow::plan(&function, &NoOptions, &arrow_fields)
                    .expect("Arrow fixture type");
                let vortex = rowfn::plan::<VortexHost, _>(&function, &NoOptions, &vortex_types)
                    .expect("Vortex fixture type");
                (arrow, vortex.output_type().clone())
            }};
        }
        let (arrow_output, vortex_output) = match case {
            Case::WrappingNegate => plan!(Negate::<false>),
            Case::WrappingMultiply => plan!(MultiplyWrapping),
            Case::ByteLengthView => plan!(ByteLength),
            Case::StringEqualViewShort => plan!(StringEqual),
            Case::StringLessThanViewScalar => plan!(StringLessThan),
            Case::LikeLiteralScalar => plan!(Like::<false, false>),
            Case::StartsWithScalar => plan!(StringPredicate::StartsWith),
        };
        let fixture = Self { case, arrow, vortex, arrow_output, vortex_output, rows };
        let native = fixture.native();
        assert_eq!(native.to_data(), fixture.arrow_rowfn().to_data());
        let expected = match native.data_type() {
            DataType::Int64 => PrimitiveArray::from_iter(native.as_any()
                .downcast_ref::<Int64Array>().expect("i64 output").iter()
                .map(|value| value.expect("numeric fixture is non-null"))).into_array(),
            DataType::Int32 => PrimitiveArray::from_option_iter(native.as_any()
                .downcast_ref::<Int32Array>().expect("i32 output").iter()).into_array(),
            DataType::Boolean => BoolArray::from_iter(native.as_any()
                .downcast_ref::<BooleanArray>().expect("Boolean output").iter()).into_array(),
            _ => unreachable!("fixture output type"),
        };
        let mut ctx = array_session().create_execution_ctx();
        assert_arrays_eq!(&fixture.vortex_rowfn(&mut ctx), &expected, &mut ctx);
        fixture
    }

    fn arrow_rowfn(&self) -> ArrowArrayRef {
        macro_rules! invoke {
            ($function:expr) => {
                rowfn_arrow::invoke(&$function, &NoOptions, &self.arrow, self.rows,
                    self.arrow_output.clone()).expect("Arrow invocation").array
            };
        }
        match self.case {
            Case::WrappingNegate => invoke!(Negate::<false>),
            Case::WrappingMultiply => invoke!(MultiplyWrapping),
            Case::ByteLengthView => invoke!(ByteLength),
            Case::StringEqualViewShort => invoke!(StringEqual),
            Case::StringLessThanViewScalar => invoke!(StringLessThan),
            Case::LikeLiteralScalar => invoke!(Like::<false, false>),
            Case::StartsWithScalar => invoke!(StringPredicate::StartsWith),
        }
    }

    fn vortex_rowfn(&self, ctx: &mut ExecutionCtx) -> VortexArrayRef {
        macro_rules! invoke {
            ($function:expr) => {
                vortex_rowfn::execute(&$function, &NoOptions, &self.vortex, self.rows,
                    &self.vortex_output, ctx).expect("Vortex invocation")
            };
        }
        match self.case {
            Case::WrappingNegate => invoke!(Negate::<false>),
            Case::WrappingMultiply => invoke!(MultiplyWrapping),
            Case::ByteLengthView => invoke!(ByteLength),
            Case::StringEqualViewShort => invoke!(StringEqual),
            Case::StringLessThanViewScalar => invoke!(StringLessThan),
            Case::LikeLiteralScalar => invoke!(Like::<false, false>),
            Case::StartsWithScalar => invoke!(StringPredicate::StartsWith),
        }
    }

    fn native(&self) -> ArrowArrayRef {
        let left = &self.arrow[0].column;
        let right = self.arrow.get(1).map(|input| &input.column);
        let scalar;
        let rhs: Option<&dyn arrow_array::Datum> = match right {
            Some(right) if matches!(self.case, Case::StringLessThanViewScalar
                | Case::LikeLiteralScalar | Case::StartsWithScalar) => {
                scalar = Scalar::new(right);
                Some(&scalar)
            }
            Some(right) => Some(right),
            None => None,
        };
        match self.case {
            Case::WrappingNegate => arrow_arith::numeric::neg_wrapping(left)
                .expect("Arrow negate"),
            Case::WrappingMultiply => arrow_arith::numeric::mul_wrapping(left,
                rhs.expect("multiply rhs")).expect("Arrow multiply"),
            Case::ByteLengthView => arrow_string::length::length(left.as_ref())
                .expect("Arrow length"),
            Case::StringEqualViewShort => Arc::new(arrow_ord::cmp::eq(left,
                rhs.expect("equal rhs")).expect("Arrow equality")),
            Case::StringLessThanViewScalar => Arc::new(arrow_ord::cmp::lt(left,
                rhs.expect("order rhs")).expect("Arrow ordering")),
            Case::LikeLiteralScalar => Arc::new(arrow_string::like::like(left,
                rhs.expect("LIKE rhs")).expect("Arrow LIKE")),
            Case::StartsWithScalar => Arc::new(arrow_string::like::starts_with(left,
                rhs.expect("prefix rhs")).expect("Arrow prefix")),
        }
    }
}

#[divan::bench(args = CASES, consts = SIZES)]
fn arrow_native<const ROWS: usize>(bencher: Bencher, case: &Case) {
    let fixture = Fixture::new(*case, ROWS);
    bencher.bench(|| black_box(&fixture).native());
}

#[divan::bench(args = CASES, consts = SIZES)]
fn arrow_rowfn<const ROWS: usize>(bencher: Bencher, case: &Case) {
    let fixture = Fixture::new(*case, ROWS);
    bencher.bench(|| black_box(&fixture).arrow_rowfn());
}

#[divan::bench(args = CASES, consts = SIZES)]
fn vortex_rowfn_bench<const ROWS: usize>(bencher: Bencher, case: &Case) {
    let fixture = Fixture::new(*case, ROWS);
    let session = array_session();
    bencher.with_inputs(|| session.create_execution_ctx())
        .bench_refs(|ctx| black_box(&fixture).vortex_rowfn(ctx));
}

#[divan::bench(args = STRING_CASES, consts = SIZES)]
fn vortex_string_decode<const ROWS: usize>(bencher: Bencher, case: &Case) {
    let fixture = Fixture::new(*case, ROWS);
    let session = array_session();
    bencher.with_inputs(|| session.create_execution_ctx()).bench_refs(|ctx| {
        black_box(<VortexHost as InputBinding<rowfn::Utf8>>::decode(
            black_box(&fixture.vortex[0]), false, ctx).expect("Vortex string decode"))
    });
}

#[derive(Clone)]
struct LegacyByteLength;

impl LegacyRowFn for LegacyByteLength {
    type Options = EmptyOptions;
    const ARG_NAMES: &'static [&'static str] = &["text"];
    const INFALLIBLE: bool = true;

    fn id(&self) -> ScalarFnId {
        static ID: CachedId = CachedId::new("bench.rowfn.legacy_byte_length");
        *ID
    }

    fn dispatch<V: legacy::RowVisitor>(&self, _: &EmptyOptions, _: &[DType], visitor: V)
        -> VortexResult<V::VisitResult> {
        visitor.visit::<(Utf8Column,), i32>(|(text,)| text.as_str().len() as i32)
    }
}

#[divan::bench(consts = SIZES)]
fn vortex_legacy_byte_length<const ROWS: usize>(bencher: Bencher) {
    let fixture = Fixture::new(Case::ByteLengthView, ROWS);
    let args = VecExecutionArgs::new(fixture.vortex.clone(), ROWS);
    let session = array_session();
    bencher.with_inputs(|| session.create_execution_ctx()).bench_refs(|ctx| {
        legacy::execute_rows(&LegacyByteLength, &EmptyOptions, black_box(&args), ctx)
            .expect("legacy Vortex byte length")
    });
}
