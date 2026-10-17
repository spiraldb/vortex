// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Existing Arrow scalar kernels versus complete rowfn invocations with matched semantics.

use std::sync::Arc;

use arrow_array::Array;
use arrow_array::ArrayRef;
use arrow_array::Int64Array;
use arrow_array::Scalar;
use arrow_array::StringArray;
use arrow_array::StringViewArray;
use arrow_schema::ArrowError;
use arrow_schema::Field;
use divan::Bencher;
use divan::black_box;
use rowfn_arrow::ArrowOperand;
use rowfn_examples::Concat;
use rowfn_examples::Multiply;
use rowfn_examples::NoOptions;
use rowfn_examples::StringPredicate;
use vortex_error::VortexExpect;

#[path = "diagnosis/strings.rs"]
mod diagnosis;

const SIZES: &[usize] = &[0, 64, 1024, 16_384];

#[derive(Clone, Copy, Debug)]
enum Case {
    StartsWithShortScalar,
    StartsWithScalar,
    StartsWithArray,
    StartsWithUtf8Scalar,
    EndsWithScalar,
    EndsWithArray,
    ContainsScalar,
    ContainsArray,
    ContainsScalarDense,
    ContainsUtf8Scalar,
    AsciiEqScalar,
    AsciiEqArray,
    ConcatShort,
    ConcatLong,
    ConcatLongDense,
    MultiplyScalar,
    MultiplyArray,
    MultiplyArrayDense,
}

const CASES: &[Case] = &[
    Case::StartsWithShortScalar,
    Case::StartsWithScalar,
    Case::StartsWithArray,
    Case::StartsWithUtf8Scalar,
    Case::EndsWithScalar,
    Case::EndsWithArray,
    Case::ContainsScalar,
    Case::ContainsArray,
    Case::ContainsScalarDense,
    Case::ContainsUtf8Scalar,
    Case::AsciiEqScalar,
    Case::AsciiEqArray,
    Case::ConcatShort,
    Case::ConcatLong,
    Case::ConcatLongDense,
    Case::MultiplyScalar,
    Case::MultiplyArray,
    Case::MultiplyArrayDense,
];

impl Case {
    fn scalar(self) -> bool {
        matches!(self, Self::StartsWithShortScalar | Self::StartsWithScalar | Self::StartsWithUtf8Scalar | Self::EndsWithScalar
            | Self::ContainsScalar | Self::ContainsScalarDense | Self::ContainsUtf8Scalar
            | Self::AsciiEqScalar | Self::MultiplyScalar)
    }

    fn dense(self) -> bool {
        matches!(self, Self::ContainsScalarDense | Self::ConcatLongDense | Self::MultiplyArrayDense)
    }

    fn concat(self) -> bool {
        matches!(self, Self::ConcatShort | Self::ConcatLong | Self::ConcatLongDense)
    }

    fn multiply(self) -> bool {
        matches!(self, Self::MultiplyScalar | Self::MultiplyArray | Self::MultiplyArrayDense)
    }

    fn predicate(self) -> StringPredicate {
        match self {
            Self::StartsWithShortScalar | Self::StartsWithScalar | Self::StartsWithArray | Self::StartsWithUtf8Scalar => StringPredicate::StartsWith,
            Self::EndsWithScalar | Self::EndsWithArray => StringPredicate::EndsWith,
            Self::ContainsScalar | Self::ContainsArray | Self::ContainsScalarDense | Self::ContainsUtf8Scalar => StringPredicate::Contains,
            Self::AsciiEqScalar | Self::AsciiEqArray => StringPredicate::EqIgnoreAsciiCase,
            _ => unreachable!("numeric and concatenation cases select their own function"),
        }
    }
}

struct Fixture {
    case: Case,
    inputs: [ArrowOperand; 2],
    rows: usize,
    output: Field,
}

fn operand(column: ArrayRef, scalar: bool) -> ArrowOperand {
    ArrowOperand {
        dtype: Field::new("input", column.data_type().clone(), column.null_count() != 0),
        column,
        scalar,
    }
}

impl Fixture {
    fn new(case: Case, rows: usize) -> Result<Self, ArrowError> {
        let valid = |index: usize, right: bool| {
            case.dense() || if right { !index.is_multiple_of(11) } else { !index.is_multiple_of(8) }
        };
        let inputs = if case.multiply() {
            let left = Int64Array::from_iter((0..rows).map(|index| valid(index, false).then_some(index as i64 - 512)));
            let right = if case.scalar() {
                Int64Array::from(vec![3])
            } else {
                Int64Array::from_iter((0..rows).map(|index| valid(index, true).then_some((index % 7) as i64 - 3)))
            };
            [operand(Arc::new(left), false), operand(Arc::new(right), case.scalar())]
        } else {
            let values = [
                "arrow needle tail",
                "ARROW NEEDLE TAIL",
                "other payload without a match",
                "arrow λneedle tail",
                "",
                "λ café 🙂",
                "arrow a longer payload with the needle near the end and a tail",
                "somethingneedle",
            ];
            let pattern = match case {
                Case::StartsWithShortScalar => "arr",
                Case::StartsWithScalar | Case::StartsWithUtf8Scalar => "arrow",
                Case::EndsWithScalar => "tail",
                Case::AsciiEqScalar => "ARROW NEEDLE TAIL",
                _ => "needle",
            };
            let left: Vec<_> = (0..rows + 3).map(|index| {
                valid(index, false).then_some(if matches!(case, Case::ConcatShort) {
                    ["arrow", "λ", "", "data"][index % 4]
                } else {
                    values[index % values.len()]
                })
            }).collect();
            let right: Vec<_> = if case.scalar() {
                vec![Some(pattern)]
            } else {
                (0..rows + 3).map(|index| valid(index, true).then_some(if case.concat() {
                    if matches!(case, Case::ConcatShort) { "-rs" } else { "-suffix with owned bytes" }
                } else {
                    ["arrow", "TAIL", "needle", "absent", "", "λ", "tail", "NEEDLE"][index % 8]
                })).collect()
            };
            let array = |values: Vec<Option<&str>>, scalar: bool| -> ArrayRef {
                let array: ArrayRef = if matches!(case, Case::StartsWithUtf8Scalar | Case::ContainsUtf8Scalar) {
                    Arc::new(StringArray::from(values))
                } else {
                    Arc::new(StringViewArray::from(values))
                };
                if scalar { array } else { array.slice(3, rows) }
            };
            [operand(array(left, false), false), operand(array(right, case.scalar()), case.scalar())]
        };
        let fields = inputs.each_ref().map(|input| input.dtype.clone());
        let output = if case.multiply() {
            rowfn_arrow::plan(&Multiply, &NoOptions, &fields)?
        } else if case.concat() {
            rowfn_arrow::plan(&Concat, &NoOptions, &fields)?
        } else {
            rowfn_arrow::plan(&case.predicate(), &NoOptions, &fields)?
        };
        let fixture = Self { case, inputs, rows, output };
        assert_eq!(fixture.native()?.to_data(), fixture.invoke()?.to_data(), "{case:?}, {rows} rows");

        Ok(fixture)
    }

    fn invoke(&self) -> Result<ArrayRef, ArrowError> {
        let output = if self.case.multiply() {
            rowfn_arrow::invoke(&Multiply, &NoOptions, &self.inputs, self.rows, self.output.clone())?
        } else if self.case.concat() {
            rowfn_arrow::invoke(&Concat, &NoOptions, &self.inputs, self.rows, self.output.clone())?
        } else {
            rowfn_arrow::invoke(&self.case.predicate(), &NoOptions, &self.inputs, self.rows, self.output.clone())?
        };

        Ok(output.array)
    }

    fn diagnostic<const TYPED: bool, const PREPARED: bool>(&self) -> Result<ArrayRef, ArrowError> {
        if self.case.multiply() || self.case.concat() {
            return self.invoke();
        }
        let predicate = self.case.predicate();
        let output = if TYPED && self.inputs[0].column.data_type() == &arrow_schema::DataType::Utf8View {
            rowfn_arrow::invoke(&diagnosis::ArrowBytes::<diagnosis::ExactView, PREPARED>::new(predicate),
                &NoOptions, &self.inputs, self.rows, self.output.clone())?
        } else {
            rowfn_arrow::invoke(&diagnosis::ArrowBytes::<rowfn::Utf8, PREPARED>::new(predicate),
                &NoOptions, &self.inputs, self.rows, self.output.clone())?
        };
        Ok(output.array)
    }

    fn native(&self) -> Result<ArrayRef, ArrowError> {
        let left = &self.inputs[0].column;
        let right = &self.inputs[1].column;
        if self.case.concat() {
            return arrow_string::concat_elements::concat_elements_dyn(left.as_ref(), right.as_ref());
        }

        let scalar;
        let rhs: &dyn arrow_array::Datum = if self.case.scalar() {
            scalar = Scalar::new(right);
            &scalar
        } else {
            right
        };
        if self.case.multiply() {
            return arrow_arith::numeric::mul(left, rhs);
        }

        let values = match self.case.predicate() {
            StringPredicate::StartsWith => arrow_string::like::starts_with(left, rhs)?,
            StringPredicate::EndsWith => arrow_string::like::ends_with(left, rhs)?,
            StringPredicate::Contains => arrow_string::like::contains(left, rhs)?,
            StringPredicate::EqIgnoreAsciiCase => arrow_string::like::eq_ignore_ascii_case(left, rhs)?,
        };

        Ok(Arc::new(values))
    }
}

#[divan::bench(args = CASES, consts = SIZES)]
fn arrow_native<const ROWS: usize>(bencher: Bencher, case: &Case) {
    let fixture = Fixture::new(*case, ROWS).vortex_expect("matched fixture results");
    bencher.bench(|| black_box(&fixture).native().vortex_expect("validated Arrow kernel"));
}

#[divan::bench(args = CASES, consts = SIZES)]
fn arrow_rowfn<const ROWS: usize>(bencher: Bencher, case: &Case) {
    let fixture = Fixture::new(*case, ROWS).vortex_expect("matched fixture results");
    bencher.bench(|| black_box(&fixture).invoke().vortex_expect("validated rowfn invocation"));
}

#[divan::bench(args = CASES, consts = SIZES)]
fn rowfn_arrow_bytes<const ROWS: usize>(bencher: Bencher, case: &Case) {
    let fixture = Fixture::new(*case, ROWS).vortex_expect("matched fixture results");
    assert_eq!(fixture.native().unwrap().to_data(), fixture.diagnostic::<false, false>().unwrap().to_data());
    bencher.bench(|| black_box(&fixture).diagnostic::<false, false>().vortex_expect("validated diagnostic"));
}

#[divan::bench(args = CASES, consts = SIZES)]
fn rowfn_typed_strings<const ROWS: usize>(bencher: Bencher, case: &Case) {
    let fixture = Fixture::new(*case, ROWS).vortex_expect("matched fixture results");
    assert_eq!(fixture.native().unwrap().to_data(), fixture.diagnostic::<true, false>().unwrap().to_data());
    bencher.bench(|| black_box(&fixture).diagnostic::<true, false>().vortex_expect("validated diagnostic"));
}

#[divan::bench(args = CASES, consts = SIZES)]
fn rowfn_prepared_pattern<const ROWS: usize>(bencher: Bencher, case: &Case) {
    let fixture = Fixture::new(*case, ROWS).vortex_expect("matched fixture results");
    assert_eq!(fixture.native().unwrap().to_data(), fixture.diagnostic::<true, true>().unwrap().to_data());
    bencher.bench(|| black_box(&fixture).diagnostic::<true, true>().vortex_expect("validated diagnostic"));
}

#[divan::bench(args = [Case::StartsWithShortScalar, Case::StartsWithScalar, Case::StartsWithArray], consts = SIZES)]
fn rowfn_view_header<const ROWS: usize>(bencher: Bencher, case: &Case) {
    let fixture = Fixture::new(*case, ROWS).vortex_expect("matched fixture results");
    let invoke = || rowfn_arrow::invoke(&diagnosis::HeaderPrefix, &NoOptions, black_box(&fixture.inputs),
        ROWS, fixture.output.clone()).vortex_expect("validated header invocation").array;
    assert_eq!(fixture.native().unwrap().to_data(), invoke().to_data());
    bencher.bench(invoke);
}

#[divan::bench(args = [Case::ConcatShort, Case::ConcatLong, Case::ConcatLongDense], consts = SIZES)]
fn arrow_plus_two_validations<const ROWS: usize>(bencher: Bencher, case: &Case) {
    let fixture = Fixture::new(*case, ROWS).vortex_expect("matched fixture results");
    bencher.bench(|| {
        let array = black_box(&fixture).native().vortex_expect("native concat");
        let view = array.as_any().downcast_ref::<StringViewArray>().vortex_expect("concat emits views");
        let validated = StringViewArray::try_new(view.views().clone(), view.data_buffers(), view.nulls().cloned())
            .vortex_expect("native output is valid");
        arrow_array::make_array(validated.to_data().into_builder().build().vortex_expect("native output is valid"))
    });
}

fn main() {
    divan::main();
}
