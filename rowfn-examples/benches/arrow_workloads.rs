// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Matched Arrow baselines cover each storage family and check logical results before timing.
//!
//! Native kernels return arrays without the field planning and validation of a full invocation.
//! Both dictionary paths materialize referenced rows. String baselines copy into Utf8View output.

use std::sync::Arc;

use arrow_array::Array;
use arrow_array::ArrayRef;
use arrow_array::BooleanArray;
use arrow_array::DictionaryArray;
use arrow_array::FixedSizeListArray;
use arrow_array::Float64Array;
use arrow_array::Int32Array;
use arrow_array::Int64Array;
use arrow_array::LargeStringArray;
use arrow_array::Scalar;
use arrow_array::StringArray;
use arrow_array::StringViewArray;
use arrow_array::TimestampNanosecondArray;
use arrow_array::builder::StringViewBuilder;
use arrow_array::types::Int32Type;
use arrow_array::types::TimestampNanosecondType;
use arrow_buffer::NullBuffer;
use arrow_schema::ArrowError;
use arrow_schema::DataType;
use arrow_schema::Field;
use divan::Bencher;
use divan::black_box;
use rowfn_arrow::ArrowOperand;
use rowfn_examples::Add;
use rowfn_examples::AdjustTicks;
use rowfn_examples::Divide;
use rowfn_examples::NoOptions;
use rowfn_examples::Not;
use rowfn_examples::Scale;
use rowfn_examples::Seven;
use rowfn_examples::Trim;
use vortex_error::VortexExpect;

const SIZES: &[usize] = &[0, 64, 1024, 16_384];

#[derive(Clone, Copy, Debug)]
enum Workload {
    CheckedAdd,
    CheckedAddNullable,
    DivideNullable,
    BooleanSlice,
    TrimUtf8,
    TrimLargeUtf8,
    TrimViewShort,
    TrimViewLong,
    ScaleList4,
    ScaleList0,
    Timestamp,
    DictionaryInteger,
    DictionaryString,
    AllNull,
    ScalarOnly,
    Nullary,
}

const WORKLOADS: &[Workload] = &[
    Workload::CheckedAdd,
    Workload::CheckedAddNullable,
    Workload::DivideNullable,
    Workload::BooleanSlice,
    Workload::TrimUtf8,
    Workload::TrimLargeUtf8,
    Workload::TrimViewShort,
    Workload::TrimViewLong,
    Workload::ScaleList4,
    Workload::ScaleList0,
    Workload::Timestamp,
    Workload::DictionaryInteger,
    Workload::DictionaryString,
    Workload::AllNull,
    Workload::ScalarOnly,
    Workload::Nullary,
];

struct Fixture {
    workload: Workload,
    rows: usize,
    inputs: Vec<ArrowOperand>,
    output: Field,
}

fn operand(column: ArrayRef, scalar: bool) -> ArrowOperand {
    ArrowOperand {
        dtype: Field::new("input", column.data_type().clone(), column.logical_null_count() != 0),
        column,
        scalar,
    }
}

impl Fixture {
    fn new(workload: Workload, rows: usize) -> Result<Self, ArrowError> {
        let nullable = |index: usize| !index.is_multiple_of(8);
        let nulls = || Some(NullBuffer::from((0..rows).map(nullable).collect::<Vec<_>>()));
        let integers = || (0..rows).map(|index| (index % 1024) as i64).collect::<Vec<_>>();
        let scalar = || operand(Arc::new(Int64Array::from(vec![2])), true);
        let string_values = |long: bool| {
            (0..rows).map(|index| {
                nullable(index).then_some(if long {
                    "  a longer UTF-8 string requiring output-owned external bytes  "
                } else {
                    "  short  "
                })
            }).collect::<Vec<_>>()
        };
        let inputs = match workload {
            Workload::CheckedAdd => vec![operand(Arc::new(Int64Array::from(integers())), false), scalar()],
            Workload::CheckedAddNullable | Workload::DivideNullable => vec![
                operand(Arc::new(Int64Array::new(integers().into(), nulls())), false),
                scalar(),
            ],
            Workload::AllNull => vec![operand(Arc::new(Int64Array::new_null(rows)), false), scalar()],
            Workload::ScalarOnly => vec![scalar(), scalar()],
            Workload::Nullary => vec![],
            Workload::BooleanSlice => {
                let array = BooleanArray::from_iter((0..rows + 3).map(|index| nullable(index).then_some(index % 3 == 0)));
                vec![operand(Arc::new(array.slice(3, rows)), false)]
            }
            Workload::TrimUtf8 => vec![operand(Arc::new(StringArray::from(string_values(true))), false)],
            Workload::TrimLargeUtf8 => vec![operand(Arc::new(LargeStringArray::from(string_values(true))), false)],
            Workload::TrimViewShort | Workload::TrimViewLong => {
                vec![operand(Arc::new(StringViewArray::from(string_values(matches!(workload, Workload::TrimViewLong)))), false)]
            }
            Workload::ScaleList4 | Workload::ScaleList0 => {
                let width = if matches!(workload, Workload::ScaleList0) { 0 } else { 4 };
                let values = Float64Array::from((0..rows * width).map(|index| index as f64).collect::<Vec<_>>());
                let array = FixedSizeListArray::try_new_with_length(
                    Arc::new(Field::new("item", DataType::Float64, false)),
                    width as i32,
                    Arc::new(values),
                    nulls(),
                    rows,
                )?;
                vec![operand(Arc::new(array), false), operand(Arc::new(Float64Array::from(vec![2.0])), true)]
            }
            Workload::Timestamp => vec![
                operand(Arc::new(TimestampNanosecondArray::new(integers().into(), nulls()).with_timezone("UTC")), false),
                scalar(),
            ],
            Workload::DictionaryInteger | Workload::DictionaryString => {
                // One referenced null value and one unused value exercise logical dictionary nulls.
                let keys = Int32Array::from_iter((0..rows).map(|index| nullable(index).then_some((index % 65) as i32)));
                let values: ArrayRef = if matches!(workload, Workload::DictionaryInteger) {
                    Arc::new(Int64Array::from_iter((0..66).map(|index| (index != 64).then_some(index as i64))))
                } else {
                    Arc::new(StringArray::from_iter((0..66).map(|index| (index != 64).then_some("  dictionary string with external bytes  "))))
                };
                let array = DictionaryArray::<Int32Type>::try_new(keys, values)?;
                let mut inputs = vec![operand(Arc::new(array), false)];
                if matches!(workload, Workload::DictionaryInteger) { inputs.push(scalar()); }
                inputs
            }
        };
        let fields: Vec<_> = inputs.iter().map(|input| input.dtype.clone()).collect();
        let output = match workload {
            Workload::CheckedAdd | Workload::CheckedAddNullable | Workload::DictionaryInteger
                | Workload::AllNull | Workload::ScalarOnly => rowfn_arrow::plan(&Add::<true>, &NoOptions, &fields)?,
            Workload::DivideNullable => rowfn_arrow::plan(&Divide, &NoOptions, &fields)?,
            Workload::BooleanSlice => rowfn_arrow::plan(&Not, &NoOptions, &fields)?,
            Workload::ScaleList4 | Workload::ScaleList0 => rowfn_arrow::plan(&Scale, &NoOptions, &fields)?,
            Workload::Timestamp => rowfn_arrow::plan(&AdjustTicks, &NoOptions, &fields)?,
            Workload::Nullary => rowfn_arrow::plan(&Seven, &NoOptions, &fields)?,
            _ => rowfn_arrow::plan(&Trim, &NoOptions, &fields)?,
        };
        let fixture = Self { workload, rows, inputs, output };
        assert_eq!(fixture.invoke()?.to_data(), fixture.native()?.to_data(), "{workload:?}, rows={rows}");
        Ok(fixture)
    }

    fn invoke(&self) -> Result<ArrayRef, ArrowError> {
        macro_rules! invoke {
            ($function:expr) => {
                rowfn_arrow::invoke(&$function, &NoOptions, &self.inputs, self.rows, self.output.clone())?.array
            };
        }
        Ok(match self.workload {
            Workload::CheckedAdd | Workload::CheckedAddNullable | Workload::DictionaryInteger
                | Workload::AllNull | Workload::ScalarOnly => invoke!(Add::<true>),
            Workload::DivideNullable => invoke!(Divide),
            Workload::BooleanSlice => invoke!(Not),
            Workload::ScaleList4 | Workload::ScaleList0 => invoke!(Scale),
            Workload::Timestamp => invoke!(AdjustTicks),
            Workload::Nullary => invoke!(Seven),
            _ => invoke!(Trim),
        })
    }

    fn native(&self) -> Result<ArrayRef, ArrowError> {
        if matches!(self.workload, Workload::Nullary | Workload::ScalarOnly) {
            let value = if matches!(self.workload, Workload::Nullary) {
                7
            } else {
                let lhs = self.inputs[0].column.as_any().downcast_ref::<Int64Array>().vortex_expect("integer scalar");
                let rhs = self.inputs[1].column.as_any().downcast_ref::<Int64Array>().vortex_expect("integer scalar");
                lhs.value(0).checked_add(rhs.value(0)).vortex_expect("fixture does not overflow")
            };
            return Ok(Arc::new(Int64Array::from(vec![value; self.rows])));
        }
        let input = rowfn_arrow::materialize(&self.inputs[0].column)?;
        match self.workload {
            Workload::CheckedAdd | Workload::CheckedAddNullable | Workload::DictionaryInteger | Workload::AllNull => {
                arrow_arith::numeric::add(&input, &Scalar::new(&self.inputs[1].column))
            }
            Workload::DivideNullable => arrow_arith::numeric::div(&input, &Scalar::new(&self.inputs[1].column)),
            Workload::BooleanSlice => {
                let input = input.as_any().downcast_ref::<BooleanArray>().vortex_expect("Boolean fixture");
                Ok(Arc::new(arrow_arith::boolean::not(input)?))
            }
            Workload::ScaleList4 | Workload::ScaleList0 => {
                let input = input.as_any().downcast_ref::<FixedSizeListArray>().vortex_expect("list fixture");
                let values = input.values().as_any().downcast_ref::<Float64Array>().vortex_expect("float children");
                let factor = self.inputs[1].column.as_any().downcast_ref::<Float64Array>().vortex_expect("float scalar").value(0);
                let scaled: Float64Array = values.unary(|value| value * factor);
                Ok(Arc::new(FixedSizeListArray::try_new_with_length(
                    Arc::new(Field::new("item", DataType::Float64, false)), input.value_length(),
                    Arc::new(scaled), input.nulls().cloned(), self.rows,
                )?))
            }
            Workload::Timestamp => {
                let input = input.as_any().downcast_ref::<TimestampNanosecondArray>().vortex_expect("timestamp fixture");
                let ticks = self.inputs[1].column.as_any().downcast_ref::<Int64Array>().vortex_expect("integer scalar").value(0);
                let output = input.try_unary::<_, TimestampNanosecondType, ArrowError>(|value| {
                    value.checked_add(ticks).ok_or_else(|| ArrowError::ComputeError("tick overflow".into()))
                })?.with_timezone("UTC");
                Ok(Arc::new(output))
            }
            _ => {
                let mut builder = StringViewBuilder::with_capacity(self.rows);
                macro_rules! trim {
                    ($array:ty) => {
                        for value in input.as_any().downcast_ref::<$array>().vortex_expect("string fixture").iter() {
                            builder.append_option(value.map(str::trim));
                        }
                    };
                }
                match input.data_type() {
                    DataType::Utf8 => trim!(StringArray),
                    DataType::LargeUtf8 => trim!(LargeStringArray),
                    DataType::Utf8View => trim!(StringViewArray),
                    _ => unreachable!("fixture is a supported string layout"),
                }
                Ok(Arc::new(builder.finish()))
            }
        }
    }
}

#[divan::bench(args = WORKLOADS, consts = SIZES)]
fn arrow_rowfn<const ROWS: usize>(bencher: Bencher, workload: &Workload) {
    let fixture = Fixture::new(*workload, ROWS).vortex_expect("matched fixture results");
    bencher.bench(|| black_box(&fixture).invoke().vortex_expect("validated invocation"));
}

#[divan::bench(args = WORKLOADS, consts = SIZES)]
fn arrow_native<const ROWS: usize>(bencher: Bencher, workload: &Workload) {
    let fixture = Fixture::new(*workload, ROWS).vortex_expect("matched fixture results");
    bencher.bench(|| black_box(&fixture).native().vortex_expect("validated native baseline"));
}

fn main() {
    divan::main();
}

#[derive(Clone)]
struct ScaleFour;

impl rowfn::RowFn<rowfn_arrow::ArrowHost> for ScaleFour {
    type Options = NoOptions;
    const ARG_NAMES: &'static [&'static str] = &["list", "factor"];
    const INFALLIBLE: bool = true;

    fn dispatch<V: rowfn::RowVisitor<rowfn_arrow::ArrowHost>>(
        &self, _: &NoOptions, args: &[Field], visitor: V,
    ) -> Result<V::VisitResult, ArrowError> {
        if !matches!(args[0].data_type(), DataType::FixedSizeList(_, 4)) {
            return Err(ArrowError::ComputeError("diagnostic requires width four".into()));
        }
        visitor.visit_prepared_into::<
            (rowfn::FixedSizeList<f64>, f64),
            rowfn::sink::FixedSizeListSink<rowfn_arrow::ArrowHost, f64>,
            Option<f64>,
            rowfn::sink::InitializedRow,
        >(4, |(_, factor)| factor, |constant, (values, factor), row| {
            let values: &[f64; 4] = values.try_into().ok().vortex_expect("dispatch validated width four");
            let row: &mut [std::mem::MaybeUninit<f64>; 4] = row.try_into().ok()
                .vortex_expect("sink allocated the planned width four");
            let factor = constant.unwrap_or(factor);
            // SAFETY: the array conversion retains the entire exact callback row. All four slots
            // remain initialized until this callback returns the token.
            unsafe { rowfn::sink::InitializedRow::fill(row, |index| values[index] * factor) }
        })
    }
}

#[divan::bench(consts = SIZES)]
fn rowfn_fixed_width<const ROWS: usize>(bencher: Bencher) {
    let fixture = Fixture::new(Workload::ScaleList4, ROWS).vortex_expect("matched list fixture");
    let invoke = || rowfn_arrow::invoke(&ScaleFour, &NoOptions, black_box(&fixture.inputs), ROWS,
        fixture.output.clone()).vortex_expect("validated fixed-width invocation").array;
    assert_eq!(fixture.native().unwrap().to_data(), invoke().to_data());
    bencher.bench(invoke);
}
