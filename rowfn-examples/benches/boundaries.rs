// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Compare collection, complete invocation, and the retained Vortex executor in one binary.
//!
//! The workspace bench profile uses 16 CGUs without LTO. Keep compiler, target features, allocator,
//! and profile identical for every result, especially the deferred Boolean retry comparisons.

use std::sync::Arc;

use arrow_array::Array;
use arrow_array::ArrayRef;
use arrow_array::BooleanArray;
use arrow_array::Int64Array;
use arrow_array::LargeStringArray;
use arrow_array::StringArray;
use arrow_array::StringViewArray;
use arrow_buffer::NullBuffer;
use arrow_schema::Field;
use divan::Bencher;
use divan::black_box;
use rowfn::InputBinding;
use rowfn::InputTuple;
use rowfn::OutputBinding;
use rowfn::OutputBuffer;
use rowfn::RowKind;
use rowfn::TextBinding;
use rowfn::TextLayout;
use rowfn::TextValue;
use rowfn::kernels::lane_kernels::LaneZip;
use rowfn_arrow::{ArrowHost, ArrowOperand};
use rowfn_examples::{Add, NoOptions, PositiveSum};
use vortex_array::{ArrayRef as VortexArrayRef, IntoArray, VortexSessionExecute, array_session};
use vortex_array::arrays::{ConstantArray, PrimitiveArray};
use vortex_array::dtype::DType;
use vortex_array::scalar_fn::{EmptyOptions, ScalarFnId, VecExecutionArgs};
use vortex_array::scalar_fn::unstable::rowfn::{self as adapter, VortexHost};
use vortex_array::scalar_fn::unstable::row::{self as legacy, RowFn as LegacyRowFn};
use vortex_array::validity::Validity;
use vortex_error::{VortexExpect, VortexResult, vortex_ensure};
use vortex_session::registry::CachedId;

const SIZES: &[usize] = &[0, 1, 64, 1024, 16_384];

fn main() {
    divan::main();
}

fn values(rows: usize) -> Vec<i64> {
    (0..rows).map(|row| i64::try_from(row % 1024).vortex_expect("fixture is below 1024")).collect()
}

fn operand(column: ArrayRef, scalar: bool) -> ArrowOperand {
    ArrowOperand {
        dtype: Field::new("input", column.data_type().clone(), column.null_count() != 0),
        column,
        scalar,
    }
}

#[divan::bench(consts = SIZES)]
fn direct_typed_collection<const ROWS: usize>(bencher: Bencher) {
    let lhs = values(ROWS);
    let rhs = values(ROWS);
    bencher.bench(|| {
        let mut output = <ArrowHost as OutputBinding<i64>>::allocate(ROWS, &mut ()).vortex_expect("benchmark fixtures must satisfy the selected signature");
        for ((slot, lhs), rhs) in output.slots().iter_mut().zip(black_box(&lhs)).zip(black_box(&rhs)) {
            slot.write(lhs.wrapping_add(*rhs));
        }
        // SAFETY: the fixed output allocation and both inputs contain exactly ROWS elements.
        unsafe { output.finish(ROWS) }
    });
}

#[divan::bench(consts = SIZES)]
fn shared_collection<const ROWS: usize>(bencher: Bencher) {
    let lhs = values(ROWS);
    let rhs = values(ROWS);
    bencher.bench(|| {
        let source = LaneZip::new(black_box(lhs.as_slice()), black_box(rhs.as_slice()));
        <ArrowHost as OutputBinding<i64>>::build_from(source, |(lhs, rhs)| lhs.wrapping_add(rhs), &mut ()).vortex_expect("benchmark fixtures must satisfy the selected signature")
    });
}

// These all-valid controls use the same output binding and allocation in both collections.
// Decoding is outside timing; shared collection includes borrowed-view length validation.
// arrow_families supplies the corresponding native-kernel and complete-invocation measurements.
#[divan::bench(consts = SIZES, args = [TextLayout::Offset32, TextLayout::Offset64, TextLayout::View])]
fn direct_text_length_collection<const ROWS: usize>(bencher: Bencher, layout: TextLayout) {
    let values = vec!["a long string outside an inline view"; ROWS];
    macro_rules! collect {
        ($native:ty, $array:expr, $lengths:expr) => {
            let baseline = arrow_string::length::length($array)
                .vortex_expect("the Arrow baseline must support the fixture layout");
            let collect = || {
                let mut output = <ArrowHost as OutputBinding<$native>>::allocate(ROWS, &mut ())
                    .vortex_expect("the benchmark output binding must allocate the fixed row count");
                for (slot, length) in output.slots()[..ROWS].iter_mut().zip($lengths) {
                    slot.write(length);
                }

                // SAFETY: each fixture array has ROWS elements. Its validated offset windows or
                // view headers yield exactly ROWS values, so the entire prefix is initialized.
                unsafe { output.finish(ROWS) }
            };
            assert_eq!(collect().to_data(), baseline.to_data());
            bencher.bench_local(collect);
        };
    }

    match layout {
        TextLayout::Offset32 => {
            let array = StringArray::from(values);
            collect!(
                i32,
                &array,
                black_box(array.value_offsets()).windows(2).map(|pair| pair[1] - pair[0])
            );
        }
        TextLayout::Offset64 => {
            let array = LargeStringArray::from(values);
            collect!(
                i64,
                &array,
                black_box(array.value_offsets()).windows(2).map(|pair| pair[1] - pair[0])
            );
        }
        TextLayout::View => {
            let array = StringViewArray::from(values);
            collect!(
                i32,
                &array,
                black_box(array.views()).iter().map(|view| *view as i32)
            );
        }
    }
}

#[divan::bench(consts = SIZES, args = [TextLayout::Offset32, TextLayout::Offset64, TextLayout::View])]
fn shared_text_length_collection<const ROWS: usize>(bencher: Bencher, layout: TextLayout) {
    let values = vec!["a long string outside an inline view"; ROWS];
    match layout {
        TextLayout::Offset32 => text_length_collection::<<ArrowHost as TextBinding>::Offset32, i32>(
            bencher,
            Arc::new(StringArray::from(values)),
            |value| value.byte_len() as i32,
        ),
        TextLayout::Offset64 => text_length_collection::<<ArrowHost as TextBinding>::Offset64, i64>(
            bencher,
            Arc::new(LargeStringArray::from(values)),
            |value| value.byte_len() as i64,
        ),
        TextLayout::View => text_length_collection::<<ArrowHost as TextBinding>::Text, i32>(
            bencher,
            Arc::new(StringViewArray::from(values)),
            |value| value.byte_len() as i32,
        ),
    }
}

fn text_length_collection<K, Out>(
    bencher: Bencher,
    array: ArrayRef,
    apply: impl Fn(K::Value<'_>) -> Out,
)
where
    K: RowKind,
    Out: Default + 'static,
    ArrowHost: InputBinding<K> + OutputBinding<Out>,
{
    let rows = array.len();
    let baseline = arrow_string::length::length(array.as_ref())
        .vortex_expect("the Arrow baseline must support the fixture layout");
    let columns = <(K,) as InputTuple<ArrowHost>>::decode(&[operand(array, false)], false, &mut ())
        .vortex_expect("the concrete text binding must decode its matching fixture layout");
    let collect = || {
        let source = <(K,) as InputTuple<ArrowHost>>::rows_source(black_box(&columns), rows)
            .vortex_expect("decoded fixture views must retain their original row count")
            .vortex_expect("the text collection fixture contains an array operand");
        <ArrowHost as OutputBinding<Out>>::build_from(source, |(value,)| apply(value), &mut ())
            .vortex_expect("the text collector must accept this infallible fixture")
    };
    assert_eq!(collect().to_data(), baseline.to_data());
    bencher.bench_local(collect);
}

#[divan::bench(consts = SIZES)]
fn shared_executor<const ROWS: usize>(bencher: Bencher) {
    let inputs = [
        operand(Arc::new(Int64Array::from(values(ROWS))), false),
        operand(Arc::new(Int64Array::from(values(ROWS))), false),
    ];
    let output = rowfn_arrow::plan(&Add::<false>, &NoOptions, &inputs.each_ref().map(|input| input.dtype.clone())).vortex_expect("benchmark fixtures must satisfy the selected signature");
    bencher.bench(|| {
        rowfn::execute::<ArrowHost, _>(&Add::<false>, &NoOptions, black_box(&inputs), ROWS, &output, &mut ()).vortex_expect("benchmark fixtures must satisfy the selected signature")
    });
}

#[divan::bench(consts = SIZES, args = [false, true])]
fn arrow_invocation<const ROWS: usize>(bencher: Bencher, scalar: bool) {
    let inputs = [
        operand(Arc::new(Int64Array::from(values(ROWS))), false),
        operand(Arc::new(Int64Array::from(if scalar { vec![1] } else { values(ROWS) })), scalar),
    ];
    let output = rowfn_arrow::plan(&Add::<false>, &NoOptions, &inputs.each_ref().map(|input| input.dtype.clone())).vortex_expect("benchmark fixtures must satisfy the selected signature");
    bencher.bench(|| rowfn_arrow::invoke(&Add::<false>, &NoOptions, black_box(&inputs), ROWS, output.clone()).vortex_expect("benchmark fixtures must satisfy the selected signature"));
}

#[divan::bench(consts = SIZES, args = [false, true])]
fn arrow_native_wrapping_add<const ROWS: usize>(bencher: Bencher, scalar: bool) {
    let lhs = Int64Array::from(values(ROWS));
    let rhs = Int64Array::from(values(ROWS));
    let constant = Int64Array::new_scalar(1);
    bencher.bench(|| {
        if scalar {
            arrow_arith::numeric::add_wrapping(black_box(&lhs), black_box(&constant)).vortex_expect("benchmark fixtures must satisfy the selected signature")
        } else {
            arrow_arith::numeric::add_wrapping(black_box(&lhs), black_box(&rhs)).vortex_expect("benchmark fixtures must satisfy the selected signature")
        }
    });
}

#[derive(Clone)]
struct LegacyAdd;
impl LegacyRowFn for LegacyAdd {
    type Options = EmptyOptions;
    const ARG_NAMES: &'static [&'static str] = &["lhs", "rhs"];
    const INFALLIBLE: bool = true;
    fn id(&self) -> ScalarFnId {
        static ID: CachedId = CachedId::new("bench.rowfn.legacy_add");
        *ID
    }
    fn dispatch<V: legacy::RowVisitor>(&self, _: &EmptyOptions, _: &[DType], visitor: V)
        -> VortexResult<V::VisitResult> {
        visitor.visit::<(i64, i64), i64>(|(lhs, rhs)| lhs.wrapping_add(rhs))
    }
}

#[derive(Clone)]
struct LegacyPredicate<const MULTIVERSIONED: bool>;
impl<const MULTIVERSIONED: bool> LegacyRowFn for LegacyPredicate<MULTIVERSIONED> {
    type Options = EmptyOptions;
    const ARG_NAMES: &'static [&'static str] = &["lhs", "rhs"];
    const INFALLIBLE: bool = false;
    fn id(&self) -> ScalarFnId {
        static ID: CachedId = CachedId::new("bench.rowfn.legacy_predicate");
        *ID
    }
    fn dispatch<V: legacy::RowVisitor>(&self, _: &EmptyOptions, _: &[DType], visitor: V)
        -> VortexResult<V::VisitResult> {
        visitor.visit_deferred_bool::<(i64, i64), bool, MULTIVERSIONED>(|(lhs, rhs)| {
            let (sum, failed) = lhs.overflowing_add(rhs);
            (sum > 0, failed)
        }, |failed| {
            vortex_ensure!(!failed, "integer overflow in positive sum");
            Ok(())
        })
    }
}

#[divan::bench(consts = SIZES, args = [false, true])]
fn vortex_add<const ROWS: usize>(bencher: Bencher, extracted: bool) {
    let inputs = [PrimitiveArray::from_iter(values(ROWS)).into_array(), ConstantArray::new(1i64, ROWS).into_array()];
    let output = rowfn::plan::<VortexHost, _>(&Add::<false>, &NoOptions, &inputs.each_ref().map(|array| array.dtype().clone())).vortex_expect("benchmark fixtures must satisfy the selected signature");
    let args = VecExecutionArgs::new(inputs.to_vec(), ROWS);
    let session = array_session();
    bencher.with_inputs(|| session.create_execution_ctx()).bench_refs(|ctx| {
        if extracted {
            adapter::execute(&Add::<false>, &NoOptions, black_box(&inputs), ROWS, output.output_type(), ctx).vortex_expect("benchmark fixtures must satisfy the selected signature")
        } else {
            legacy::execute_rows(&LegacyAdd, &EmptyOptions, black_box(&args), ctx).vortex_expect("benchmark fixtures must satisfy the selected signature")
        }
    });
}

#[derive(Clone, Copy, Debug)]
enum BoolHost { LegacyVortex, ExtractedVortex, Arrow, ArrowBaseline }
const BOOL_HOSTS: &[BoolHost] = &[BoolHost::LegacyVortex, BoolHost::ExtractedVortex, BoolHost::Arrow, BoolHost::ArrowBaseline];

#[divan::bench(args = BOOL_HOSTS, consts = [false, true])]
fn nullable_bool<const NULL_FAILURE: bool>(bencher: Bencher, host: &BoolHost) {
    const ROWS: usize = 16_384;
    let valid: Vec<_> = (0..ROWS).map(|row| !row.is_multiple_of(8)).collect();
    let values: Vec<_> = values(ROWS).into_iter().zip(&valid)
        .map(|(value, valid)| if NULL_FAILURE && !valid { i64::MAX } else { value }).collect();
    let lhs = Int64Array::new(values.clone().into(), Some(NullBuffer::from(valid.clone())));
    let inputs = [operand(Arc::new(lhs.clone()), false), operand(Arc::new(Int64Array::from(vec![1])), true)];
    let arrow_output = rowfn_arrow::plan(&PositiveSum::<true>, &NoOptions, &inputs.each_ref().map(|input| input.dtype.clone())).vortex_expect("benchmark fixtures must satisfy the selected signature");
    let vortex: [VortexArrayRef; 2] = [
        PrimitiveArray::new(values, Validity::from_iter(valid)).into_array(),
        ConstantArray::new(1i64, ROWS).into_array(),
    ];
    let output = rowfn::plan::<VortexHost, _>(&PositiveSum::<true>, &NoOptions, &vortex.each_ref().map(|array| array.dtype().clone())).vortex_expect("benchmark fixtures must satisfy the selected signature");
    let args = VecExecutionArgs::new(vortex.to_vec(), ROWS);
    let session = array_session();
    bencher.with_inputs(|| session.create_execution_ctx()).bench_refs(|ctx| {
        match host {
            BoolHost::LegacyVortex => { black_box(legacy::execute_rows(&LegacyPredicate::<true>, &EmptyOptions, &args, ctx).vortex_expect("benchmark fixtures must satisfy the selected signature")); }
            BoolHost::ExtractedVortex => { black_box(adapter::execute(&PositiveSum::<true>, &NoOptions, &vortex, ROWS, output.output_type(), ctx).vortex_expect("benchmark fixtures must satisfy the selected signature")); }
            BoolHost::Arrow => { black_box(rowfn_arrow::invoke(&PositiveSum::<true>, &NoOptions, &inputs, ROWS, arrow_output.clone()).vortex_expect("benchmark fixtures must satisfy the selected signature")); }
            BoolHost::ArrowBaseline => {
                // This baseline observes errors only on valid rows, matching strict row semantics.
                let output = BooleanArray::from_iter(lhs.iter().map(|value| value.map(|value| {
                    value.checked_add(1).vortex_expect("only null fixture payloads can overflow") > 0
                })));
                black_box(output);
            }
        }
    });
}
