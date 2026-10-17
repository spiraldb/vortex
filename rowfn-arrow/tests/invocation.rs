// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Complete invocation also has a fixture that compiles without any Vortex dependency.

use std::sync::Arc;

use arrow_array::Int64Array;
use arrow_schema::ArrowError;
use arrow_schema::DataType;
use arrow_schema::Field;
use rowfn::Host;
use rowfn::HostResult;
use rowfn::InputBinding;
use rowfn::OutputBinding;
use rowfn::RowFn;
use rowfn::RowVisitor;
use rowfn::sink::ElementSink;
use rowfn_arrow::ArrowOperand;

#[derive(Clone)]
struct Increment;

impl<H> RowFn<H> for Increment
where
    H: Host + InputBinding<i64> + OutputBinding<i64>,
{
    type Options = ();
    const ARG_NAMES: &'static [&'static str] = &["value"];
    const INFALLIBLE: bool = true;

    fn dispatch<V: RowVisitor<H>>(
        &self,
        _: &(),
        _: &[H::NativeType],
        visitor: V,
    ) -> HostResult<H, V::VisitResult> {
        visitor.visit::<(i64,), i64>(|(value,)| value.wrapping_add(1))
    }
}

#[test]
fn independent_arrow_invocation_preserves_nullable_values() -> Result<(), ArrowError> {
    let input = ArrowOperand {
        column: Arc::new(Int64Array::from(vec![Some(i64::MAX), None, Some(-2)])),
        dtype: Field::new("value", DataType::Int64, true),
        scalar: false,
    };
    let field = rowfn_arrow::plan(&Increment, &(), &[input.dtype.clone()])?;
    let result = rowfn_arrow::invoke(&Increment, &(), &[input], 3, field.clone())?;
    let values = result.array.as_any().downcast_ref::<Int64Array>().unwrap();

    assert_eq!(values.iter().collect::<Vec<_>>(), vec![Some(i64::MIN), None, Some(-1)]);
    assert_eq!(result.field, field);
    Ok(())
}

#[derive(Clone)]
struct CheckedQuotient;

impl<H> RowFn<H> for CheckedQuotient
where
    H: Host + InputBinding<i64> + OutputBinding<i64>,
{
    type Options = ();
    const ARG_NAMES: &'static [&'static str] = &["lhs", "rhs"];
    const INFALLIBLE: bool = false;

    fn dispatch<V: RowVisitor<H>>(
        &self,
        _: &(),
        _: &[H::NativeType],
        visitor: V,
    ) -> HostResult<H, V::VisitResult> {
        visitor.visit_into::<(i64, i64), ElementSink<H, i64>, _>((), |(lhs, rhs), row| {
            *row = lhs.checked_div(rhs).ok_or_else(|| H::error("invalid integer division"))?;
            Ok::<(), H::Error>(())
        })
    }
}

fn quotient(lhs: Vec<Option<i64>>, rhs: Vec<Option<i64>>) -> Result<rowfn_arrow::Output, ArrowError> {
    let rows = lhs.len();
    let field = Field::new("input", DataType::Int64, true);
    let inputs = [
        ArrowOperand {
            column: Arc::new(Int64Array::from(lhs)),
            dtype: field.clone(),
            scalar: false,
        },
        ArrowOperand {
            column: Arc::new(Int64Array::from(rhs)),
            dtype: field,
            scalar: false,
        },
    ];
    let fields = inputs.iter().map(|input| input.dtype.clone()).collect::<Vec<_>>();
    let output = rowfn_arrow::plan(&CheckedQuotient, &(), &fields)?;
    rowfn_arrow::invoke(&CheckedQuotient, &(), &inputs, rows, output)
}

#[test]
fn safe_sink_callback_skips_null_payload_errors() -> Result<(), ArrowError> {
    let result = quotient(vec![None, Some(8), Some(6)], vec![Some(0), Some(2), Some(3)])?;
    let values = result.array.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(values.iter().collect::<Vec<_>>(), vec![None, Some(4), Some(2)]);
    Ok(())
}

#[test]
fn safe_sink_callback_abandons_output_after_a_later_error() {
    let error = quotient(vec![Some(8), Some(6)], vec![Some(2), Some(0)]).err().unwrap();
    assert!(error.to_string().contains("invalid integer division"));
}
