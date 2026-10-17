// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

mod common;

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int64Array};
use arrow_buffer::ScalarBuffer;
use arrow_schema::{ArrowError, DataType, Field};
use rowfn::{Host, HostResult, InputBinding, RowFn, RowKind, RowView, RowVisitor};
use rowfn_arrow::{ArrowHost, ArrowOperand};
use rowfn_examples::NoOptions;

use common::TestResult;

struct Nonnegative;
impl RowKind for Nonnegative { type Value<'a> = i64; }
struct NonnegativeView<'a>(&'a [i64]);
// SAFETY: decoded storage is an initialized primitive buffer retained for the view lifetime.
unsafe impl<'a> RowView<'a, Nonnegative> for NonnegativeView<'a> {
    fn len(&self) -> usize { self.0.len() }
    unsafe fn get_unchecked(&self, index: usize) -> i64 {
        // SAFETY: the caller bounds the index by the retained slice length.
        unsafe { *self.0.get_unchecked(index) }
    }
}
impl InputBinding<Nonnegative> for ArrowHost {
    type Decoded = ScalarBuffer<i64>;
    type View<'a> = NonnegativeView<'a>;
    const DENSE_SAFE: bool = false;
    const DECODE_INFALLIBLE: bool = false;
    fn validate(dtype: &Field) -> Result<(), ArrowError> { <Self as InputBinding<i64>>::validate(dtype) }
    fn decode(column: &ArrayRef, scalar: bool, ctx: &mut ()) -> Result<Self::Decoded, ArrowError> {
        let values = <Self as InputBinding<i64>>::decode(column, scalar, ctx)?;
        if values.iter().any(|value| *value < 0) { return Err(Self::error("negative decoded domain")); }
        Ok(values)
    }
    fn can_decode_null_tolerant(_: &ArrayRef) -> Result<bool, ArrowError> { Ok(false) }
    fn decode_null_tolerant(_: &ArrayRef, _: bool, _: &mut ()) -> Result<Self::Decoded, ArrowError> {
        Err(Self::error("this domain requires ordinary or filtered decoding"))
    }
    fn view(decoded: &Self::Decoded) -> NonnegativeView<'_> { NonnegativeView(decoded.as_ref()) }
}
#[derive(Clone)]
struct Identity;
impl RowFn<ArrowHost> for Identity {
    type Options = NoOptions;
    const ARG_NAMES: &'static [&'static str] = &["value"];
    const INFALLIBLE: bool = true;
    fn dispatch<V: RowVisitor<ArrowHost>>(&self, _: &NoOptions, _: &[Field], visitor: V)
        -> HostResult<ArrowHost, V::VisitResult> {
        visitor.visit::<(Nonnegative,), i64>(|(value,)| value)
    }
}

#[test]
fn custom_decoder_uses_filtered_domain_and_scattered_output() -> TestResult {
    let array: ArrayRef = Arc::new(Int64Array::new(vec![4, -1, 9].into(), Some(vec![true, false, true].into())));
    let input = ArrowOperand { column: array, dtype: Field::new("input", DataType::Int64, true), scalar: false };
    let output = rowfn_arrow::plan(&Identity, &NoOptions, &[input.dtype.clone()])?;
    let result = rowfn_arrow::invoke(&Identity, &NoOptions, &[input], 3, output)?;
    assert_eq!(result.array.as_any().downcast_ref::<Int64Array>().unwrap().iter().collect::<Vec<_>>(), vec![Some(4), None, Some(9)]);
    Ok(())
}

#[test]
fn decoder_error_is_terminal_on_a_valid_row() -> TestResult {
    let array: ArrayRef = Arc::new(Int64Array::from(vec![Some(4), Some(-1), None]));
    let input = ArrowOperand { column: array, dtype: Field::new("input", DataType::Int64, true), scalar: false };
    let output = rowfn_arrow::plan(&Identity, &NoOptions, &[input.dtype.clone()])?;
    let error = rowfn_arrow::invoke(&Identity, &NoOptions, &[input], 3, output).err().unwrap();
    assert!(error.to_string().contains("negative decoded domain"));
    Ok(())
}

#[derive(Clone)]
struct AddNonnegativeConstant;

impl RowFn<ArrowHost> for AddNonnegativeConstant {
    type Options = NoOptions;
    const ARG_NAMES: &'static [&'static str] = &["constant", "value"];
    const INFALLIBLE: bool = true;

    fn dispatch<V: RowVisitor<ArrowHost>>(
        &self,
        _: &NoOptions,
        _: &[Field],
        visitor: V,
    ) -> HostResult<ArrowHost, V::VisitResult> {
        visitor.visit::<(Nonnegative, i64), i64>(|(constant, value)| constant.wrapping_add(value))
    }
}

#[test]
fn selected_scalar_uses_ordinary_decoder() -> TestResult {
    let constant = ArrowOperand {
        column: Arc::new(Int64Array::from(vec![4])),
        dtype: Field::new("constant", DataType::Int64, false),
        scalar: true,
    };
    let values = ArrowOperand {
        column: Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])),
        dtype: Field::new("value", DataType::Int64, true),
        scalar: false,
    };
    let fields = [constant.dtype.clone(), values.dtype.clone()];
    let output = rowfn_arrow::plan(&AddNonnegativeConstant, &NoOptions, &fields)?;
    let result = rowfn_arrow::invoke(
        &AddNonnegativeConstant,
        &NoOptions,
        &[constant, values],
        3,
        output,
    )?;
    let values = result.array.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(values.iter().collect::<Vec<_>>(), vec![Some(5), None, Some(7)]);
    Ok(())
}

struct FetchedInt;
impl RowKind for FetchedInt { type Value<'a> = i64; }

// SAFETY: this retained slice has the same stable initialized domain as NonnegativeView.
unsafe impl<'a> RowView<'a, FetchedInt> for NonnegativeView<'a> {
    fn len(&self) -> usize { self.0.len() }
    unsafe fn get_unchecked(&self, index: usize) -> i64 {
        // SAFETY: the caller bounds the index by the retained slice length.
        unsafe { *self.0.get_unchecked(index) }
    }
}

impl InputBinding<FetchedInt> for ArrowHost {
    type Decoded = ScalarBuffer<i64>;
    type View<'a> = NonnegativeView<'a>;
    const DENSE_SAFE: bool = true;
    const DECODE_INFALLIBLE: bool = true;
    fn validate(dtype: &Field) -> Result<(), ArrowError> {
        <Self as InputBinding<i64>>::validate(dtype)
    }
    fn decode(column: &ArrayRef, scalar: bool, ctx: &mut ()) -> Result<Self::Decoded, ArrowError> {
        let values = <Self as InputBinding<i64>>::decode(column, scalar, ctx)?;
        // This fixture models a resource failure while fetching the original encoded batch.
        if values.contains(&-1) { return Err(Self::error("input transport unavailable")); }
        Ok(values)
    }
    fn can_decode_null_tolerant(_: &ArrayRef) -> Result<bool, ArrowError> { Ok(true) }
    fn view(decoded: &Self::Decoded) -> NonnegativeView<'_> { NonnegativeView(decoded.as_ref()) }
}

#[derive(Clone)]
struct DeferredIdentity;
impl RowFn<ArrowHost> for DeferredIdentity {
    type Options = NoOptions;
    const ARG_NAMES: &'static [&'static str] = &["value"];
    const INFALLIBLE: bool = false;
    fn dispatch<V: RowVisitor<ArrowHost>>(&self, _: &NoOptions, _: &[Field], visitor: V)
        -> HostResult<ArrowHost, V::VisitResult> {
        visitor.visit_deferred::<(FetchedInt,), i64, bool>(|(value,)| (value, false), |_| Ok(()))
    }
}

#[test]
fn dense_decoder_failure_cannot_be_suppressed_by_filtering_null_payloads() -> TestResult {
    let column: ArrayRef = Arc::new(Int64Array::new(vec![4, -1, 9].into(), Some(vec![true, false, true].into())));
    let input = ArrowOperand { column, dtype: Field::new("input", DataType::Int64, true), scalar: false };
    let output = rowfn_arrow::plan(&DeferredIdentity, &NoOptions, &[input.dtype.clone()])?;
    let error = rowfn_arrow::invoke(&DeferredIdentity, &NoOptions, &[input], 3, output).err().unwrap();
    assert!(error.to_string().contains("input transport unavailable"));
    Ok(())
}
