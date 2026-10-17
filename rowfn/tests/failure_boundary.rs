// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! A host with unresolved validity distinguishes row evidence from terminal adapter failures.

use std::io;
use std::mem::MaybeUninit;

use rowfn::kernels::bit::BitmapView;
use rowfn::{BatchBinding, Host, InputBinding, Operand, OutputBinding, OutputBuffer, RowFn, RowVisitor,
    Selection, TypeBinding, ValiditySummary};

struct TestHost;

#[derive(Clone)]
struct Column {
    values: Vec<i64>,
    valid: Vec<bool>,
}

#[derive(Clone)]
struct Rows(Vec<bool>);

#[derive(Clone, Copy)]
enum Failure { None, Decode, Allocate, Publish }

impl Host for TestHost {
    type Column = Column;
    type NativeType = bool;
    type Context = Failure;
    type Error = io::Error;

    fn error(message: &str) -> io::Error { io::Error::other(message) }
}

impl TypeBinding for TestHost {
    fn nullable(dtype: &bool) -> bool { *dtype }
    fn with_nullable(_: &bool, nullable: bool) -> bool { nullable }
    fn validate_label(_: &bool, _: &bool) -> io::Result<()> { Ok(()) }
}

// SAFETY: the immutable Boolean vector defines the exact ordered selection and its count.
unsafe impl Selection for Rows {
    fn len(&self) -> usize { self.0.len() }
    fn count(&self) -> usize { self.0.iter().filter(|valid| **valid).count() }
    fn try_for_each<E>(&self, mut visit: impl FnMut(usize) -> Result<(), E>) -> Result<(), E> {
        for (index, valid) in self.0.iter().enumerate() {
            if *valid { visit(index)?; }
        }
        Ok(())
    }
}

impl BatchBinding for TestHost {
    type Validity = Rows;
    type Selection = Rows;

    fn validate_operand(input: &Operand<Self>, rows: usize) -> io::Result<()> {
        if input.scalar || input.column.values.len() != rows || input.column.valid.len() != rows {
            return Err(Self::error("fixture requires full-length array inputs"));
        }
        Ok(())
    }
    fn validity(inputs: &[Operand<Self>], _: usize, _: &mut Failure) -> io::Result<Rows> {
        Ok(Rows(inputs[0].column.valid.clone()))
    }
    fn validity_summary(_: &Rows) -> ValiditySummary { ValiditySummary::Unknown }
    fn selection(validity: &Rows, _: usize, _: &mut Failure) -> io::Result<Rows> {
        Ok(validity.clone())
    }
    fn filter(_: &Operand<Self>, _: &Rows, _: &mut Failure) -> io::Result<Operand<Self>> {
        Err(Self::error("fixture must use its null-tolerant input binding"))
    }
    fn all_null(_: &bool, rows: usize, _: &mut Failure) -> io::Result<Column> {
        Ok(Column { values: vec![0; rows], valid: vec![false; rows] })
    }
    fn broadcast(_: Column, _: usize, _: &mut Failure) -> io::Result<Column> {
        Err(Self::error("fixture has no scalar operands"))
    }
    fn publish(mut column: Column, _: &bool, _: &bool, valid: &Rows, _: usize, failure: &mut Failure)
        -> io::Result<Column> {
        if matches!(failure, Failure::Publish) { return Err(Self::error("publication unavailable")); }
        column.valid = valid.0.clone();
        Ok(column)
    }
}

impl InputBinding<i64> for TestHost {
    type Decoded = Vec<i64>;
    type View<'a> = &'a [i64];
    const DENSE_SAFE: bool = true;
    const DECODE_INFALLIBLE: bool = true;

    fn validate(_: &bool) -> io::Result<()> { Ok(()) }
    fn decode(column: &Column, _: bool, failure: &mut Failure) -> io::Result<Vec<i64>> {
        if matches!(failure, Failure::Decode) { return Err(Self::error("decoder unavailable")); }
        Ok(column.values.clone())
    }
    fn can_decode_null_tolerant(_: &Column) -> io::Result<bool> { Ok(true) }
    fn view(decoded: &Vec<i64>) -> &[i64] { decoded }
}

struct Slots(Vec<MaybeUninit<i64>>);

// SAFETY: the vector keeps its fixed length and safely abandons any subset of initialized slots.
unsafe impl OutputBuffer<i64> for Slots {
    type Finished = Column;
    fn slots(&mut self) -> &mut [MaybeUninit<i64>] { &mut self.0 }
    unsafe fn finish(self, len: usize) -> Column {
        let values = self.0[..len].iter().map(|value| {
            // SAFETY: finish's caller initialized every slot in this prefix.
            unsafe { value.assume_init() }
        }).collect();
        Column { values, valid: vec![true; len] }
    }
}

impl OutputBinding<i64> for TestHost {
    type Buffer = Slots;
    fn output_type() -> bool { false }
    fn allocate(rows: usize, failure: &mut Failure) -> io::Result<Slots> {
        if matches!(failure, Failure::Allocate) { return Err(Self::error("allocator unavailable")); }
        Ok(Slots(vec![MaybeUninit::uninit(); rows]))
    }
}

#[derive(Clone)]
struct CheckedIncrement;
impl RowFn<TestHost> for CheckedIncrement {
    type Options = ();
    const ARG_NAMES: &'static [&'static str] = &["value"];
    const INFALLIBLE: bool = false;

    fn dispatch<V: RowVisitor<TestHost>>(&self, _: &(), _: &[bool], visitor: V)
        -> io::Result<V::VisitResult> {
        visitor.visit_deferred::<(i64,), i64, bool>(|(value,)| value.overflowing_add(1), |failed| {
            if failed { Err(TestHost::error("row overflow")) } else { Ok(()) }
        })
    }
}

fn invoke(value: i64, failure: &mut Failure) -> io::Result<Column> {
    let input = Operand { column: Column { values: vec![value; 3], valid: vec![false; 3] },
        dtype: true, scalar: false };
    rowfn::execute::<TestHost, _>(&CheckedIncrement, &(), &[input], 3, &true, failure)
}

#[test]
fn lazy_all_null_validity_suppresses_only_rejected_row_evidence() -> io::Result<()> {
    let result = invoke(i64::MAX, &mut Failure::None)?;
    assert_eq!(result.valid, vec![false; 3]);
    Ok(())
}

#[test]
fn lazy_all_null_validity_cannot_suppress_infrastructure_errors() {
    let cases = [
        (Failure::Decode, "decoder unavailable"),
        (Failure::Allocate, "allocator unavailable"),
        (Failure::Publish, "publication unavailable"),
    ];
    for (mut failure, message) in cases {
        let error = invoke(1, &mut failure).err().unwrap();
        assert_eq!(error.to_string(), message);
    }
}


#[test]
fn bitmap_selection_bounds_sliced_rows_and_stops_at_first_error() {
    for offset in 0..8 {
        for len in [0, 1, 63, 64, 65, 129] {
            for bytes in [[0xff; 18], [0xa5; 18]] {
                let selection = BitmapView::new(&bytes, offset, len);
                let expected: Vec<_> = (0..len).filter(|i| bytes[(i + offset) / 8] & (1 << ((i + offset) % 8)) != 0).collect();
                let mut actual = Vec::new();
                selection.for_each(|i| actual.push(i));
                assert_eq!(actual, expected);
                assert_eq!(selection.count(), expected.len());
                let mut visited = Vec::new();
                let result = selection.try_for_each(|i| {
                    visited.push(i);
                    if visited.len() == 2 { Err("stop") } else { Ok(()) }
                });
                assert_eq!(visited, expected[..expected.len().min(2)]);
                assert_eq!(result, if expected.len() >= 2 { Err("stop") } else { Ok(()) });
            }
        }
    }
}
