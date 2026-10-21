// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! List sums read from how their elements are encoded, without decoding them.
//!
//! A run-end array holds runs of one value, and a sparse array a fill value between its
//! patches. Lists whose elements follow one another are summed in one sweep over the runs or
//! patches, adding each run's value, or a transform of it, once per element of it a list holds.

use vortex::array::ArrayRef;
use vortex::array::ExecutionCtx;
use vortex::array::arrays::PrimitiveArray;
use vortex::array::arrays::Slice;
use vortex::array::arrays::slice::SliceArraySlotsExt;
use vortex::array::builtins::ArrayBuiltins;
use vortex::array::match_each_integer_ptype;
use vortex::buffer::BitBuffer;
use vortex::buffer::Buffer;
use vortex::dtype::DType;
use vortex::dtype::IntegerPType;
use vortex::dtype::Nullability;
use vortex::dtype::PType;
use vortex::encodings::runend::RunEnd;
use vortex::encodings::runend::RunEndArrayExt;
use vortex::encodings::runend::RunEndArraySlotsExt;
use vortex::encodings::sparse::Sparse;
use vortex::encodings::sparse::SparseExt;
use vortex::error::VortexResult;
use vortex::expr::Expression;
use vortex::scalar_fn::fns::binary::Binary;
use vortex::scalar_fn::fns::get_item::GetItem;
use vortex::scalar_fn::fns::literal::Literal;
use vortex::scalar_fn::fns::operators::Operator;

use super::ELEMENT;
use super::Lists;
use super::Valid;

/// The largest integer every smaller one of which `f64` holds exactly.
const EXACT: u128 = 1 << f64::MANTISSA_DIGITS;

/// What a list sum adds for each element: the element, or a comparison of it.
///
/// A sum of the elements' validity alone, as of `IS NOT NULL`, is a count of a validity mask's
/// bits, which costs less than a sweep over the patches.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Transform {
    Identity,
    Eq(i128),
    NotEq(i128),
}

impl Transform {
    /// The transform `element`, an expression over the struct of [`ELEMENT`], is, when it is one
    /// of these.
    pub(super) fn of(element: Option<&Expression>) -> Option<Self> {
        let Some(element) = element else {
            return Some(Self::Identity);
        };
        let is_element = |expr: &Expression| {
            expr.as_opt::<GetItem>()
                .is_some_and(|field| field.as_ref() == ELEMENT)
                && expr.children().first().is_some_and(Expression::is_root)
        };
        let operator = element.as_opt::<Binary>()?;
        let [left, right] = element.children() else {
            return None;
        };
        let literal = match (is_element(left), is_element(right)) {
            (true, false) => right,
            (false, true) => left,
            _ => return None,
        };
        let literal = literal.as_opt::<Literal>()?.as_primitive();
        if !literal.ptype().is_int() {
            return None;
        }
        let value = match_each_integer_ptype!(literal.ptype(), |T| {
            literal.typed_value::<T>().map(widen)
        })?;
        match operator {
            Operator::Eq => Some(Self::Eq(value)),
            Operator::NotEq => Some(Self::NotEq(value)),
            _ => None,
        }
    }

    /// What an element of `value`, null when `None`, adds to its list's sum, null when `None`.
    fn apply(self, value: Option<i128>) -> Option<i128> {
        match self {
            Self::Identity => value,
            Self::Eq(literal) => value.map(|value| i128::from(value == literal)),
            Self::NotEq(literal) => value.map(|value| i128::from(value != literal)),
        }
    }
}

/// Each of `lists`' sums of `transform` of its elements, and whether it has any valid one, when
/// the elements are run-end or sparse integers in list order and every list's sum is exact as
/// `f64`; `None` otherwise.
///
/// While the magnitudes of a list's values add up to at most [`EXACT`], so does every partial
/// sum of them, and adding them as `f64` in order, as DataFusion does, gives the exact sum.
pub(super) fn sum_runs(
    lists: &Lists,
    transform: Transform,
    ctx: &mut ExecutionCtx,
) -> VortexResult<Option<(Buffer<f64>, BitBuffer)>> {
    if !matches!(lists.elements.dtype(), DType::Primitive(ptype, _) if ptype.is_int())
        || !lists.ordered()
    {
        return Ok(None);
    }
    let mut shift = 0;
    let mut elements = lists.elements.clone();
    while let Some(slice) = elements.as_opt::<Slice>() {
        shift += slice.slice_range().start;
        let child = slice.child().clone();
        elements = child;
    }
    let mut sums = Sums::new(lists.starts.len());
    let exact = if let Some(runs) = elements.as_opt::<RunEnd>() {
        let ends = positions(runs.ends(), ctx)?;
        let shift = shift + runs.offset();
        with_values(runs.values(), ctx, |value| {
            sum_runs_of(lists, transform, shift, &ends, value, &mut sums)
        })?
    } else if let Some(sparse) = elements.as_opt::<Sparse>() {
        let patches = sparse.patches();
        let indices = positions(patches.indices(), ctx)?;
        let fill = sparse.fill_scalar().as_primitive();
        let fill =
            match_each_integer_ptype!(fill.ptype(), |T| { fill.typed_value::<T>().map(widen) });
        let shift = shift + patches.offset();
        with_values(patches.values(), ctx, |value| {
            sum_patches_of(lists, transform, shift, &indices, value, fill, &mut sums)
        })?
    } else {
        return Ok(None);
    };
    Ok(exact.then(|| sums.finish()))
}

/// Calls `sum` with each of `values` widened, or null.
fn with_values(
    values: &ArrayRef,
    ctx: &mut ExecutionCtx,
    sum: impl FnOnce(&dyn Fn(usize) -> Option<i128>) -> bool,
) -> VortexResult<bool> {
    let valid = values.validity()?.execute_mask(values.len(), ctx)?;
    let valid = Valid::new(&valid);
    let values = values.clone().execute::<PrimitiveArray>(ctx)?;
    Ok(match_each_integer_ptype!(values.ptype(), |T| {
        let values = values.as_slice::<T>();
        sum(&|index| valid.get(index).then(|| widen(values[index])))
    }))
}

/// Sums lists of run-end elements: run `i` holds `value(i)` up to `ends[i]`, and a list's
/// elements are `shift` past its offsets among the runs.
fn sum_runs_of(
    lists: &Lists,
    transform: Transform,
    shift: usize,
    ends: &[u64],
    value: &dyn Fn(usize) -> Option<i128>,
    sums: &mut Sums,
) -> bool {
    let mut run = 0;
    for (start, end, list_valid) in lists.rows() {
        let (mut position, end) = ((start + shift) as u64, (end + shift) as u64);
        while run < ends.len() && ends[run] <= position {
            run += 1;
        }
        let mut sum = Sum::default();
        while position < end && run < ends.len() {
            let stop = ends[run].min(end);
            if let Some(value) = transform.apply(value(run)) {
                sum.add(value, stop - position);
            }
            position = stop;
            if stop == ends[run] {
                run += 1;
            }
        }
        if !sums.push(sum, list_valid) {
            return false;
        }
    }
    true
}

/// Sums lists of sparse elements: `fill`, or null when it is `None`, but at `indices`, the
/// `i`th of which holds `value(i)`. A list's elements are `shift` past its offsets among the
/// indices.
fn sum_patches_of(
    lists: &Lists,
    transform: Transform,
    shift: usize,
    indices: &[u64],
    value: &dyn Fn(usize) -> Option<i128>,
    fill: Option<i128>,
    sums: &mut Sums,
) -> bool {
    let fill = transform.apply(fill);
    let mut patch = 0;
    for (start, end, list_valid) in lists.rows() {
        let (start, end) = ((start + shift) as u64, (end + shift) as u64);
        while patch < indices.len() && indices[patch] < start {
            patch += 1;
        }
        let mut sum = Sum::default();
        let first = patch;
        while patch < indices.len() && indices[patch] < end {
            if let Some(value) = transform.apply(value(patch)) {
                sum.add(value, 1);
            }
            patch += 1;
        }
        if let Some(fill) = fill {
            sum.add(fill, end - start - (patch - first) as u64);
        }
        if !sums.push(sum, list_valid) {
            return false;
        }
    }
    true
}

/// A list's valid transformed elements: their count, sum, and the sum of their magnitudes.
#[derive(Default)]
struct Sum {
    count: u64,
    sum: i128,
    magnitude: u128,
}

impl Sum {
    fn add(&mut self, value: i128, count: u64) {
        self.count += count;
        self.sum += value * i128::from(count);
        self.magnitude += value.unsigned_abs() * u128::from(count);
    }
}

/// The lists' sums so far, and which are valid.
struct Sums {
    sums: Vec<f64>,
    validity: Vec<bool>,
}

impl Sums {
    fn new(len: usize) -> Self {
        Self {
            sums: Vec::with_capacity(len),
            validity: Vec::with_capacity(len),
        }
    }

    /// Adds the next list's sum, or returns `false` when it is not exact as `f64`.
    #[expect(
        clippy::cast_precision_loss,
        reason = "the sum is an integer of magnitude at most EXACT, which f64 holds exactly"
    )]
    fn push(&mut self, sum: Sum, list_valid: bool) -> bool {
        let any = list_valid && sum.count > 0;
        if any && sum.magnitude > EXACT {
            return false;
        }
        self.sums.push(if any { sum.sum as f64 } else { 0.0 });
        self.validity.push(any);
        true
    }

    fn finish(self) -> (Buffer<f64>, BitBuffer) {
        (Buffer::from(self.sums), BitBuffer::from_iter(self.validity))
    }
}

fn widen<T: IntegerPType>(value: T) -> i128 {
    value.to_i128().unwrap_or_default()
}

/// Positions, as run ends or patch indices, decoded.
fn positions(array: &ArrayRef, ctx: &mut ExecutionCtx) -> VortexResult<Buffer<u64>> {
    array
        .clone()
        .cast(DType::Primitive(PType::U64, Nullability::NonNullable))?
        .execute::<Buffer<u64>>(ctx)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use vortex::VortexSessionDefault;
    use vortex::array::ArrayRef;
    use vortex::array::ExecutionCtx;
    use vortex::array::IntoArray;
    use vortex::array::VortexSessionExecute;
    use vortex::array::arrays::ListArray;
    use vortex::array::arrays::ListViewArray;
    use vortex::array::arrays::PrimitiveArray;
    use vortex::array::arrays::SliceArray;
    use vortex::array::validity::Validity;
    use vortex::buffer::buffer;
    use vortex::dtype::Nullability;
    use vortex::encodings::runend::RunEnd;
    use vortex::encodings::sparse::Sparse;
    use vortex::error::VortexResult;
    use vortex::expr::Expression;
    use vortex::expr::eq;
    use vortex::expr::get_item;
    use vortex::expr::is_not_null;
    use vortex::expr::is_null;
    use vortex::expr::lit;
    use vortex::expr::not_eq;
    use vortex::expr::root;
    use vortex::scalar::Scalar;
    use vortex::session::VortexSession;
    use vortex_array::assert_arrays_eq;

    use super::super::ELEMENT;
    use super::super::ListSumF64Options;
    use super::super::Lists;
    use super::super::sum_lists;
    use super::Transform;
    use super::sum_runs;

    fn element() -> Expression {
        get_item(ELEMENT, root())
    }

    /// The transforms summed: none, and comparisons of the element.
    fn transforms() -> [Option<Expression>; 3] {
        [
            None,
            Some(eq(element(), lit(2u64))),
            Some(not_eq(lit(0u64), element())),
        ]
    }

    /// Validity alone is summed from a validity mask, and other transforms by decoding.
    #[test]
    fn other_transforms_are_not_summed_from_runs() {
        for element in [
            is_not_null(element()),
            is_null(element()),
            eq(element(), element()),
            eq(element(), lit(1.5f64)),
        ] {
            assert_eq!(Transform::of(Some(&element)), None, "{element}");
        }
    }

    /// The elements every case sums, with runs, nulls, and a value past the fill.
    fn elements() -> PrimitiveArray {
        PrimitiveArray::from_option_iter([
            Some(0u64),
            Some(0),
            Some(0),
            Some(1),
            Some(1),
            None,
            None,
            Some(2),
            Some(2),
            Some(2),
            Some(0),
            Some(0),
            Some(5),
            None,
            Some(0),
            Some(0),
            Some(7),
            Some(7),
        ])
    }

    fn run_end(ctx: &mut ExecutionCtx) -> VortexResult<ArrayRef> {
        Ok(RunEnd::encode(elements().into_array(), ctx)?.into_array())
    }

    /// The elements as patches over `fill`, a null one when `None`.
    fn sparse(fill: Option<u64>, ctx: &mut ExecutionCtx) -> VortexResult<ArrayRef> {
        let elements = elements();
        let (indices, values): (Vec<u64>, Vec<Option<u64>>) = (0..elements.len())
            .map(|index| {
                let value = elements
                    .is_valid(index, ctx)?
                    .then(|| elements.as_slice::<u64>()[index]);
                Ok((index as u64, value))
            })
            .collect::<VortexResult<Vec<_>>>()?
            .into_iter()
            .filter(|(_, value)| *value != fill)
            .unzip();
        let fill = match fill {
            Some(fill) => Scalar::primitive(fill, Nullability::Nullable),
            None => Scalar::null_native::<u64>(),
        };
        Ok(Sparse::try_new(
            PrimitiveArray::from_iter(indices).into_array(),
            PrimitiveArray::from_option_iter(values).into_array(),
            elements.len(),
            fill,
            ctx,
        )?
        .into_array())
    }

    /// Lists over `elements` in list order, with an empty and a null list, as views between
    /// gaps when `views`.
    fn lists(elements: ArrayRef, views: bool, ctx: &mut ExecutionCtx) -> VortexResult<ArrayRef> {
        let validity = Validity::from_iter([true, true, false, true, true]);
        if views {
            return Ok(ListViewArray::new(
                elements,
                buffer![0u32, 3, 4, 9, 12].into_array(),
                buffer![2u32, 0, 4, 2, 4].into_array(),
                validity,
                ctx,
            )
            .into_array());
        }
        Ok(ListArray::try_new(
            elements,
            buffer![0u32, 3, 3, 7, 12, 16].into_array(),
            validity,
            ctx,
        )?
        .into_array())
    }

    /// Run-end and sparse elements sum as their decoded values do, sliced or not, and so do
    /// the transforms of them.
    #[rstest]
    #[case::run_end(None, false)]
    #[case::zero_fill(Some(Some(0)), false)]
    #[case::null_fill(Some(None), false)]
    #[case::run_end_views(None, true)]
    #[case::sparse_views(Some(Some(0)), true)]
    fn encoded_sums_match_decoded(
        #[case] sparse_fill: Option<Option<u64>>,
        #[case] views: bool,
        #[values(0, 2)] shift: u32,
        #[values(0, 1, 2)] transform: usize,
    ) -> VortexResult<()> {
        let element = transforms()[transform].clone();
        let transform = Transform::of(element.as_ref()).expect("a transform summed from the runs");
        let mut ctx = VortexSession::default().create_execution_ctx();
        let encoded = match sparse_fill {
            None => run_end(&mut ctx)?,
            Some(fill) => sparse(fill, &mut ctx)?,
        };
        let (encoded, decoded) = if shift == 0 {
            (encoded, elements().into_array())
        } else {
            let range = shift as usize..encoded.len();
            (
                SliceArray::try_new(encoded, range.clone())?.into_array(),
                elements().into_array().slice(range)?,
            )
        };
        let encoded = Lists::new(lists(encoded, views, &mut ctx)?, &mut ctx)?;
        let decoded = Lists::new(lists(decoded, views, &mut ctx)?, &mut ctx)?;

        let (sums, valid) = sum_runs(&encoded, transform, &mut ctx)?.expect("summed from the runs");
        let (expected, expected_valid) =
            sum_lists(&decoded, &ListSumF64Options { element }, &mut ctx)?;
        assert_arrays_eq!(
            PrimitiveArray::new(
                sums,
                Validity::from_bit_buffer(valid, Nullability::Nullable)
            ),
            PrimitiveArray::new(
                expected,
                Validity::from_bit_buffer(expected_valid, Nullability::Nullable)
            ),
            &mut ctx
        );
        Ok(())
    }
}
