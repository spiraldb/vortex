// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! DataFusion's `array_sum` as a Vortex scalar function, so a V2 scan sums each list, or a
//! transform of its elements, where it decodes them.
//!
//! A query such as `array_sum(array_transform(GT, g -> g IS NOT NULL))` otherwise hands every
//! element to DataFusion, which casts the list to `Float64`, evaluates the lambda into another
//! list, casts that, and sums it: three passes over lists of thousands of values per row, to
//! produce one number. Here the transform is a Vortex expression over the elements, and the sum
//! one pass over them.

use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;
use std::ops::Range;
use std::sync::Arc;

use arrow_array::Array;
use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_schema::DataType;
use arrow_schema::Field;
use arrow_schema::FieldRef;
use arrow_schema::Schema;
use datafusion_common::Result as DFResult;
use datafusion_common::exec_datafusion_err;
use datafusion_common::tree_node::Transformed;
use datafusion_common::tree_node::TreeNode;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_physical_expr::HigherOrderFunctionExpr;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::ScalarFunctionExpr;
use datafusion_physical_expr::expressions::CastExpr;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::expressions::LambdaExpr;
use datafusion_physical_expr::expressions::LambdaVariable;
use datafusion_physical_expr::projection::ProjectionExpr;
use datafusion_physical_expr::projection::ProjectionExprs;
use datafusion_physical_expr::utils::collect_columns;
use datafusion_physical_expr::utils::reassign_expr_columns;
use vortex::array::ArrayRef;
use vortex::array::ExecutionCtx;
use vortex::array::IntoArray;
use vortex::array::VortexSessionExecute;
use vortex::array::arrays::BoolArray;
use vortex::array::arrays::List;
use vortex::array::arrays::ListViewArray;
use vortex::array::arrays::PrimitiveArray;
use vortex::array::arrays::StructArray;
use vortex::array::arrays::list::ListArraySlotsExt;
use vortex::array::arrays::listview::ListViewArraySlotsExt;
use vortex::array::builtins::ArrayBuiltins;
use vortex::array::validity::Validity;
use vortex::buffer::BitBuffer;
use vortex::buffer::Buffer;
use vortex::dtype::DType;
use vortex::dtype::Nullability;
use vortex::dtype::PType;
use vortex::error::VortexResult;
use vortex::error::vortex_bail;
use vortex::error::vortex_err;
use vortex::expr::BoundExpression;
use vortex::expr::Expression;
use vortex::expr::get_item;
use vortex::expr::pack;
use vortex::expr::root;
use vortex::layout::scan::v2::RowFilter;
use vortex::mask::Mask;
use vortex::scalar_fn::Arity;
use vortex::scalar_fn::ChildName;
use vortex::scalar_fn::ExecutionArgs;
use vortex::scalar_fn::ScalarFnId;
use vortex::scalar_fn::ScalarFnVTable;
use vortex::scalar_fn::ScalarFnVTableExt;
use vortex::session::VortexSession;
use vortex::session::registry::CachedId;
use vortex_arrow::ArrowSessionExt;

use crate::convert::exprs::ExpressionConverter;
use crate::convert::exprs::ProcessedProjection;
use crate::convert::schema::calculate_physical_schema;

/// The field of the one-field struct an element transform is evaluated over.
pub(crate) const ELEMENT: &str = "element";

/// DataFusion's `array_sum` over each list, or over `element` evaluated on its elements: the
/// valid values cast to `Float64` and added in order, or null for a null list or one with no
/// valid value.
#[derive(Clone)]
pub(crate) struct ListSumF64;

/// The transform of the elements summed, bound to a struct of one field, [`ELEMENT`], holding
/// them. `None` sums the elements themselves.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ListSumF64Options {
    pub(crate) element: Option<Expression>,
}

impl Display for ListSumF64Options {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self.element {
            Some(element) => write!(f, "{element}"),
            None => Ok(()),
        }
    }
}

impl ScalarFnVTable for ListSumF64 {
    type Options = ListSumF64Options;

    fn id(&self) -> ScalarFnId {
        static ID: CachedId = CachedId::new("vortex.datafusion.array_sum");
        *ID
    }

    fn arity(&self, _options: &Self::Options) -> Arity {
        Arity::Exact(1)
    }

    fn child_name(&self, _options: &Self::Options, _child_idx: usize) -> ChildName {
        ChildName::from("list")
    }

    fn return_dtype(&self, _options: &Self::Options, args: &[DType]) -> VortexResult<DType> {
        if !matches!(args.first(), Some(DType::List(..))) {
            vortex_bail!("array_sum requires a list, got {:?}", args.first());
        }
        Ok(DType::Primitive(PType::F64, Nullability::Nullable))
    }

    fn execute(
        &self,
        options: &Self::Options,
        args: &dyn ExecutionArgs,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<ArrayRef> {
        let lists = Lists::new(args.get(0)?, ctx)?;
        let mut sums = Vec::with_capacity(lists.starts.len());
        let mut validity = Vec::with_capacity(lists.starts.len());
        // A transform runs over whole encoded elements: its kernels over sliced ones, or over
        // elements decoded first, cost more than the cache saves.
        let groups = if options.element.is_none() {
            lists.groups()?
        } else {
            vec![lists.trimmed()?]
        };
        for group in groups {
            let (group_sums, group_validity) = sum_lists(&group, options, ctx)?;
            sums.extend_from_slice(&group_sums);
            validity.extend(group_validity.iter());
        }
        let sums = Buffer::from(sums);
        let validity = BitBuffer::from_iter(validity);
        Ok(PrimitiveArray::new(
            sums,
            Validity::from_bit_buffer(validity, Nullability::Nullable),
        )
        .into_array())
    }

    fn is_strict(&self, _options: &Self::Options) -> bool {
        true
    }

    fn is_infallible(&self, _options: &Self::Options) -> bool {
        true
    }
}

/// Sums each of `lists`, of `options`' transform of its elements when given.
fn sum_lists(
    lists: &Lists,
    options: &ListSumF64Options,
    ctx: &mut ExecutionCtx,
) -> VortexResult<(Buffer<f64>, BitBuffer)> {
    let values = match &options.element {
        Some(element) => StructArray::from_fields(&[(ELEMENT, lists.elements.clone())])?
            .into_array()
            .apply(element)?,
        None => lists.elements.clone(),
    };
    let valid = values.validity()?.execute_mask(values.len(), ctx)?;
    Ok(match values.dtype() {
        DType::Bool(_) => {
            let bits = values.execute::<BoolArray>(ctx)?.into_bit_buffer();
            lists.sum_bits(&bits, &valid)
        }
        DType::Primitive(ptype, _) => {
            let ptype = *ptype;
            let values = values.execute::<PrimitiveArray>(ctx)?;
            match ptype {
                PType::F32 => lists.sum(values.as_slice::<f32>(), &valid, f64::from),
                PType::F64 => lists.sum(values.as_slice::<f64>(), &valid, |value| value),
                PType::U8 => lists.sum_unsigned(values.as_slice::<u8>(), &valid, u64::from),
                PType::U16 => lists.sum_unsigned(values.as_slice::<u16>(), &valid, u64::from),
                PType::U32 => lists.sum_unsigned(values.as_slice::<u32>(), &valid, u64::from),
                PType::U64 => lists.sum_unsigned(values.as_slice::<u64>(), &valid, |v| v),
                PType::I8 => lists.sum_signed(values.as_slice::<i8>(), &valid, i64::from),
                PType::I16 => lists.sum_signed(values.as_slice::<i16>(), &valid, i64::from),
                PType::I32 => lists.sum_signed(values.as_slice::<i32>(), &valid, i64::from),
                PType::I64 => lists.sum_signed(values.as_slice::<i64>(), &valid, |v| v),
                ptype => vortex_bail!("array_sum cannot sum {ptype}"),
            }
        }
        dtype => vortex_bail!("array_sum cannot sum {dtype}"),
    })
}

/// Elements a group of lists decodes and sums at once, so the decoded values are still in
/// cache when they are added rather than written out to memory and read back.
const ELEMENTS_PER_GROUP: u64 = 1 << 20;

/// Where each list's elements are, which lists are valid, and the elements.
#[derive(Clone)]
struct Lists {
    starts: Buffer<u64>,
    ends: Buffer<u64>,
    valid: Mask,
    elements: ArrayRef,
}

fn offsets(array: &ArrayRef, ctx: &mut ExecutionCtx) -> VortexResult<Buffer<u64>> {
    array
        .clone()
        .cast(DType::Primitive(PType::U64, Nullability::NonNullable))?
        .execute::<Buffer<u64>>(ctx)
}

impl Lists {
    fn new(array: ArrayRef, ctx: &mut ExecutionCtx) -> VortexResult<Self> {
        let len = array.len();
        let valid = array.validity()?.execute_mask(len, ctx)?;
        if let Some(list) = array.as_opt::<List>() {
            let offsets = offsets(list.offsets(), ctx)?;
            if offsets.len() != len + 1 {
                vortex_bail!("A list of {len} rows with {} offsets", offsets.len());
            }
            return Ok(Self {
                starts: offsets.slice(0..len),
                ends: offsets.slice(1..len + 1),
                valid,
                elements: list.elements().clone(),
            });
        }
        let list = array.execute::<ListViewArray>(ctx)?;
        let starts = offsets(list.offsets(), ctx)?;
        let sizes = offsets(list.sizes(), ctx)?;
        let ends = starts
            .iter()
            .zip(sizes.iter())
            .map(|(start, size)| start + size)
            .collect();
        Ok(Self {
            starts,
            ends,
            valid,
            elements: list.elements().clone(),
        })
    }

    /// The lists in groups of consecutive lists holding about [`ELEMENTS_PER_GROUP`] elements,
    /// each with only its elements, or all of them in one group when the lists' elements are
    /// not in list order.
    fn groups(&self) -> VortexResult<Vec<Lists>> {
        let ordered = self
            .starts
            .iter()
            .zip(self.ends.iter())
            .all(|(s, e)| s <= e)
            && self
                .ends
                .iter()
                .zip(self.starts.iter().skip(1))
                .all(|(end, next)| end <= next);
        let len = self.starts.len();
        if !ordered || len == 0 {
            return Ok(vec![self.clone()]);
        }
        if self.ends[len - 1] - self.starts[0] <= ELEMENTS_PER_GROUP {
            return Ok(vec![self.part(0..len)?]);
        }
        let mut groups = Vec::new();
        let mut first = 0;
        while first < len {
            let mut end = first + 1;
            while end < len && self.ends[end] - self.starts[first] <= ELEMENTS_PER_GROUP {
                end += 1;
            }
            groups.push(self.part(first..end)?);
            first = end;
        }
        Ok(groups)
    }

    /// The lists with only their elements, as a slice of lists keeps every element of the lists
    /// around it, when their elements are in list order.
    fn trimmed(self) -> VortexResult<Lists> {
        let len = self.starts.len();
        let ordered = self
            .starts
            .iter()
            .zip(self.ends.iter())
            .all(|(s, e)| s <= e)
            && self
                .ends
                .iter()
                .zip(self.starts.iter().skip(1))
                .all(|(end, next)| end <= next);
        if len == 0 || !ordered {
            return Ok(self);
        }
        self.part(0..len)
    }

    /// Lists `rows`, with only their elements.
    fn part(&self, rows: Range<usize>) -> VortexResult<Lists> {
        let low = self.starts[rows.start];
        let high = self.ends[rows.end - 1];
        Ok(Lists {
            starts: self
                .starts
                .slice(rows.clone())
                .iter()
                .map(|start| start - low)
                .collect(),
            ends: self
                .ends
                .slice(rows.clone())
                .iter()
                .map(|end| end - low)
                .collect(),
            valid: self.valid.slice(rows),
            elements: self
                .elements
                .slice(usize::try_from(low)?..usize::try_from(high)?)?,
        })
    }

    #[expect(
        clippy::cast_possible_truncation,
        reason = "offsets index elements held in memory, so they fit in usize"
    )]
    fn rows(&self) -> impl Iterator<Item = (usize, usize, bool)> + '_ {
        self.starts
            .iter()
            .zip(self.ends.iter())
            .enumerate()
            .map(|(row, (&start, &end))| (start as usize, end as usize, self.valid.value(row)))
    }

    /// Sums `values`, cast by `cast`, in order over each list's valid values.
    fn sum<T: Copy>(
        &self,
        values: &[T],
        valid: &Mask,
        cast: impl Fn(T) -> f64,
    ) -> (Buffer<f64>, BitBuffer) {
        let valid = Valid::new(valid);
        let mut sums = Vec::with_capacity(self.starts.len());
        let mut validity = Vec::with_capacity(self.starts.len());
        for (start, end, list_valid) in self.rows() {
            let any = list_valid && valid.any(start, end);
            sums.push(if any {
                sum_in_order(&values[start..end], &valid, start, &cast)
            } else {
                0.0
            });
            validity.push(any);
        }
        (Buffer::from(sums), BitBuffer::from_iter(validity))
    }

    /// Sums unsigned `values` as [`sum`](Self::sum) does, by adding them exactly.
    ///
    /// Every partial sum of unsigned values is at most the total, so while the total is an
    /// integer `f64` holds exactly, adding the values as `f64` in order gives it exactly too.
    /// A list past that is added as `f64` in order.
    fn sum_unsigned<T: Copy>(
        &self,
        values: &[T],
        valid: &Mask,
        widen: impl Fn(T) -> u64 + Copy,
    ) -> (Buffer<f64>, BitBuffer) {
        const EXACT: u128 = 1 << f64::MANTISSA_DIGITS;
        let valid = Valid::new(valid);
        let mut sums = Vec::with_capacity(self.starts.len());
        let mut validity = Vec::with_capacity(self.starts.len());
        for (start, end, list_valid) in self.rows() {
            let any = list_valid && valid.any(start, end);
            let sum = if !any {
                0.0
            } else {
                let list = &values[start..end];
                let sum = valid.sum_unsigned(list, start, widen);
                if sum <= EXACT {
                    sum as f64
                } else {
                    sum_in_order(list, &valid, start, &|value| widen(value) as f64)
                }
            };
            sums.push(sum);
            validity.push(any);
        }
        (Buffer::from(sums), BitBuffer::from_iter(validity))
    }

    /// Sums signed `values` as [`sum`](Self::sum) does. See [`sum_exact`](Self::sum_exact).
    fn sum_signed<T: Copy>(
        &self,
        values: &[T],
        valid: &Mask,
        widen: impl Fn(T) -> i64 + Copy,
    ) -> (Buffer<f64>, BitBuffer) {
        self.sum_exact(
            values,
            valid,
            move |value| widen(value) as u64,
            move |value| widen(value).unsigned_abs(),
            |sum| sum as i64 as f64,
            move |value| widen(value) as f64,
        )
    }

    /// Sums integer `values` as [`sum`](Self::sum) does, by adding them exactly.
    ///
    /// While a list's length times its largest magnitude is an integer `f64` holds exactly,
    /// so is every partial sum, and adding the values as `f64` in order gives the exact sum.
    /// Values are added as wrapping two's complement `bits`, which under that bound is the
    /// exact sum, and `finish` reads it. A list past the bound is added as `f64` in order.
    fn sum_exact<T: Copy>(
        &self,
        values: &[T],
        valid: &Mask,
        bits: impl Fn(T) -> u64 + Copy,
        magnitude: impl Fn(T) -> u64 + Copy,
        finish: impl Fn(u64) -> f64,
        cast: impl Fn(T) -> f64,
    ) -> (Buffer<f64>, BitBuffer) {
        const EXACT: u128 = 1 << f64::MANTISSA_DIGITS;
        let valid = Valid::new(valid);
        let mut sums = Vec::with_capacity(self.starts.len());
        let mut validity = Vec::with_capacity(self.starts.len());
        for (start, end, list_valid) in self.rows() {
            let any = list_valid && valid.any(start, end);
            let sum = if !any {
                0.0
            } else {
                let list = &values[start..end];
                let (sum, largest) = valid.wrapping_sum(list, start, bits, magnitude);
                if u128::from(largest) * list.len() as u128 <= EXACT {
                    finish(sum)
                } else {
                    sum_in_order(list, &valid, start, &cast)
                }
            };
            sums.push(sum);
            validity.push(any);
        }
        (Buffer::from(sums), BitBuffer::from_iter(validity))
    }

    /// Counts each list's valid true values, the sum of booleans cast to `Float64`.
    fn sum_bits(&self, values: &BitBuffer, valid: &Mask) -> (Buffer<f64>, BitBuffer) {
        let (trues, valid) = match valid {
            Mask::AllTrue(_) => (values.clone(), None),
            Mask::AllFalse(_) => (values.clone(), Some(BitBuffer::new_unset(values.len()))),
            Mask::Values(valid) => (
                values & valid.bit_buffer(),
                Some(valid.bit_buffer().clone()),
            ),
        };
        let mut sums = Vec::with_capacity(self.starts.len());
        let mut validity = Vec::with_capacity(self.starts.len());
        for (start, end, list_valid) in self.rows() {
            let any = list_valid
                && start < end
                && valid
                    .as_ref()
                    .is_none_or(|valid| valid.count_range(start, end) > 0);
            sums.push(if any {
                trues.count_range(start, end) as f64
            } else {
                0.0
            });
            validity.push(any);
        }
        (Buffer::from(sums), BitBuffer::from_iter(validity))
    }
}

/// An `array_sum` over `list`, of `element` evaluated on its elements when given.
pub(crate) fn list_sum_f64(list: Expression, element: Option<Expression>) -> Expression {
    ListSumF64.new_expr(ListSumF64Options { element }, [list])
}

/// Expressions over a file with their list sums replaced by columns a V2 scan computes, and
/// what the scan reads for them.
pub(crate) struct ListSums {
    /// The expressions, reading the raw columns and the sums by name.
    pub(crate) exprs: Vec<Arc<dyn PhysicalExpr>>,
    /// The raw columns the expressions read, then the sums: what the scan produces.
    pub(crate) scan: Expression,
    /// The Arrow schema of what the scan produces.
    pub(crate) schema: Schema,
}

impl ListSums {
    /// `exprs` over `schema` with every `array_sum(CAST(list AS List(Float64)))`, where `list`
    /// is a list column or `array_transform` of one by a lambda Vortex can evaluate, replaced
    /// by a column, or `None` when they hold no such sum.
    pub(crate) fn extract(
        exprs: &[Arc<dyn PhysicalExpr>],
        schema: &Schema,
        converter: &dyn ExpressionConverter,
    ) -> DFResult<Option<Self>> {
        let mut sums: Vec<(String, Expression)> = Vec::new();
        let mut rewritten = Vec::with_capacity(exprs.len());
        for expr in exprs {
            let expr = Arc::clone(expr)
                .transform_down(|node| {
                    let Some(sum) = list_sum(&node, schema, converter)? else {
                        return Ok(Transformed::no(node));
                    };
                    let name = format!("{SUM_PREFIX}{}", sums.len());
                    sums.push((name.clone(), sum));
                    let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new(&name, 0));
                    Ok(Transformed::new(column, true, TreeNodeRecursion::Jump))
                })?
                .data;
            rewritten.push(expr);
        }
        if sums.is_empty() {
            return Ok(None);
        }
        let mut indices: Vec<usize> = rewritten
            .iter()
            .flat_map(collect_columns)
            .filter(|column| !column.name().starts_with(SUM_PREFIX))
            .map(|column| column.index())
            .collect();
        indices.sort_unstable();
        indices.dedup();
        let mut fields: Vec<FieldRef> = Vec::with_capacity(indices.len() + sums.len());
        let mut columns = Vec::with_capacity(indices.len() + sums.len());
        for index in indices {
            let field = schema.fields().get(index).ok_or_else(|| {
                exec_datafusion_err!(
                    "An expression reads column {index} of a {}-column file",
                    schema.fields().len()
                )
            })?;
            columns.push((
                field.name().clone(),
                get_item(field.name().as_str(), root()),
            ));
            fields.push(Arc::clone(field));
        }
        for (name, sum) in sums {
            fields.push(Arc::new(Field::new(&name, DataType::Float64, true)));
            columns.push((name, sum));
        }
        Ok(Some(Self {
            exprs: rewritten,
            scan: pack(columns, Nullability::NonNullable),
            schema: Schema::new(fields),
        }))
    }
}

/// The names of the columns the scan computes the sums into.
const SUM_PREFIX: &str = "__vortex_list_sum_";

/// Rewrites `projection` over `schema` so a V2 scan computes its list sums, and DataFusion
/// the rest over those sums and the raw columns the rest reads. `None` when the projection
/// holds no list sum. See [`ListSums::extract`].
pub(crate) fn push_list_sums(
    projection: &ProjectionExprs,
    schema: &Schema,
    converter: &dyn ExpressionConverter,
) -> DFResult<Option<ProcessedProjection>> {
    let exprs: Vec<_> = projection.iter().map(|p| Arc::clone(&p.expr)).collect();
    let Some(sums) = ListSums::extract(&exprs, schema, converter)? else {
        return Ok(None);
    };
    let leftover = sums
        .exprs
        .into_iter()
        .zip(projection.iter())
        .map(|(expr, projected)| ProjectionExpr::new(expr, projected.alias.clone()));
    Ok(Some(ProcessedProjection {
        scan_projection: sums.scan,
        scan_reference_schema: sums.schema,
        leftover_projection: ProjectionExprs::new(leftover),
    }))
}

/// A filter holding list sums, evaluated by a V2 scan: the scan computes the sums and the raw
/// columns the filter reads, and DataFusion's own evaluator the rest of it over them, so the
/// rows it keeps are exactly those DataFusion's filter would.
pub(crate) struct ListSumRowFilter {
    projection: BoundExpression,
    target: Field,
    expr: Arc<dyn PhysicalExpr>,
    session: VortexSession,
}

impl ListSumRowFilter {
    /// A row filter for `filter` over `schema`, the file's Arrow schema, or `None` when it
    /// holds no list sum.
    pub(crate) fn try_new(
        filter: &Arc<dyn PhysicalExpr>,
        schema: &Schema,
        converter: &dyn ExpressionConverter,
        file_dtype: &DType,
        session: &VortexSession,
    ) -> DFResult<Option<Self>> {
        let Some(sums) = ListSums::extract(std::slice::from_ref(filter), schema, converter)? else {
            return Ok(None);
        };
        let projection = sums
            .scan
            .bind(file_dtype)
            .and_then(|projection| projection.optimize_recursive())
            .map_err(|e| exec_datafusion_err!("Couldn't bind list sum filter columns: {e}"))?;
        let physical =
            calculate_physical_schema(projection.dtype(), &sums.schema, &session.arrow())?;
        let [expr] = <[_; 1]>::try_from(sums.exprs)
            .map_err(|_| exec_datafusion_err!("One filter rewrites to one expression"))?;
        let expr = reassign_expr_columns(expr, &physical)?;
        Ok(Some(Self {
            projection,
            target: Field::new_struct("", physical.fields().clone(), false),
            expr,
            session: session.clone(),
        }))
    }
}

impl RowFilter for ListSumRowFilter {
    fn projection(&self) -> &BoundExpression {
        &self.projection
    }

    fn evaluate(&self, array: ArrayRef) -> VortexResult<Mask> {
        let mut ctx = self.session.create_execution_ctx();
        let columns = self
            .session
            .arrow()
            .execute_arrow(array, Some(&self.target), &mut ctx)?;
        let columns = RecordBatch::from(columns.as_struct().clone());
        let mask = self
            .expr
            .evaluate(&columns)
            .and_then(|mask| mask.into_array(columns.num_rows()))
            .map_err(|e| vortex_err!("Failed to evaluate filter {}: {e}", self.expr))?;
        let mask = mask.as_boolean();
        // A null result removes the row, as it does in a filter.
        let mask = match mask.nulls() {
            Some(nulls) => mask.values() & nulls.inner(),
            None => mask.values().clone(),
        };
        Ok(Mask::from_buffer(BitBuffer::from(mask)))
    }
}

/// The Vortex expression for `expr` when it is a list sum this function evaluates.
fn list_sum(
    expr: &Arc<dyn PhysicalExpr>,
    schema: &Schema,
    converter: &dyn ExpressionConverter,
) -> DFResult<Option<Expression>> {
    let Some(function) = expr.downcast_ref::<ScalarFunctionExpr>() else {
        return Ok(None);
    };
    let [arg] = function.args() else {
        return Ok(None);
    };
    if function.fun().name() != "array_sum" {
        return Ok(None);
    }
    // array_sum coerces its list to Float64 elements; the sum casts each value the same way.
    let list = match arg.downcast_ref::<CastExpr>() {
        Some(cast) if matches!(cast.cast_type(), DataType::List(field) if field.data_type() == &DataType::Float64) => {
            cast.expr()
        }
        _ => arg,
    };
    if let Some(column) = list.downcast_ref::<Column>() {
        let Some(element) = element_field(column, schema) else {
            return Ok(None);
        };
        if !summable_arrow(element.data_type()) {
            return Ok(None);
        }
        return Ok(Some(list_sum_f64(get_item(column.name(), root()), None)));
    }
    let Some(transform) = list.downcast_ref::<HigherOrderFunctionExpr>() else {
        return Ok(None);
    };
    let [column, lambda] = transform.args() else {
        return Ok(None);
    };
    if transform.name() != "array_transform" {
        return Ok(None);
    }
    let (Some(column), Some(lambda)) = (
        column.downcast_ref::<Column>(),
        lambda.downcast_ref::<LambdaExpr>(),
    ) else {
        return Ok(None);
    };
    let Some(element) = element_field(column, schema) else {
        return Ok(None);
    };
    if lambda.params().len() != 1 {
        return Ok(None);
    }
    // The lambda's element becomes the one field of a struct the body is converted over.
    let element_schema = Schema::new(vec![Field::new(
        ELEMENT,
        element.data_type().clone(),
        element.is_nullable(),
    )]);
    let mut outer = false;
    let body = Arc::clone(lambda.body())
        .transform_up(|node| {
            if node.downcast_ref::<LambdaVariable>().is_some() {
                let column: Arc<dyn PhysicalExpr> = Arc::new(Column::new(ELEMENT, 0));
                return Ok(Transformed::yes(column));
            }
            outer |= node.downcast_ref::<Column>().is_some();
            Ok(Transformed::no(node))
        })?
        .data;
    if outer || !summable_arrow(&body.data_type(&element_schema)?) {
        return Ok(None);
    }
    let Some(body) = converter.try_convert(&body, &element_schema)? else {
        return Ok(None);
    };
    Ok(Some(list_sum_f64(
        get_item(column.name(), root()),
        Some(body),
    )))
}

/// The element field of list column `column`.
fn element_field<'a>(column: &Column, schema: &'a Schema) -> Option<&'a FieldRef> {
    match schema.fields().get(column.index())?.data_type() {
        DataType::List(element) | DataType::LargeList(element) => Some(element),
        _ => None,
    }
}

/// Whether DataFusion casts values of `dtype` to `Float64` as [`ListSumF64`] does.
fn summable_arrow(dtype: &DataType) -> bool {
    matches!(
        dtype,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
    )
}

/// The valid values among elements, read without bounds checks per value.
enum Valid<'a> {
    All,
    None,
    Some {
        bytes: &'a [u8],
        offset: usize,
        bits: &'a BitBuffer,
    },
}

impl<'a> Valid<'a> {
    fn new(mask: &'a Mask) -> Self {
        match mask {
            Mask::AllTrue(_) => Valid::All,
            Mask::AllFalse(_) => Valid::None,
            Mask::Values(values) => {
                let bits = values.bit_buffer();
                Valid::Some {
                    bytes: bits.inner().as_slice(),
                    offset: bits.offset(),
                    bits,
                }
            }
        }
    }

    #[inline]
    fn get(&self, index: usize) -> bool {
        match self {
            Valid::All => true,
            Valid::None => false,
            Valid::Some { bytes, offset, .. } => {
                let bit = index + offset;
                (bytes[bit >> 3] >> (bit & 7)) & 1 == 1
            }
        }
    }

    /// The valid bits of the 64 elements from `index`, low bit first, zero past the end.
    #[inline]
    #[expect(
        clippy::cast_possible_truncation,
        reason = "keeps the 64 bits from the element's, dropping those past them"
    )]
    fn word(&self, index: usize) -> u64 {
        match self {
            Valid::All => u64::MAX,
            Valid::None => 0,
            Valid::Some { bytes, offset, .. } => {
                let bit = index + offset;
                let byte = bit >> 3;
                let mut word = [0u8; 16];
                let available = bytes.len().saturating_sub(byte).min(16);
                word[..available].copy_from_slice(&bytes[byte..byte + available]);
                (u128::from_le_bytes(word) >> (bit & 7)) as u64
            }
        }
    }

    /// The sum of the valid elements of `values`, the elements from `start`, masked 64 at a
    /// time without branching.
    fn sum_unsigned<T: Copy>(&self, values: &[T], start: usize, widen: impl Fn(T) -> u64) -> u128 {
        let mut sum = 0u128;
        let (chunks, remainder) = values.as_chunks::<64>();
        let mut index = start;
        for chunk in chunks {
            let word = self.word(index);
            let mut block = 0u128;
            for (lane, &value) in chunk.iter().enumerate() {
                block += u128::from(widen(value) & 0u64.wrapping_sub((word >> lane) & 1));
            }
            sum += block;
            index += 64;
        }
        let word = self.word(index);
        for (lane, &value) in remainder.iter().enumerate() {
            sum += u128::from(widen(value) & 0u64.wrapping_sub((word >> lane) & 1));
        }
        sum
    }

    /// The wrapping sum of the `bits` of the valid elements of `values`, the elements from
    /// `start`, and their largest `magnitude`. Invalid elements are masked out 64 at a time,
    /// without branching, so the additions vectorize.
    fn wrapping_sum<T: Copy>(
        &self,
        values: &[T],
        start: usize,
        bits: impl Fn(T) -> u64,
        magnitude: impl Fn(T) -> u64,
    ) -> (u64, u64) {
        let mut sum = 0u64;
        let mut largest = 0u64;
        let (chunks, remainder) = values.as_chunks::<64>();
        let mut index = start;
        for chunk in chunks {
            let word = self.word(index);
            for (lane, &value) in chunk.iter().enumerate() {
                let keep = 0u64.wrapping_sub((word >> lane) & 1);
                sum = sum.wrapping_add(bits(value) & keep);
                largest = largest.max(magnitude(value) & keep);
            }
            index += 64;
        }
        let word = self.word(index);
        for (lane, &value) in remainder.iter().enumerate() {
            let keep = 0u64.wrapping_sub((word >> lane) & 1);
            sum = sum.wrapping_add(bits(value) & keep);
            largest = largest.max(magnitude(value) & keep);
        }
        (sum, largest)
    }

    /// Whether any of `start..end` is valid.
    fn any(&self, start: usize, end: usize) -> bool {
        match self {
            Valid::All => start < end,
            Valid::None => false,
            Valid::Some { bits, .. } => bits.count_range(start, end) > 0,
        }
    }
}

/// Adds the valid values of `values`, the elements from `start`, as `f64` in order.
fn sum_in_order<T: Copy>(
    values: &[T],
    valid: &Valid<'_>,
    start: usize,
    cast: &impl Fn(T) -> f64,
) -> f64 {
    match valid {
        Valid::All => values.iter().fold(0.0, |sum, &value| sum + cast(value)),
        _ => values.iter().enumerate().fold(0.0, |sum, (index, &value)| {
            if valid.get(start + index) {
                sum + cast(value)
            } else {
                sum
            }
        }),
    }
}
