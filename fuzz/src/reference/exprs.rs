// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Type-directed synthesis of well-typed expression trees, checked against the reference.
//!
//! An [`Expr`] is a tree of [`ScalarOp`]s whose operands are themselves trees, rooted at the
//! scope (the generated array). Trees are synthesized from the same [`ScalarOpKind`] registry as
//! single ops: [`Synthesizer::grow`] applies a random kind to a subtree, and the kind asks for
//! its operands by dtype, which [`Synthesizer::synth`] produces backwards from the target dtype
//! (a column or literal of that dtype, or a grown subtree that returns it). Every node is type
//! checked by Vortex as it is built.
//!
//! The reference evaluates a tree bottom-up over plain values with each op's reference, so the
//! oracle is independent of Vortex. The tree is then evaluated by Vortex in memory, after
//! expression optimization, and through a file scan with the tree as projection and filter.

use std::cell::Cell;

use arbitrary::Arbitrary;
use arbitrary::Error::IncorrectFormat;
use arbitrary::Unstructured;
use strum::IntoEnumIterator;
use vortex_array::ArrayRef;
use vortex_array::ExecutionCtx;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::StructArray;
use vortex_array::dtype::DType;
use vortex_array::dtype::FieldNames;
use vortex_array::dtype::Nullability;
use vortex_array::expr::Expression;
use vortex_array::expr::lit;
use vortex_array::scalar::Scalar;
use vortex_array::scalar_fn::ScalarFnRef;
use vortex_array::validity::Validity;
use vortex_error::VortexResult;
use vortex_error::vortex_err;

use super::check_values;
use super::decode;
use super::encodings::Case;
use super::encodings::EncodingKind;
use super::encodings::Limits;
use super::encodings::arb_leaf_dtype;
use super::encodings::buildable;
use super::encodings::is_leaf;
use super::encodings::operand;
use super::err;
use super::generate::Shape;
use super::generate::gen_one;
use super::scalar_fns::Expected;
use super::scalar_fns::NodeBuilder;
use super::scalar_fns::Operand;
use super::scalar_fns::OperandGen;
use super::scalar_fns::ScalarOp;
use super::scalar_fns::ScalarOpKind;
use super::value::Value;
use super::value::to_scalar;
use crate::SESSION;
use crate::error::VortexFuzzResult;

/// Maximum depth of synthesized trees.
const MAX_DEPTH: usize = 3;
/// Maximum number of scalar function nodes in one tree.
const MAX_NODES: usize = 32;

/// Whether `kind` can appear in an expression tree.
///
/// `stat` reads statistics of the whole input, so it is not a row-wise function: its value
/// changes when a file scan filters rows before projecting.
pub fn in_trees(kind: ScalarOpKind) -> bool {
    kind != ScalarOpKind::Stat
}

/// An expression tree over the scope.
#[derive(Debug, Clone)]
pub enum Expr {
    /// The scope itself.
    Root,
    /// A scalar function applied to `input` and the operands inside `op`.
    Apply {
        input: Box<Expr>,
        op: Box<ScalarOp<Expr>>,
        /// The dtype Vortex derives for this node.
        dtype: DType,
    },
}

impl Expr {
    /// This tree's dtype over `scope`.
    pub fn dtype<'a>(&'a self, scope: &'a DType) -> &'a DType {
        match self {
            Expr::Root => scope,
            Expr::Apply { dtype, .. } => dtype,
        }
    }

    /// The Vortex expression for this tree.
    pub fn to_expression(&self, scope: &DType) -> VortexResult<Expression> {
        match self {
            Expr::Root => Ok(Expression::Root),
            Expr::Apply { input, op, .. } => op.build(
                input.to_expression(scope)?,
                input.dtype(scope),
                &ExprBuilder { scope },
            ),
        }
    }

    /// Evaluate this tree with the reference over the scope's values.
    pub fn evaluate(&self, scope: &Evaluated) -> Result<Evaluated, Unknown> {
        match self {
            Expr::Root => Ok(scope.clone()),
            Expr::Apply { input, op, dtype } => {
                let input = input.evaluate(scope)?;
                let op = op.try_map_operands(|o| o.evaluate(scope))?;
                match op.reference(&input.values, &input.dtype) {
                    Expected::Values(values) => Ok(Evaluated {
                        dtype: dtype.clone(),
                        values,
                    }),
                    Expected::Error(reason) => Err(Unknown::Error(reason)),
                    Expected::AnyOf(_) => Err(Unknown::Ambiguous),
                }
            }
        }
    }

    /// The kinds of every node in this tree.
    pub fn kinds(&self, out: &mut Vec<ScalarOpKind>) {
        if let Expr::Apply { input, op, .. } = self {
            out.push(op.kind());
            input.kinds(out);
            // Operands are visited through a map that never fails.
            op.try_map_operands(|o| {
                o.kinds(out);
                Ok::<_, ()>(())
            })
            .ok();
        }
    }
}

/// A tree's reference result that cannot be compared.
#[derive(Debug)]
pub enum Unknown {
    /// The reference expects an error. Vortex may legitimately skip the failing subtree (e.g.
    /// when a simplification drops it, or a scan filters the failing rows), so either outcome
    /// is accepted.
    Error(String),
    /// More than one result is acceptable.
    Ambiguous,
}

/// The reference result of a (sub)tree.
#[derive(Debug, Clone)]
pub struct Evaluated {
    pub dtype: DType,
    pub values: Vec<Value>,
}

impl Operand for Evaluated {
    fn dtype(&self) -> &DType {
        &self.dtype
    }

    fn values(&self) -> &[Value] {
        &self.values
    }
}

/// Builds Vortex expressions from trees.
struct ExprBuilder<'a> {
    scope: &'a DType,
}

impl NodeBuilder for ExprBuilder<'_> {
    type Operand = Expr;
    type Node = Expression;

    fn operand(&self, operand: &Expr) -> VortexResult<Expression> {
        operand.to_expression(self.scope)
    }

    fn constant(&self, scalar: Scalar) -> VortexResult<Expression> {
        Ok(lit(scalar))
    }

    fn prepared_set(&self, list: Scalar) -> VortexResult<Expression> {
        Ok(lit(list))
    }

    fn apply(&self, f: ScalarFnRef, children: Vec<Expression>) -> VortexResult<Expression> {
        Expression::try_new(f, children)
    }
}

/// Synthesizes expression trees over a scope.
pub struct Synthesizer<'a> {
    scope: &'a DType,
    /// The scope and every (nested) struct field reachable from it.
    columns: Vec<(Expr, DType)>,
    /// Remaining scalar function nodes.
    budget: Cell<usize>,
    kinds: Vec<ScalarOpKind>,
}

impl<'a> Synthesizer<'a> {
    pub fn new(scope: &'a DType) -> Self {
        let mut columns = Vec::new();
        collect_columns(Expr::Root, scope, scope, &mut columns);
        Self {
            scope,
            columns,
            budget: Cell::new(MAX_NODES),
            kinds: ScalarOpKind::iter().filter(|k| in_trees(*k)).collect(),
        }
    }

    /// A tree of any dtype.
    pub fn tree(&self, u: &mut Unstructured<'_>) -> arbitrary::Result<Expr> {
        let depth = u.int_in_range(1..=MAX_DEPTH)?;
        Ok(self.grow(u, depth)?.0)
    }

    /// A tree of exactly `target`.
    pub fn tree_of(&self, u: &mut Unstructured<'_>, target: &DType) -> arbitrary::Result<Expr> {
        let depth = u.int_in_range(1..=MAX_DEPTH)?;
        self.synth(u, target, depth)
    }

    /// A leaf, then up to two random ops applied on top, each with synthesized operands.
    pub fn grow(&self, u: &mut Unstructured<'_>, depth: usize) -> arbitrary::Result<(Expr, DType)> {
        let (mut expr, mut dtype) = if u.ratio(5, 6)? {
            u.choose(&self.columns)?.clone()
        } else {
            let dtype = arb_leaf_dtype(u)?;
            (self.literal(u, &dtype)?, dtype)
        };
        if depth == 0 {
            return Ok((expr, dtype));
        }
        for _ in 0..u.int_in_range(1..=3)? {
            // Most kinds only apply to some dtypes, so try a few.
            for _ in 0..8 {
                if self.budget.get() == 0 || u.is_empty() {
                    return Ok((expr, dtype));
                }
                let kind = *u.choose(&self.kinds)?;
                let mut operands = TreeOperands {
                    synth: self,
                    depth: depth - 1,
                };
                let Ok(op) = kind.generate_with(u, &dtype, &mut operands) else {
                    continue;
                };
                if let Ok(node) = self.apply(expr.clone(), op) {
                    dtype = node.dtype(self.scope).clone();
                    expr = node;
                    break;
                }
            }
        }
        Ok((expr, dtype))
    }

    /// A tree of exactly `target`: a grown subtree that returns it (up to nullability), or a
    /// column or literal of it.
    pub fn synth(
        &self,
        u: &mut Unstructured<'_>,
        target: &DType,
        depth: usize,
    ) -> arbitrary::Result<Expr> {
        if depth > 0 && self.budget.get() > 0 && u.ratio(2, 3)? {
            for _ in 0..3 {
                let (expr, dtype) = self.grow(u, depth)?;
                if dtype.eq_ignore_nullability(target)
                    && let Ok(expr) = self.adapt(u, expr, &dtype, target)
                {
                    return Ok(expr);
                }
            }
        }
        let matching: Vec<_> = self
            .columns
            .iter()
            .filter(|(_, d)| d.eq_ignore_nullability(target))
            .collect();
        if !matching.is_empty() && u.ratio(3, 4)? {
            let (expr, dtype) = (*u.choose(&matching)?).clone();
            return self.adapt(u, expr, &dtype, target);
        }
        self.literal(u, target)
    }

    /// A literal of `dtype`.
    fn literal(&self, u: &mut Unstructured<'_>, dtype: &DType) -> arbitrary::Result<Expr> {
        let value = literal_value(u, dtype)?;
        self.constant(dtype, value)
    }

    fn constant(&self, dtype: &DType, value: Value) -> arbitrary::Result<Expr> {
        to_scalar(dtype, &value).map_err(|_| IncorrectFormat)?;
        Ok(Expr::Apply {
            input: Box::new(Expr::Root),
            op: Box::new(ScalarOp::Literal(dtype.clone(), value)),
            dtype: dtype.clone(),
        })
    }

    /// Change the nullability of `expr` from `from` to `to`, which are otherwise equal.
    fn adapt(
        &self,
        u: &mut Unstructured<'_>,
        expr: Expr,
        from: &DType,
        to: &DType,
    ) -> arbitrary::Result<Expr> {
        if from == to {
            return Ok(expr);
        }
        // TODO: union and variant arrays cannot be cast, and fill_null has no kernel for
        // non-leaf dtypes (e.g. extensions).
        let op = if to.is_nullable() && buildable(to) {
            ScalarOp::Cast(to.clone())
        } else if !to.is_nullable() && is_leaf(to) {
            ScalarOp::FillNull(to.clone(), gen_one(u, to, Shape::Random, 0)?)
        } else {
            return Err(IncorrectFormat);
        };
        let node = self.apply(expr, op)?;
        if node.dtype(self.scope) == to {
            Ok(node)
        } else {
            Err(IncorrectFormat)
        }
    }

    /// Apply `op` to `input`, letting Vortex type check the node.
    fn apply(&self, input: Expr, op: ScalarOp<Expr>) -> arbitrary::Result<Expr> {
        let budget = self.budget.get().checked_sub(1).ok_or(IncorrectFormat)?;
        let mut node = Expr::Apply {
            input: Box::new(input),
            op: Box::new(op),
            dtype: DType::Null,
        };
        let dtype = node
            .to_expression(self.scope)
            .and_then(|e| e.return_dtype(self.scope))
            .map_err(|_| IncorrectFormat)?;
        if let Expr::Apply { dtype: d, .. } = &mut node {
            *d = dtype;
        }
        self.budget.set(budget);
        Ok(node)
    }
}

/// A value of `dtype`, null with some probability when nullable.
fn literal_value(u: &mut Unstructured<'_>, dtype: &DType) -> arbitrary::Result<Value> {
    if dtype.is_nullable() && u.ratio(1, 6)? {
        return Ok(Value::Null);
    }
    match dtype {
        DType::Struct(fields, _) => Ok(Value::Tuple(
            fields
                .fields()
                .map(|f| literal_value(u, &f))
                .collect::<arbitrary::Result<_>>()?,
        )),
        _ => gen_one(u, dtype, Shape::Random, 0),
    }
}

fn collect_columns(expr: Expr, dtype: &DType, scope: &DType, out: &mut Vec<(Expr, DType)>) {
    out.push((expr.clone(), dtype.clone()));
    let DType::Struct(fields, _) = dtype else {
        return;
    };
    for idx in 0..fields.nfields() {
        let node = Expr::Apply {
            input: Box::new(expr.clone()),
            op: Box::new(ScalarOp::GetItem(idx)),
            dtype: DType::Null,
        };
        let Ok(field_dtype) = node
            .to_expression(scope)
            .and_then(|e| e.return_dtype(scope))
        else {
            continue;
        };
        let Expr::Apply { input, op, .. } = node else {
            unreachable!()
        };
        collect_columns(
            Expr::Apply {
                input,
                op,
                dtype: field_dtype.clone(),
            },
            &field_dtype,
            scope,
            out,
        );
    }
}

/// Operands as synthesized subtrees.
struct TreeOperands<'s, 'a> {
    synth: &'s Synthesizer<'a>,
    depth: usize,
}

impl OperandGen for TreeOperands<'_, '_> {
    type Operand = Expr;

    fn operand(&mut self, u: &mut Unstructured<'_>, dtype: &DType) -> arbitrary::Result<Expr> {
        self.synth.synth(u, dtype, self.depth)
    }

    fn constant(&mut self, dtype: &DType, value: Value) -> arbitrary::Result<Expr> {
        self.synth.constant(dtype, value)
    }

    fn sample(&mut self, _u: &mut Unstructured<'_>) -> arbitrary::Result<Option<Value>> {
        Ok(None)
    }
}

/// Generate a scope: usually a table of columns in random encodings, otherwise any case.
pub fn gen_scope(u: &mut Unstructured<'_>, ctx: &mut ExecutionCtx) -> arbitrary::Result<Case> {
    let kinds = EncodingKind::iter().collect::<Vec<_>>();
    if u.ratio(1, 4)? {
        let kind = *u.choose(&kinds)?;
        return kind.generate(u, None, Limits::default(), ctx);
    }
    let first = u
        .choose(&kinds)?
        .generate(u, None, Limits::default(), ctx)?;
    let len = first.values.len();
    let mut columns = vec![first];
    for _ in 0..u.int_in_range(0..=3)? {
        let dtype = u.choose(&kinds)?.pick_dtype(u)?;
        if let Ok(case) = operand(u, &dtype, len, ctx) {
            columns.push(case);
        }
    }
    let names: FieldNames = (0..columns.len()).map(|i| format!("c{i}")).collect();
    let array = StructArray::try_new(
        names,
        columns.iter().map(|c| c.array.clone()),
        len,
        Validity::NonNullable,
    )
    .map_err(|_| IncorrectFormat)?
    .into_array();
    let values = (0..len)
        .map(|i| Value::Tuple(columns.iter().map(|c| c.values[i].clone()).collect()))
        .collect();
    Ok(Case {
        kind: EncodingKind::Struct,
        dtype: array.dtype().clone(),
        values,
        array,
    })
}

/// Fuzz input: a scope and expression trees evaluated over it.
#[derive(Debug)]
pub struct FuzzExprReference {
    pub scope: Case,
    pub actions: Vec<ExprAction>,
}

/// How a tree is evaluated by Vortex.
#[derive(Debug)]
pub enum ExprAction {
    /// Applied to the scope in memory, as built and after expression optimization.
    Project(Expr),
    /// Through a file holding the scope, with an optional filter tree.
    Scan {
        projection: Expr,
        filter: Option<Expr>,
        compact: bool,
        layout_reader_cache: bool,
    },
}

impl<'a> Arbitrary<'a> for FuzzExprReference {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let mut ctx = SESSION.create_execution_ctx();
        let scope = gen_scope(u, &mut ctx)?;
        let synth = Synthesizer::new(&scope.dtype);
        let mut actions = Vec::new();
        for _ in 0..u.int_in_range(1..=3)? {
            synth.budget.set(MAX_NODES);
            let action = if cfg!(not(target_arch = "wasm32")) && u.ratio(1, 3)? {
                let filter = if u.arbitrary()? {
                    let n: Nullability = u.arbitrary()?;
                    Some(synth.tree_of(u, &DType::Bool(n))?)
                } else {
                    None
                };
                synth.budget.set(MAX_NODES);
                ExprAction::Scan {
                    projection: synth.tree(u)?,
                    filter,
                    compact: u.arbitrary()?,
                    layout_reader_cache: u.arbitrary()?,
                }
            } else {
                ExprAction::Project(synth.tree(u)?)
            };
            actions.push(action);
        }
        Ok(Self { scope, actions })
    }
}

/// Run the expression reference fuzzer. Returns `Ok(true)` to keep the input in the corpus.
#[expect(clippy::result_large_err)]
pub fn run_expr_reference(fuzz: FuzzExprReference) -> VortexFuzzResult<bool> {
    let FuzzExprReference { scope, actions } = fuzz;
    let mut ctx = SESSION.create_execution_ctx();
    let scope_values = Evaluated {
        dtype: scope.dtype.clone(),
        values: scope.values.clone(),
    };
    for (step, action) in actions.iter().enumerate() {
        let label = format!("step {step}");
        let result = match action {
            ExprAction::Project(tree) => check_project(&scope, &scope_values, tree, &mut ctx),
            #[cfg(not(target_arch = "wasm32"))]
            ExprAction::Scan {
                projection,
                filter,
                compact,
                layout_reader_cache,
            } => scan::check_scan(
                &scope,
                &scope_values,
                projection,
                filter.as_ref(),
                *compact,
                *layout_reader_cache,
                &mut ctx,
            ),
            #[cfg(target_arch = "wasm32")]
            ExprAction::Scan { .. } => Ok(()),
        };
        result.map_err(|e| {
            err(vortex_err!(
                "{label}: {e}\naction: {action:?}\nscope:\n{}",
                scope.array.display_tree()
            ))
        })?;
    }
    Ok(true)
}

/// Check one Vortex result of `tree` against the reference.
fn check_result(
    label: &str,
    expected: &Result<Evaluated, Unknown>,
    actual: VortexResult<ArrayRef>,
    ctx: &mut ExecutionCtx,
) -> VortexResult<()> {
    let expected = match expected {
        Ok(expected) => expected,
        Err(_) => return Ok(()),
    };
    let actual = actual.map_err(|e| vortex_err!("{label}: {e}"))?;
    if actual.dtype() != &expected.dtype {
        return Err(vortex_err!(
            "{label}: dtype {} does not match the expression's return dtype {}",
            actual.dtype(),
            expected.dtype
        ));
    }
    let values = decode(&actual, ctx).map_err(|e| vortex_err!("{label}: {e}"))?;
    check_values(&values, &expected.values, label)
        .map_err(|e| vortex_err!("{e}\nresult:\n{}", actual.display_tree()))
}

fn check_project(
    scope: &Case,
    scope_values: &Evaluated,
    tree: &Expr,
    ctx: &mut ExecutionCtx,
) -> VortexResult<()> {
    let expected = tree.evaluate(scope_values);
    let expr = tree.to_expression(&scope.dtype)?;
    check_result(
        &format!("apply {expr}"),
        &expected,
        scope.array.clone().apply(&expr),
        ctx,
    )?;
    let optimized = expr.bind(&scope.dtype).and_then(|e| e.optimize_recursive());
    let label = match &optimized {
        Ok(e) => format!("apply optimized {e}"),
        Err(_) => format!("optimize {expr}"),
    };
    check_result(
        &label,
        &expected,
        optimized.and_then(|e| scope.array.clone().apply_bound(&e)),
        ctx,
    )
}

#[cfg(not(target_arch = "wasm32"))]
mod scan {
    use itertools::Itertools;
    use vortex_array::ArrayRef;
    use vortex_array::Canonical;
    use vortex_array::ExecutionCtx;
    use vortex_array::IntoArray;
    use vortex_array::arrays::ChunkedArray;
    use vortex_array::dtype::DType;
    use vortex_array::dtype::Nullability;
    use vortex_btrblocks::BtrBlocksCompressorBuilder;
    use vortex_error::VortexResult;
    use vortex_error::vortex_err;
    use vortex_file::OpenOptionsSessionExt;
    use vortex_file::WriteOptionsSessionExt;
    use vortex_file::WriteStrategyBuilder;

    use super::Evaluated;
    use super::Expr;
    use super::Unknown;
    use super::check_result;
    use crate::RUNTIME;
    use crate::SESSION;
    use crate::reference::Case;
    use crate::reference::Value;

    /// Write the scope to a file and scan it with `projection` and `filter` pushed down.
    pub(super) fn check_scan(
        scope: &Case,
        scope_values: &Evaluated,
        projection: &Expr,
        filter: Option<&Expr>,
        compact: bool,
        layout_reader_cache: bool,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<()> {
        if !writable(&scope.dtype) {
            return Ok(());
        }
        let expected = expected_scan(scope_values, projection, filter);

        let write_options = if compact {
            SESSION.write_options().with_strategy(
                WriteStrategyBuilder::from_session(&SESSION)
                    .with_btrblocks_builder(
                        BtrBlocksCompressorBuilder::from_session(&SESSION).with_compact(),
                    )
                    .build(),
            )
        } else {
            SESSION.write_options()
        };
        let mut buf = Vec::new();
        write_options
            .blocking(&*RUNTIME)
            .write(&mut buf, scope.array.to_array_iterator())
            .map_err(|e| vortex_err!("write: {e}"))?;
        let open_options = if layout_reader_cache {
            SESSION.open_options().with_layout_reader_cache()
        } else {
            SESSION.open_options()
        };
        let file = open_options
            .open_buffer(buf)
            .map_err(|e| vortex_err!("open: {e}"))?;

        let projection_expr = projection.to_expression(&scope.dtype)?;
        let filter_expr = filter.map(|f| f.to_expression(&scope.dtype)).transpose()?;
        let label = format!(
            "scan projection {projection_expr} filter {}",
            filter_expr
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_default()
        );
        let scan = || -> VortexResult<_> {
            let projection = projection_expr
                .bind(file.dtype())
                .and_then(|e| e.optimize_recursive())?;
            let filter = filter_expr
                .as_ref()
                .map(|f| f.bind(file.dtype()).and_then(|e| e.optimize_recursive()))
                .transpose()?;
            let dtype = projection.dtype().clone();
            let chunks: Vec<_> = file
                .scan()?
                .with_projection(projection)
                .with_some_filter(filter)
                .into_array_iter(&*RUNTIME)?
                .try_collect()?;
            Ok(concat(chunks, &dtype))
        };
        check_result(&label, &expected, scan(), ctx)?;
        if layout_reader_cache {
            // A second scan reuses the cached `LayoutReader`.
            check_result(&format!("{label} (cached)"), &expected, scan(), ctx)?;
        }
        Ok(())
    }

    /// The reference result of a scan: the projection over the rows the filter keeps.
    fn expected_scan(
        scope: &Evaluated,
        projection: &Expr,
        filter: Option<&Expr>,
    ) -> Result<Evaluated, Unknown> {
        let projected = projection.evaluate(scope)?;
        let Some(filter) = filter else {
            return Ok(projected);
        };
        let keep = filter.evaluate(scope)?;
        Ok(Evaluated {
            dtype: projected.dtype,
            values: projected
                .values
                .into_iter()
                .zip(keep.values)
                .filter(|(_, k)| *k == Value::Bool(true))
                .map(|(v, _)| v)
                .collect(),
        })
    }

    fn concat(mut chunks: Vec<ArrayRef>, dtype: &DType) -> ArrayRef {
        match chunks.len() {
            0 => Canonical::empty(dtype).into_array(),
            1 => chunks.remove(0),
            _ => ChunkedArray::from_iter(chunks).into_array(),
        }
    }

    /// Files hold struct columns only when they are non-nullable and uniquely named.
    // TODO: nullable structs and duplicate field names do not round trip through files, and
    // union and variant arrays have no builders for file statistics.
    fn writable(dtype: &DType) -> bool {
        match dtype {
            DType::Union(..) | DType::Variant(_) => false,
            DType::Struct(fields, n) => {
                let names = fields.names();
                *n == Nullability::NonNullable
                    && names.iter().all_unique()
                    && fields.fields().all(|f| writable(&f))
            }
            DType::List(e, _) | DType::FixedSizeList(e, ..) => writable(e),
            _ => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use strum::IntoEnumIterator;
    use vortex_utils::aliases::hash_set::HashSet;

    use super::*;
    use crate::reference::tests::panic_message;
    use crate::reference::tests::test_bytes;

    fn seeds(default: u64) -> u64 {
        std::env::var("EXPR_SMOKE_SEEDS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(default)
    }

    /// Run `seeds` deterministic inputs, returning the failures and the kinds in passing trees.
    fn sweep(seeds: u64) -> (Vec<String>, HashSet<ScalarOpKind>) {
        let mut failures = Vec::new();
        let mut kinds = HashSet::default();
        for seed in 0..seeds {
            let data = test_bytes(seed);
            let generated = std::panic::catch_unwind(|| {
                FuzzExprReference::arbitrary(&mut Unstructured::new(&data))
            });
            let input = match generated {
                Ok(Ok(input)) => input,
                Ok(Err(_)) => continue,
                Err(e) => {
                    failures.push(format!(
                        "seed {seed}: generation panicked: {}",
                        panic_message(&e)
                    ));
                    continue;
                }
            };
            let mut input_kinds = Vec::new();
            for action in &input.actions {
                match action {
                    ExprAction::Project(tree) => tree.kinds(&mut input_kinds),
                    ExprAction::Scan {
                        projection, filter, ..
                    } => {
                        projection.kinds(&mut input_kinds);
                        if let Some(f) = filter {
                            f.kinds(&mut input_kinds);
                        }
                    }
                }
            }
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                run_expr_reference(input).map_err(|e| e.to_string())
            }));
            match result {
                Ok(Ok(_)) => kinds.extend(input_kinds),
                Ok(Err(e)) => failures.push(format!("seed {seed}: {e}")),
                Err(e) => failures.push(format!("seed {seed}: panicked: {}", panic_message(&e))),
            }
        }
        (failures, kinds)
    }

    /// Every kind allowed in trees must appear in some synthesized tree that agrees with the
    /// reference.
    #[test]
    fn every_tree_kind_is_synthesized() {
        let (failures, kinds) = sweep(seeds(2000));
        let missing: Vec<_> = ScalarOpKind::iter()
            .filter(|k| in_trees(*k) && !kinds.contains(k))
            .collect();
        assert!(
            failures.is_empty() && missing.is_empty(),
            "kinds never synthesized: {missing:?}\n\n{}",
            failures.join("\n\n")
        );
    }
}
