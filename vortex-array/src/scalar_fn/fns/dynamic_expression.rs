// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use parking_lot::Mutex;
use vortex_error::VortexResult;
use vortex_error::vortex_ensure;
use vortex_session::VortexSession;
use vortex_session::registry::CachedId;

use crate::ArrayRef;
use crate::ExecutionCtx;
use crate::IntoArray;
use crate::aggregate_fn::AggregateFnRef;
use crate::aggregate_fn::AggregateFnVTableExt;
use crate::aggregate_fn::EmptyOptions as AggregateEmptyOptions;
use crate::aggregate_fn::fns::all_nan::AllNan;
use crate::aggregate_fn::fns::all_non_nan::AllNonNan;
use crate::aggregate_fn::fns::all_non_null::AllNonNull;
use crate::aggregate_fn::fns::all_null::AllNull;
use crate::arrays::StructArray;
use crate::builtins::ArrayBuiltins;
use crate::dtype::DType;
use crate::dtype::FieldName;
use crate::dtype::FieldNames;
use crate::dtype::Nullability;
use crate::dtype::StructFields;
use crate::expr::BoundExpression;
use crate::expr::bound::get_item;
use crate::expr::bound::lit;
use crate::expr::display::ExprDisplay;
use crate::expr::stats::Stat;
use crate::scalar_fn::Arity;
use crate::scalar_fn::ChildName;
use crate::scalar_fn::EmptyOptions;
use crate::scalar_fn::ExecutionArgs;
use crate::scalar_fn::ScalarFnId;
use crate::scalar_fn::ScalarFnVTable;
use crate::scalar_fn::ScalarFnVTableExt;
use crate::scalar_fn::fns::get_item::GetItem;
use crate::scalar_fn::fns::stat::StatFn;
use crate::scalar_fn::internal::row_count::RowCount;
use crate::stats::bound::stat;
use crate::validity::Validity;

/// A source of a predicate that can change while a query runs, such as the build-side bounds of
/// a hash join or the threshold of a top-N operator in a compute engine.
pub trait DynamicExpressionSource: Debug + Display + Send + Sync + 'static {
    /// A counter that changes whenever [`Self::current`] can return a different predicate.
    fn generation(&self) -> u64;

    /// The current predicate, bound against `scope`.
    ///
    /// The scope is a struct whose field `i`, named by [`input_field_name`], holds the child `i`
    /// of the dynamic expression. The predicate must return a boolean.
    fn current(&self, scope: &DType) -> VortexResult<BoundExpression>;
}

/// The name of the scope field that holds child `idx` of a dynamic expression.
pub fn input_field_name(idx: usize) -> FieldName {
    FieldName::from(idx.to_string())
}

/// A predicate that a [`DynamicExpressionSource`] can replace while a query runs.
///
/// The children of the expression are the inputs that every predicate of the source can read, so
/// the columns a scan must read are known before the source has its first predicate. Each
/// evaluation reads the current predicate of the source.
#[derive(Clone)]
pub struct DynamicExpression;

impl ScalarFnVTable for DynamicExpression {
    type Options = DynamicExpressionOptions;

    fn id(&self) -> ScalarFnId {
        static ID: CachedId = CachedId::new("vortex.dynamic_expression");
        *ID
    }

    fn arity(&self, _options: &Self::Options) -> Arity {
        Arity::Variadic { min: 0, max: None }
    }

    fn child_name(&self, _options: &Self::Options, child_idx: usize) -> ChildName {
        ChildName::from(Arc::from(input_field_name(child_idx).as_ref()))
    }

    fn fmt_sql(
        &self,
        options: &Self::Options,
        expr: &dyn ExprDisplay,
        f: &mut Formatter<'_>,
    ) -> std::fmt::Result {
        write!(f, "dynamic({}", options.source)?;
        for idx in 0..expr.display_children_count() {
            write!(f, ", ")?;
            Display::fmt(expr.display_child(idx), f)?;
        }
        write!(f, ")")
    }

    fn return_dtype(&self, _options: &Self::Options, _arg_dtypes: &[DType]) -> VortexResult<DType> {
        // The predicate can change, so the dtype must cover every predicate of the source.
        Ok(DType::Bool(Nullability::Nullable))
    }

    fn execute(
        &self,
        options: &Self::Options,
        args: &dyn ExecutionArgs,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<ArrayRef> {
        let inputs = (0..args.num_inputs())
            .map(|idx| args.get(idx))
            .collect::<VortexResult<Vec<_>>>()?;
        let names = (0..inputs.len())
            .map(input_field_name)
            .collect::<FieldNames>();
        let scope = StructArray::try_new(names, inputs, args.row_count(), Validity::NonNullable)?
            .into_array();

        let predicate = options.current(scope.dtype())?;
        scope
            .apply_bound(&predicate)?
            .cast(DType::Bool(Nullability::Nullable))?
            .execute(ctx)
    }
}

/// The options of a [`DynamicExpression`]: its source, and the predicate last bound from it.
#[derive(Clone)]
pub struct DynamicExpressionOptions {
    source: Arc<dyn DynamicExpressionSource>,
    bound: Arc<Mutex<Option<BoundPredicate>>>,
}

/// A predicate of the source, bound against a scope, as of a generation of the source.
struct BoundPredicate {
    generation: u64,
    scope: DType,
    predicate: BoundExpression,
}

impl DynamicExpressionOptions {
    /// Create options that read predicates from `source`.
    pub fn new(source: Arc<dyn DynamicExpressionSource>) -> Self {
        Self {
            source,
            bound: Arc::new(Mutex::new(None)),
        }
    }

    /// The source of the predicates.
    pub fn source(&self) -> &Arc<dyn DynamicExpressionSource> {
        &self.source
    }

    /// The current predicate of the source, bound against `scope`.
    ///
    /// A predicate is bound once for each generation of the source.
    pub fn current(&self, scope: &DType) -> VortexResult<BoundExpression> {
        let generation = self.source.generation();
        let mut bound = self.bound.lock();
        if let Some(cached) = bound.as_ref()
            && cached.generation == generation
            && &cached.scope == scope
        {
            return Ok(cached.predicate.clone());
        }

        let predicate = self.source.current(scope)?;
        vortex_ensure!(
            matches!(predicate.dtype(), DType::Bool(_)),
            "Dynamic expression {} must return a boolean, got {}",
            self.source,
            predicate.dtype()
        );
        *bound = Some(BoundPredicate {
            generation,
            scope: scope.clone(),
            predicate: predicate.clone(),
        });
        Ok(predicate)
    }
}

impl Debug for DynamicExpressionOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DynamicExpressionOptions")
            .field("source", &self.source)
            .finish()
    }
}

impl Display for DynamicExpressionOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.source, f)
    }
}

/// Equality and hashing use the identity of the source, so a new predicate of the source does not
/// change the identity of the expression tree.
impl PartialEq for DynamicExpressionOptions {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.source, &other.source)
    }
}

impl Eq for DynamicExpressionOptions {}

impl Hash for DynamicExpressionOptions {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.source).cast::<()>().hash(state);
    }
}

/// Which stats proof a [`StatsProofSource`] builds.
#[derive(Clone, Copy, Debug)]
pub(crate) enum StatsProof {
    Falsify,
    Satisfy,
}

/// Builds a dynamic stats proof for a dynamic expression.
///
/// Zone pruning builds and lowers the proof of a predicate once, then evaluates it again when a
/// dynamic expression changes. Thus the proof is itself a dynamic expression. Its children are
/// the statistics of the inputs that a proof can use, and the row count, so lowering binds them
/// to the stats of each zone. Each generation proves the current predicate again.
pub(crate) fn dynamic_stats_proof(
    expr: &BoundExpression,
    session: &VortexSession,
    proof: StatsProof,
) -> VortexResult<Option<BoundExpression>> {
    let options = expr.as_::<DynamicExpression>();
    let input_dtypes = expr
        .children()
        .iter()
        .map(|child| child.dtype().clone())
        .collect::<Vec<_>>();

    let mut stats = Vec::new();
    let mut children = Vec::new();
    for (input_idx, input) in expr.children().iter().enumerate() {
        for aggregate_fn in proof_aggregates() {
            if aggregate_fn.return_dtype(input.dtype()).is_some() {
                children.push(stat(input.clone(), aggregate_fn.clone()));
                stats.push((input_idx, aggregate_fn));
            }
        }
    }
    children.push(RowCount.try_new_bound_expr(EmptyOptions, [])?);

    let source = StatsProofSource {
        predicate: options.clone(),
        input_dtypes,
        stats,
        proof,
        session: session.clone(),
    };
    Ok(Some(DynamicExpression.try_new_bound_expr(
        DynamicExpressionOptions::new(Arc::new(source)),
        children,
    )?))
}

/// The statistics a proof of a dynamic expression can read.
fn proof_aggregates() -> Vec<AggregateFnRef> {
    [Stat::Min, Stat::Max, Stat::NullCount, Stat::NaNCount]
        .into_iter()
        .filter_map(|stat| stat.aggregate_fn())
        .chain([
            AllNull.bind(AggregateEmptyOptions),
            AllNonNull.bind(AggregateEmptyOptions),
            AllNan.bind(AggregateEmptyOptions),
            AllNonNan.bind(AggregateEmptyOptions),
        ])
        .collect()
}

/// The source of a dynamic stats proof, see [`dynamic_stats_proof`].
#[derive(Debug)]
struct StatsProofSource {
    predicate: DynamicExpressionOptions,
    input_dtypes: Vec<DType>,
    /// The input and aggregate of each statistic child, in child order. The row count follows.
    stats: Vec<(usize, AggregateFnRef)>,
    proof: StatsProof,
    session: VortexSession,
}

impl StatsProofSource {
    /// Replace the statistics and row count in `proof` by fields of `scope`. Returns `None` when
    /// the proof reads a statistic that is not a child.
    fn bind_children(
        &self,
        proof: &BoundExpression,
        scope: &DType,
    ) -> VortexResult<Option<BoundExpression>> {
        if let Some(options) = proof.as_opt::<StatFn>() {
            let child = proof.child(0);
            let Some(field) = child.as_opt::<GetItem>() else {
                return Ok(None);
            };
            if !child.child(0).is_root() {
                return Ok(None);
            }
            let Some(stat_idx) = self.stats.iter().position(|(input_idx, aggregate_fn)| {
                input_field_name(*input_idx) == *field && aggregate_fn == options.aggregate_fn()
            }) else {
                return Ok(None);
            };
            return Ok(Some(scope_field(stat_idx, scope)?));
        }

        if proof.is::<RowCount>() {
            return Ok(Some(scope_field(self.stats.len(), scope)?));
        }

        if proof.is_root() {
            return Ok(None);
        }

        let mut children = Vec::with_capacity(proof.children().len());
        for child in proof.children() {
            let Some(child) = self.bind_children(child, scope)? else {
                return Ok(None);
            };
            children.push(child);
        }
        Ok(Some(proof.clone().with_children(children)?))
    }
}

fn scope_field(idx: usize, scope: &DType) -> VortexResult<BoundExpression> {
    Ok(get_item(
        input_field_name(idx),
        BoundExpression::new_root(scope.clone()),
    ))
}

impl Display for StatsProofSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let proof = match self.proof {
            StatsProof::Falsify => "falsify",
            StatsProof::Satisfy => "satisfy",
        };
        write!(f, "{proof}({})", self.predicate)
    }
}

impl DynamicExpressionSource for StatsProofSource {
    fn generation(&self) -> u64 {
        self.predicate.source.generation()
    }

    fn current(&self, scope: &DType) -> VortexResult<BoundExpression> {
        let names = (0..self.input_dtypes.len())
            .map(input_field_name)
            .collect::<FieldNames>();
        let input_scope = DType::Struct(
            StructFields::new(names, self.input_dtypes.clone()),
            Nullability::NonNullable,
        );
        let predicate = self.predicate.current(&input_scope)?;
        let proof = match self.proof {
            StatsProof::Falsify => predicate.falsify(&self.session)?,
            StatsProof::Satisfy => predicate.satisfy(&self.session)?,
        };

        // A proof that is `false` proves nothing.
        Ok(match proof {
            Some(proof) => self
                .bind_children(&proof, scope)?
                .unwrap_or_else(|| lit(false)),
            None => lit(false),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicI32;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;

    use vortex_buffer::buffer;
    use vortex_error::VortexResult;

    use super::*;
    use crate::VortexSessionExecute;
    use crate::array_session;
    use crate::arrays::BoolArray;
    use crate::assert_arrays_eq;
    use crate::dtype::PType;
    use crate::expr::col;
    use crate::expr::dynamic_expression;
    use crate::expr::get_item as unbound_get_item;
    use crate::expr::lit as unbound_lit;
    use crate::expr::lt;
    use crate::expr::root;

    /// `input 0 < threshold`, where a threshold of zero means there is no bound yet.
    #[derive(Debug, Default)]
    struct Threshold {
        value: AtomicI32,
        generation: AtomicU64,
    }

    impl Threshold {
        fn set(&self, value: i32) {
            self.value.store(value, Ordering::SeqCst);
            self.generation.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl Display for Threshold {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "threshold({})", self.value.load(Ordering::SeqCst))
        }
    }

    impl DynamicExpressionSource for Threshold {
        fn generation(&self) -> u64 {
            self.generation.load(Ordering::SeqCst)
        }

        fn current(&self, scope: &DType) -> VortexResult<BoundExpression> {
            let value = self.value.load(Ordering::SeqCst);
            if value == 0 {
                return unbound_lit(true).bind(scope);
            }
            lt(
                unbound_get_item(input_field_name(0), root()),
                unbound_lit(value),
            )
            .bind(scope)
        }
    }

    #[test]
    fn execute_reads_the_current_predicate() -> VortexResult<()> {
        let mut ctx = array_session().create_execution_ctx();
        let threshold = Arc::new(Threshold::default());
        let expr = dynamic_expression(threshold.clone(), [col("a")]);
        let input =
            StructArray::from_fields(&[("a", buffer![1i32, 5, 10].into_array())])?.into_array();

        let result = input.clone().apply(&expr)?;
        assert_arrays_eq!(
            result,
            BoolArray::from_iter([Some(true), Some(true), Some(true)]),
            &mut ctx
        );

        threshold.set(6);
        let result = input.apply(&expr)?;
        assert_arrays_eq!(
            result,
            BoolArray::from_iter([Some(true), Some(true), Some(false)]),
            &mut ctx
        );
        Ok(())
    }

    #[test]
    fn falsify_follows_the_current_predicate() -> VortexResult<()> {
        let mut ctx = array_session().create_execution_ctx();
        let session = array_session();
        let threshold = Arc::new(Threshold::default());
        let scope = DType::Struct(
            StructFields::new(
                FieldNames::from(vec!["a"]),
                vec![DType::Primitive(PType::I32, Nullability::NonNullable)],
            ),
            Nullability::NonNullable,
        );
        let expr = dynamic_expression(threshold.clone(), [col("a")]).bind(&scope)?;

        let falsifier = expr
            .falsify(&session)?
            .expect("dynamic expressions always have a dynamic falsifier");
        assert!(falsifier.is::<DynamicExpression>());

        // The statistics of a zone where `a` is in 7..=9 and has no nulls or NaNs.
        let min = Stat::Min.aggregate_fn().expect("min");
        let max = Stat::Max.aggregate_fn().expect("max");
        let null_count = Stat::NullCount.aggregate_fn().expect("null count");
        let stats_scope = falsifier
            .children()
            .iter()
            .map(|child| {
                if let Some(options) = child.as_opt::<StatFn>() {
                    let aggregate_fn = options.aggregate_fn();
                    if *aggregate_fn == min {
                        return buffer![7i32].into_array();
                    }
                    if *aggregate_fn == max {
                        return buffer![9i32].into_array();
                    }
                    if *aggregate_fn == null_count {
                        return buffer![0u64].into_array();
                    }
                }
                crate::arrays::ConstantArray::new(
                    crate::scalar::Scalar::null(child.dtype().as_nullable()),
                    1,
                )
                .into_array()
            })
            .collect::<Vec<_>>();
        let names = (0..stats_scope.len())
            .map(input_field_name)
            .collect::<FieldNames>();
        let stats = StructArray::try_new(names, stats_scope, 1, Validity::NonNullable)?;
        let predicate = falsifier
            .as_::<DynamicExpression>()
            .current(stats.dtype())?;

        // Without a bound, nothing is proven.
        let result = stats.clone().into_array().apply_bound(&predicate)?;
        assert_arrays_eq!(result, BoolArray::from_iter([false]), &mut ctx);

        // A bound below the minimum proves every row false.
        threshold.set(5);
        let predicate = falsifier
            .as_::<DynamicExpression>()
            .current(stats.dtype())?;
        let result = stats.into_array().apply_bound(&predicate)?;
        assert_arrays_eq!(
            result.cast(DType::Bool(Nullability::Nullable))?,
            BoolArray::from_iter([Some(true)]),
            &mut ctx
        );
        Ok(())
    }
}
