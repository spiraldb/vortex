// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::fmt;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::hash::Hash;

use arcref::ArcRef;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_err;
use vortex_session::VortexSession;

use crate::ArrayRef;
use crate::ExecutionCtx;
use crate::dtype::DType;
use crate::expr::BoundExpression;
use crate::expr::Expression;
use crate::expr::display::ExprDisplay;
use crate::scalar_fn::OptimizeVTable;
use crate::scalar_fn::ScalarFnId;
use crate::scalar_fn::ScalarFnRef;
use crate::scalar_fn::TypedScalarFnInstance;

/// This trait defines the interface for scalar function vtables, including methods for
/// serialization, deserialization, validation, child naming, return type computation,
/// and evaluation.
///
/// This trait is non-object safe and allows the implementer to make use of associated types
/// for improved type safety, while allowing Vortex to enforce runtime checks on the inputs and
/// outputs of each function.
///
/// The [`ScalarFnVTable`] trait should be implemented for a struct that holds global data across
/// all instances of the expression. In almost all cases, this struct will be an empty unit
/// struct, since most expressions do not require any global state.
pub trait ScalarFnVTable: 'static + Sized + Clone + Send + Sync {
    /// Options for this expression.
    type Options: 'static + Send + Sync + Clone + Debug + Display + PartialEq + Eq + Hash;

    /// Optimization rules for this scalar function.
    ///
    /// Use `Self` to implement custom rules, or `()` when no rules are needed.
    type OptimizeVTable: OptimizeVTable<Self>;

    /// Returns the ID of the scalar function vtable.
    fn id(&self) -> ScalarFnId;

    /// Serialize the options for this expression.
    ///
    /// Should return `Ok(None)` if the expression is not serializable, and `Ok(vec![])` if it is
    /// serializable but has no metadata.
    fn serialize(&self, options: &Self::Options) -> VortexResult<Option<Vec<u8>>> {
        _ = options;
        Ok(None)
    }

    /// Deserialize the options of this expression.
    fn deserialize(
        &self,
        _metadata: &[u8],
        _session: &VortexSession,
    ) -> VortexResult<Self::Options> {
        vortex_bail!("Expression {} is not deserializable", self.id());
    }

    /// Returns the arity of this expression.
    fn arity(&self, options: &Self::Options) -> Arity;

    /// Returns the name of the nth child of the expr.
    fn child_name(&self, options: &Self::Options, child_idx: usize) -> ChildName;

    /// Format an expression tree in a human-readable SQL-style format.
    ///
    /// The expression may be either an [`Expression`] or a
    /// [`bound expression`](crate::expr::BoundExpression).
    fn fmt_sql(
        &self,
        options: &Self::Options,
        expr: &dyn ExprDisplay,
        f: &mut Formatter<'_>,
    ) -> fmt::Result {
        write!(f, "{}(", self.id())?;
        let nchildren = expr.display_children_count();
        for i in 0..nchildren {
            Display::fmt(expr.display_child(i), f)?;
            if i + 1 < nchildren {
                write!(f, ", ")?;
            }
        }
        let opts = format!("{}", options);
        if !opts.is_empty() {
            write!(f, ", opts={}", opts)?;
        }
        write!(f, ")")
    }

    /// Compute the return [`DType`] of the expression if evaluated over the given input types.
    ///
    /// # Preconditions
    ///
    /// The length of `args` must match the [`Arity`] of this function. Callers are responsible
    /// for validating this (e.g., [`Expression::try_new`] checks arity at construction time).
    /// Implementations may assume correct arity and will panic or return nonsensical results if
    /// violated.
    ///
    /// [`Expression::try_new`]: crate::expr::Expression::try_new
    fn return_dtype(&self, options: &Self::Options, args: &[DType]) -> VortexResult<DType>;

    /// Execute the expression over the input arguments.
    ///
    /// Implementations are encouraged to check their inputs for constant arrays to perform
    /// more optimized execution.
    ///
    /// If the input arguments cannot be directly used for execution (for example, an expression
    /// may require canonical input arrays), then the implementation should perform a single
    /// child execution and return a new [`crate::arrays::ScalarFnArray`] wrapping up the new child.
    ///
    /// This provides maximum opportunities for array-level optimizations using execute_parent
    /// kernels.
    fn execute(
        &self,
        options: &Self::Options,
        args: &dyn ExecutionArgs,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<ArrayRef>;

    /// Returns an expression that evaluates to the validity of the result of this expression.
    ///
    /// If a validity expression cannot be constructed, returns `None` and the expression will
    /// be evaluated as normal before extracting the validity mask from the result.
    ///
    /// This is essentially a specialized form of a `reduce_parent`
    fn validity(
        &self,
        options: &Self::Options,
        expression: &Expression,
    ) -> VortexResult<Option<Expression>> {
        _ = (options, expression);
        Ok(None)
    }

    /// Returns whether this scalar function is strict.
    ///
    /// A strict function returns null for a row when any argument is null for that row. This
    /// matches [PostgreSQL's `STRICT` convention](https://www.postgresql.org/docs/current/sql-createfunction.html)
    /// for null propagation.
    ///
    /// Return `true` only when this holds for every argument. `add` is strict, but Kleene `AND`
    /// is not because `false AND null` returns `false`. `is_null` is also not strict.
    ///
    /// Strictness does not require valid inputs to produce a valid output. For example,
    /// [`crate::expr::list_sum`] returns null for a valid empty list. Implement
    /// [`ScalarFnVTable::validity`] only when the output validity can be derived without
    /// evaluation.
    ///
    /// [`ScalarFnVTable::return_dtype`] must return a nullable output dtype when any input dtype is
    /// nullable. A `cast` that forces a non-nullable output dtype is therefore not strict.
    ///
    /// This property applies only to the scalar function, not its child expressions. Nullary
    /// functions are vacuously strict. The default is conservatively `false`.
    fn is_strict(&self, options: &Self::Options) -> bool {
        _ = options;
        false
    }

    /// Returns whether this scalar function can never raise a semantic error.
    ///
    /// Return `true` only when a well-typed call cannot error because of its values. `checked_add`
    /// is fallible on integer overflow, and integer division is fallible when its divisor is zero.
    /// A null result is not an error: [`crate::expr::list_sum`] is infallible for an empty list.
    ///
    /// Ignore incidental execution errors, such as canonicalization failures, allocation errors,
    /// and encoding mismatches. They are not part of the function's semantics.
    ///
    /// Returning `true` permits optimizations that evaluate the function over values that no input
    /// row references. Dictionary push-down, for example, evaluates every dictionary value, so a
    /// fallible function could error on a value that row-wise evaluation would never reach.
    ///
    /// This applies only to the scalar function, not its child expressions, and only to inputs
    /// accepted by [`ScalarFnVTable::return_dtype`]. The default is conservatively `false`.
    fn is_infallible(&self, options: &Self::Options) -> bool {
        _ = options;
        false
    }
}

/// The arity (number of arguments) of a function.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Arity {
    Exact(usize),
    Variadic { min: usize, max: Option<usize> },
}

impl Display for Arity {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Arity::Exact(n) => write!(f, "{}", n),
            Arity::Variadic { min, max } => match max {
                Some(max) if min == max => write!(f, "{}", min),
                Some(max) => write!(f, "{}..{}", min, max),
                None => write!(f, "{}+", min),
            },
        }
    }
}

impl Arity {
    /// Whether the given argument count matches this arity.
    pub fn matches(&self, arg_count: usize) -> bool {
        match self {
            Arity::Exact(m) => *m == arg_count,
            Arity::Variadic { min, max } => {
                if arg_count < *min {
                    return false;
                }
                if let Some(max) = max
                    && arg_count > *max
                {
                    return false;
                }
                true
            }
        }
    }
}

/// Arguments for expression execution.
pub trait ExecutionArgs {
    /// Returns the input array at the given index.
    fn get(&self, index: usize) -> VortexResult<ArrayRef>;

    /// Returns the number of inputs.
    fn num_inputs(&self) -> usize;

    /// Returns the row count of the execution scope.
    fn row_count(&self) -> usize;
}

/// A concrete [`ExecutionArgs`] backed by a `Vec<ArrayRef>`.
pub struct VecExecutionArgs {
    inputs: Vec<ArrayRef>,
    row_count: usize,
}

impl VecExecutionArgs {
    /// Create a new `VecExecutionArgs`.
    pub fn new(inputs: Vec<ArrayRef>, row_count: usize) -> Self {
        Self { inputs, row_count }
    }
}

impl ExecutionArgs for VecExecutionArgs {
    fn get(&self, index: usize) -> VortexResult<ArrayRef> {
        self.inputs.get(index).cloned().ok_or_else(|| {
            vortex_err!(
                "Input index {} out of bounds (num_inputs={})",
                index,
                self.inputs.len()
            )
        })
    }

    fn num_inputs(&self) -> usize {
        self.inputs.len()
    }

    fn row_count(&self) -> usize {
        self.row_count
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct EmptyOptions;
impl Display for EmptyOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "")
    }
}

/// Factory functions for vtables.
pub trait ScalarFnVTableExt: ScalarFnVTable {
    /// Bind this vtable with the given options into a [`ScalarFnRef`].
    fn bind(&self, options: Self::Options) -> ScalarFnRef {
        TypedScalarFnInstance::new(self.clone(), options).erased()
    }

    /// Create a new expression with this vtable and the given options and children.
    fn new_expr(
        &self,
        options: Self::Options,
        children: impl IntoIterator<Item = Expression>,
    ) -> Expression {
        Self::try_new_expr(self, options, children).vortex_expect("Failed to create expression")
    }

    /// Try to create a new expression with this vtable and the given options and children.
    fn try_new_expr(
        &self,
        options: Self::Options,
        children: impl IntoIterator<Item = Expression>,
    ) -> VortexResult<Expression> {
        Expression::try_new(self.bind(options), children)
    }

    /// Try to create a bound expression with this vtable, the given options, and bound children.
    fn try_new_bound_expr(
        &self,
        options: Self::Options,
        children: impl IntoIterator<Item = BoundExpression>,
    ) -> VortexResult<BoundExpression> {
        BoundExpression::try_new(self.bind(options), children)
    }
}
impl<V: ScalarFnVTable> ScalarFnVTableExt for V {}

/// A reference to the name of a child expression.
pub type ChildName = ArcRef<str>;
