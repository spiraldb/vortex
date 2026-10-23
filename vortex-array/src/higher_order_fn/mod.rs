// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Higher-order functions: functions that take lambdas as well as values, such as
//! `list_transform`.
//!
//! A higher-order function is its own kind of node, parallel to a scalar function: an
//! [`Expression::HigherOrder`] holds its value children and its lambdas as written, a
//! [`BoundExpression::HigherOrder`](crate::expr::BoundExpression::HigherOrder) holds them bound, and a
//! [`HigherOrderArray`](crate::arrays::HigherOrderArray) runs it. The function decides how each
//! lambda's parameters are typed ([`HigherOrderFnVTable::bind_lambdas`]) and when the lambdas run
//! ([`HigherOrderFnVTable::execute`]).

pub mod fns;

use std::any::Any;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use vortex_error::VortexResult;
use vortex_utils::debug_with::DebugWith;

use crate::ArrayRef;
use crate::ExecutionCtx;
use crate::dtype::DType;
use crate::expr::BoundLambda;
use crate::expr::Expression;
use crate::expr::Lambda;
use crate::expr::display::ExprDisplay;
use crate::scalar_fn::Arity;
use crate::scalar_fn::ChildName;
use crate::scalar_fn::ExecutionArgs;
use crate::scalar_fn::ScalarFnId;

/// A function that takes lambdas as well as values.
pub trait HigherOrderFnVTable: 'static + Sized + Clone + Send + Sync {
    /// Options other than the lambdas.
    type Options: 'static + Send + Sync + Clone + Debug + Display + PartialEq + Eq + Hash;

    /// The function's id.
    fn id(&self) -> ScalarFnId;

    /// How many value children the function takes.
    fn arity(&self, options: &Self::Options) -> Arity;

    /// The name of a value child.
    fn child_name(&self, options: &Self::Options, child_idx: usize) -> ChildName;

    /// How many lambdas the function takes.
    fn lambda_count(&self, options: &Self::Options) -> usize;

    /// Bind the lambdas against the value children's dtypes.
    ///
    /// The function decides each lambda's parameter dtypes, in whatever order it needs: a fold
    /// binds the lambda that merges an element into the accumulator before the one that finishes.
    fn bind_lambdas(
        &self,
        options: &Self::Options,
        args: &[DType],
        lambdas: &[Lambda],
    ) -> VortexResult<Vec<BoundLambda>>;

    /// The result dtype, given the value children's dtypes and the bound lambdas.
    fn return_dtype(
        &self,
        options: &Self::Options,
        args: &[DType],
        lambdas: &[BoundLambda],
    ) -> VortexResult<DType>;

    /// Run the function over its value children, with its lambdas bound.
    fn execute(
        &self,
        options: &Self::Options,
        args: &dyn ExecutionArgs,
        lambdas: &[BoundLambda],
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<ArrayRef>;

    /// Whether a null in any value child makes the result null.
    fn is_strict(&self, options: &Self::Options) -> bool {
        _ = options;
        false
    }

    /// Whether the function itself can never fail. Its lambdas are checked separately.
    fn is_infallible(&self, options: &Self::Options) -> bool {
        _ = options;
        false
    }

    /// Format the call as SQL, given a way to display the value children.
    fn fmt_sql(
        &self,
        options: &Self::Options,
        expr: &dyn ExprDisplay,
        lambdas: &[Lambda],
        f: &mut Formatter<'_>,
    ) -> fmt::Result {
        write!(f, "{}(", self.id())?;
        for idx in 0..expr.display_children_count() {
            write!(f, "{}, ", expr.display_child(idx))?;
        }
        for (idx, lambda) in lambdas.iter().enumerate() {
            if idx > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{lambda}")?;
        }
        _ = options;
        write!(f, ")")
    }
}

/// Construct higher-order function nodes.
pub trait HigherOrderFnVTableExt: HigherOrderFnVTable {
    /// This function with these options, type-erased.
    fn bind(&self, options: Self::Options) -> HigherOrderFnRef {
        HigherOrderFnRef(Arc::new(Instance {
            vtable: self.clone(),
            options,
        }))
    }

    /// A written node applying this function to these value children and lambdas.
    fn new_expr(
        &self,
        options: Self::Options,
        children: impl IntoIterator<Item = Expression>,
        lambdas: impl IntoIterator<Item = Lambda>,
    ) -> VortexResult<Expression> {
        Expression::try_new_higher_order(self.bind(options), children, lambdas)
    }
}

impl<V: HigherOrderFnVTable> HigherOrderFnVTableExt for V {}

struct Instance<V: HigherOrderFnVTable> {
    vtable: V,
    options: V::Options,
}

trait DynHigherOrderFn: 'static + Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn id(&self) -> ScalarFnId;
    fn arity(&self) -> Arity;
    fn child_name(&self, child_idx: usize) -> ChildName;
    fn lambda_count(&self) -> usize;
    fn bind_lambdas(&self, args: &[DType], lambdas: &[Lambda]) -> VortexResult<Vec<BoundLambda>>;
    fn return_dtype(&self, args: &[DType], lambdas: &[BoundLambda]) -> VortexResult<DType>;
    fn execute(
        &self,
        args: &dyn ExecutionArgs,
        lambdas: &[BoundLambda],
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<ArrayRef>;
    fn is_strict(&self) -> bool;
    fn is_infallible(&self) -> bool;
    fn fmt_sql(
        &self,
        expr: &dyn ExprDisplay,
        lambdas: &[Lambda],
        f: &mut Formatter<'_>,
    ) -> fmt::Result;
    fn options_any(&self) -> &dyn Any;
    fn options_eq(&self, other: &dyn Any) -> bool;
    fn options_hash(&self, hasher: &mut dyn Hasher);
    fn options_display(&self, f: &mut Formatter<'_>) -> fmt::Result;
    fn options_debug(&self, f: &mut Formatter<'_>) -> fmt::Result;
}

impl<V: HigherOrderFnVTable> DynHigherOrderFn for Instance<V> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn id(&self) -> ScalarFnId {
        self.vtable.id()
    }

    fn arity(&self) -> Arity {
        self.vtable.arity(&self.options)
    }

    fn child_name(&self, child_idx: usize) -> ChildName {
        self.vtable.child_name(&self.options, child_idx)
    }

    fn lambda_count(&self) -> usize {
        self.vtable.lambda_count(&self.options)
    }

    fn bind_lambdas(&self, args: &[DType], lambdas: &[Lambda]) -> VortexResult<Vec<BoundLambda>> {
        self.vtable.bind_lambdas(&self.options, args, lambdas)
    }

    fn return_dtype(&self, args: &[DType], lambdas: &[BoundLambda]) -> VortexResult<DType> {
        self.vtable.return_dtype(&self.options, args, lambdas)
    }

    fn execute(
        &self,
        args: &dyn ExecutionArgs,
        lambdas: &[BoundLambda],
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<ArrayRef> {
        self.vtable.execute(&self.options, args, lambdas, ctx)
    }

    fn is_strict(&self) -> bool {
        self.vtable.is_strict(&self.options)
    }

    fn is_infallible(&self) -> bool {
        self.vtable.is_infallible(&self.options)
    }

    fn fmt_sql(
        &self,
        expr: &dyn ExprDisplay,
        lambdas: &[Lambda],
        f: &mut Formatter<'_>,
    ) -> fmt::Result {
        self.vtable.fmt_sql(&self.options, expr, lambdas, f)
    }

    fn options_any(&self) -> &dyn Any {
        &self.options
    }

    fn options_eq(&self, other: &dyn Any) -> bool {
        other
            .downcast_ref::<V::Options>()
            .is_some_and(|other| &self.options == other)
    }

    fn options_hash(&self, mut hasher: &mut dyn Hasher) {
        self.options.hash(&mut hasher);
    }

    fn options_display(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.options, f)
    }

    fn options_debug(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Debug::fmt(&self.options, f)
    }
}

/// A type-erased higher-order function with its options, but not its lambdas: the node holds
/// those, as written or bound.
#[derive(Clone)]
pub struct HigherOrderFnRef(Arc<dyn DynHigherOrderFn>);

impl HigherOrderFnRef {
    /// The function's id.
    pub fn id(&self) -> ScalarFnId {
        self.0.id()
    }

    /// Whether this is the given function.
    pub fn is<V: HigherOrderFnVTable>(&self) -> bool {
        self.0.as_any().is::<Instance<V>>()
    }

    /// The options, if this is the given function.
    pub fn as_opt<V: HigherOrderFnVTable>(&self) -> Option<&V::Options> {
        self.0
            .as_any()
            .downcast_ref::<Instance<V>>()
            .map(|instance| &instance.options)
    }

    /// How many value children the function takes.
    pub fn arity(&self) -> Arity {
        self.0.arity()
    }

    /// The name of a value child.
    pub fn child_name(&self, child_idx: usize) -> ChildName {
        self.0.child_name(child_idx)
    }

    /// How many lambdas the function takes.
    pub fn lambda_count(&self) -> usize {
        self.0.lambda_count()
    }

    /// Bind the lambdas against the value children's dtypes. See
    /// [`HigherOrderFnVTable::bind_lambdas`].
    pub fn bind_lambdas(
        &self,
        args: &[DType],
        lambdas: &[Lambda],
    ) -> VortexResult<Vec<BoundLambda>> {
        self.0.bind_lambdas(args, lambdas)
    }

    /// The result dtype.
    pub fn return_dtype(&self, args: &[DType], lambdas: &[BoundLambda]) -> VortexResult<DType> {
        self.0.return_dtype(args, lambdas)
    }

    /// Run the function.
    pub fn execute(
        &self,
        args: &dyn ExecutionArgs,
        lambdas: &[BoundLambda],
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<ArrayRef> {
        self.0.execute(args, lambdas, ctx)
    }

    /// Whether a null in any value child makes the result null.
    pub fn is_strict(&self) -> bool {
        self.0.is_strict()
    }

    /// Whether neither the function nor any of these lambdas can fail.
    pub fn is_infallible(&self, lambdas: &[BoundLambda]) -> bool {
        self.0.is_infallible() && lambdas.iter().all(BoundLambda::is_infallible)
    }

    /// Format the call as SQL.
    pub fn fmt_sql(
        &self,
        expr: &dyn ExprDisplay,
        lambdas: &[Lambda],
        f: &mut Formatter<'_>,
    ) -> fmt::Result {
        self.0.fmt_sql(expr, lambdas, f)
    }

    /// Bind a written node's lambdas and compute its dtype: the lambdas bound to these value
    /// dtypes, and the result dtype.
    pub(crate) fn bind_node(
        &self,
        args: &[DType],
        lambdas: &[Lambda],
    ) -> VortexResult<(Vec<BoundLambda>, DType)> {
        let bound = self.bind_lambdas(args, lambdas)?;
        let dtype = self.return_dtype(args, &bound)?;
        Ok((bound, dtype))
    }
}

impl Debug for HigherOrderFnRef {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("HigherOrderFnRef")
            .field("id", &self.0.id())
            .field("options", &DebugWith(|fmt| self.0.options_debug(fmt)))
            .finish()
    }
}

impl Display for HigherOrderFnRef {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}(", self.0.id())?;
        self.0.options_display(f)?;
        write!(f, ")")
    }
}

impl PartialEq for HigherOrderFnRef {
    fn eq(&self, other: &Self) -> bool {
        self.0.id() == other.0.id() && self.0.options_eq(other.0.options_any())
    }
}

impl Eq for HigherOrderFnRef {}

impl Hash for HigherOrderFnRef {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.id().hash(state);
        self.0.options_hash(state);
    }
}

/// Lambdas of a bound node, rebound if the value children's dtypes changed. Used when a bound
/// node is rebuilt with new children.
pub(crate) fn rebind_if_needed(
    function: &HigherOrderFnRef,
    args: &[DType],
    written: &[Lambda],
    bound: &Arc<Vec<BoundLambda>>,
) -> VortexResult<(Arc<Vec<BoundLambda>>, DType)> {
    let rebound = function.bind_lambdas(args, written)?;
    let rebound = if rebound.as_slice() == bound.as_slice() {
        Arc::clone(bound)
    } else {
        Arc::new(rebound)
    };
    // The result dtype can change even when the lambdas don't, such as a list's own nullability.
    let dtype = function.return_dtype(args, &rebound)?;
    Ok((rebound, dtype))
}
