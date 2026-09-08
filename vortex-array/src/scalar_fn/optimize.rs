// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Scalar function optimization hooks and tree reduction adapters.

use std::borrow::Cow;

use vortex_error::VortexExpect;
use vortex_error::VortexResult;

use crate::ArrayRef;
use crate::IntoArray;
use crate::arrays::ScalarFn;
use crate::arrays::ScalarFnArray;
use crate::dtype::DType;
use crate::expr::Expression;
use crate::scalar_fn::ScalarFnRef;
use crate::scalar_fn::ScalarFnVTable;

/// Rewrite rules for a scalar function's expression and array nodes.
///
/// Selected by [`ScalarFnVTable::OptimizeVTable`]. Hooks receive the function's vtable and
/// bound options, and return `Ok(None)` when no rule applies. The optimizer controls rule
/// ordering and iteration; implementations only rewrite the supplied node.
///
/// Use `()` for scalar functions without optimization rules. The generic [`Self::reduce`]
/// hook is dispatched separately for expression and array trees by the scalar function's
/// existing type-erased wrapper.
pub trait OptimizeVTable<V: ScalarFnVTable> {
    /// Implement an abstract reduction rule over a tree of scalar functions.
    ///
    /// The [`ReduceNode`] can be used to traverse children, inspect their types, and
    /// construct the result via [`ReduceNode::new_node`]. The rule is generic over the node
    /// type and is instantiated once per reducible tree kind (expressions and arrays).
    ///
    /// Return `Ok(None)` if no reduction is possible.
    fn reduce<T: ReduceNode>(
        _vtable: &V,
        _options: &V::Options,
        _node: &T,
    ) -> VortexResult<Option<T>> {
        Ok(None)
    }

    /// Simplify the expression using type information from the context.
    fn simplify(
        _vtable: &V,
        _options: &V::Options,
        _expr: &Expression,
        _ctx: &dyn SimplifyCtx,
    ) -> VortexResult<Option<Expression>> {
        Ok(None)
    }

    /// Simplify the expression without type information.
    fn simplify_untyped(
        _vtable: &V,
        _options: &V::Options,
        _expr: &Expression,
    ) -> VortexResult<Option<Expression>> {
        Ok(None)
    }
}

impl<V: ScalarFnVTable> OptimizeVTable<V> for () {}

/// A node used for implementing abstract reduction rules over a tree of scalar functions.
///
/// Reduction rules are generic over the node type, so a rule is written once and monomorphized
/// per reducible tree kind: [`ExpressionReduceNode`] for expression trees and
/// [`ArrayReduceNode`] for array trees. Nodes borrow from the tree being reduced, making
/// traversal allocation-free, while nodes produced by [`ReduceNode::new_node`] own their
/// freshly-built subtrees.
pub trait ReduceNode: Clone {
    /// Return the data type of this node.
    fn node_dtype(&self) -> VortexResult<DType>;

    /// Return this node's scalar function if it is indeed a scalar fn.
    fn scalar_fn(&self) -> Option<&ScalarFnRef>;

    /// Descend to the child of this node.
    fn child(&self, idx: usize) -> Self;

    /// Returns the number of children of this node.
    fn child_count(&self) -> usize;

    /// Create a new node from the given scalar function and children, inheriting this node's
    /// reduction context (e.g. the expression scope, or the array row count).
    fn new_node(&self, scalar_fn: ScalarFnRef, children: &[Self]) -> VortexResult<Self>;
}

/// A [`ReduceNode`] over an expression tree, typed within a scope.
#[derive(Clone)]
pub struct ExpressionReduceNode<'a> {
    expression: Cow<'a, Expression>,
    scope: &'a DType,
}

impl<'a> ExpressionReduceNode<'a> {
    /// Creates a node borrowing the given expression and scope.
    pub fn new(expression: &'a Expression, scope: &'a DType) -> Self {
        Self {
            expression: Cow::Borrowed(expression),
            scope,
        }
    }

    /// Returns the expression backing this node.
    pub fn expression(&self) -> &Expression {
        &self.expression
    }

    /// Consumes this node and returns the backing expression.
    pub fn into_expression(self) -> Expression {
        self.expression.into_owned()
    }
}

impl ReduceNode for ExpressionReduceNode<'_> {
    fn node_dtype(&self) -> VortexResult<DType> {
        self.expression.return_dtype(self.scope)
    }

    fn scalar_fn(&self) -> Option<&ScalarFnRef> {
        self.expression.as_scalar()
    }

    fn child(&self, idx: usize) -> Self {
        let expression = match &self.expression {
            Cow::Borrowed(expression) => Cow::Borrowed(expression.child(idx)),
            Cow::Owned(expression) => Cow::Owned(expression.child(idx).clone()),
        };
        Self {
            expression,
            scope: self.scope,
        }
    }

    fn child_count(&self) -> usize {
        self.expression.children().len()
    }

    fn new_node(&self, scalar_fn: ScalarFnRef, children: &[Self]) -> VortexResult<Self> {
        let expression = Expression::try_new(
            scalar_fn,
            children
                .iter()
                .map(|c| c.expression.as_ref().clone())
                .collect::<Vec<_>>(),
        )?;
        Ok(Self {
            expression: Cow::Owned(expression),
            scope: self.scope,
        })
    }
}

/// A [`ReduceNode`] over an array tree.
#[derive(Clone)]
pub struct ArrayReduceNode<'a> {
    array: Cow<'a, ArrayRef>,
}

impl<'a> ArrayReduceNode<'a> {
    /// Creates a node borrowing the given array.
    pub fn new(array: &'a ArrayRef) -> Self {
        Self {
            array: Cow::Borrowed(array),
        }
    }

    /// Returns the array backing this node.
    pub fn array(&self) -> &ArrayRef {
        &self.array
    }

    /// Consumes this node and returns the backing array.
    pub fn into_array(self) -> ArrayRef {
        self.array.into_owned()
    }
}

impl ReduceNode for ArrayReduceNode<'_> {
    fn node_dtype(&self) -> VortexResult<DType> {
        Ok(self.array.dtype().clone())
    }

    fn scalar_fn(&self) -> Option<&ScalarFnRef> {
        self.array
            .as_opt::<ScalarFn>()
            .map(|a| a.data().scalar_fn())
    }

    fn child(&self, idx: usize) -> Self {
        let array = match &self.array {
            Cow::Borrowed(array) => Cow::Borrowed(
                array
                    .children_iter()
                    .nth(idx)
                    .vortex_expect("child idx out of bounds"),
            ),
            Cow::Owned(array) => Cow::Owned(
                array
                    .nth_child(idx)
                    .vortex_expect("child idx out of bounds"),
            ),
        };
        Self { array }
    }

    fn child_count(&self) -> usize {
        self.array.nchildren()
    }

    fn new_node(&self, scalar_fn: ScalarFnRef, children: &[Self]) -> VortexResult<Self> {
        let array = ScalarFnArray::try_new_with_len(
            scalar_fn,
            children.iter().map(|c| c.array.as_ref().clone()).collect(),
            self.array.len(),
        )?;
        Ok(Self {
            array: Cow::Owned(array.into_array()),
        })
    }
}

/// Context for simplification.
///
/// Used to lazily compute input data types where simplification requires them.
pub trait SimplifyCtx {
    /// Get the data type of the given expression.
    fn return_dtype(&self, expr: &Expression) -> VortexResult<DType>;
}
