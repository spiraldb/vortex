// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Expressions that name a scalar function by id and resolve it against a session later.

use std::fmt::Debug;
use std::fmt::Display;
use std::hash::Hash;

use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_session::VortexSession;

use crate::expr::Expression;
use crate::expr::traversal::NodeExt;
use crate::expr::traversal::Transformed;
use crate::scalar_fn::LazyScalarFn;
use crate::scalar_fn::ScalarFnId;
use crate::scalar_fn::session::ScalarFnSessionExt;

/// Creates an expression calling the scalar function registered under `name` with `options`.
///
/// The function is looked up only when the expression is [resolved](Expression::resolve), so
/// building it needs neither a session nor the crate that defines the function. `options` must be
/// that function's options type, such as [`EmptyOptions`](crate::scalar_fn::EmptyOptions) for a
/// function without options. Until it is resolved, the expression cannot be bound, executed, or
/// serialized, and its arity is not checked.
pub fn lazy<O>(
    name: &str,
    options: O,
    children: impl IntoIterator<Item = Expression>,
) -> Expression
where
    O: 'static + Send + Sync + Debug + Display + PartialEq + Eq + Hash,
{
    #[expect(
        clippy::disallowed_methods,
        reason = "interning a dynamic function name"
    )]
    let id = ScalarFnId::new(name);
    Expression::try_new(LazyScalarFn::make_scalar_fn(id, options), children)
        .vortex_expect("a lazy scalar function accepts any number of children")
}

impl Expression {
    /// Replaces every [`lazy`] node with the function registered under its id in `session`, built
    /// from the node's options.
    ///
    /// # Errors
    ///
    /// Returns an error if a lazy node names a function that `session` does not register, holds
    /// options of a different type than that function takes, or has a number of children that
    /// does not match its arity.
    pub fn resolve(self, session: &VortexSession) -> VortexResult<Expression> {
        self.transform_down(|node| {
            if !node.is::<LazyScalarFn>() {
                return Ok(Transformed::no(node));
            }
            let id = node
                .as_scalar()
                .vortex_expect("lazy nodes are scalar nodes")
                .id();
            let scalar_fn = session.scalar_fn_by_id(id, node.as_::<LazyScalarFn>().as_any())?;
            let resolved = Expression::try_new(scalar_fn, node.children().iter().cloned())?;
            Ok(Transformed::yes(resolved))
        })
        .map(Transformed::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::LazyLock;

    use vortex_error::VortexResult;
    use vortex_session::VortexSession;

    use crate::dtype::DType;
    use crate::dtype::Nullability;
    use crate::dtype::PType;
    use crate::expr::cast;
    use crate::expr::is_null;
    use crate::expr::lazy;
    use crate::expr::not;
    use crate::expr::proto::ExprSerializeProtoExt;
    use crate::expr::root;
    use crate::scalar_fn::EmptyOptions;
    use crate::scalar_fn::LazyScalarFn;

    static SESSION: LazyLock<VortexSession> = LazyLock::new(VortexSession::empty);

    fn i64_dtype() -> DType {
        DType::Primitive(PType::I64, Nullability::Nullable)
    }

    #[test]
    fn resolves_to_the_registered_function() -> VortexResult<()> {
        let expr = lazy("vortex.cast", i64_dtype(), [root()]);
        assert!(expr.is::<LazyScalarFn>());

        let resolved = expr.resolve(&SESSION)?;
        assert!(!resolved.contains::<LazyScalarFn>()?);
        assert_eq!(resolved, cast(root(), i64_dtype()));
        Ok(())
    }

    #[test]
    fn resolves_nested_nodes() -> VortexResult<()> {
        let expr = lazy(
            "vortex.not",
            EmptyOptions,
            [lazy("vortex.is_null", EmptyOptions, [root()])],
        );
        assert_eq!(expr.resolve(&SESSION)?, not(is_null(root())));
        Ok(())
    }

    #[test]
    fn resolved_expression_binds() -> VortexResult<()> {
        let scope = DType::Primitive(PType::I32, Nullability::Nullable);
        let bound = lazy("vortex.cast", i64_dtype(), [root()])
            .resolve(&SESSION)?
            .bind(&scope)?;
        assert_eq!(bound.dtype(), &i64_dtype());
        Ok(())
    }

    #[test]
    fn displays_like_the_registered_function() {
        assert_eq!(
            lazy("vortex.not", EmptyOptions, [root()]).to_string(),
            not(root()).to_string()
        );
    }

    #[test]
    fn unresolved_expression_cannot_bind_or_serialize() {
        let expr = lazy("vortex.not", EmptyOptions, [root()]);

        let err = expr
            .bind(&DType::Bool(Nullability::NonNullable))
            .unwrap_err();
        assert!(err.to_string().contains("resolved against a session"));

        let err = expr.serialize_proto().unwrap_err();
        assert!(err.to_string().contains("resolved against a session"));
    }

    #[test]
    fn unknown_function() {
        let err = lazy("vortex.no_such_function", EmptyOptions, [root()])
            .resolve(&SESSION)
            .unwrap_err();
        assert!(err.to_string().contains("unknown scalar function"));
    }

    #[test]
    fn wrong_options_type() {
        let err = lazy("vortex.cast", EmptyOptions, [root()])
            .resolve(&SESSION)
            .unwrap_err();
        assert!(err.to_string().contains("takes options of type"));
    }

    #[test]
    fn arity_is_checked_on_resolution() {
        let err = lazy("vortex.not", EmptyOptions, [root(), root()])
            .resolve(&SESSION)
            .unwrap_err();
        assert!(err.to_string().contains("arity mismatch"));
    }

    #[test]
    fn equality_compares_options() {
        let expr = lazy("vortex.cast", i64_dtype(), [root()]);
        assert_eq!(expr, lazy("vortex.cast", i64_dtype(), [root()]));
        assert_ne!(
            expr,
            lazy("vortex.cast", DType::Bool(Nullability::NonNullable), [root()])
        );
        assert_ne!(expr, cast(root(), i64_dtype()));
    }
}
