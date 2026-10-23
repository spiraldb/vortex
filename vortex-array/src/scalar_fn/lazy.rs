// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Scalar functions named by id, resolved against a session later.

use std::any::Any;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use vortex_error::VortexError;
use vortex_error::VortexResult;
use vortex_error::vortex_err;

use crate::ArrayRef;
use crate::ExecutionCtx;
use crate::dtype::DType;
use crate::scalar_fn::Arity;
use crate::scalar_fn::ChildName;
use crate::scalar_fn::ExecutionArgs;
use crate::scalar_fn::ScalarFnId;
use crate::scalar_fn::ScalarFnRef;
use crate::scalar_fn::ScalarFnVTable;
use crate::scalar_fn::TypedScalarFnInstance;

/// The options of a registered scalar function, held with their type erased until the function
/// they belong to is looked up.
#[derive(Clone, Debug)]
pub struct LazyScalarFnOptions {
    options: Arc<dyn DynOptions>,
}

impl LazyScalarFnOptions {
    /// Erases `options`, which must be the options type of the function they will be passed to.
    pub fn new<O>(options: O) -> Self
    where
        O: 'static + Send + Sync + Debug + Display + PartialEq + Eq + Hash,
    {
        Self {
            options: Arc::new(options),
        }
    }

    /// The erased options, in the form [`ScalarFnPlugin::bind_any`] takes.
    ///
    /// [`ScalarFnPlugin::bind_any`]: crate::scalar_fn::ScalarFnPlugin::bind_any
    pub fn as_any(&self) -> &dyn Any {
        self.options.as_any()
    }
}

impl Display for LazyScalarFnOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.options, f)
    }
}

impl PartialEq for LazyScalarFnOptions {
    fn eq(&self, other: &Self) -> bool {
        self.options.dyn_eq(other.options.as_any())
    }
}

impl Eq for LazyScalarFnOptions {}

impl Hash for LazyScalarFnOptions {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.options.dyn_hash(state);
    }
}

/// Object-safe view of an options value, with equality and hashing dispatched by type.
trait DynOptions: Send + Sync + Debug + Display {
    fn as_any(&self) -> &dyn Any;
    fn dyn_eq(&self, other: &dyn Any) -> bool;
    fn dyn_hash(&self, hasher: &mut dyn Hasher);
}

impl<O> DynOptions for O
where
    O: 'static + Send + Sync + Debug + Display + PartialEq + Eq + Hash,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn dyn_eq(&self, other: &dyn Any) -> bool {
        other.downcast_ref::<O>().is_some_and(|o| self == o)
    }

    fn dyn_hash(&self, mut hasher: &mut dyn Hasher) {
        self.hash(&mut hasher);
    }
}

/// A scalar function that stands in for the function registered under its id until an expression
/// holding it is [resolved](crate::expr::Expression::resolve) against a session.
///
/// It reports the id of the function it stands for, so it displays like that function. It accepts
/// any number of children, since the arity is not known until resolution, and it cannot be bound,
/// executed, or serialized.
#[derive(Clone, Debug)]
pub struct LazyScalarFn {
    id: ScalarFnId,
}

impl LazyScalarFn {
    pub fn new(id: ScalarFnId) -> Self {
        Self { id }
    }

    /// Builds the lazy stand-in for the function registered under `id`, called with `options`.
    pub fn make_scalar_fn<O>(id: ScalarFnId, options: O) -> ScalarFnRef
    where
        O: 'static + Send + Sync + Debug + Display + PartialEq + Eq + Hash,
    {
        TypedScalarFnInstance::new(Self::new(id), LazyScalarFnOptions::new(options))
            .erased()
    }

    fn unresolved(&self, action: &str) -> VortexError {
        vortex_err!(
            "scalar function {} must be resolved against a session before it is {action}",
            self.id
        )
    }
}

impl ScalarFnVTable for LazyScalarFn {
    type Options = LazyScalarFnOptions;

    fn id(&self) -> ScalarFnId {
        self.id
    }

    fn serialize(&self, _options: &Self::Options) -> VortexResult<Option<Vec<u8>>> {
        Err(self.unresolved("serialized"))
    }

    fn arity(&self, _options: &Self::Options) -> Arity {
        Arity::Variadic { min: 0, max: None }
    }

    fn child_name(&self, _options: &Self::Options, child_idx: usize) -> ChildName {
        ChildName::new_arc(format!("arg{child_idx}").into())
    }

    fn return_dtype(&self, _options: &Self::Options, _args: &[DType]) -> VortexResult<DType> {
        Err(self.unresolved("bound"))
    }

    fn execute(
        &self,
        _options: &Self::Options,
        _args: &dyn ExecutionArgs,
        _ctx: &mut ExecutionCtx,
    ) -> VortexResult<ArrayRef> {
        Err(self.unresolved("executed"))
    }
}
