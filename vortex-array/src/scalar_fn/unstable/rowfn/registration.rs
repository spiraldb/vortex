// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Registration through the existing scalar-function vtable and session registry.

use std::fmt::{Debug, Display};
use std::hash::Hash;

use ::rowfn::RowFn;
use vortex_error::VortexResult;

use super::{VortexHost, execute};
use crate::{ArrayRef, ExecutionCtx};
use crate::dtype::DType;
use crate::expr::{Expression, union_child_validities};
use crate::scalar_fn::{Arity, ChildName, ExecutionArgs, ScalarFnId, ScalarFnVTable};

/// Explicit Vortex identity and registry boundary for an independently defined row function.
///
/// This wrapper leaves serialization unsupported. A package that needs persistence or custom
/// optimizer hooks can implement its own vtable and delegate to [`super::execute`]. Whole-batch
/// implementations continue to implement [`ScalarFnVTable`] directly.
#[derive(Clone)]
pub struct VortexRowFn<F> { id: ScalarFnId, function: F }
impl<F> VortexRowFn<F> {
    /// Associate a host registry ID with a portable function definition.
    pub fn new(id: ScalarFnId, function: F) -> Self { Self { id, function } }
}
impl<F: RowFn<VortexHost>> ScalarFnVTable for VortexRowFn<F>
where F::Options: 'static + Send + Sync + Clone + Debug + Display + PartialEq + Eq + Hash {
    type Options = F::Options;
    fn id(&self) -> ScalarFnId { self.id }
    fn arity(&self, _: &Self::Options) -> Arity { Arity::Exact(F::ARG_NAMES.len()) }
    fn child_name(&self, _: &Self::Options, index: usize) -> ChildName { F::ARG_NAMES[index].into() }
    fn return_dtype(&self, options: &Self::Options, args: &[DType]) -> VortexResult<DType> {
        Ok(::rowfn::plan::<VortexHost, F>(&self.function, options, args)?.output_type().clone())
    }
    fn execute(&self, options: &Self::Options, args: &dyn ExecutionArgs, ctx: &mut ExecutionCtx) -> VortexResult<ArrayRef> {
        let inputs = (0..args.num_inputs()).map(|index| args.get(index)).collect::<VortexResult<Vec<_>>>()?;
        let types: Vec<_> = inputs.iter().map(|input| input.dtype().clone()).collect();
        let output = self.return_dtype(options, &types)?;
        execute(&self.function, options, &inputs, args.row_count(), &output, ctx)
    }
    fn validity(&self, _: &Self::Options, expression: &Expression) -> VortexResult<Option<Expression>> {
        union_child_validities(expression)
    }
    fn is_strict(&self, _: &Self::Options) -> bool { true }
    fn is_infallible(&self, _: &Self::Options) -> bool { F::INFALLIBLE }
}
