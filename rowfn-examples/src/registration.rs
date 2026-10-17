// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! External Vortex registration keeps persistence and identity outside the portable function.

use vortex_array::{ArrayRef, ExecutionCtx};
use vortex_array::dtype::DType;
use vortex_array::expr::Expression;
use vortex_array::scalar_fn::{Arity, ChildName, ExecutionArgs, ScalarFnId, ScalarFnVTable};
use vortex_array::scalar_fn::unstable::rowfn::VortexRowFn;
use vortex_error::{VortexResult, vortex_ensure};
use vortex_session::VortexSession;
use vortex_session::registry::CachedId;

use crate::{NoOptions, Seven};

/// A serializable Vortex plugin defined entirely outside `vortex-array` and `rowfn`.
#[derive(Clone)]
pub struct SevenPlugin;

impl SevenPlugin {
    fn row_fn(&self) -> VortexRowFn<Seven> {
        VortexRowFn::new(self.id(), Seven)
    }
}

impl ScalarFnVTable for SevenPlugin {
    type Options = NoOptions;

    fn id(&self) -> ScalarFnId {
        static ID: CachedId = CachedId::new("external.rowfn.seven");
        *ID
    }

    fn serialize(&self, _: &NoOptions) -> VortexResult<Option<Vec<u8>>> {
        Ok(Some(Vec::new()))
    }

    fn deserialize(&self, metadata: &[u8], _: &VortexSession) -> VortexResult<NoOptions> {
        vortex_ensure!(metadata.is_empty(), "seven has no serialized options");
        Ok(NoOptions)
    }

    fn arity(&self, options: &NoOptions) -> Arity {
        self.row_fn().arity(options)
    }

    fn child_name(&self, options: &NoOptions, index: usize) -> ChildName {
        self.row_fn().child_name(options, index)
    }

    fn return_dtype(&self, options: &NoOptions, args: &[DType]) -> VortexResult<DType> {
        self.row_fn().return_dtype(options, args)
    }

    fn execute(&self, options: &NoOptions, args: &dyn ExecutionArgs, ctx: &mut ExecutionCtx)
        -> VortexResult<ArrayRef> {
        self.row_fn().execute(options, args, ctx)
    }

    fn validity(&self, options: &NoOptions, expression: &Expression)
        -> VortexResult<Option<Expression>> {
        self.row_fn().validity(options, expression)
    }

    fn is_strict(&self, options: &NoOptions) -> bool {
        self.row_fn().is_strict(options)
    }

    fn is_infallible(&self, options: &NoOptions) -> bool {
        self.row_fn().is_infallible(options)
    }
}
