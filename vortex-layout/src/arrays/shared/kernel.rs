// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use vortex_error::VortexResult;
use vortex_session::VortexSession;

use vortex_array::ArrayRef;
use vortex_array::ExecutionCtx;
use vortex_array::ArrayView;
use crate::arrays::shared::Shared;
use crate::arrays::shared::current_array_ref_for_dispatch;
use vortex_array::optimizer::kernels::ArrayKernelsExt;
use vortex_array::scalar_fn::ScalarFnVTable;
use vortex_array::scalar_fn::fns::like::Like;
use vortex_array::scalar_fn::fns::like::LikeExecuteAdaptor;
use vortex_array::scalar_fn::fns::like::LikeKernel;
use vortex_array::scalar_fn::fns::like::LikeOptions;
use vortex_array::scalar_fn::fns::like::try_execute_like_without_fallback;

impl LikeKernel for Shared {
    fn like(
        array: ArrayView<'_, Self>,
        pattern: &ArrayRef,
        options: LikeOptions,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Option<ArrayRef>> {
        let mut current = current_array_ref_for_dispatch(array)?.clone();
        while let Some(shared) = current.as_opt::<Shared>() {
            current = current_array_ref_for_dispatch(shared)?.clone();
        }

        // Give the current encoding's LIKE kernel a chance without removing Shared. If it
        // declines, normal execution will materialize Shared and retain that result for reuse.
        try_execute_like_without_fallback(&current, pattern, options, ctx)
    }
}

pub(crate) fn initialize(session: &VortexSession) {
    session
        .kernels()
        .register_execute_parent_kernel(Like.id(), Shared, LikeExecuteAdaptor(Shared));
}
