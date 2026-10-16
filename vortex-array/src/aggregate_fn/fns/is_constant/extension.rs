// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use vortex_error::VortexResult;
use vortex_session::registry::CachedId;

use super::IsConstant;
use super::is_constant;
use crate::ArrayRef;
use crate::ExecutionCtx;
use crate::aggregate_fn::AggregateFnId;
use crate::aggregate_fn::EmptyOptions;
use crate::aggregate_fn::ExtensionDelegate;
use crate::aggregate_fn::StorageLifter;
use crate::arrays::ExtensionArray;
use crate::scalar::Scalar;

/// Lifts [`IsConstant`] over an extension's storage; constancy does not depend on the extension.
#[derive(Clone, Debug)]
pub(super) struct IsConstantLifter;

impl StorageLifter for IsConstantLifter {
    type Storage = IsConstant;

    fn id(&self) -> AggregateFnId {
        static ID: CachedId = CachedId::new("vortex.is_constant.extension");
        *ID
    }

    fn storage(&self) -> Self::Storage {
        IsConstant
    }

    fn aggregate_storage(
        &self,
        _options: &EmptyOptions,
        storage: &ArrayRef,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Scalar> {
        Ok(is_constant(storage, ctx)?.into())
    }
}

/// Check if an extension array is constant by delegating to its storage array.
pub(super) fn check_extension_constant(
    e: &ExtensionArray,
    ctx: &mut ExecutionCtx,
) -> VortexResult<bool> {
    bool::try_from(&ExtensionDelegate::new(IsConstantLifter).aggregate(&EmptyOptions, e, ctx)?)
}
