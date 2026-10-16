// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use vortex_error::VortexResult;
use vortex_session::registry::CachedId;

use super::IsSorted;
use super::IsSortedOptions;
use super::is_sorted;
use super::is_strict_sorted;
use crate::ArrayRef;
use crate::ExecutionCtx;
use crate::aggregate_fn::AggregateFnId;
use crate::aggregate_fn::ExtensionDelegate;
use crate::aggregate_fn::StorageLifter;
use crate::arrays::ExtensionArray;
use crate::scalar::Scalar;

/// Lifts [`IsSorted`] over an extension's storage, which defines the extension's order.
#[derive(Clone, Debug)]
pub(super) struct IsSortedLifter;

impl StorageLifter for IsSortedLifter {
    type Storage = IsSorted;

    fn id(&self) -> AggregateFnId {
        static ID: CachedId = CachedId::new("vortex.is_sorted.extension");
        *ID
    }

    fn storage(&self) -> Self::Storage {
        IsSorted
    }

    fn aggregate_storage(
        &self,
        options: &IsSortedOptions,
        storage: &ArrayRef,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Scalar> {
        let sorted = if options.strict {
            is_strict_sorted(storage, ctx)?
        } else {
            is_sorted(storage, ctx)?
        };
        Ok(sorted.into())
    }
}

pub(super) fn check_extension_sorted(
    array: &ExtensionArray,
    strict: bool,
    ctx: &mut ExecutionCtx,
) -> VortexResult<bool> {
    let options = IsSortedOptions { strict };
    bool::try_from(&ExtensionDelegate::new(IsSortedLifter).aggregate(&options, array, ctx)?)
}
