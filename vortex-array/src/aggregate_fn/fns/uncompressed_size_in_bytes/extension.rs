// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use vortex_error::VortexResult;
use vortex_session::registry::CachedId;

use super::UncompressedSizeInBytes;
use super::UncompressedSizeOpts;
use super::uncompressed_size_in_bytes_u64;
use crate::ArrayRef;
use crate::ExecutionCtx;
use crate::aggregate_fn::AggregateFnId;
use crate::aggregate_fn::ExtensionDelegate;
use crate::aggregate_fn::StorageLifter;
use crate::arrays::ExtensionArray;
use crate::scalar::Scalar;

/// Lifts [`UncompressedSizeInBytes`] over an extension's storage, which holds all its buffers.
#[derive(Clone, Debug)]
pub(super) struct UncompressedSizeInBytesLifter;

impl StorageLifter for UncompressedSizeInBytesLifter {
    type Storage = UncompressedSizeInBytes;

    fn id(&self) -> AggregateFnId {
        static ID: CachedId = CachedId::new("vortex.uncompressed_size_in_bytes.extension");
        *ID
    }

    fn storage(&self) -> Self::Storage {
        UncompressedSizeInBytes
    }

    fn aggregate_storage(
        &self,
        options: &UncompressedSizeOpts,
        storage: &ArrayRef,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Scalar> {
        Ok(uncompressed_size_in_bytes_u64(storage, *options, ctx)?.into())
    }
}

pub(super) fn extension_uncompressed_size_in_bytes(
    array: &ExtensionArray,
    opts: UncompressedSizeOpts,
    ctx: &mut ExecutionCtx,
) -> VortexResult<u64> {
    u64::try_from(
        &ExtensionDelegate::new(UncompressedSizeInBytesLifter).aggregate(&opts, array, ctx)?,
    )
}
