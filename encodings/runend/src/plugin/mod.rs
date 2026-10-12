// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! ArrayPlugin implementation for RunEnd.

use vortex_array::ArrayDeserialization;
use vortex_array::ArrayId;
use vortex_array::ArrayPlugin;
use vortex_array::ArrayRef;
use vortex_array::ArraySerialization;
use vortex_array::VTable;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_err;
use vortex_session::VortexSession;
use vortex_session::registry::CachedId;

use crate::RunEnd;
use crate::RunEndArrayExt;

#[cfg(test)]
mod tests;
mod v1;
mod v2;

/// The frozen RunEnd serialized ID, which stores the value of every run as a child. It is also
/// the in-memory RunEnd ID.
pub fn runend_v1_id() -> ArrayId {
    VTable::id(&RunEnd)
}

/// The serialized ID for boolean RunEnd arrays whose values alternate between runs. It stores
/// only the value of the first run, plus the per-run validity of nullable arrays.
pub fn runend_v2_id() -> ArrayId {
    static ID: CachedId = CachedId::new("vortex.runend.v2");
    *ID
}

/// Serde for the [`RunEnd`] array.
///
/// Arrays with explicit values serialize as `vortex.runend`. Boolean arrays whose values
/// alternate, see [`RunEndArrayExt::alternating_start`], serialize as `vortex.runend.v2`. The
/// serialized ID follows the in-memory form, so a writer that may not emit `vortex.runend.v2`
/// must not produce alternating arrays.
///
/// Register this plugin, or call [`crate::initialize`], to enable serde. Direct registration of
/// [`RunEnd`] does not support serde.
#[derive(Clone, Debug)]
pub struct RunEndPlugin;

impl ArrayPlugin for RunEndPlugin {
    fn id(&self) -> ArrayId {
        VTable::id(&RunEnd)
    }

    fn serialized_ids(&self) -> Vec<ArrayId> {
        vec![runend_v1_id(), runend_v2_id()]
    }

    fn serialize(
        &self,
        array: &ArrayRef,
        _session: &VortexSession,
    ) -> VortexResult<Option<ArraySerialization>> {
        let view = array.as_opt::<RunEnd>().ok_or_else(|| {
            vortex_err!("RunEnd plugin cannot serialize {}", array.encoding_id())
        })?;
        Ok(Some(match view.alternating_start() {
            Some(start) => v2::serialize(view, start)?,
            None => v1::serialize(view, array)?,
        }))
    }

    fn deserialize(
        &self,
        parts: ArrayDeserialization<'_>,
        session: &VortexSession,
    ) -> VortexResult<ArrayRef> {
        if parts.serialized_id == runend_v1_id() {
            v1::deserialize(parts)
        } else if parts.serialized_id == runend_v2_id() {
            v2::deserialize(parts, session)
        } else {
            vortex_bail!(
                "RunEnd plugin does not recognize serialized ID {}",
                parts.serialized_id
            )
        }
    }
}
