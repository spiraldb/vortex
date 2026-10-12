// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Serde for `vortex.runend.v2`, which stores boolean runs whose values alternate.
//!
//! The values are implied by the first run's value, so only the run ends and, for nullable
//! arrays with nulls, the per-run validity are stored as children.

use prost::Message as _;
use vortex_array::ArrayDeserialization;
use vortex_array::ArrayRef;
use vortex_array::ArraySerialization;
use vortex_array::ArrayView;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::dtype::DType;
use vortex_array::dtype::Nullability;
use vortex_array::dtype::PType;
use vortex_array::validity::Validity;
use vortex_array::vtable::child_to_validity;
use vortex_array::vtable::validity_to_child;
use vortex_error::VortexResult;
use vortex_error::vortex_ensure;
use vortex_session::VortexSession;

use super::runend_v2_id;
use crate::RunEnd;
use crate::RunEndArrayExt;
use crate::RunEndArraySlotsExt;

/// Metadata for `vortex.runend.v2`. The run ends are the first child and the per-run validity,
/// when present, is the second.
#[derive(Clone, prost::Message)]
pub(super) struct RunEndV2Metadata {
    #[prost(enumeration = "PType", tag = "1")]
    pub(super) ends_ptype: i32,
    #[prost(uint64, tag = "2")]
    pub(super) num_runs: u64,
    #[prost(uint64, tag = "3")]
    pub(super) offset: u64,
    /// The value of the first run. Run `i` holds `start ^ (i % 2 == 1)`, or null.
    #[prost(bool, tag = "4")]
    pub(super) start: bool,
}

pub(super) fn serialize(
    array: ArrayView<'_, RunEnd>,
    start: bool,
) -> VortexResult<ArraySerialization> {
    let num_runs = array.ends().len();
    let metadata = RunEndV2Metadata {
        ends_ptype: PType::try_from(array.ends().dtype())? as i32,
        num_runs: num_runs as u64,
        offset: array.offset() as u64,
        start,
    };
    let mut children = vec![array.ends().clone()];
    children.extend(validity_to_child(&array.values().validity()?, num_runs));
    Ok(ArraySerialization::new(
        runend_v2_id(),
        metadata.encode_to_vec(),
        vec![],
        children,
    ))
}

pub(super) fn deserialize(
    parts: ArrayDeserialization<'_>,
    session: &VortexSession,
) -> VortexResult<ArrayRef> {
    vortex_ensure!(
        parts.dtype.is_boolean(),
        "{} requires a boolean dtype, got {}",
        runend_v2_id(),
        parts.dtype
    );
    vortex_ensure!(
        parts.buffers.is_empty(),
        "RunEndArray expects 0 buffers, got {}",
        parts.buffers.len()
    );
    let nullability = parts.dtype.nullability();
    let max_children = match nullability {
        Nullability::NonNullable => 1,
        Nullability::Nullable => 2,
    };
    vortex_ensure!(
        (1..=max_children).contains(&parts.children.len()),
        "Expected 1 to {max_children} children for {} of {}, got {}",
        runend_v2_id(),
        parts.dtype,
        parts.children.len()
    );

    let metadata = RunEndV2Metadata::decode(parts.metadata)?;
    let ends_dtype = DType::Primitive(metadata.ends_ptype(), Nullability::NonNullable);
    let runs = usize::try_from(metadata.num_runs)?;
    let ends = parts.children.get(0, &ends_dtype, runs)?;
    let validity_child = (parts.children.len() == 2)
        .then(|| parts.children.get(1, &Validity::DTYPE, runs))
        .transpose()?;
    let validity = child_to_validity(validity_child.as_ref(), nullability);
    Ok(RunEnd::try_new_alternating_offset_length(
        ends,
        metadata.start,
        validity,
        usize::try_from(metadata.offset)?,
        parts.len,
        &mut session.create_execution_ctx(),
    )?
    .into_array())
}
