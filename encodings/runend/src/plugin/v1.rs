// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Serde for `vortex.runend`, which stores the run ends and the value of every run as children.

use prost::Message as _;
use vortex_array::Array;
use vortex_array::ArrayDeserialization;
use vortex_array::ArrayParts;
use vortex_array::ArrayRef;
use vortex_array::ArraySerialization;
use vortex_array::ArrayView;
use vortex_array::IntoArray;
use vortex_array::dtype::DType;
use vortex_array::dtype::Nullability;
use vortex_array::dtype::PType;
use vortex_error::VortexResult;
use vortex_error::vortex_ensure;
use vortex_error::vortex_ensure_eq;

use super::runend_v1_id;
use crate::RunEnd;
use crate::RunEndArrayExt;
use crate::RunEndArraySlotsExt;
use crate::RunEndData;
use crate::RunEndMetadata;
use crate::RunEndSlots;

pub(super) fn serialize(
    array: ArrayView<'_, RunEnd>,
    array_ref: &ArrayRef,
) -> VortexResult<ArraySerialization> {
    let metadata = RunEndMetadata {
        ends_ptype: PType::try_from(array.ends().dtype())? as i32,
        num_runs: array.ends().len() as u64,
        offset: array.offset() as u64,
    };
    Ok(ArraySerialization::from_array(
        runend_v1_id(),
        array_ref,
        metadata.encode_to_vec(),
    ))
}

pub(super) fn deserialize(parts: ArrayDeserialization<'_>) -> VortexResult<ArrayRef> {
    vortex_ensure!(
        parts.buffers.is_empty(),
        "RunEndArray expects 0 buffers, got {}",
        parts.buffers.len()
    );
    vortex_ensure_eq!(
        parts.children.len(),
        2,
        "Expected 2 children for {}",
        runend_v1_id()
    );
    let metadata = RunEndMetadata::decode(parts.metadata)?;
    let ends_dtype = DType::Primitive(metadata.ends_ptype(), Nullability::NonNullable);
    let runs = usize::try_from(metadata.num_runs)?;
    let ends = parts.children.get(0, &ends_dtype, runs)?;
    let values = parts.children.get(1, parts.dtype, runs)?;
    let offset = usize::try_from(metadata.offset)?;
    let slots = RunEndSlots { ends, values }.into_slots();
    Ok(Array::<RunEnd>::try_from_parts(ArrayParts::new(
        RunEnd,
        parts.dtype.clone(),
        parts.len,
        RunEndData::new(offset),
        slots,
    ))?
    .into_array())
}
