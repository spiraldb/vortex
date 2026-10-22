// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! [`ArrayPlugin`] implementation for Pco that handles both wire formats.

use prost::Message;
use vortex_array::Array;
use vortex_array::ArrayDeserialization;
use vortex_array::ArrayId;
use vortex_array::ArrayParts;
use vortex_array::ArrayPlugin;
use vortex_array::ArrayRef;
use vortex_array::ArraySerialization;
use vortex_array::ArrayVTable;
use vortex_array::IntoArray;
use vortex_array::vtable::validity_to_child;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_ensure_eq;
use vortex_error::vortex_err;
use vortex_session::VortexSession;
use vortex_session::registry::CachedId;

use crate::Pco;
use crate::PcoArray;
use crate::PcoData;
use crate::PcoMetadata;
use crate::PcoSlots;
use crate::PcoV2Metadata;
use crate::array::PcoIndex;
use crate::array::deserialize_validity;
use crate::array::host_buffers;

/// The frozen Pco serialized ID: chunks of pages, no random-access index.
pub fn pco_v1_id() -> ArrayId {
    static ID: CachedId = CachedId::new("vortex.pco");
    *ID
}

/// The draft Pco serialized ID for arrays with a random-access index.
pub fn pco_v2_id() -> ArrayId {
    static ID: CachedId = CachedId::new("vortex.pco.v2");
    *ID
}

/// Serde for the [`Pco`] array using the frozen v1 and draft v2 wire formats.
///
/// Arrays without a random-access index serialize as `vortex.pco`, so older readers can read
/// them; arrays with one serialize as `vortex.pco.v2`. Call [`PcoData::without_index`] to write
/// an indexed array for readers that predate v2.
///
/// Register this plugin, or call [`crate::initialize`], to enable both formats. Direct
/// registration of [`Pco`] supports only v1.
#[derive(Clone, Debug)]
pub struct PcoPlugin;

impl ArrayPlugin for PcoPlugin {
    fn id(&self) -> ArrayId {
        ArrayVTable::id(&Pco)
    }

    fn serialized_ids(&self) -> Vec<ArrayId> {
        vec![pco_v1_id(), pco_v2_id()]
    }

    fn serialize(
        &self,
        array: &ArrayRef,
        _session: &VortexSession,
    ) -> VortexResult<Option<ArraySerialization>> {
        let view = array
            .as_opt::<Pco>()
            .ok_or_else(|| vortex_err!("Pco plugin cannot serialize {}", array.encoding_id()))?;
        let data = view.data();
        let serialization = match &data.index {
            None => ArraySerialization::new(
                pco_v1_id(),
                data.metadata.encode_to_vec(),
                data.buffers().cloned().collect(),
                array.children(),
            ),
            Some(index) => {
                let metadata = PcoV2Metadata {
                    header: data.metadata.header.clone(),
                    chunks: data.metadata.chunks.clone(),
                    checkpoint_intervals: index.checkpoint_intervals.clone(),
                    block_stats: index.block_stats.is_some(),
                };
                ArraySerialization::new(
                    pco_v2_id(),
                    metadata.encode_to_vec(),
                    data.buffers().cloned().collect(),
                    array.children(),
                )
            }
        };
        Ok(Some(serialization))
    }

    fn deserialize(
        &self,
        parts: ArrayDeserialization<'_>,
        session: &VortexSession,
    ) -> VortexResult<ArrayRef> {
        if parts.serialized_id == pco_v1_id() {
            let parts = <Pco as ArrayVTable>::deserialize(
                &Pco,
                parts.dtype,
                parts.len,
                parts.metadata,
                parts.buffers,
                parts.children,
                session,
            )?;
            Ok(Array::<Pco>::try_from_parts(parts)?.into_array())
        } else if parts.serialized_id == pco_v2_id() {
            Ok(deserialize_v2(parts)?.into_array())
        } else {
            vortex_bail!(
                "Pco plugin does not recognize serialized ID {}",
                parts.serialized_id
            )
        }
    }
}

fn deserialize_v2(parts: ArrayDeserialization<'_>) -> VortexResult<PcoArray> {
    let metadata = PcoV2Metadata::decode(parts.metadata)?;
    let validity = deserialize_validity(parts.dtype, parts.len, parts.children)?;

    let n_chunks = metadata.chunks.len();
    let n_pages = metadata
        .chunks
        .iter()
        .map(|chunk| chunk.pages.len())
        .sum::<usize>();
    let expected = 2 * n_chunks + n_pages + usize::from(metadata.block_stats);
    vortex_ensure_eq!(
        parts.buffers.len(),
        expected,
        "Pco v2 expected {expected} buffers"
    );
    let mut buffers = host_buffers(parts.buffers)?.into_iter();
    let chunk_metas = buffers.by_ref().take(n_chunks).collect();
    let pages = buffers.by_ref().take(n_pages).collect();
    let checkpoints = buffers.by_ref().take(n_chunks).collect();
    let block_stats = buffers.next();

    let index = PcoIndex {
        checkpoint_intervals: metadata.checkpoint_intervals,
        checkpoints,
        block_stats,
    };
    let data = PcoData::try_new(
        chunk_metas,
        pages,
        parts.dtype.as_ptype(),
        PcoMetadata {
            header: metadata.header,
            chunks: metadata.chunks,
        },
        Some(index),
        parts.len,
    )?;
    let slots = PcoSlots {
        validity: validity_to_child(&validity, parts.len),
    }
    .into_slots();
    // `try_from_parts` validates the data and the index before publishing the array.
    Array::try_from_parts(ArrayParts::new(
        Pco,
        parts.dtype.clone(),
        parts.len,
        data,
        slots,
    ))
}
