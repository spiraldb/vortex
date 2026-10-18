// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! The compact preset writes Pco v2 when the session enables the `pco` edition, and the frozen v1
//! format otherwise.

#![cfg(feature = "pco")]

use rstest::rstest;
use vortex_array::ArrayContext;
use vortex_array::ArrayId;
use vortex_array::ArrayRef;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::assert_arrays_eq;
use vortex_array::serde::SerializeOptions;
use vortex_array::serde::SerializedArray;
use vortex_btrblocks::BtrBlocksCompressorBuilder;
use vortex_buffer::ByteBufferMut;
use vortex_edition::EDITION_DECLARATIONS;
use vortex_edition::EDITION_FAMILIES;
use vortex_edition::EditionSession;
use vortex_edition::EditionSessionExt;
use vortex_edition::declarations::core::CORE_2026_08_3;
use vortex_error::VortexResult;
use vortex_pco::editions::PCO_2026_10;
use vortex_pco::pco_v1_id;
use vortex_pco::pco_v2_id;
use vortex_session::VortexSession;
use vortex_session::registry::ReadContext;

/// A session with the latest core edition, and the `pco` edition when `pco_edition` is set.
fn session(pco_edition: bool) -> VortexResult<VortexSession> {
    let session = vortex_array::array_session().with::<EditionSession>();
    for family in EDITION_FAMILIES {
        session.editions().declare_family(family)?;
    }
    for declaration in EDITION_DECLARATIONS {
        session.register_edition(declaration)?;
    }
    vortex_fastlanes::initialize(&session);
    vortex_pco::initialize(&session);
    session.enable_edition(CORE_2026_08_3)?;
    if pco_edition {
        session.enable_edition(PCO_2026_10)?;
    }
    Ok(session)
}

/// Sorted timestamps with jitter, which Pco compresses far better than FoR and bit-packing.
fn timestamps() -> ArrayRef {
    let mut t = 1_700_000_000_000_000i64;
    PrimitiveArray::from_iter((0..50_000i64).map(|i| {
        t += 1_000_000 + (i * 7919) % 1000;
        t
    }))
    .into_array()
}

/// Compresses with the compact preset, round trips the result through serialization, and returns
/// the serialized IDs.
fn compact_roundtrip(session: &VortexSession, array: &ArrayRef) -> VortexResult<Vec<ArrayId>> {
    let mut ctx = session.create_execution_ctx();
    let compressed = BtrBlocksCompressorBuilder::from_session(session)
        .with_compact()
        .build()
        .compress(array, &mut ctx)?;
    assert_arrays_eq!(compressed, array, &mut ctx);

    let array_ctx = ArrayContext::empty();
    let mut bytes = ByteBufferMut::empty();
    for buffer in compressed.serialize(&array_ctx, session, &SerializeOptions::default())? {
        bytes.extend_from_slice(&buffer);
    }
    let read = SerializedArray::try_from(bytes.freeze())?.decode(
        array.dtype(),
        array.len(),
        &ReadContext::new(array_ctx.to_ids()),
        session,
    )?;
    assert_arrays_eq!(read, array, &mut ctx);
    Ok(array_ctx.to_ids())
}

#[rstest]
#[case::core_only(false, pco_v1_id())]
#[case::with_pco_edition(true, pco_v2_id())]
fn compact_preset_follows_pco_edition(
    #[case] pco_edition: bool,
    #[case] expected: ArrayId,
) -> VortexResult<()> {
    let ids = compact_roundtrip(&session(pco_edition)?, &timestamps())?;
    assert!(ids.contains(&expected), "{ids:?} lacks {expected}");
    let other = if pco_edition {
        pco_v1_id()
    } else {
        pco_v2_id()
    };
    assert!(!ids.contains(&other), "{ids:?} contains {other}");
    Ok(())
}
