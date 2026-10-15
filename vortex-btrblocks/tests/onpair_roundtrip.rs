// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors
//
//! Configurable OnPair indexes through the cascading compressor.

#![allow(clippy::cast_possible_truncation, clippy::tests_outside_test_module)]

use std::sync::LazyLock;

use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::VarBinViewArray;
use vortex_array::dtype::DType;
use vortex_array::dtype::Nullability;
use vortex_btrblocks::BtrBlocksCompressorBuilder;
use vortex_btrblocks::schemes::string::OnPairScheme;
use vortex_error::VortexResult;
use vortex_onpair::OnPair;
use vortex_onpair::OnPairArrayExt;
use vortex_onpair::OnPairArraySlotsExt;
use vortex_session::VortexSession;

static SESSION: LazyLock<VortexSession> = LazyLock::new(|| {
    let session = vortex_array::array_session();
    vortex_onpair::initialize(&session);
    session
});

const UNINDEXED_ONPAIR: OnPairScheme = OnPairScheme::new();
const FREQUENCY_INDEXED_ONPAIR: OnPairScheme = OnPairScheme::new().with_token_frequency_index();

/// Helper: synthetic short-string corpus that the cascading compressor should
/// route through OnPair.
fn corpus(n: usize) -> Vec<String> {
    let templates: &[&str] = &[
        "https://www.example.com/products/{id}",
        "https://cdn.example.com/img/{id}.webp",
        "https://api.example.com/v2/orders/{id}",
        "https://www.example.com/users/{id}/profile",
        "INFO  request_id={id} status=200 method=GET",
        "WARN  request_id={id} status=429 method=POST",
        "ERROR request_id={id} status=500 method=PUT",
    ];
    let mut out = Vec::with_capacity(n);
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    for _ in 0..n {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let pick = (state as usize) % templates.len();
        let id = state as u32;
        out.push(templates[pick].replace("{id}", &format!("{:08x}", id)));
    }
    out
}

#[test]
fn onpair_indexes_are_configurable() -> VortexResult<()> {
    let strings = corpus(4096);
    let array = VarBinViewArray::from_iter(
        strings.iter().map(|string| Some(string.as_str())),
        DType::Utf8(Nullability::NonNullable),
    )
    .into_array();
    let compressor = BtrBlocksCompressorBuilder::empty()
        .with_new_scheme(&FREQUENCY_INDEXED_ONPAIR)
        .build();
    let mut ctx = SESSION.create_execution_ctx();
    let compressed = compressor.compress(&array, &mut ctx)?;
    let onpair = compressed.try_downcast::<OnPair>().map_err(|array| {
        vortex_error::vortex_err!("expected OnPair, got {}", array.encoding_id())
    })?;
    let frequencies = onpair
        .token_frequency_index(&mut ctx)?
        .ok_or_else(|| vortex_error::vortex_err!("missing token-frequency index"))?;

    assert_eq!(
        frequencies.cumulative().last().copied(),
        Some(u32::try_from(onpair.codes().len())?)
    );

    let compressor = BtrBlocksCompressorBuilder::empty()
        .with_new_scheme(&UNINDEXED_ONPAIR)
        .build();
    let compressed = compressor.compress(&array, &mut ctx)?;
    let onpair = compressed.try_downcast::<OnPair>().map_err(|array| {
        vortex_error::vortex_err!("expected OnPair, got {}", array.encoding_id())
    })?;
    assert!(onpair.token_frequency_index_child().is_none());
    Ok(())
}

