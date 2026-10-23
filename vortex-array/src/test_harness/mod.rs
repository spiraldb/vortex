// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::io::Write;

use goldenfile::Mint;
use goldenfile::differs::binary_diff;
use itertools::Itertools;
use vortex_error::VortexError;
use vortex_error::VortexResult;

use crate::ExecutionCtx;
use crate::arrays::BoolArray;
use crate::arrays::bool::BoolArrayExt;

#[cfg(not(codspeed))]
pub mod trace;

/// Check that a named metadata matches its previous versioning.
///
/// Goldenfile takes care of checking for equality against a checked-in file.
#[expect(clippy::unwrap_used)]
pub fn check_metadata(name: &str, metadata: &[u8]) {
    let mut mint = Mint::new("goldenfiles/");
    let mut f = mint
        .new_goldenfile_with_differ(name, Box::new(binary_diff))
        .unwrap();
    f.write_all(metadata).unwrap();
}

/// Asserts that `result` is a [`VortexError::Serde`] error whose message is exactly `expected`.
///
/// A read path reports malformed stored data as a `Serde` error. A substring match on the
/// message cannot show that the error has this category or that it ends with the right raw
/// value, so tests of rejected stored values use this helper instead.
#[track_caller]
#[expect(clippy::panic)]
pub fn assert_serde_error<T>(result: VortexResult<T>, expected: &str) {
    match result {
        Err(VortexError::Serde(message, _)) => assert_eq!(&*message, expected),
        Err(err) => panic!("expected a Serde error, got {err}"),
        Ok(_) => panic!("expected a Serde error, got Ok"),
    }
}

/// Outputs the indices of the true values in a BoolArray
pub fn to_int_indices(indices_bits: BoolArray, ctx: &mut ExecutionCtx) -> VortexResult<Vec<u64>> {
    let buffer = indices_bits.to_bit_buffer();
    let mask = indices_bits
        .as_ref()
        .validity()?
        .execute_mask(indices_bits.as_ref().len(), ctx)?;
    Ok(buffer
        .iter()
        .enumerate()
        .filter_map(|(idx, v)| (v && mask.value(idx)).then_some(idx as u64))
        .collect_vec())
}
