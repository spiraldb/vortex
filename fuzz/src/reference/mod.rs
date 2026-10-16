// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Reference fuzzing: every array encoding is checked against a plain Rust oracle.
//!
//! A single fuzz target covers all encodings. [`EncodingKind`] has one generator per encoding,
//! chosen uniformly by the fuzzer, and its tests assert that every encoding registered in the
//! session is generated. Each generator returns the encoded array along with plain Rust
//! [`Value`]s, and every operation is applied to both and compared.

pub mod encodings;
pub mod generate;
pub mod ops;
pub mod value;

use arbitrary::Arbitrary;
use arbitrary::Unstructured;
pub use encodings::Case;
pub use encodings::EncodingKind;
use encodings::Limits;
use ops::Outcome;
use ops::RefAction;
use strum::IntoEnumIterator;
use tracing::debug;
pub use value::Value;
pub use value::decode;
pub use value::from_scalar;
use vortex_array::ArrayRef;
use vortex_array::VortexSessionExecute;
use vortex_error::VortexResult;
use vortex_error::vortex_err;

use crate::SESSION;
use crate::error::Backtrace;
use crate::error::VortexFuzzError;
use crate::error::VortexFuzzResult;

/// Fuzz input: an encoded array with its reference values and a chain of actions.
#[derive(Debug)]
pub struct FuzzArrayReference {
    pub case: Case,
    pub actions: Vec<RefAction>,
}

impl<'a> Arbitrary<'a> for FuzzArrayReference {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let kinds = EncodingKind::iter().collect::<Vec<_>>();
        let kind = *u.choose(&kinds)?;
        let mut ctx = SESSION.create_execution_ctx();
        let case = kind.generate(u, None, Limits::default(), &mut ctx)?;

        let mut len = case.values.len();
        let n_actions = u.int_in_range(0..=4)?;
        let mut actions = Vec::with_capacity(n_actions);
        for _ in 0..n_actions {
            let action = RefAction::arbitrary(u, len)?;
            len = match &action {
                RefAction::Slice(r) => r.len(),
                RefAction::Take(i) => i.len(),
                RefAction::Filter(m) => m.iter().filter(|b| **b).count(),
                RefAction::ScalarAt(_) => len,
            };
            actions.push(action);
        }
        Ok(Self { case, actions })
    }
}

/// Run the reference fuzzer.
///
/// Returns:
/// - `Ok(true)` - keep in corpus
/// - `Ok(false)` - reject from corpus
/// - `Err(_)` - a mismatch against the reference was found
#[expect(clippy::result_large_err)]
pub fn run_array_reference(fuzz: FuzzArrayReference) -> VortexFuzzResult<bool> {
    let FuzzArrayReference { case, actions } = fuzz;
    let mut ctx = SESSION.create_execution_ctx();
    debug!(kind = ?case.kind, tree = %case.array.display_tree(), "reference input");

    let mut array = case.array;
    let mut values = case.values;
    check_array(&array, &values, "canonicalize", &mut ctx).map_err(err)?;

    for (step, action) in actions.into_iter().enumerate() {
        debug!(step, ?action);
        let label = format!("step {step}: {action:?}");
        let actual = action.vortex(&array, &mut ctx).map_err(err)?;
        match (action.reference(&values), actual) {
            (Outcome::Array(expected), Outcome::Array(actual)) => {
                check_array(&actual, &expected, &label, &mut ctx).map_err(err)?;
                array = actual;
                values = expected;
            }
            (Outcome::Scalars(expected), Outcome::Scalars(actual)) => {
                check_values(&actual, &expected, &label).map_err(err)?;
            }
            _ => unreachable!("reference and vortex outcomes always have the same shape"),
        }
    }
    Ok(true)
}

fn err(e: vortex_error::VortexError) -> VortexFuzzError {
    VortexFuzzError::VortexError(e, Backtrace::capture())
}

/// Decode `array` and compare it with the expected reference values.
pub fn check_array(
    array: &ArrayRef,
    expected: &[Value],
    label: &str,
    ctx: &mut vortex_array::ExecutionCtx,
) -> VortexResult<()> {
    let actual = decode(array, ctx)?;
    check_values(&actual, expected, label)
        .map_err(|e| vortex_err!("{e}\narray:\n{}", array.display_tree()))
}

/// Compare decoded values with the expected reference values.
pub fn check_values(actual: &[Value], expected: &[Value], label: &str) -> VortexResult<()> {
    if actual.len() != expected.len() {
        return Err(vortex_err!(
            "{label}: length mismatch, expected {} got {}",
            expected.len(),
            actual.len()
        ));
    }
    if let Some((idx, (a, e))) = actual
        .iter()
        .zip(expected)
        .enumerate()
        .find(|(_, (a, e))| a != e)
    {
        return Err(vortex_err!(
            "{label}: mismatch at index {idx}, expected {e} got {a}"
        ));
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// Deterministic pseudo-random bytes so the generators can be driven without libFuzzer.
    pub(crate) fn test_bytes(seed: u64) -> Vec<u8> {
        let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        (0..4096)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state.to_le_bytes()[3]
            })
            .collect()
    }

    /// Drive the fuzz entry point with deterministic inputs, as a quick smoke test without
    /// libFuzzer.
    #[test]
    fn smoke() {
        let mut failures = Vec::new();
        let seeds: u64 = std::env::var("REFERENCE_SMOKE_SEEDS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(500);
        for seed in 0..seeds {
            let data = test_bytes(seed);
            let Ok(input) = FuzzArrayReference::arbitrary(&mut Unstructured::new(&data)) else {
                continue;
            };
            let kind = input.case.kind;
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                run_array_reference(input).map_err(|e| e.to_string())
            }));
            match result {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => failures.push(format!("seed {seed} {kind:?}: {e}")),
                Err(e) => failures.push(format!(
                    "seed {seed} {kind:?}: panicked: {}",
                    e.downcast_ref::<String>()
                        .map(String::as_str)
                        .or_else(|| e.downcast_ref::<&str>().copied())
                        .unwrap_or_default()
                        .lines()
                        .next()
                        .unwrap_or_default()
                )),
            }
        }
        assert!(failures.is_empty(), "{}", failures.join("\n\n"));
    }
}
