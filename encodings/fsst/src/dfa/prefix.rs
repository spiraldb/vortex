// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Flat `u8` transition table DFA for prefix matching (`LIKE 'prefix%'`).
//!
//! Supports prefixes up to 253 bytes (states: 0..N progress + accept + fail +
//! sentinel ≤ 256).
//!
//! TODO(joe): for short prefixes (≤13 bytes), a shift-packed `[u64; 256]`
//! representation would be simpler and easier to read — all state transitions
//! for one input byte fit in a single `u64`. Benchmarks showed no meaningful
//! perf difference, so we use flat-only for
//! now to keep the code simple and support long prefixes.

use fsst::Symbol;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;

use super::build_fused_table;
use super::build_symbol_transitions;
use super::ordered_byte_transitions;

/// Flat `u8` transition table DFA for prefix matching on FSST codes.
///
/// States 0..prefix_len track match progress, plus ACCEPT, FAIL, and an
/// escape SENTINEL. Transitions are stored in a flat `Vec<u8>` indexed as
/// `[state * 256 + byte]`.
///
/// ```text
/// Prefix: "http"  (4 progress states + accept + fail)
///
///          Input byte
/// State    'h'    't'    'p'    other
/// ─────    ────   ────   ────   ─────
///   0       1      F      F      F     ← want 'h'
///   1       F      2      F      F     ← want 't'
///   2       F      3      F      F     ← want 't'
///   3       F      F      4✓     F     ← want 'p'
///   4✓      4✓     4✓     4✓     4✓    ← accept (sticky)
///   F       F      F      F      F     ← fail (sticky)
///
/// Escape handling: code 255 → sentinel → read next literal byte → byte table
/// ```
pub(crate) struct FlatPrefixDfa {
    /// `transitions[state * 256 + byte]` -> next state.
    transitions: Vec<u8>,
    /// `escape_transitions[state * 256 + byte]` -> next state for escaped bytes.
    escape_transitions: Vec<u8>,
    accept_state: u8,
    fail_state: u8,
    sentinel: u8,
}

impl FlatPrefixDfa {
    pub(crate) const MAX_PREFIX_LEN: usize = (u8::MAX - 2) as usize;

    pub(crate) fn new(
        symbols: &[Symbol],
        symbol_lengths: &[u8],
        prefix: &[u8],
    ) -> VortexResult<Self> {
        if prefix.len() > Self::MAX_PREFIX_LEN {
            vortex_bail!(
                "prefix length {} exceeds maximum {} for flat prefix DFA",
                prefix.len(),
                Self::MAX_PREFIX_LEN
            );
        }

        let accept_state = u8::try_from(prefix.len()).vortex_expect("prefix fits in u8");
        let fail_state = accept_state + 1;
        Self::from_byte_table(
            symbols,
            symbol_lengths,
            accept_state,
            build_prefix_byte_table(prefix, accept_state, fail_state),
        )
    }

    /// Builds a DFA for an anchored prefix followed by ordered literal pieces.
    pub(crate) fn new_ordered(
        symbols: &[Symbol],
        symbol_lengths: &[u8],
        needles: &[&[u8]],
    ) -> VortexResult<Self> {
        let total_len = needles.iter().map(|needle| needle.len()).sum::<usize>();
        if total_len > Self::MAX_PREFIX_LEN {
            vortex_bail!("ordered prefix literals exceed the flat DFA state space");
        }
        Self::from_byte_table(
            symbols,
            symbol_lengths,
            u8::try_from(total_len).vortex_expect("bounded DFA state"),
            ordered_byte_transitions(needles, true),
        )
    }

    fn from_byte_table(
        symbols: &[Symbol],
        symbol_lengths: &[u8],
        accept_state: u8,
        byte_table: Vec<u8>,
    ) -> VortexResult<Self> {
        let fail_state = accept_state + 1;
        let n_states = fail_state + 1;
        let sentinel = fail_state + 1;

        let sym_trans =
            build_symbol_transitions(symbols, symbol_lengths, &byte_table, n_states, accept_state);

        let transitions = build_fused_table(
            &sym_trans,
            symbols.len(),
            n_states,
            |_| sentinel,
            fail_state,
        );

        Ok(Self {
            transitions,
            escape_transitions: byte_table,
            accept_state,
            fail_state,
            sentinel,
        })
    }

    pub(crate) fn matches(&self, codes: &[u8]) -> bool {
        let mut state = 0u8;
        let mut pos = 0;
        while pos < codes.len() {
            let code = codes[pos];
            pos += 1;
            let next = self.transitions[usize::from(state) * 256 + usize::from(code)];
            if next == self.sentinel {
                if pos >= codes.len() {
                    return false;
                }
                let b = codes[pos];
                pos += 1;
                state = self.escape_transitions[usize::from(state) * 256 + usize::from(b)];
            } else {
                state = next;
            }
            if state == self.accept_state {
                return true;
            }
            if state == self.fail_state {
                return false;
            }
        }
        state == self.accept_state
    }
}

/// Build a byte-level transition table for prefix matching.
///
/// For each state, only the correct next byte advances; everything else goes
/// to the fail state.
fn build_prefix_byte_table(prefix: &[u8], accept_state: u8, fail_state: u8) -> Vec<u8> {
    let n_states = fail_state + 1;
    let mut table = vec![fail_state; usize::from(n_states) * 256];

    for state in 0..n_states {
        let s = usize::from(state);
        if state == accept_state {
            for byte in 0..256 {
                table[s * 256 + byte] = accept_state;
            }
        } else if state != fail_state {
            // Only the correct next byte advances; everything else fails.
            let next_byte = prefix[s];
            let next_state = if s + 1 >= prefix.len() {
                accept_state
            } else {
                state + 1
            };
            table[s * 256 + usize::from(next_byte)] = next_state;
        }
    }
    table
}
