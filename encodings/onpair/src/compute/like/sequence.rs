// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use memchr::memmem;
use onpair::CompactDictionaryView;
use onpair::search;
use onpair::search::Finder;
use onpair::search::PrefilterAnalysis;
use onpair::search::Verify;
use vortex_array::ArrayView;
use vortex_array::ExecutionCtx;
use vortex_error::VortexResult;
use vortex_error::vortex_err;

use super::Matches;
use crate::OnPair;
use crate::decode::CodesWindow;
use crate::decode::collect_codes_window;
use crate::index::token_frequency_index;

pub(super) fn contains_sequence(
    array: ArrayView<'_, OnPair>,
    dict: CompactDictionaryView<'_>,
    fragments: &[Vec<u8>],
    ctx: &mut ExecutionCtx,
) -> VortexResult<Option<Matches>> {
    let Some(frequencies) = token_frequency_index(array, ctx)? else {
        return Ok(None);
    };
    // Every match contains every fragment. Use the most selective cover, then verify
    // ordering and non-overlap on its candidates; a superset alone is not a LIKE result.
    let Some(analysis) = fragments
        .iter()
        .filter(|fragment| fragment.len() <= search::MAX_PATTERN_LEN)
        .map(|fragment| search::analyze_prefilter(fragment, dict, frequencies, array.len()))
        .min_by_key(PrefilterAnalysis::covered_frequency)
    else {
        return Ok(None);
    };
    let finders: Vec<_> = fragments
        .iter()
        .map(|fragment| memmem::Finder::new(fragment))
        .collect();
    let window = collect_codes_window(array, ctx)?;
    let rows = match &window {
        CodesWindow::U32(window) => {
            scan_sequence(window.as_column_view(dict), &analysis, &finders)?
        }
        CodesWindow::U64(window) => {
            scan_sequence(window.as_column_view(dict), &analysis, &finders)?
        }
    };
    Ok(Some(Matches::from_indices(array.len(), rows)))
}

fn scan_sequence<O: onpair::Offset>(
    view: onpair::ColumnView<'_, O>,
    analysis: &PrefilterAnalysis,
    finders: &[memmem::Finder<'_>],
) -> VortexResult<Vec<usize>> {
    let mut rows = Vec::new();
    Finder::new().find(view, analysis, Verify::Superset, None, &mut rows);
    let mut decoded = Vec::<u8>::new();
    let mut kept = 0;
    for index in 0..rows.len() {
        let row = rows[index];
        let codes = view.row_codes(row);
        decoded.clear();
        // Bound the output without walking the dictionary once to size it and again
        // to decode. Keep this scratch buffer across all candidate rows.
        decoded.reserve(codes.len().saturating_mul(onpair::MAX_TOKEN_SIZE));
        let written = onpair::try_decode_into(codes, view.dict, decoded.spare_capacity_mut())
            .map_err(|error| vortex_err!("OnPair LIKE candidate decoding failed: {error}"))?;
        // SAFETY: try_decode_into initialized exactly the first `written` bytes.
        unsafe { decoded.set_len(written) };
        if matches_sequence(&decoded, finders) {
            rows[kept] = row;
            kept += 1;
        }
    }
    rows.truncate(kept);
    Ok(rows)
}

fn matches_sequence(mut remaining: &[u8], finders: &[memmem::Finder<'_>]) -> bool {
    for finder in finders {
        let Some(start) = finder.find(remaining) else {
            return false;
        };
        remaining = &remaining[start + finder.needle().len()..];
    }
    true
}
