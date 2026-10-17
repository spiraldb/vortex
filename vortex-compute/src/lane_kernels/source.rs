// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Shared lane kernels, with Vortex storage conformance tests.

pub use rowfn_kernels::lane_kernels::source::*;

#[cfg(test)]
mod tests {
    use super::LaneZip;

    #[test]
    #[should_panic(expected = "LaneZip operands must have the same length")]
    fn rejects_mismatched_lengths() {
        _ = LaneZip::new(&[1_u8][..], &[2_u8, 3][..]);
    }
}
