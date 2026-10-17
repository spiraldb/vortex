// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Allocation-free typed lane traversal and Boolean packing.
//!
//! Hosts retain buffer ownership and lend validated byte or element slices to these kernels.

#![deny(missing_docs)]

pub mod bit;
pub mod lane_kernels;
