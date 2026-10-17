// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Indexed lane sources and output traversal.
//!
//! Kernels write caller-owned storage and do not allocate.

pub mod source;
pub use source::IndexedSource;
pub use source::LaneZip;

pub mod map_into;
pub use map_into::IndexedSourceExt;

pub(crate) const CHUNK_LEN: usize = 64;
