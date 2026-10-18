// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Pco with mid-page checkpoints: compress with large pages, decode small blocks.
//!
//! [`PcoArray`](crate::PcoArray) can only start decoding at the beginning of a pco page, so its
//! random-access granularity is its page size, and small pages cost a page header plus a Vortex
//! buffer each. [`Pco2Array`] instead writes one pco page per chunk (262,144 values by default)
//! and records the decoder state every `checkpoint_interval` values (1,024 by default) in a
//! separate index buffer. Reads that need only part of a page resume pco's decoder at the nearest
//! checkpoint, so slices, scalar reads, `take`, and `filter` decode only the blocks they touch.
//! See the `checkpoint` module for how the decoder state is captured and resumed.
//!
//! Each block also stores its minimum and maximum (unless disabled), which lets comparisons
//! against a constant answer whole blocks without decoding them.
//!
//! Pco2 is experimental and is not part of the default file encodings. Register it with
//! [`initialize`]:
//!
//! ```rust
//! let session = vortex_array::array_session();
//! vortex_pco::pco2::initialize(&session);
//! ```

mod array;
mod checkpoint;
mod compute;
mod probe;
#[cfg(test)]
mod tests;

pub use array::*;
use vortex_array::ArrayVTable;
use vortex_array::arrays::Dict;
use vortex_array::arrays::Filter;
use vortex_array::arrays::dict::TakeExecuteAdaptor;
use vortex_array::arrays::filter::FilterExecuteAdaptor;
use vortex_array::optimizer::kernels::ArrayKernelsExt;
use vortex_array::scalar_fn::ScalarFnVTable;
use vortex_array::scalar_fn::fns::binary::Binary;
use vortex_array::scalar_fn::fns::binary::CompareExecuteAdaptor;
use vortex_array::session::ArraySessionExt;
use vortex_session::VortexSession;

/// Register the Pco2 encoding and its compute kernels in the given session.
pub fn initialize(session: &VortexSession) {
    session.arrays().register(Pco2);
    let kernels = session.kernels();
    kernels.register_execute_parent_kernel(Binary.id(), Pco2, CompareExecuteAdaptor(Pco2));
    kernels.register_execute_parent_kernel(Filter.id(), Pco2, FilterExecuteAdaptor(Pco2));
    kernels.register_execute_parent_kernel(Dict.id(), Pco2, TakeExecuteAdaptor(Pco2));
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
/// Metadata for one pco page of a [`Pco2Array`].
pub struct Pco2PageInfo {
    /// Number of valid primitive values stored in this page.
    #[prost(uint32, tag = "1")]
    pub n_values: u32,
    /// Length of this page in its chunk's page buffer.
    #[prost(uint32, tag = "2")]
    pub n_bytes: u32,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
/// Metadata for one pco chunk of a [`Pco2Array`].
pub struct Pco2ChunkInfo {
    /// Pages of this chunk, stored back to back in the chunk's page buffer.
    #[prost(message, repeated, tag = "1")]
    pub pages: Vec<Pco2PageInfo>,
    /// Size of each checkpoint record: a little-endian `u32` bit position in the page, followed
    /// by a pco page header resuming decoding there. Zero when the chunk has no checkpoints,
    /// which is only allowed when no page is longer than the checkpoint interval.
    #[prost(uint32, tag = "2")]
    pub checkpoint_record_bytes: u32,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
/// Serialized metadata for a [`Pco2Array`].
pub struct Pco2Metadata {
    /// Pco file header bytes.
    #[prost(bytes, tag = "1")]
    pub header: Vec<u8>,
    /// Number of values between checkpoints, a positive multiple of 256.
    #[prost(uint32, tag = "2")]
    pub checkpoint_interval: u32,
    /// Metadata for each compressed chunk.
    #[prost(message, repeated, tag = "3")]
    pub chunks: Vec<Pco2ChunkInfo>,
    /// Whether the array stores the minimum and maximum of every checkpoint block.
    #[prost(bool, tag = "4")]
    pub block_stats: bool,
}
