// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Pco-backed numeric compression encoding for Vortex arrays.
//!
//! [`PcoArray`] stores valid primitive numeric values in Pco chunks and pages, while Vortex
//! validity tracks null rows separately. Reads decompress only the blocks they touch.
//!
//! The array has two wire formats, chosen by how it was compressed:
//!
//! - `vortex.pco` (v1, frozen): chunks of pages. A page is the smallest unit a read decodes, so
//!   random access costs a page, and small pages cost compression ratio.
//! - `vortex.pco.v2` (draft): the same chunks and pages plus a random-access index. Checkpoints
//!   record pco's decoder state every `checkpoint_interval` values inside a page, so v2 writes one
//!   large page per chunk yet reads blocks as small as the interval, and a read stops at the
//!   256-value batch holding its last value. Records are bit-packed against a linear fit, and a
//!   near-constant chunk spaces its checkpoints out until the index is a small share of its size.
//!   Each block's minimum and maximum let comparisons against a constant skip whole blocks, and
//!   are kept only where blocks narrow the value range. Dropping the index leaves valid v1 data,
//!   so writing for older readers needs no recompression.
//!
//! Compress with [`Pco::from_primitive`] for v1 or [`Pco::compress`] with [`PcoOptions::v2`] for
//! v2. Pco supports integer and floating-point primitive dtypes handled by the upstream `pco`
//! crate. It is normally selected through the BtrBlocks compressor when the `pco` feature is
//! enabled. To read and write both formats, initialize the session:
//!
//! ```rust
//! let session = vortex_array::array_session();
//! vortex_pco::initialize(&session);
//! ```

mod array;
mod checkpoint;
mod compute;
pub mod editions;
mod plugin;
mod probe;
mod rules;
mod slice;

pub use array::*;
pub use plugin::PcoPlugin;
pub use plugin::pco_v1_id;
pub use plugin::pco_v2_id;
use vortex_array::ArrayVTable;
use vortex_array::aggregate_fn::AggregateFnVTable;
use vortex_array::aggregate_fn::fns::is_constant::IsConstant;
use vortex_array::aggregate_fn::session::AggregateFnSessionExt;
use vortex_array::arrays::Dict;
use vortex_array::arrays::Filter;
use vortex_array::arrays::dict::TakeExecuteAdaptor;
use vortex_array::arrays::filter::FilterExecuteAdaptor;
use vortex_array::optimizer::kernels::ArrayKernelsExt;
use vortex_array::scalar_fn::ScalarFnVTable;
use vortex_array::scalar_fn::fns::between::Between;
use vortex_array::scalar_fn::fns::between::BetweenExecuteAdaptor;
use vortex_array::scalar_fn::fns::binary::Binary;
use vortex_array::scalar_fn::fns::binary::CompareExecuteAdaptor;
use vortex_array::session::ArraySessionExt;
use vortex_edition::EditionSessionExt;
use vortex_error::VortexExpect;
use vortex_session::VortexSession;

/// Register Pco's array plugin, compute kernels, and edition declaration with a session.
///
/// This lets the session read and write both wire formats. Writers still emit `vortex.pco.v2`
/// only when the [`editions::PCO_2026_10`] draft edition is enabled.
pub fn initialize(session: &VortexSession) {
    session.arrays().register(PcoPlugin);
    let kernels = session.kernels();
    kernels.register_execute_parent_kernel(Between.id(), Pco, BetweenExecuteAdaptor(Pco));
    kernels.register_execute_parent_kernel(Binary.id(), Pco, CompareExecuteAdaptor(Pco));
    kernels.register_execute_parent_kernel(Filter.id(), Pco, FilterExecuteAdaptor(Pco));
    kernels.register_execute_parent_kernel(Dict.id(), Pco, TakeExecuteAdaptor(Pco));
    session.aggregate_fns().register_aggregate_kernel(
        Pco.id(),
        Some(IsConstant.id()),
        &compute::PcoIsConstantKernel,
    );

    if session.editions().find(&editions::PCO_2026_10).is_none() {
        session
            .editions()
            .declare_family(&editions::FAMILY)
            .vortex_expect("Pco edition family is valid");
        session
            .register_edition(&editions::DECLARATION)
            .vortex_expect("Pco edition declaration is valid");
    }
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
/// Metadata for one Pco page.
pub struct PcoPageInfo {
    // Since pco limits to 2^24 values per chunk, u32 is sufficient for the
    // count of values.
    /// Number of valid primitive values stored in this page.
    #[prost(uint32, tag = "1")]
    pub n_values: u32,
}

// We're calling this Info instead of Metadata because ChunkMeta refers to a specific
// component of a Pco file.
#[derive(Clone, PartialEq, Eq, prost::Message)]
/// Metadata for one Pco chunk.
pub struct PcoChunkInfo {
    /// Pages contained in this chunk.
    #[prost(message, repeated, tag = "1")]
    pub pages: Vec<PcoPageInfo>,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
/// Serialized metadata for the v1 (`vortex.pco`) wire format.
pub struct PcoMetadata {
    // would be nice to reuse one header per vortex file, but it's really only 1 byte, so
    // no issue duplicating it here per PcoArray
    /// Pco file header bytes.
    #[prost(bytes, tag = "1")]
    pub header: Vec<u8>,
    /// Metadata for each compressed chunk.
    #[prost(message, repeated, tag = "2")]
    pub chunks: Vec<PcoChunkInfo>,
}

#[derive(Clone, PartialEq, Eq, prost::Message)]
/// Serialized metadata for the v2 (`vortex.pco.v2`) wire format.
///
/// The v2 buffers are the v1 buffers (every chunk's metadata, then every page) followed by one
/// checkpoint buffer per chunk and, when `block_stats` is set, one block stats buffer.
pub struct PcoV2Metadata {
    /// Pco file header bytes.
    #[prost(bytes, tag = "1")]
    pub header: Vec<u8>,
    /// Metadata for each compressed chunk, as in v1.
    #[prost(message, repeated, tag = "2")]
    pub chunks: Vec<PcoChunkInfo>,
    /// For each chunk, the values between its checkpoints, a positive multiple of 256. A page is
    /// read in blocks of this many values, and its checkpoint buffer holds a record for each
    /// block after the first: the bit position in the page and the pco page header that resumes
    /// decoding there, packed column by column. Zero for a chunk without checkpoints, whose pages
    /// are its blocks.
    #[prost(uint32, repeated, tag = "3")]
    pub checkpoint_intervals: Vec<u32>,
    /// Whether the array stores the minimum and maximum of every block: a little-endian `u32`
    /// length, the pco-compressed minimums of that length, then the pco-compressed maximums.
    #[prost(bool, tag = "4")]
    pub block_stats: bool,
}

#[cfg(test)]
mod tests;
