// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Pco-backed numeric compression encoding for Vortex arrays.
//!
//! [`PcoArray`] stores valid primitive numeric values in Pco chunks and pages, while Vortex
//! validity tracks null rows separately. Page metadata lets slices decompress only the components
//! required for the requested row range.
//!
//! Pco supports integer and floating-point primitive dtypes handled by the upstream `pco` crate.
//! It is normally selected through the BtrBlocks compressor when the `pco` feature is enabled.
//! To deserialize arrays manually, register the encoding in the array session:
//!
//! ```rust
//! use vortex_session::VortexSession;
//!
//! let session = VortexSession::empty();
//! vortex_pco::initialize(&session);
//! ```

mod access;
mod array;
mod compute;
mod page_index;
mod probe;
mod rules;
mod slice;
mod take;

pub use array::*;

#[derive(Clone, PartialEq, prost::Message)]
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
#[derive(Clone, PartialEq, prost::Message)]
/// Metadata for one Pco chunk.
pub struct PcoChunkInfo {
    /// Pages contained in this chunk.
    #[prost(message, repeated, tag = "1")]
    pub pages: Vec<PcoPageInfo>,
}

#[derive(Clone, PartialEq, prost::Message)]
/// Serialized metadata for a [`PcoArray`].
pub struct PcoMetadata {
    // would be nice to reuse one header per vortex file, but it's really only 1 byte, so
    // no issue duplicating it here per PcoArray
    /// Pco file header bytes.
    #[prost(bytes, tag = "1")]
    pub header: Vec<u8>,
    /// Metadata for each compressed chunk.
    #[prost(message, repeated, tag = "2")]
    pub chunks: Vec<PcoChunkInfo>,
    /// Optional logical-row boundaries for selective decoding. Older readers ignore this field.
    #[prost(message, optional, tag = "3")]
    pub logical_pages: Option<PcoLogicalPages>,
}

/// Versioned optional logical-row page index. Unknown versions retain legacy decoding.
#[derive(Clone, PartialEq, prost::Message)]
pub struct PcoLogicalPages {
    /// Index interpretation version; currently one.
    #[prost(uint32, tag = "1")]
    pub version: u32,
    /// Exclusive unsliced logical-row ends in physical page order.
    ///
    /// The final page includes trailing null rows. An empty index has no valid values.
    #[prost(uint64, repeated, tag = "2")]
    pub ends: Vec<u64>,
}

/// Register PCO encoding and its selective take execution kernel in a session.
///
/// Registering only `Pco` in the array registry is sufficient for legacy reads, but does not
/// enable the batched take kernel.
pub fn initialize(session: &vortex_session::VortexSession) {
    use vortex_array::ArrayVTable;
    use vortex_array::arrays::Dict;
    use vortex_array::arrays::dict::TakeExecuteAdaptor;
    use vortex_array::optimizer::kernels::ArrayKernelsExt;
    use vortex_array::session::ArraySessionExt;
    session.arrays().register(Pco);
    session
        .kernels()
        .register_execute_parent_kernel(Dict.id(), Pco, TakeExecuteAdaptor(Pco));
}

#[cfg(test)]
mod select_tests;
#[cfg(test)]
mod tests;
