// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Encodings for non-decreasing integer sequences.
//!
//! [`Unary`] stores a non-decreasing, non-nullable unsigned column as one bit array, writing
//! each value's gap from the previous one in unary. It takes `len + (max - min)` bits, so it suits
//! dense sequences, where values rise by about one per row or less. It is also meant to hold the
//! high bits of the Elias-Fano encoding this crate is being built towards.
//!
//! None of these encodings is in a file edition or registered by `vortex-file`, so the default
//! writers reject them for now. [`initialize`] registers them in a session.

mod unary;

pub use unary::Unary;
pub use unary::UnaryArray;
pub use unary::UnaryData;
pub use unary::unary_encode;
use vortex_array::session::ArraySessionExt;
use vortex_session::VortexSession;

/// Register this crate's encodings in `session`.
pub fn initialize(session: &VortexSession) {
    session.arrays().register(Unary);
}
