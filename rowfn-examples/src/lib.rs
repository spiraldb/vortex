// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Cross-host fixtures and external Vortex registration for portable `rowfn` functions.
//!
//! Re-exported functions belong to `rowfn-functions`. This package enables both host mappings for
//! conformance tests and benchmarks, while the function package remains host-independent by default.

#![deny(missing_docs)]

pub use rowfn_functions::*;

mod registration;
pub use registration::SevenPlugin;
