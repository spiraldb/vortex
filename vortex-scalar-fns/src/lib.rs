// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Array functions built on vortex-array that the core array crate does not itself depend on.
//!
//! [`interleave`] holds the [`Interleave`](interleave::Interleave) encoding, a lazy random-access
//! gather across several arrays.

pub mod interleave;
