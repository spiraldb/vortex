// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Minimal CUDA driver API bindings for Vortex.
//!
//! The driver library is loaded lazily, so dependents build and run without CUDA until
//! they first use it. The raw bindings and result wrappers operate on CUDA driver handles.

pub mod result;
pub mod sys;

pub use result::DriverError;
