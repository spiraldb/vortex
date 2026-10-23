// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Minimal CUDA driver API bindings for Vortex.
//!
//! Owns CUDA contexts, streams, events, and loaded modules. The driver library is loaded
//! lazily, so dependents build and run without CUDA until they first use it.

mod context;
mod module;
pub mod result;
mod stream;
pub mod sys;

pub use context::CudaContext;
pub use module::CudaFunction;
pub use module::CudaModule;
pub use result::DriverError;
pub use stream::CudaEvent;
pub use stream::CudaStream;
