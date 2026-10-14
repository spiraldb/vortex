// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Iterator and async stream adapters over chunked Vortex arrays.

use std::pin::Pin;

pub use adapter::*;
pub use ext::*;
use futures::Stream;
use futures::stream;
use vortex_array::ArrayRef;
use vortex_array::dtype::DType;
use vortex_error::VortexResult;

use crate::iter::ArrayRefIterExt;

mod adapter;
mod ext;
pub mod iter;

/// A stream of array chunks along with a DType.
///
/// Can be thought of as equivalent to Arrow's RecordBatchReader.
pub trait ArrayStream: Stream<Item = VortexResult<ArrayRef>> {
    fn dtype(&self) -> &DType;
}

/// Trait for a [`Stream`] of [`ArrayRef`]s that can be passed between threads.
pub type SendableArrayStream = Pin<Box<dyn ArrayStream + Send>>;

impl ArrayStream for SendableArrayStream {
    fn dtype(&self) -> &DType {
        (**self).dtype()
    }
}

/// Extension methods to stream an [`ArrayRef`] chunk by chunk.
pub trait ArrayRefStreamExt {
    /// Create an [`ArrayStream`] over the array.
    fn to_array_stream(&self) -> impl ArrayStream + 'static;
}

impl ArrayRefStreamExt for ArrayRef {
    fn to_array_stream(&self) -> impl ArrayStream + 'static {
        ArrayStreamAdapter::new(self.dtype().clone(), stream::iter(self.to_array_iterator()))
    }
}
