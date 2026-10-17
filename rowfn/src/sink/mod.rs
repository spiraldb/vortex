// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Row writers and initialization evidence, with host-controlled storage.
//!
//! Token constructors remain unsafe because a zero-sized token cannot encode the exact callback
//! row. A callback must return evidence for its supplied row and preserve that initialization.

use crate::{Host, HostResult};

mod element;
pub use element::{InitializedElement, UninitElementSink};

mod initialized;
pub use initialized::ElementSink;

mod list;
pub use list::{FixedSizeListSink, InitializedRow, ListOutput};

/// A sink lending a distinct row handle for every output position.
///
/// # Safety
/// Row count and index mapping must remain stable across views and moves. Safe use of a row handle
/// cannot invalidate other rows. An uninitialized handle requires unforgeable initialization
/// evidence for that exact row. The sink must permit safe abandonment after any callback prefix,
/// error, or unwind. `initialize` must make every row safe to publish, including zero-width rows.
pub unsafe trait OutputSink<H: Host>: Sized + 'static {
    /// Physical parameters known before traversal.
    type Params: 'static;
    /// A view of writable rows, borrowed once before the loop.
    type Rows<'a>;
    /// One writable row, borrowed from the retained view.
    type Row<'a>;
    /// Evidence returned by each successful callback.
    type WriteToken: 'static;
    /// Non-nullable native storage metadata.
    fn storage_type(params: &Self::Params) -> HostResult<H, H::NativeType>;
    /// Allocate using invocation resources. Failures here are terminal infrastructure errors.
    fn allocate(rows: usize, params: &Self::Params, ctx: &mut H::Context) -> HostResult<H, Self>;
    /// Lend stable row access.
    fn rows(&mut self) -> Self::Rows<'_>;
    /// Number of addressable rows in this exact view.
    fn len(rows: &Self::Rows<'_>) -> usize;
    /// Initialize all rows before traversal that skips invalid rows.
    fn initialize(rows: &mut Self::Rows<'_>);
    /// Lend the exact row at `index`.
    ///
    /// # Safety
    /// The index must be below `len(rows)`.
    unsafe fn row<'a>(rows: &'a mut Self::Rows<'_>, index: usize) -> Self::Row<'a>;
    /// Publish initialized storage using host resources.
    ///
    /// # Safety
    /// Every callback must have returned evidence for its supplied row. Skipped rows must have been
    /// initialized before traversal. All initialization must remain intact.
    unsafe fn finish(self, ctx: &mut H::Context) -> HostResult<H, H::Column>;
}

/// A consuming UTF-8 row writer that copies the result into output-owned storage.
pub trait WriteUtf8 {
    /// Replace the row with a valid string. The result must not borrow `value`.
    fn write(self, value: &str);

    /// Concatenate valid strings directly into output-owned storage.
    ///
    /// The default supports existing writers. Hosts can override it to avoid temporary storage.
    fn write_parts(self, parts: &[&str]) where Self: Sized {
        self.write(&parts.concat());
    }
}

/// Host binding for an owned string sink.
pub trait Utf8Output: Host {
    /// Host-specific storage and row handles.
    type Sink: OutputSink<Self, Params = (), WriteToken = ()>;
}

/// Immediate row success or error, with initialization evidence on success.
pub trait SinkResult<E>: private::Sealed {
    /// Evidence required by the selected sink.
    type WriteToken: 'static;
    /// Whether the result can represent a semantic error.
    const INFALLIBLE: bool;
    /// Consume success evidence or return the immediate row error.
    fn into_result(self) -> Result<(), E>;
}

macro_rules! token_result {
    ($token:ty) => {
        impl private::Sealed for $token {}
        impl<E> SinkResult<E> for $token {
            type WriteToken = $token;
            const INFALLIBLE: bool = true;
            fn into_result(self) -> Result<(), E> { Ok(()) }
        }
        impl<E> private::Sealed for Result<$token, E> {}
        impl<E> SinkResult<E> for Result<$token, E> {
            type WriteToken = $token;
            const INFALLIBLE: bool = false;
            fn into_result(self) -> Result<(), E> { self.map(|_| ()) }
        }
    };
}
token_result!(());
token_result!(InitializedElement);
token_result!(InitializedRow);

mod private { pub trait Sealed {} }
