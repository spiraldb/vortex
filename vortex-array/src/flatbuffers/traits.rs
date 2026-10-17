// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Traits for reading and writing Vortex types as flatbuffers.

use flatbuffers::FlatBufferBuilder;
use flatbuffers::Follow;
use flatbuffers::InvalidFlatbuffer;
use flatbuffers::Verifiable;
use flatbuffers::VerifierOptions;
use flatbuffers::WIPOffset;
use flatbuffers::root;
use flatbuffers::root_with_opts;
use vortex_buffer::ByteBuffer;
use vortex_buffer::ConstByteBuffer;
use vortex_error::VortexResult;

/// We define a const-aligned byte buffer for flatbuffers with 8-byte alignment.
///
/// This is based on the assumption that the maximum primitive type is 8 bytes.
/// See: <https://groups.google.com/g/flatbuffers/c/PSgQeWeTx_g>
pub type FlatBuffer = ConstByteBuffer<8>;

/// Verifier options for a flatbuffer of `len` bytes, allowing as many tables as it can hold.
///
/// The verifier counts the tables it visits to bound its work, and by default rejects a buffer
/// with more than a million. The dtype, layout and statistics of a file with a million columns
/// each hold more than that. A table occupies at least four bytes, so a limit proportional to the
/// buffer's length never rejects a valid buffer while still bounding the work of verifying one,
/// even if its tables are shared to make the verifier revisit them.
pub fn verifier_options(len: usize) -> VerifierOptions {
    let defaults = VerifierOptions::default();
    VerifierOptions {
        max_tables: defaults.max_tables.max(len / 4),
        ..defaults
    }
}

/// Verifies and returns the root of a flatbuffer with the [`verifier_options`] for its length.
pub fn root_verified<'buf, T>(buf: &'buf [u8]) -> Result<T::Inner, InvalidFlatbuffer>
where
    T: 'buf + Follow<'buf> + Verifiable,
{
    root_with_opts::<T>(&verifier_options(buf.len()), buf)
}

/// Marker trait for types that can be the root of a FlatBuffer.
pub trait FlatBufferRoot {}

/// Trait for reading a type from a FlatBuffer.
pub trait ReadFlatBuffer: Sized {
    /// The FlatBuffer type that this type can be read from.
    type Source<'a>: Verifiable + Follow<'a>;
    /// The error type returned when reading fails.
    type Error: From<InvalidFlatbuffer>;

    /// Reads this type from a FlatBuffer source.
    fn read_flatbuffer<'buf>(
        fb: &<Self::Source<'buf> as Follow<'buf>>::Inner,
    ) -> Result<Self, Self::Error>;

    /// Reads this type from bytes representing a FlatBuffer source.
    fn read_flatbuffer_bytes<'buf>(bytes: &'buf [u8]) -> Result<Self, Self::Error>
    where
        <Self as ReadFlatBuffer>::Source<'buf>: 'buf,
    {
        let fb = root::<Self::Source<'buf>>(bytes)?;
        Self::read_flatbuffer(&fb)
    }
}

/// Trait for writing a type to a FlatBuffer.
pub trait WriteFlatBuffer {
    /// The FlatBuffer type that this type can be written to.
    type Target<'a>;

    /// Writes this type to a FlatBuffer builder.
    fn write_flatbuffer<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> VortexResult<WIPOffset<Self::Target<'fb>>>;
}

/// Extension trait for types that can be written as FlatBuffer root objects.
pub trait WriteFlatBufferExt: WriteFlatBuffer + FlatBufferRoot {
    /// Writes self as a FlatBuffer root object into a [`FlatBuffer`] byte buffer.
    fn write_flatbuffer_bytes(&self) -> VortexResult<FlatBuffer>;
}

impl<F: WriteFlatBuffer + FlatBufferRoot> WriteFlatBufferExt for F {
    fn write_flatbuffer_bytes(&self) -> VortexResult<FlatBuffer> {
        let mut fbb = FlatBufferBuilder::new();
        let root_offset = self.write_flatbuffer(&mut fbb)?;
        fbb.finish_minimal(root_offset);
        let (vec, start) = fbb.collapse();
        let end = vec.len();
        Ok(FlatBuffer::align_from(
            ByteBuffer::from(vec).slice(start..end),
        ))
    }
}
