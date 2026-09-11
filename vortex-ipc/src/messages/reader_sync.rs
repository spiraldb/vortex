// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::io;
use std::io::Read;

use vortex_buffer::Alignment;
use vortex_buffer::ByteBuffer;
use vortex_buffer::ByteBufferMut;
use vortex_error::VortexResult;

use crate::messages::DecoderMessage;
use crate::messages::MessageDecoder;
use crate::messages::PollRead;

/// An IPC message reader backed by a `Read` stream.
///
/// Every frame the decoder asks for is read into a fresh buffer aligned to
/// [`Alignment::DEFAULT_ALIGNMENT`], so message bodies are handed out as slices of it rather than
/// copied.
pub struct SyncMessageReader<R> {
    read: R,
    buffer: ByteBuffer,
    decoder: MessageDecoder,
}

impl<R: Read> SyncMessageReader<R> {
    pub fn new(read: R) -> Self {
        SyncMessageReader {
            read,
            buffer: ByteBuffer::empty(),
            decoder: MessageDecoder::default(),
        }
    }
}

impl<R: Read> Iterator for SyncMessageReader<R> {
    type Item = VortexResult<DecoderMessage>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.decoder.read_next(&mut self.buffer) {
                Ok(PollRead::Some(msg)) => return Some(Ok(msg)),
                Ok(PollRead::NeedMore(nbytes)) => {
                    let leftover = std::mem::take(&mut self.buffer);
                    match fill(&mut self.read, &leftover, nbytes) {
                        Ok(Some(buffer)) => self.buffer = buffer,
                        // EOF on a message boundary.
                        Ok(None) => return None,
                        Err(e) => return Some(Err(e.into())),
                    }
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

/// Read into a fresh frame of `nbytes`, starting with whatever the decoder left unconsumed.
///
/// Returns `None` if the stream ended before a single new byte was read.
fn fill<R: Read>(read: &mut R, leftover: &[u8], nbytes: usize) -> io::Result<Option<ByteBuffer>> {
    let mut buffer = ByteBufferMut::zeroed_aligned(nbytes, Alignment::DEFAULT_ALIGNMENT);
    buffer[..leftover.len()].copy_from_slice(leftover);
    let mut filled = leftover.len();
    while filled < nbytes {
        match read.read(&mut buffer[filled..]) {
            Ok(0) if filled == leftover.len() => return Ok(None),
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!(
                        "unexpected EOF during partial read: read {filled} of {nbytes} expected bytes"
                    ),
                ));
            }
            Ok(n) => filled += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(Some(buffer.freeze()))
}
