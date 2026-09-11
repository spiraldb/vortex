// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use vortex_buffer::ByteBuffer;
use vortex_error::VortexResult;
use vortex_error::vortex_err;

use crate::messages::DecoderMessage;
use crate::messages::MessageDecoder;
use crate::messages::PollRead;

/// An IPC message reader over a buffer that already holds the whole stream.
pub struct BufMessageReader {
    buffer: ByteBuffer,
    decoder: MessageDecoder,
}

impl BufMessageReader {
    pub fn new(buffer: ByteBuffer) -> Self {
        BufMessageReader {
            buffer,
            decoder: MessageDecoder::default(),
        }
    }
}

impl Iterator for BufMessageReader {
    type Item = VortexResult<DecoderMessage>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buffer.is_empty() {
            // End-of-buffer reached
            return None;
        }
        match self.decoder.read_next(&mut self.buffer) {
            Ok(PollRead::Some(msg)) => Some(Ok(msg)),
            Ok(PollRead::NeedMore(_)) => Some(Err(vortex_err!(
                "Buffer did not have sufficient bytes for an IPC message"
            ))),
            Err(e) => Some(Err(e)),
        }
    }
}
