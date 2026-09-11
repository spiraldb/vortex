// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::task::ready;

use futures::AsyncRead;
use futures::Stream;
use pin_project_lite::pin_project;
use vortex_buffer::Alignment;
use vortex_buffer::ByteBuffer;
use vortex_buffer::ByteBufferMut;
use vortex_error::VortexResult;
use vortex_error::vortex_err;

use crate::messages::DecoderMessage;
use crate::messages::MessageDecoder;
use crate::messages::PollRead;

pin_project! {
    /// An IPC message reader backed by an `AsyncRead` stream.
    ///
    /// Every frame the decoder asks for is read into a fresh buffer aligned to
    /// [`Alignment::DEFAULT_ALIGNMENT`], so message bodies are handed out as slices of it rather
    /// than copied.
    pub struct AsyncMessageReader<R> {
        #[pin]
        read: R,
        buffer: ByteBuffer,
        decoder: MessageDecoder,
        state: ReadState,
    }
}

impl<R> AsyncMessageReader<R> {
    pub fn new(read: R) -> Self {
        AsyncMessageReader {
            read,
            buffer: ByteBuffer::empty(),
            decoder: MessageDecoder::default(),
            state: ReadState::default(),
        }
    }
}

/// The state of an in-progress read operation.
#[derive(Default)]
enum ReadState {
    /// Ready to consult the decoder for the next operation.
    #[default]
    AwaitingDecoder,
    /// Filling a frame with data from the underlying reader.
    ///
    /// Async readers may return fewer bytes than requested (partial reads), especially over network
    /// connections. This state persists across multiple `poll_next` calls until the frame is
    /// completely filled, at which point we transition back to [`Self::AwaitingDecoder`].
    Filling {
        /// The frame being filled.
        frame: ByteBufferMut,
        /// The number of bytes the frame already holds.
        filled: usize,
    },
}

/// Result of polling the reader to fill the frame.
enum FillResult {
    /// The frame has been completely filled.
    Filled,
    /// Need more data (partial read occurred).
    Pending,
    /// Clean EOF at a message boundary.
    Eof,
}

/// Polls the reader to fill the frame, handling partial reads.
fn poll_fill_frame<R: AsyncRead>(
    read: Pin<&mut R>,
    frame: &mut [u8],
    filled: &mut usize,
    cx: &mut Context<'_>,
) -> Poll<VortexResult<FillResult>> {
    let unfilled = &mut frame[*filled..];

    let bytes_read = ready!(read.poll_read(cx, unfilled))?;

    // `0` bytes read indicates an EOF.
    Poll::Ready(if bytes_read == 0 {
        if *filled > 0 {
            Err(vortex_err!(
                "unexpected EOF during partial read: read {filled} of {} expected bytes",
                frame.len()
            ))
        } else {
            Ok(FillResult::Eof)
        }
    } else {
        *filled += bytes_read;
        if *filled == frame.len() {
            Ok(FillResult::Filled)
        } else {
            debug_assert!(*filled < frame.len());
            Ok(FillResult::Pending)
        }
    })
}

impl<R: AsyncRead> Stream for AsyncMessageReader<R> {
    type Item = VortexResult<DecoderMessage>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            match this.state {
                ReadState::AwaitingDecoder => match this.decoder.read_next(this.buffer)? {
                    PollRead::Some(msg) => return Poll::Ready(Some(Ok(msg))),
                    PollRead::NeedMore(nbytes) => {
                        // Start the new frame with whatever the decoder left unconsumed.
                        let leftover = std::mem::take(this.buffer);
                        let mut frame =
                            ByteBufferMut::zeroed_aligned(nbytes, Alignment::DEFAULT_ALIGNMENT);
                        frame[..leftover.len()].copy_from_slice(&leftover);
                        *this.state = ReadState::Filling {
                            frame,
                            filled: leftover.len(),
                        };
                    }
                },
                ReadState::Filling { frame, filled } => {
                    match ready!(poll_fill_frame(this.read.as_mut(), frame, filled, cx)) {
                        Err(e) => return Poll::Ready(Some(Err(e))),
                        Ok(FillResult::Eof) => return Poll::Ready(None),
                        Ok(FillResult::Pending) => {}
                        Ok(FillResult::Filled) => {
                            let ReadState::Filling { frame, .. } = std::mem::take(this.state)
                            else {
                                unreachable!("the frame was being filled")
                            };
                            *this.buffer = frame.freeze();
                        }
                    }
                }
            }
        }
    }
}
