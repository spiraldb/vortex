// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::fmt::Debug;
use std::sync::Arc;

use flatbuffers::root;
use flatbuffers::root_unchecked;
use vortex_array::ArrayId;
use vortex_array::serde::SerializedArray;
use vortex_buffer::Alignment;
use vortex_buffer::ByteBuffer;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_err;
use vortex_flatbuffers::FlatBuffer;
use vortex_flatbuffers::message as fb;
use vortex_flatbuffers::message::MessageHeader;
use vortex_flatbuffers::message::MessageVersion;
use vortex_session::registry::ReadContext;

/// A message decoded from an IPC stream.
#[derive(Debug)]
pub enum DecoderMessage {
    Array((SerializedArray, ReadContext, usize)),
    Buffer(ByteBuffer),
    DType(FlatBuffer),
}

#[derive(Default)]
enum State {
    #[default]
    Length,
    Header(usize),
    Reading(FlatBuffer),
}

#[derive(Debug)]
pub enum PollRead {
    /// A complete message was decoded.
    Some(DecoderMessage),
    /// The decoder needs more data to make progress.
    ///
    /// The inner value is the **total** number of bytes the buffer handed to the next
    /// [`MessageDecoder::read_next`] call must hold, not the incremental amount needed: whatever
    /// the decoder did not consume from the previous buffer, followed by enough new bytes to make
    /// up the total. Callers should fill the buffer completely before calling `read_next` again.
    NeedMore(usize),
}

/// A stateful reader for decoding IPC messages from an arbitrary stream of bytes.
///
/// The decoder consumes from the front of the [`ByteBuffer`] it is given and hands message bodies
/// out as slices of it wherever their alignment allows, so a caller that provides buffers aligned
/// to [`Alignment::DEFAULT_ALIGNMENT`] decodes without copying. A body that does not lie at the
/// alignment its message asks for is copied.
#[derive(Default)]
pub struct MessageDecoder {
    /// The current state of the decoder.
    state: State,
}

impl MessageDecoder {
    /// Attempt to read the next message from `bytes`, consuming what it reads from the front.
    ///
    /// If the message is incomplete, the function will return `NeedMore` with the _total_ number
    /// of bytes needed to make progress. The next call to read_next _should_ provide at least
    /// this number of bytes otherwise it will be given the same `NeedMore` response.
    pub fn read_next(&mut self, bytes: &mut ByteBuffer) -> VortexResult<PollRead> {
        loop {
            match &self.state {
                State::Length => {
                    if bytes.len() < 4 {
                        return Ok(PollRead::NeedMore(4));
                    }

                    let length = take(bytes, 4, Alignment::none());
                    let msg_length = u32::from_le_bytes(
                        length
                            .as_slice()
                            .try_into()
                            .ok()
                            .vortex_expect("four bytes were taken"),
                    );
                    self.state = State::Header(msg_length as usize);
                }
                State::Header(msg_length) => {
                    if bytes.len() < *msg_length {
                        return Ok(PollRead::NeedMore(*msg_length));
                    }

                    let msg_bytes =
                        FlatBuffer::try_from(take(bytes, *msg_length, FlatBuffer::alignment()))?;
                    let msg = root::<fb::Message>(msg_bytes.as_ref())?;
                    if msg.version() != MessageVersion::V0 {
                        vortex_bail!("Unsupported message version {:?}", msg.version());
                    }

                    self.state = State::Reading(msg_bytes);
                }
                State::Reading(msg_bytes) => {
                    // SAFETY: we've already validated the header in the previous state
                    let msg = unsafe { root_unchecked::<fb::Message>(msg_bytes.as_ref()) };

                    // Now we read the body
                    let body_length = usize::try_from(msg.body_size()).map_err(|_| {
                        vortex_err!("body size {} is too large for usize", msg.body_size())
                    })?;
                    if bytes.len() < body_length {
                        return Ok(PollRead::NeedMore(body_length));
                    }

                    match msg.header_type() {
                        MessageHeader::ArrayMessage => {
                            // We don't care about alignment here since ArrayParts will handle it.
                            let body = take(bytes, body_length, Alignment::none());
                            let parts = SerializedArray::try_from(body)?;

                            let header = msg
                                .header_as_array_message()
                                .vortex_expect("header is array");

                            #[expect(clippy::disallowed_methods, reason = "interning a dynamic id")]
                            let encoding_ids: Arc<_> = header
                                .encodings()
                                .iter()
                                .flat_map(|e| e.iter())
                                .map(ArrayId::new)
                                .collect();

                            let ctx = ReadContext::new(encoding_ids);
                            let row_count = header.row_count() as usize;

                            self.state = Default::default();
                            return Ok(PollRead::Some(DecoderMessage::Array((
                                parts, ctx, row_count,
                            ))));
                        }
                        MessageHeader::BufferMessage => {
                            let alignment = Alignment::try_from_untrusted_exponent(
                                msg.header_as_buffer_message()
                                    .vortex_expect("header is buffer")
                                    .alignment_exponent(),
                            )?;
                            let body = take(bytes, body_length, alignment);

                            self.state = Default::default();
                            return Ok(PollRead::Some(DecoderMessage::Buffer(body)));
                        }
                        MessageHeader::DTypeMessage => {
                            let dtype = FlatBuffer::try_from(take(
                                bytes,
                                body_length,
                                FlatBuffer::alignment(),
                            ))?;
                            self.state = Default::default();
                            return Ok(PollRead::Some(DecoderMessage::DType(dtype)));
                        }
                        _ => {
                            vortex_bail!("Unsupported message header {:?}", msg.header_type());
                        }
                    }
                }
            }
        }
    }
}

/// Split the first `len` bytes off the front of `bytes`, aligned to `alignment`.
///
/// The part is a slice of `bytes` when its address already satisfies `alignment`, and a copy
/// otherwise. What remains in `bytes` may start anywhere, so it promises no alignment.
fn take(bytes: &mut ByteBuffer, len: usize, alignment: Alignment) -> ByteBuffer {
    let unaligned = std::mem::take(bytes).aligned(Alignment::none());
    let part = unaligned.slice(0..len).aligned(alignment);
    *bytes = unaligned.slice(len..);
    part
}

#[cfg(test)]
mod test {
    use vortex_array::ArrayRef;
    use vortex_array::IntoArray;
    use vortex_array::arrays::ConstantArray;
    use vortex_buffer::ByteBufferMut;
    use vortex_buffer::buffer;
    use vortex_error::vortex_panic;

    use super::*;
    use crate::messages::EncoderMessage;
    use crate::messages::MessageEncoder;
    use crate::test::SESSION;

    fn write_and_read(expected: &ArrayRef) {
        let mut ipc_bytes = ByteBufferMut::empty();
        let mut encoder = MessageEncoder::new(SESSION.clone());
        for buf in encoder.encode(EncoderMessage::Array(expected)).unwrap() {
            ipc_bytes.extend_from_slice(&buf);
        }

        let mut decoder = MessageDecoder::default();

        // Since we provide all bytes up-front, we should never hit a NeedMore.
        let mut buffer = ipc_bytes.freeze();
        let (array_parts, ctx, row_count) = match decoder.read_next(&mut buffer).unwrap() {
            PollRead::Some(DecoderMessage::Array(array_parts)) => array_parts,
            otherwise => vortex_panic!("Expected an array, got {:?}", otherwise),
        };
        assert!(buffer.is_empty(), "the whole message was consumed");

        // Decode the array parts with the context
        let actual = array_parts
            .decode(expected.dtype(), row_count, &ctx, &SESSION)
            .unwrap();

        assert_eq!(expected.len(), actual.len());
        assert_eq!(expected.encoding_id(), actual.encoding_id());
    }

    #[test]
    fn array_ipc() {
        write_and_read(&buffer![0i32, 1, 2, 3].into_array());
    }

    #[test]
    fn array_no_buffers() {
        // Constant arrays have a single buffer
        let array = ConstantArray::new(10i32, 20);
        assert_eq!(array.nbuffers(), 1, "Array should have a single buffer");
        write_and_read(&array.into_array());
    }

    #[test]
    fn aligned_frames_decode_without_copying() {
        let expected =
            ByteBuffer::copy_from_aligned([1u8, 2, 3, 4, 5, 6, 7, 8], Alignment::new(64));
        let mut encoder = MessageEncoder::new(SESSION.clone());
        let frames = encoder.encode(EncoderMessage::Buffer(&expected)).unwrap();

        // Hand the decoder each frame in a fresh, default-aligned buffer, as the stream readers
        // do, and check that the body it hands back is a slice of the frame it arrived in.
        let mut decoder = MessageDecoder::default();
        let mut decoded = None;
        for frame in frames {
            let mut buffer = ByteBuffer::copy_from(&frame);
            let frame_ptr = buffer.as_ptr();
            if let PollRead::Some(DecoderMessage::Buffer(body)) =
                decoder.read_next(&mut buffer).unwrap()
            {
                assert!(buffer.is_empty(), "the whole frame was consumed");
                decoded = Some((body, frame_ptr));
            }
        }
        let (body, frame_ptr) = decoded.expect("a buffer message");
        assert_eq!(body.as_slice(), expected.as_slice());
        assert_eq!(body.alignment(), Alignment::new(64));
        assert_eq!(body.as_ptr(), frame_ptr, "the body aliases its frame");
    }
}
