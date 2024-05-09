use vortex_buffer::Buffer;

mod futures;
mod message_reader;
mod message_stream;

pub enum Message {
    FlatBuffer(Buffer),
    ByteBuffer(Buffer),
}
