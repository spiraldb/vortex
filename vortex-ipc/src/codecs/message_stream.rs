use futures_util::Stream;
use vortex_buffer::Buffer;
use vortex_error::VortexResult;

/// A message stream allows the caller to consume Vortex messages as well as arbitrary buffers.
pub trait MessageStream: Stream<Item = VortexResult<Buffer>> {
    fn read_exact<B>(&mut self, buffer: B) -> VortexResult<B>;
}
