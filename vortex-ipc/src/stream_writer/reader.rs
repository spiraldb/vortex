use vortex::Context;
use vortex_error::VortexResult;

use crate::io::VortexRead;
use crate::MessageReader;

pub struct ArrayReader;

impl<R: VortexRead> ArrayReader {
    pub async fn read_array(read: R, ctx: &Context) -> VortexResult<R> {
        let mut msgs = MessageReader::try_new(read).await?;
        msgs.array_stream_from_messages(ctx)
    }
}
