use vortex_dtype::DType;
use vortex_error::VortexResult;

use crate::io::VortexRead;
use crate::message_reader::MessageReader;

/// Reader for serialized dtype messages
pub struct DTypeReader<R: VortexRead> {
    msgs: MessageReader<R>,
}

impl<R: VortexRead> DTypeReader<R> {
    /// Create new ['DTypeReader'] given readable contents
    pub async fn new(read: R) -> VortexResult<Self> {
        Ok(Self {
            msgs: MessageReader::try_new(read).await?,
        })
    }

    /// Deserialize dtype out of ipc serialized format
    pub async fn read_dtype(&mut self) -> VortexResult<DType> {
        self.msgs.read_dtype().await
    }

    /// Deconstruct this reader into its underlying contents for further reuse
    pub fn into_inner(self) -> R {
        self.msgs.into_inner()
    }
}
