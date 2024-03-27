use crate::context::IPCContext;
use crate::flatbuffers::ipc::Message;
use flatbuffers::{root, Follow, Verifiable};
use std::io;
use std::io::{BufReader, Read};
use vortex::array::ArrayRef;
use vortex_error::{VortexError, VortexResult};

#[allow(dead_code)]
pub struct StreamReader<R: Read> {
    read: R,

    ctx: IPCContext,
    // Optionally take a projection?

    // Use replace to swap the scratch buffer.
    // std::mem::replace
    scratch: Vec<u8>,
}

impl<R: Read> StreamReader<BufReader<R>> {
    pub fn try_new(read: R) -> VortexResult<Self> {
        Self::try_new_unbuffered(BufReader::new(read))
    }
}

impl<R: Read> StreamReader<R> {
    pub fn try_new_unbuffered(mut read: R) -> VortexResult<Self> {
        let mut msg_vec = Vec::new();
        let fb_msg = read
            .read_flatbuffer::<Message>(&mut msg_vec)?
            .ok_or_else(|| VortexError::InvalidSerde("Invalid Vortex IPC format".into()))?;
        let fb_ctx = fb_msg.header_as_context().ok_or_else(|| {
            VortexError::InvalidSerde("Expected IPC Context as first message in stream".into())
        })?;
        let ctx: IPCContext = fb_ctx.try_into()?;

        Ok(Self {
            read,
            ctx,
            scratch: Vec::with_capacity(1024),
        })
    }

    // TODO(ngates): avoid returning a heap-allocated array.
    // TODO(ngates): return an ArrayStream that can iterate over batches.
    pub fn next_array(&mut self) -> VortexResult<Option<ArrayRef>> {
        let msg = match self.read.read_flatbuffer::<Message>(&mut self.scratch)? {
            None => return Ok(None),
            Some(msg) => msg,
        };
        let _schema = msg.header_as_schema().ok_or_else(|| {
            VortexError::InvalidSerde("Expected IPC Schema as message header".into())
        })?;
        todo!()
    }
}

trait FlatBufferReader {
    fn read_flatbuffer<'a, F>(&mut self, buffer: &'a mut Vec<u8>) -> VortexResult<Option<F>>
    where
        F: 'a + Follow<'a, Inner = F> + Verifiable;
}

impl<R: Read> FlatBufferReader for R {
    fn read_flatbuffer<'a, F>(&mut self, buffer: &'a mut Vec<u8>) -> VortexResult<Option<F>>
    where
        F: 'a + Follow<'a, Inner = F> + Verifiable,
    {
        let mut msg_size: [u8; 4] = [0; 4];
        if let Err(e) = self.read_exact(&mut msg_size) {
            return match e.kind() {
                io::ErrorKind::UnexpectedEof => Ok(None),
                _ => Err(e.into()),
            };
        }

        let msg_size = u32::from_le_bytes(msg_size) as u64;
        self.take(msg_size).read_to_end(buffer)?;
        Ok(Some(root::<F>(buffer)?))
    }
}
