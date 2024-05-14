use std::future::Future;
use std::sync::Arc;

use flatbuffers::root;
use itertools::Itertools;
use vortex::{Array, ArrayView, Context, IntoArray, ToArray, ViewContext};
use vortex_buffer::Buffer;
use vortex_dtype::DType;
use vortex_error::{vortex_bail, vortex_err, VortexError, VortexResult};

use crate::codecs::MessageReader;
use crate::messages::SerdeContextDeserializer;
use crate::ALIGNMENT;

pub trait MessageReaderExt: MessageReader {
    fn read_view_context(
        &mut self,
        ctx: &Context,
    ) -> impl Future<Output = VortexResult<ViewContext>> {
        async {
            if self.peek().and_then(|m| m.header_as_context()).is_none() {
                vortex_bail!("Expected context message");
            }

            let view_ctx = SerdeContextDeserializer {
                fb: self.next().await?.header_as_context().unwrap(),
                ctx,
            }
            .try_into()?;

            Ok(view_ctx)
        }
    }

    fn read_dtype(&mut self) -> impl Future<Output = VortexResult<DType>> {
        async {
            if self.peek().and_then(|m| m.header_as_schema()).is_none() {
                vortex_bail!("Expected schema message");
            }

            let schema_msg = self.next().await?.header_as_schema().unwrap();

            let dtype = DType::try_from(
                schema_msg
                    .dtype()
                    .ok_or_else(|| vortex_err!(InvalidSerde: "Schema missing DType"))?,
            )
            .map_err(|e| vortex_err!(InvalidSerde: "Failed to parse DType: {}", e))?;

            Ok(dtype)
        }
    }

    fn maybe_read_chunk(
        &mut self,
        view_ctx: Arc<ViewContext>,
        dtype: DType,
    ) -> impl Future<Output = VortexResult<Option<Array>>> {
        async {
            if self.peek().and_then(|m| m.header_as_chunk()).is_none() {
                return Ok(None);
            }

            let buffers = self.read_buffers().await?;
            let flatbuffer = self.next_raw().await?;

            let view = ArrayView::try_new(
                view_ctx,
                dtype,
                flatbuffer,
                |flatbuffer| {
                    root::<crate::flatbuffers::ipc::Message>(flatbuffer)
                        .map_err(VortexError::from)
                        .map(|msg| msg.header_as_chunk().unwrap())
                        .and_then(|chunk| chunk.array().ok_or(vortex_err!("Chunk missing Array")))
                },
                buffers,
            )?;

            // Validate it
            view.to_array().with_dyn(|_| Ok::<(), VortexError>(()))?;

            Ok(Some(view.into_array()))
        }
    }

    /// Fetch the buffers associated with this message.
    fn read_buffers(&mut self) -> impl Future<Output = VortexResult<Vec<Buffer>>> {
        async {
            let Some(chunk_msg) = self.peek().and_then(|m| m.header_as_chunk()) else {
                // We could return an error here?
                return Ok(Vec::new());
            };

            // Initialize the column's buffers for a vectored read.
            // To start with, we include the padding and then truncate the buffers after.
            // TODO(ngates): improve the flatbuffer format instead of storing offset/len per buffer.
            let buffers = chunk_msg
                .buffers()
                .unwrap_or_default()
                .iter()
                .map(|buffer| {
                    // FIXME(ngates): this assumes the next buffer offset == the aligned length of
                    //  the previous buffer. I will fix this by improving the flatbuffer format instead
                    //  of fiddling with the logic here.
                    let len_width_padding =
                        (buffer.length() as usize + (ALIGNMENT - 1)) & !(ALIGNMENT - 1);
                    // TODO(ngates): switch to use uninitialized
                    // TODO(ngates): allocate the entire thing in one go and then split
                    vec![0u8; len_width_padding]
                })
                .collect_vec();

            // Just sanity check the above
            assert_eq!(
                buffers.iter().map(|b| b.len()).sum::<usize>(),
                chunk_msg.buffer_size() as usize
            );

            // Issue a vectored read to fill all buffers
            let buffers: Vec<Vec<u8>> = self.read_into(buffers).await?;

            // Truncate each buffer to strip the padding.
            let buffers = buffers
                .into_iter()
                .zip(
                    self.peek()
                        .unwrap()
                        .header_as_chunk()
                        .unwrap()
                        .buffers()
                        .unwrap_or_default()
                        .iter(),
                )
                .map(|(mut vec, buf)| {
                    vec.truncate(buf.length() as usize);
                    Buffer::from(vec)
                })
                .collect_vec();

            Ok(buffers)
        }
    }
}

impl<M: MessageReader> MessageReaderExt for M {}
