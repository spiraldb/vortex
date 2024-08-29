use std::sync::Arc;

use bytes::{Buf, Bytes};
use flatbuffers::root_unchecked;
use vortex::Context;
use vortex_error::{vortex_err, VortexResult};
use vortex_flatbuffers::{message as fb, ReadFlatBuffer};

use crate::layouts::read::cache::RelativeLayoutCache;
use crate::layouts::read::selection::RowSelector;
use crate::layouts::{LayoutReader, PlanResult, PruningScan, ReadResult};
use crate::message_reader::ArrayBufferReader;
use crate::messages::IPCDType;
use crate::stream_writer::ByteRange;

#[derive(Debug)]
enum FlatLayoutState {
    Init,
    ReadBatch,
    Finished,
}

#[derive(Debug)]
pub struct FlatLayout {
    range: ByteRange,
    ctx: Arc<Context>,
    cache: RelativeLayoutCache,
    state: FlatLayoutState,
}

impl FlatLayout {
    pub fn new(begin: u64, end: u64, ctx: Arc<Context>, cache: RelativeLayoutCache) -> Self {
        Self {
            range: ByteRange { begin, end },
            ctx,
            cache,
            state: FlatLayoutState::Init,
        }
    }
}

impl LayoutReader for FlatLayout {
    fn with_selected_rows(&mut self, _row_selector: &RowSelector) {
        // Nothing to do
    }

    fn read_next(&mut self) -> VortexResult<Option<ReadResult>> {
        match self.state {
            FlatLayoutState::Init => {
                self.state = FlatLayoutState::ReadBatch;
                Ok(Some(ReadResult::ReadMore(vec![(
                    self.cache.absolute_id(&[]),
                    self.range,
                )])))
            }
            FlatLayoutState::ReadBatch => {
                let mut buf = self.cache.remove(&[]).ok_or_else(|| {
                    vortex_err!(
                        "Wrong state transition, message {:?} (with range {}) should have been fetched",
                        self.cache.absolute_id(&[]),
                        self.range
                    )
                })?;

                let dtype = if self.cache.has_dtype() {
                    self.cache.dtype().clone()
                } else {
                    let dtype_fb_length = buf.get_u32_le();
                    let dtype_buf = buf.split_to(dtype_fb_length as usize);
                    let msg = unsafe { root_unchecked::<fb::Message>(&dtype_buf) }
                        .header_as_schema()
                        .ok_or_else(|| {
                            vortex_err!(
                                "Expected schema message; this was checked earlier in the function"
                            )
                        })?;
                    IPCDType::read_flatbuffer(&msg)?.0
                };

                let mut array_reader = ArrayBufferReader::new();
                let mut read_buf = Bytes::new();
                while let Some(u) = array_reader.read(read_buf)? {
                    read_buf = buf.split_to(u);
                }

                let array = array_reader.into_array(self.ctx.clone(), dtype)?;
                self.state = FlatLayoutState::Finished;
                Ok(Some(ReadResult::Batch(array)))
            }
            FlatLayoutState::Finished => Ok(None),
        }
    }

    fn plan(&mut self, _scan: PruningScan) -> VortexResult<Option<PlanResult>> {
        Ok(None)
    }
}
