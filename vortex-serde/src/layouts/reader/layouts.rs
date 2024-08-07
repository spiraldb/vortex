use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Bytes;
use vortex::Context;
use vortex_dtype::DType;
use vortex_error::{vortex_bail, vortex_err, VortexResult};

use crate::flatbuffers::footer as fb;
use crate::layouts::reader::batch::BatchReader;
use crate::layouts::reader::buffered::BufferedReader;
use crate::layouts::reader::context::{LayoutId, LayoutReader, LayoutSpec};
use crate::layouts::reader::{Layout, ReadResult, RelativeLayoutCache, Scan};
use crate::writer::ByteRange;
use crate::ArrayBufferReader;

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

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.range.len()
    }
}

impl Layout for FlatLayout {
    fn read(&mut self) -> VortexResult<Option<ReadResult>> {
        match self.state {
            FlatLayoutState::Init => {
                self.state = FlatLayoutState::ReadBatch;
                Ok(Some(ReadResult::GetMsgs(vec![(
                    self.cache.absolute_id(&[]),
                    self.range,
                )])))
            }
            FlatLayoutState::ReadBatch => {
                let mut buf = self.cache.remove(&[]).ok_or_else(|| {
                    vortex_err!("Wrong state transition, message should have been fetched")
                })?;

                let mut array_reader = ArrayBufferReader::new();
                let mut read_buf = Bytes::new();
                while let Some(u) = array_reader.read(read_buf)? {
                    read_buf = buf.split_to(u);
                }

                let array = array_reader.into_array(self.ctx.clone(), self.cache.get_dtype())?;
                self.state = FlatLayoutState::Finished;
                Ok(Some(ReadResult::Batch(array)))
            }
            FlatLayoutState::Finished => Ok(None),
        }
    }
}

#[derive(Debug)]
pub struct ColumnLayoutSpec;

impl ColumnLayoutSpec {
    pub const ID: LayoutId = LayoutId(2);
}

impl LayoutSpec for ColumnLayoutSpec {
    fn id(&self) -> LayoutId {
        Self::ID
    }

    fn layout(
        &self,
        fb_bytes: Bytes,
        fb_loc: usize,
        scan: Scan,
        layout_serde: LayoutReader,
        message_cache: RelativeLayoutCache,
    ) -> Box<dyn Layout> {
        Box::new(ColumnLayout::new(
            fb_bytes,
            fb_loc,
            scan,
            layout_serde,
            message_cache,
        ))
    }
}

#[derive(Debug)]
pub enum ColumnLayoutState {
    Init,
    ReadColumns(BatchReader),
}

#[derive(Debug)]
pub struct ColumnLayout {
    fb_bytes: Bytes,
    fb_loc: usize,
    scan: Scan,
    layout_serde: LayoutReader,
    message_cache: RelativeLayoutCache,
    state: ColumnLayoutState,
}

impl ColumnLayout {
    pub fn new(
        fb_bytes: Bytes,
        fb_loc: usize,
        scan: Scan,

        layout_serde: LayoutReader,
        message_cache: RelativeLayoutCache,
    ) -> Self {
        Self {
            fb_bytes,
            fb_loc,
            scan,
            layout_serde,
            message_cache,
            state: ColumnLayoutState::Init,
        }
    }

    pub fn flatbuffer(&self) -> fb::NestedLayout {
        let fb_layout = unsafe {
            let tab = flatbuffers::Table::new(&self.fb_bytes, self.fb_loc);
            fb::Layout::init_from_table(tab)
        };
        fb_layout.layout_as_nested_layout().expect("must be nested")
    }
}

impl Layout for ColumnLayout {
    fn read(&mut self) -> VortexResult<Option<ReadResult>> {
        match &mut self.state {
            ColumnLayoutState::Init => {
                let DType::Struct(s, ..) = self.message_cache.get_dtype() else {
                    vortex_bail!("Column layout must have struct dtype")
                };

                let fb_children = self.flatbuffer().children().expect("must have children");

                let columns = fb_children
                    .into_iter()
                    .enumerate()
                    .zip(s.dtypes().iter().cloned())
                    .map(|((idx, child), dtype)| {
                        self.layout_serde.read(
                            self.fb_bytes.clone(),
                            child._tab.loc(),
                            self.scan.clone(),
                            self.message_cache.relative(idx as u16, dtype),
                        )
                    })
                    .collect::<VortexResult<Vec<_>>>()?;

                let mut reader = BatchReader::new(s.names().clone(), columns);
                let rr = reader.read()?;
                self.state = ColumnLayoutState::ReadColumns(reader);
                Ok(rr)
            }
            ColumnLayoutState::ReadColumns(br) => br.read(),
        }
    }
}

#[derive(Debug)]
pub struct ChunkedLayoutSpec;

impl ChunkedLayoutSpec {
    pub const ID: LayoutId = LayoutId(1);
}

impl LayoutSpec for ChunkedLayoutSpec {
    fn id(&self) -> LayoutId {
        Self::ID
    }

    fn layout(
        &self,
        fb_bytes: Bytes,
        fb_loc: usize,
        scan: Scan,
        layout_serde: LayoutReader,
        message_cache: RelativeLayoutCache,
    ) -> Box<dyn Layout> {
        Box::new(ChunkedLayout::new(
            fb_bytes,
            fb_loc,
            scan,
            layout_serde,
            message_cache,
        ))
    }
}

#[derive(Debug)]
pub enum ChunkedLayoutState {
    Init,
    ReadChunks(BufferedReader),
}

#[derive(Debug)]
pub struct ChunkedLayout {
    fb_bytes: Bytes,
    fb_loc: usize,
    scan: Scan,
    layout_serde: LayoutReader,
    message_cache: RelativeLayoutCache,
    state: ChunkedLayoutState,
}

impl ChunkedLayout {
    pub fn new(
        fb_bytes: Bytes,
        fb_loc: usize,
        scan: Scan,
        layout_serde: LayoutReader,
        message_cache: RelativeLayoutCache,
    ) -> Self {
        Self {
            fb_bytes,
            fb_loc,
            scan,
            layout_serde,
            message_cache,
            state: ChunkedLayoutState::Init,
        }
    }

    pub fn flatbuffer(&self) -> fb::NestedLayout {
        let fb_layout = unsafe {
            let tab = flatbuffers::Table::new(&self.fb_bytes, self.fb_loc);
            fb::Layout::init_from_table(tab)
        };
        fb_layout.layout_as_nested_layout().expect("must be nested")
    }
}

impl Layout for ChunkedLayout {
    fn read(&mut self) -> VortexResult<Option<ReadResult>> {
        match &mut self.state {
            ChunkedLayoutState::Init => {
                let children = self
                    .flatbuffer()
                    .children()
                    .expect("must have children")
                    .iter()
                    .enumerate()
                    // Skip over the metadata table of this layout
                    .skip(1)
                    .map(|(i, c)| {
                        self.layout_serde.read(
                            self.fb_bytes.clone(),
                            c._tab.loc(),
                            self.scan.clone(),
                            self.message_cache
                                .relative(i as u16, self.message_cache.get_dtype().clone()),
                        )
                    })
                    .collect::<VortexResult<VecDeque<_>>>()?;
                let mut reader = BufferedReader::new(children, self.scan.batch_size);
                let rr = reader.read();
                self.state = ChunkedLayoutState::ReadChunks(reader);
                rr
            }
            ChunkedLayoutState::ReadChunks(cr) => cr.read(),
        }
    }
}
