use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Bytes;
use flatbuffers::{ForwardsUOffset, Vector};
use vortex::Context;
use vortex_dtype::DType;
use vortex_error::{vortex_bail, vortex_err, VortexResult};
use vortex_flatbuffers::footer as fb;

use super::projections::Projection;
use crate::layouts::reader::batch::BatchReader;
use crate::layouts::reader::buffered::BufferedReader;
use crate::layouts::reader::context::{LayoutDeserializer, LayoutId, LayoutSpec};
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

                let array = array_reader.into_array(self.ctx.clone(), self.cache.dtype())?;
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
        layout_serde: LayoutDeserializer,
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

/// In memory representation of Columnar NestedLayout.
///
/// Each child represents a column
#[derive(Debug)]
pub struct ColumnLayout {
    fb_bytes: Bytes,
    fb_loc: usize,
    scan: Scan,
    layout_serde: LayoutDeserializer,
    message_cache: RelativeLayoutCache,
    state: ColumnLayoutState,
}

impl ColumnLayout {
    pub fn new(
        fb_bytes: Bytes,
        fb_loc: usize,
        scan: Scan,
        layout_serde: LayoutDeserializer,
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

    fn read_child(
        &self,
        idx: usize,
        children: Vector<ForwardsUOffset<fb::Layout>>,
        dtype: DType,
    ) -> VortexResult<Box<dyn Layout>> {
        let layout = children.get(idx);

        // TODO: Figure out complex nested schema projections
        let mut child_scan = self.scan.clone();
        child_scan.projection = Projection::All;

        self.layout_serde.read_layout(
            self.fb_bytes.clone(),
            layout._tab.loc(),
            child_scan,
            self.message_cache.relative(idx as u16, dtype),
        )
    }
}

impl Layout for ColumnLayout {
    fn read(&mut self) -> VortexResult<Option<ReadResult>> {
        match &mut self.state {
            ColumnLayoutState::Init => {
                let DType::Struct(s, ..) = self.message_cache.dtype() else {
                    vortex_bail!("Column layout must have struct dtype")
                };

                let fb_children = self.flatbuffer().children().expect("must have children");

                let column_layouts = match self.scan.projection {
                    Projection::All => (0..fb_children.len())
                        .map(|idx| self.read_child(idx, fb_children, s.dtypes()[idx].clone()))
                        .collect::<VortexResult<Vec<_>>>()?,
                    Projection::Partial(ref v) => v
                        .iter()
                        .enumerate()
                        .map(|(position, &projection_idx)| {
                            self.read_child(
                                projection_idx,
                                fb_children,
                                s.dtypes()[position].clone(),
                            )
                        })
                        .collect::<VortexResult<Vec<_>>>()?,
                };

                let reader = BatchReader::new(s.names().clone(), column_layouts);
                self.state = ColumnLayoutState::ReadColumns(reader);
                self.read()
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
        layout_serde: LayoutDeserializer,
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

/// In memory representation of Chunked NestedLayout.
///
/// First child in the list is the metadata table
/// Subsequent children are consecutive chunks of this layout
#[derive(Debug)]
pub struct ChunkedLayout {
    fb_bytes: Bytes,
    fb_loc: usize,
    scan: Scan,
    layout_builder: LayoutDeserializer,
    message_cache: RelativeLayoutCache,
    state: ChunkedLayoutState,
}

impl ChunkedLayout {
    pub fn new(
        fb_bytes: Bytes,
        fb_loc: usize,
        scan: Scan,
        layout_serde: LayoutDeserializer,
        message_cache: RelativeLayoutCache,
    ) -> Self {
        Self {
            fb_bytes,
            fb_loc,
            scan,
            layout_builder: layout_serde,
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
                        self.layout_builder.read_layout(
                            self.fb_bytes.clone(),
                            c._tab.loc(),
                            self.scan.clone(),
                            self.message_cache
                                .relative(i as u16, self.message_cache.dtype().clone()),
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
