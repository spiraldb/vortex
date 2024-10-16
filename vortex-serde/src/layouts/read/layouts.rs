use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Bytes;
use flatbuffers::{ForwardsUOffset, Vector};
use vortex::Context;
use vortex_dtype::field::Field;
use vortex_dtype::DType;
use vortex_error::{vortex_bail, vortex_err, VortexExpect as _, VortexResult};
use vortex_flatbuffers::footer as fb;
use vortex_schema::projection::Projection;

use crate::layouts::read::batch::BatchReader;
use crate::layouts::read::buffered::BufferedReader;
use crate::layouts::read::cache::RelativeLayoutCache;
use crate::layouts::read::context::{LayoutDeserializer, LayoutId, LayoutSpec};
use crate::layouts::read::{LayoutReader, ReadResult, Scan};
use crate::stream_writer::ByteRange;
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
}

impl LayoutReader for FlatLayout {
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
                let mut buf = self.cache.get(&[]).ok_or_else(|| {
                    vortex_err!(
                        "Wrong state transition, message {:?} (with range {}) should have been fetched",
                        self.cache.absolute_id(&[]),
                        self.range
                    )
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
    ) -> Box<dyn LayoutReader> {
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
        fb_layout
            .layout_as_nested_layout()
            .vortex_expect("ColumnLayout: Failed to read nested layout from flatbuffer")
    }

    fn read_child(
        &self,
        idx: usize,
        children: Vector<ForwardsUOffset<fb::Layout>>,
        dtype: DType,
    ) -> VortexResult<Box<dyn LayoutReader>> {
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

impl LayoutReader for ColumnLayout {
    fn read_next(&mut self) -> VortexResult<Option<ReadResult>> {
        match &mut self.state {
            ColumnLayoutState::Init => {
                let DType::Struct(s, ..) = self.message_cache.dtype() else {
                    vortex_bail!("Column layout must have struct dtype")
                };

                let fb_children = self
                    .flatbuffer()
                    .children()
                    .ok_or_else(|| vortex_err!("Missing children"))?;

                let column_layouts = match self.scan.projection {
                    Projection::All => (0..fb_children.len())
                        .map(|idx| self.read_child(idx, fb_children, s.dtypes()[idx].clone()))
                        .collect::<VortexResult<Vec<_>>>()?,
                    Projection::Flat(ref v) => v
                        .iter()
                        .zip(s.dtypes().iter().cloned())
                        .map(|(projected_field, dtype)| {
                            let child_idx = match projected_field {
                                Field::Name(n) => s.find_name(n.as_ref()).ok_or_else(|| {
                                    vortex_err!("Invalid projection, trying to select  {n}")
                                })?,
                                Field::Index(i) => *i,
                            };
                            self.read_child(child_idx, fb_children, dtype)
                        })
                        .collect::<VortexResult<Vec<_>>>()?,
                };

                let reader = BatchReader::new(s.names().clone(), column_layouts);
                self.state = ColumnLayoutState::ReadColumns(reader);
                self.read_next()
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
    ) -> Box<dyn LayoutReader> {
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
        fb_layout
            .layout_as_nested_layout()
            .vortex_expect("ChunkedLayout: Failed to read nested layout from flatbuffer")
    }
}

impl LayoutReader for ChunkedLayout {
    fn read_next(&mut self) -> VortexResult<Option<ReadResult>> {
        match &mut self.state {
            ChunkedLayoutState::Init => {
                let children = self
                    .flatbuffer()
                    .children()
                    .ok_or_else(|| vortex_err!("Missing children"))?
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
                let reader = BufferedReader::new(children, self.scan.batch_size);
                self.state = ChunkedLayoutState::ReadChunks(reader);
                self.read_next()
            }
            ChunkedLayoutState::ReadChunks(cr) => cr.read(),
        }
    }
}
