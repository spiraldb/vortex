use std::collections::VecDeque;
use std::fmt::Debug;
use std::sync::{Arc, RwLock};

use ahash::{HashMap, HashMapExt};
use bytes::Bytes;
use vortex::{Array, Context};
use vortex_dtype::DType;
use vortex_error::{vortex_bail, vortex_err, VortexResult};

use crate::flatbuffers::footer as fb;
use crate::flatbuffers::footer::LayoutVariant;
use crate::layout::reader::batch::BatchReader;
use crate::layout::reader::buffered::BufferedLayoutReader;
use crate::layout::reader::filtering::RowFilter;
use crate::layout::reader::projections::Projection;
use crate::writer::ByteRange;
use crate::ArrayBufferReader;

mod footer;
pub mod reader;
pub mod writer;

#[cfg(test)]
mod tests;

pub const FULL_FOOTER_SIZE: usize = 20;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct LayoutId(u16);

pub trait LayoutSpec: Debug + Send + Sync {
    fn id(&self) -> LayoutId;

    fn layout(
        &self,
        fb_bytes: Bytes,
        fb_loc: usize,
        scan: Scan,
        layout_reader: LayoutReader,
        message_cache: RelativeMessageCache,
    ) -> Box<dyn Layout>;
}

pub type LayoutRef = &'static dyn LayoutSpec;

#[derive(Debug, Clone)]
pub struct LayoutContext {
    layout_refs: HashMap<LayoutId, LayoutRef>,
}

impl LayoutContext {
    pub fn new(layout_refs: HashMap<LayoutId, LayoutRef>) -> Self {
        Self { layout_refs }
    }

    pub fn lookup_layout_def(&self, id: &LayoutId) -> Option<LayoutRef> {
        self.layout_refs.get(id).cloned()
    }
}

impl Default for LayoutContext {
    fn default() -> Self {
        Self::new(
            [
                &ColumnLayoutDefinition as LayoutRef,
                &ChunkedLayoutDefinition,
            ]
            .into_iter()
            .map(|l| (l.id(), l))
            .collect(),
        )
    }
}

#[derive(Debug, Clone)]
pub struct LayoutReader {
    ctx: Arc<Context>,
    layout_ctx: Arc<LayoutContext>,
}

impl LayoutReader {
    pub fn new(ctx: Arc<Context>, layout_ctx: Arc<LayoutContext>) -> Self {
        Self { ctx, layout_ctx }
    }

    pub fn read(
        &self,
        fb_bytes: Bytes,
        fb_loc: usize,
        scan: Scan,
        message_cache: RelativeMessageCache,
    ) -> VortexResult<Box<dyn Layout>> {
        let fb_layout = unsafe {
            let tab = flatbuffers::Table::new(&fb_bytes, fb_loc);
            fb::Layout::init_from_table(tab)
        };

        match fb_layout.layout_type() {
            LayoutVariant::FlatLayout => {
                let flat_layout = fb_layout.layout_as_flat_layout().expect("must be flat");
                Ok(Box::new(FlatLayout::new(
                    flat_layout.begin(),
                    flat_layout.end(),
                    self.ctx.clone(),
                    message_cache,
                )))
            }
            LayoutVariant::NestedLayout => {
                let nested_layout = fb_layout
                    .layout_as_nested_layout()
                    .expect("must be nested layout");
                Ok(self
                    .layout_ctx
                    .lookup_layout_def(&LayoutId(nested_layout.encoding()))
                    .ok_or_else(|| {
                        vortex_err!("Unknown layout definition {}", nested_layout.encoding())
                    })?
                    .layout(fb_bytes, fb_loc, scan, self.clone(), message_cache))
            }
            _ => unreachable!("Unknown flatbuffer layout"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Scan {
    indices: Option<Array>,
    projection: Projection,
    filter: Option<RowFilter>,
    batch_size: usize,
}

/// Unique identifier for a message within a layout
pub type MessageId = u16;

#[derive(Debug)]
pub enum ReadResult {
    GetMsgs(
        Vec<(Vec<MessageId>, ByteRange)>,
        Vec<(Vec<MessageId>, ByteRange)>,
    ),
    Batch(Array),
}

pub enum PlanResult {
    GetMsg(Vec<(Vec<MessageId>, ByteRange)>),
    Batch(Vec<MessageId>, Array),
}

#[derive(Debug)]
pub struct MessagesCache {
    cache: HashMap<Vec<MessageId>, Bytes>,
}

impl Default for MessagesCache {
    fn default() -> Self {
        Self::new()
    }
}

impl MessagesCache {
    pub fn new() -> Self {
        Self {
            cache: HashMap::new(),
        }
    }

    pub fn get(&self, path: &[MessageId]) -> Option<Bytes> {
        self.cache.get(path).cloned()
    }

    pub fn set(&mut self, path: Vec<MessageId>, value: Bytes) {
        self.cache.insert(path, value);
    }
}

#[derive(Debug)]
pub struct RelativeMessageCache {
    cache_ref: Arc<RwLock<MessagesCache>>,
    path: Vec<MessageId>,
    dtype: DType,
}

impl RelativeMessageCache {
    pub fn new(dtype: DType, cache_ref: Arc<RwLock<MessagesCache>>, path: Vec<MessageId>) -> Self {
        Self {
            cache_ref,
            path,
            dtype,
        }
    }

    pub fn relative(&self, id: MessageId, dtype: DType) -> Self {
        let mut new_path = self.path.clone();
        new_path.push(id);
        Self::new(dtype, self.cache_ref.clone(), new_path)
    }

    pub fn get(&self, path: &[MessageId]) -> Option<Bytes> {
        self.cache_ref.read().unwrap().get(&self.absolute_id(path))
    }

    pub fn get_dtype(&self) -> DType {
        self.dtype.clone()
    }

    pub fn absolute_id(&self, path: &[MessageId]) -> Vec<MessageId> {
        let mut lookup_key = self.path.clone();
        lookup_key.extend_from_slice(path);
        lookup_key
    }
}

pub trait Layout: Debug + Send {
    fn read(&mut self) -> VortexResult<Option<ReadResult>>;

    fn plan(&mut self, scan: Scan) -> VortexResult<Option<PlanResult>>;
}

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
    cache: RelativeMessageCache,
    state: FlatLayoutState,
}

impl FlatLayout {
    pub fn new(begin: u64, end: u64, ctx: Arc<Context>, cache: RelativeMessageCache) -> Self {
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
                Ok(Some(ReadResult::GetMsgs(
                    vec![(self.cache.absolute_id(&[]), self.range)],
                    Vec::new(),
                )))
            }
            FlatLayoutState::ReadBatch => {
                let mut buf = self.cache.get(&[]).ok_or_else(|| {
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

    fn plan(&mut self, _scan: Scan) -> VortexResult<Option<PlanResult>> {
        // Nothing to do when planning... should this level handle AllFalse row filters?
        Ok(None)
    }
}

#[derive(Debug)]
pub struct ColumnLayoutDefinition;

impl ColumnLayoutDefinition {
    const ID: LayoutId = LayoutId(2);
}

impl LayoutSpec for ColumnLayoutDefinition {
    fn id(&self) -> LayoutId {
        Self::ID
    }

    fn layout(
        &self,
        fb_bytes: Bytes,
        fb_loc: usize,
        scan: Scan,
        layout_serde: LayoutReader,
        message_cache: RelativeMessageCache,
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
    message_cache: RelativeMessageCache,
    state: ColumnLayoutState,
}

impl ColumnLayout {
    pub fn new(
        fb_bytes: Bytes,
        fb_loc: usize,
        scan: Scan,

        layout_serde: LayoutReader,
        message_cache: RelativeMessageCache,
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

    fn plan(&mut self, _scan: Scan) -> VortexResult<Option<PlanResult>> {
        // Delegate the plan to each of the children that are included in the row filter
        Ok(None)
    }
}

#[derive(Debug)]
pub struct ChunkedLayoutDefinition;

impl ChunkedLayoutDefinition {
    const ID: LayoutId = LayoutId(1);
}

impl LayoutSpec for ChunkedLayoutDefinition {
    fn id(&self) -> LayoutId {
        Self::ID
    }

    fn layout(
        &self,
        fb_bytes: Bytes,
        fb_loc: usize,
        scan: Scan,
        layout_serde: LayoutReader,
        message_cache: RelativeMessageCache,
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
    ReadChunks(BufferedLayoutReader),
}

#[derive(Debug)]
pub struct ChunkedLayout {
    fb_bytes: Bytes,
    fb_loc: usize,
    scan: Scan,
    layout_serde: LayoutReader,
    message_cache: RelativeMessageCache,
    state: ChunkedLayoutState,
}

impl ChunkedLayout {
    pub fn new(
        fb_bytes: Bytes,
        fb_loc: usize,
        scan: Scan,
        layout_serde: LayoutReader,
        message_cache: RelativeMessageCache,
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
                let mut reader = BufferedLayoutReader::new(children, self.scan.batch_size);
                let rr = reader.read();
                self.state = ChunkedLayoutState::ReadChunks(reader);
                rr
            }
            ChunkedLayoutState::ReadChunks(cr) => cr.read(),
        }
    }

    fn plan(&mut self, _scan: Scan) -> VortexResult<Option<PlanResult>> {
        // Read the metadata range, then decode the metadata table
        Ok(None)
    }
}
