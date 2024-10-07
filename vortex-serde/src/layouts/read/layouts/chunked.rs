use std::collections::VecDeque;

use bytes::Bytes;
use vortex_error::{vortex_bail, vortex_err, VortexExpect, VortexResult};
use vortex_flatbuffers::footer;

use crate::layouts::read::batch::FilterLayoutReader;
use crate::layouts::read::buffered::BufferedReader;
use crate::layouts::read::cache::RelativeLayoutCache;
use crate::layouts::read::selection::RowSelector;
use crate::layouts::{
    LayoutDeserializer, LayoutId, LayoutReader, LayoutSpec, Messages, RangeResult, ReadResult,
    Scan, ScanExpr,
};
#[derive(Default, Debug)]
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
        _length: u64,
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
    InitFilter,
    FilterChunks(FilterLayoutReader),
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
        let state = match scan.expr {
            ScanExpr::Projection(_) => ChunkedLayoutState::Init,
            ScanExpr::Filter(_) => ChunkedLayoutState::InitFilter,
        };
        Self {
            fb_bytes,
            fb_loc,
            scan,
            layout_builder: layout_serde,
            message_cache,
            state,
        }
    }

    fn flatbuffer(&self) -> footer::NestedLayout {
        let fb_layout = unsafe {
            let tab = flatbuffers::Table::new(&self.fb_bytes, self.fb_loc);
            footer::Layout::init_from_table(tab)
        };
        fb_layout
            .layout_as_nested_layout()
            .vortex_expect("ChunkedLayout: Failed to read nested layout from flatbuffer")
    }

    fn child_reader(&self) -> VortexResult<BufferedReader> {
        let dtype = self.message_cache.dtype()?;
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
                    // FIXME
                    0,
                    self.scan.clone(),
                    self.message_cache.relative(i as u16, dtype.clone()),
                )
            })
            .collect::<VortexResult<VecDeque<_>>>()?;
        Ok(BufferedReader::new(children, self.scan.batch_size))
    }
}

impl LayoutReader for ChunkedLayout {
    fn read_next(&mut self, selection: RowSelector) -> VortexResult<Option<ReadResult>> {
        match &mut self.state {
            ChunkedLayoutState::Init => {
                self.state = ChunkedLayoutState::ReadChunks(self.child_reader()?);
                self.read_next(selection)
            }
            ChunkedLayoutState::ReadChunks(cr) => cr.read_next_batch(selection),
            _ => vortex_bail!("We are returning chunks"),
        }
    }

    fn read_range(&mut self) -> VortexResult<Option<RangeResult>> {
        match &mut self.state {
            ChunkedLayoutState::InitFilter => {
                let ScanExpr::Filter(rf) = self.scan.expr.clone() else {
                    vortex_bail!("Must have a filter")
                };
                let reader = self.child_reader()?;
                self.state =
                    ChunkedLayoutState::FilterChunks(FilterLayoutReader::new(Box::new(reader), rf));
                self.read_range()
            }
            ChunkedLayoutState::FilterChunks(_) => panic!("NOOOOO"),
            _ => vortex_bail!("We are returning ranges"),
        }
    }

    fn advance(&mut self, _up_to_row: usize) -> VortexResult<Messages> {
        todo!()
    }
}
