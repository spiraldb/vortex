use std::collections::VecDeque;

use bytes::Bytes;
use vortex_error::{vortex_err, VortexResult};
use vortex_flatbuffers::footer;

use crate::layouts::read::buffered::BufferedReader;
use crate::layouts::read::cache::RelativeLayoutCache;
use crate::layouts::{
    LayoutDeserializer, LayoutId, LayoutReader, LayoutSpec, ReadResult, Scan, CHUNKED_LAYOUT_ID,
};

#[derive(Debug)]
pub struct ChunkedLayoutSpec;

impl ChunkedLayoutSpec {
    pub const ID: LayoutId = CHUNKED_LAYOUT_ID;
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
    reader: Option<BufferedReader>,
}

impl ChunkedLayout {
    pub fn new(
        fb_bytes: Bytes,
        fb_loc: usize,
        scan: Scan,
        layout_builder: LayoutDeserializer,
        message_cache: RelativeLayoutCache,
    ) -> Self {
        Self {
            fb_bytes,
            fb_loc,
            scan,
            layout_builder,
            message_cache,
            reader: None,
        }
    }

    fn flatbuffer(&self) -> footer::Layout {
        unsafe {
            let tab = flatbuffers::Table::new(&self.fb_bytes, self.fb_loc);
            footer::Layout::init_from_table(tab)
        }
    }

    fn has_metadata(&self) -> bool {
        self.flatbuffer()
            .metadata()
            .map(|b| b.bytes()[0] != 0)
            .unwrap_or(false)
    }
}

impl LayoutReader for ChunkedLayout {
    fn read_next(&mut self) -> VortexResult<Option<ReadResult>> {
        if let Some(cr) = &mut self.reader {
            cr.read()
        } else {
            let children = self
                .flatbuffer()
                .children()
                .ok_or_else(|| vortex_err!("Missing children"))?
                .iter()
                .enumerate()
                // Skip over the metadata table of this layout
                .skip(if self.has_metadata() { 1 } else { 0 })
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
            self.reader = Some(BufferedReader::new(children, self.scan.batch_size));
            self.read_next()
        }
    }
}
