use std::fmt::Debug;
use std::sync::Arc;

use ahash::HashMap;
use bytes::Bytes;
use vortex::Context;
use vortex_error::{vortex_err, VortexResult};
use vortex_flatbuffers::footer as fb;
use vortex_flatbuffers::footer::LayoutVariant;

use crate::layouts::reader::layouts::{ChunkedLayoutSpec, ColumnLayoutSpec, FlatLayout};
use crate::layouts::reader::{Layout, RelativeLayoutCache, Scan};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct LayoutId(pub u16);

pub trait LayoutSpec: Debug + Send + Sync {
    fn id(&self) -> LayoutId;

    fn layout(
        &self,
        fb_bytes: Bytes,
        fb_loc: usize,
        scan: Scan,
        layout_reader: LayoutDeserializer,
        message_cache: RelativeLayoutCache,
    ) -> Box<dyn Layout>;
}

pub type LayoutSpecRef = &'static dyn LayoutSpec;

#[derive(Debug, Clone)]
pub struct LayoutContext {
    layout_refs: HashMap<LayoutId, LayoutSpecRef>,
}

impl LayoutContext {
    pub fn new(layout_refs: HashMap<LayoutId, LayoutSpecRef>) -> Self {
        Self { layout_refs }
    }

    pub fn lookup_layout(&self, id: &LayoutId) -> Option<LayoutSpecRef> {
        self.layout_refs.get(id).cloned()
    }
}

impl Default for LayoutContext {
    fn default() -> Self {
        Self::new(
            [&ColumnLayoutSpec as LayoutSpecRef, &ChunkedLayoutSpec]
                .into_iter()
                .map(|l| (l.id(), l))
                .collect(),
        )
    }
}

#[derive(Default, Debug, Clone)]
pub struct LayoutDeserializer {
    ctx: Arc<Context>,
    layout_ctx: Arc<LayoutContext>,
}

impl LayoutDeserializer {
    pub fn new(ctx: Arc<Context>, layout_ctx: Arc<LayoutContext>) -> Self {
        Self { ctx, layout_ctx }
    }

    pub fn read_layout(
        &self,
        fb_bytes: Bytes,
        fb_loc: usize,
        scan: Scan,
        message_cache: RelativeLayoutCache,
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
                    .lookup_layout(&LayoutId(nested_layout.encoding()))
                    .ok_or_else(|| {
                        vortex_err!("Unknown layout definition {}", nested_layout.encoding())
                    })?
                    .layout(fb_bytes, fb_loc, scan, self.clone(), message_cache))
            }
            _ => unreachable!("Unknown flatbuffer layout"),
        }
    }
}
