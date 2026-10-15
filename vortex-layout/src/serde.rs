// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::env;
use std::sync::Arc;
use std::sync::LazyLock;

use flatbuffers::FlatBufferBuilder;
use flatbuffers::ForwardsUOffset;
use flatbuffers::Vector;
use flatbuffers::VerifierOptions;
use flatbuffers::WIPOffset;
use flatbuffers::root_with_opts;
use vortex_array::dtype::DType;
use vortex_array::flatbuffers::FlatBuffer;
use vortex_array::flatbuffers::FlatBufferRoot;
use vortex_array::flatbuffers::WriteFlatBuffer;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_session::VortexSession;
use vortex_session::registry::ReadContext;
use vortex_utils::aliases::hash_map::Entry;
use vortex_utils::aliases::hash_map::HashMap;

use crate::DynLayout;
use crate::LayoutBuildContext;
use crate::LayoutContext;
use crate::LayoutRef;
use crate::children::ViewedLayoutChildren;
use crate::children::resolve_segments;
use crate::flatbuffers::layout;
use crate::layouts::foreign::new_foreign_layout;
use crate::session::LayoutSessionExt;

static LAYOUT_VERIFIER: LazyLock<VerifierOptions> = LazyLock::new(|| {
    VerifierOptions {
        // Overridden
        max_tables: env::var("VORTEX_MAX_LAYOUT_TABLES")
            .ok()
            .and_then(|lmt| lmt.parse::<usize>().ok())
            .unwrap_or(1000000),
        max_depth: env::var("VORTEX_MAX_LAYOUT_DEPTH")
            .ok()
            .and_then(|lmt| lmt.parse::<usize>().ok())
            .unwrap_or(64),
        // Defaults from flatbuffers
        max_apparent_size: 1 << 31,
        ignore_missing_null_terminator: false,
    }
});

/// Parse a [`LayoutRef`] from a layout flatbuffer.
pub fn layout_from_flatbuffer(
    flatbuffer: FlatBuffer,
    dtype: &DType,
    layout_ctx: &ReadContext,
    ctx: &ReadContext,
    session: &VortexSession,
) -> VortexResult<LayoutRef> {
    layout_from_flatbuffer_with_options(flatbuffer, dtype, layout_ctx, ctx, session, false)
}

/// Parse a [`LayoutRef`] from a layout flatbuffer with unknown-encoding behavior control.
pub fn layout_from_flatbuffer_with_options(
    flatbuffer: FlatBuffer,
    dtype: &DType,
    layout_ctx: &ReadContext,
    ctx: &ReadContext,
    session: &VortexSession,
    allow_unknown: bool,
) -> VortexResult<LayoutRef> {
    let layout_session = session.layouts();
    let layouts = layout_session.registry();
    let fb_layout = root_with_opts::<layout::Layout>(&LAYOUT_VERIFIER, &flatbuffer)?;
    let encoding_id = layout_ctx
        .resolve(fb_layout.encoding())
        .ok_or_else(|| vortex_err!("Invalid encoding ID: {}", fb_layout.encoding()))?;
    let encoding = layouts.get(&encoding_id);

    if encoding.is_none() && allow_unknown {
        return foreign_layout_from_fb(fb_layout, dtype, layout_ctx, None);
    }
    let encoding =
        encoding.ok_or_else(|| vortex_err!("Invalid encoding ID: {}", fb_layout.encoding()))?;

    let (segments, segment_map) = resolve_segments(fb_layout, None)?;

    // SAFETY: we validate the flatbuffer above in the `root` call, and extract a loc.
    let viewed_children = unsafe {
        ViewedLayoutChildren::new_unchecked(
            flatbuffer.clone(),
            fb_layout._tab.loc(),
            ctx.clone(),
            layout_ctx.clone(),
            layouts.clone(),
            allow_unknown,
            session.clone(),
            segment_map,
        )
    };

    let build_ctx = LayoutBuildContext {
        session,
        array_read_ctx: ctx,
    };
    let layout = encoding.build(
        dtype,
        fb_layout.row_count(),
        fb_layout
            .metadata()
            .map(|m| m.bytes())
            .unwrap_or_else(|| &[]),
        segments,
        &viewed_children,
        &build_ctx,
    )?;

    Ok(layout)
}

fn foreign_layout_from_fb(
    fb_layout: layout::Layout<'_>,
    dtype: &DType,
    layout_ctx: &ReadContext,
    enclosing: Option<&Arc<[u32]>>,
) -> VortexResult<LayoutRef> {
    let (segments, segment_map) = resolve_segments(fb_layout, enclosing)?;
    let encoding_id = layout_ctx
        .resolve(fb_layout.encoding())
        .ok_or_else(|| vortex_err!("Invalid encoding ID: {}", fb_layout.encoding()))?;

    let children = fb_layout
        .children()
        .unwrap_or_default()
        .iter()
        .map(|child| foreign_layout_from_fb(child, dtype, layout_ctx, segment_map.as_ref()))
        .collect::<VortexResult<Vec<_>>>()?;

    Ok(new_foreign_layout(
        encoding_id,
        dtype.clone(),
        fb_layout.row_count(),
        fb_layout
            .metadata()
            .map(|m| m.bytes().to_vec())
            .unwrap_or_default(),
        segments,
        children,
    ))
}

impl dyn DynLayout + '_ {
    /// Serialize the layout into a [`FlatBufferBuilder`].
    pub fn flatbuffer_writer<'a>(
        &'a self,
        ctx: &'a LayoutContext,
    ) -> impl WriteFlatBuffer<Target<'a> = layout::Layout<'a>> + FlatBufferRoot + 'a {
        LayoutFlatBufferWriter { layout: self, ctx }
    }

    /// Serialize the layout, storing each distinct subtree once using `segment_map`s.
    ///
    /// Readers that predate `segment_map` misread the result, so only use this behind a file
    /// format version check.
    pub fn dedup_flatbuffer_writer<'a>(
        &'a self,
        ctx: &'a LayoutContext,
    ) -> impl WriteFlatBuffer<Target<'a> = layout::Layout<'a>> + FlatBufferRoot + 'a {
        DedupLayoutFlatBufferWriter { layout: self, ctx }
    }
}

/// An adapter struct for writing a layout to a FlatBuffer.
struct LayoutFlatBufferWriter<'a> {
    layout: &'a dyn DynLayout,
    ctx: &'a LayoutContext,
}

impl FlatBufferRoot for LayoutFlatBufferWriter<'_> {}

impl WriteFlatBuffer for LayoutFlatBufferWriter<'_> {
    type Target<'fb> = layout::Layout<'fb>;

    fn write_flatbuffer<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> VortexResult<WIPOffset<Self::Target<'fb>>> {
        // First we recurse into the children and write them out
        let child_layouts = self.layout.children()?;
        let children = child_layouts
            .iter()
            .map(|layout| {
                LayoutFlatBufferWriter {
                    layout: layout.as_ref(),
                    ctx: self.ctx,
                }
                .write_flatbuffer(fbb)
            })
            .collect::<VortexResult<Vec<_>>>()?;
        let children = (!children.is_empty()).then(|| fbb.create_vector(&children));

        // Next we write out the metadata if it's non-empty.
        let metadata = self.layout.metadata();
        let metadata = (!metadata.is_empty()).then(|| fbb.create_vector(&metadata));

        let segments = self
            .layout
            .segment_ids()
            .into_iter()
            .map(|s| *s)
            .collect::<Vec<_>>();
        let segments = (!segments.is_empty()).then(|| fbb.create_vector(&segments));

        // Dictionary-encode the layout ID
        let encoding = self.ctx.intern(&self.layout.encoding_id()).ok_or_else(|| {
            vortex_err!(
                "Layout encoding {} not permitted by ctx",
                self.layout.encoding_id()
            )
        })?;

        Ok(layout::Layout::create(
            fbb,
            &layout::LayoutArgs {
                encoding,
                row_count: self.layout.row_count(),
                metadata,
                children,
                segments,
                segment_map: None,
            },
        ))
    }
}

/// Subtrees with at most this many segments get their own segment map, see
/// [`DedupLayoutFlatBufferWriter`].
const MAX_SEGMENT_MAP_LEN: usize = 1024;

/// Writes a layout flatbuffer that stores each distinct subtree once.
///
/// The writer walks down from the root until it reaches a subtree with at most
/// [`MAX_SEGMENT_MAP_LEN`] segments. That node gets a `segment_map` listing the subtree's file
/// segment ids, and every segment id below it is written as a slot index into the map. Columns
/// with the same layout shape then serialize to identical subtrees, and identical tables and
/// vectors are written once and shared by offset.
struct DedupLayoutFlatBufferWriter<'a> {
    layout: &'a dyn DynLayout,
    ctx: &'a LayoutContext,
}

#[derive(PartialEq, Eq, Hash)]
struct NodeKey {
    encoding: u16,
    row_count: u64,
    metadata: Option<u32>,
    children: Option<u32>,
    segments: Option<u32>,
}

#[derive(Default)]
struct DedupState<'fb> {
    bytes: HashMap<Vec<u8>, WIPOffset<Vector<'fb, u8>>>,
    slots: HashMap<Vec<u32>, WIPOffset<Vector<'fb, u32>>>,
    children: HashMap<Vec<u32>, WIPOffset<Vector<'fb, ForwardsUOffset<layout::Layout<'fb>>>>>,
    nodes: HashMap<NodeKey, WIPOffset<layout::Layout<'fb>>>,
}

impl<'fb> DedupState<'fb> {
    fn bytes(
        &mut self,
        fbb: &mut FlatBufferBuilder<'fb>,
        bytes: &[u8],
    ) -> WIPOffset<Vector<'fb, u8>> {
        if let Some(offset) = self.bytes.get(bytes) {
            return *offset;
        }
        let offset = fbb.create_vector(bytes);
        self.bytes.insert(bytes.to_vec(), offset);
        offset
    }

    fn u32s(
        &mut self,
        fbb: &mut FlatBufferBuilder<'fb>,
        values: Vec<u32>,
    ) -> WIPOffset<Vector<'fb, u32>> {
        match self.slots.entry(values) {
            Entry::Occupied(e) => *e.get(),
            Entry::Vacant(e) => {
                let offset = fbb.create_vector(e.key());
                *e.insert(offset)
            }
        }
    }
}

/// Count the segments in `layout`'s subtree, stopping early once the count exceeds `limit`.
fn count_segments(layout: &dyn DynLayout, limit: usize) -> VortexResult<usize> {
    let mut count = layout.segment_ids().len();
    for child in layout.children()? {
        if count > limit {
            break;
        }
        count += count_segments(child.as_ref(), limit - count.min(limit))?;
    }
    Ok(count)
}

impl DedupLayoutFlatBufferWriter<'_> {
    fn write_node<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
        state: &mut DedupState<'fb>,
        layout: &dyn DynLayout,
        map: Option<&mut Vec<u32>>,
    ) -> VortexResult<WIPOffset<layout::Layout<'fb>>> {
        let mut own_map = Vec::new();
        let (map, owns_map) = match map {
            Some(map) => (Some(map), false),
            None if count_segments(layout, MAX_SEGMENT_MAP_LEN)? <= MAX_SEGMENT_MAP_LEN => {
                (Some(&mut own_map), true)
            }
            None => (None, false),
        };

        let (children, segments) = match map {
            Some(map) => {
                let children = layout
                    .children()?
                    .iter()
                    .map(|child| self.write_node(fbb, state, child.as_ref(), Some(map)))
                    .collect::<VortexResult<Vec<_>>>()?;
                let segments = layout
                    .segment_ids()
                    .into_iter()
                    .map(|id| {
                        map.push(*id);
                        u32::try_from(map.len() - 1)
                            .map_err(|_| vortex_err!("segment map exceeds u32::MAX entries"))
                    })
                    .collect::<VortexResult<Vec<_>>>()?;
                (children, segments)
            }
            None => {
                let children = layout
                    .children()?
                    .iter()
                    .map(|child| self.write_node(fbb, state, child.as_ref(), None))
                    .collect::<VortexResult<Vec<_>>>()?;
                let segments = layout.segment_ids().into_iter().map(|s| *s).collect();
                (children, segments)
            }
        };

        let children = (!children.is_empty()).then(|| {
            let key = children.iter().map(|c| c.value()).collect::<Vec<_>>();
            *state
                .children
                .entry(key)
                .or_insert_with(|| fbb.create_vector(&children))
        });
        let metadata = layout.metadata();
        let metadata = (!metadata.is_empty()).then(|| state.bytes(fbb, &metadata));
        let segments = (!segments.is_empty()).then(|| state.u32s(fbb, segments));
        let encoding = self.ctx.intern(&layout.encoding_id()).ok_or_else(|| {
            vortex_err!(
                "Layout encoding {} not permitted by ctx",
                layout.encoding_id()
            )
        })?;

        let args = layout::LayoutArgs {
            encoding,
            row_count: layout.row_count(),
            metadata,
            children,
            segments,
            segment_map: None,
        };
        if owns_map {
            let segment_map = Some(state.u32s(fbb, own_map));
            return Ok(layout::Layout::create(
                fbb,
                &layout::LayoutArgs {
                    segment_map,
                    ..args
                },
            ));
        }

        let key = NodeKey {
            encoding,
            row_count: args.row_count,
            metadata: metadata.map(|o| o.value()),
            children: children.map(|o| o.value()),
            segments: segments.map(|o| o.value()),
        };
        Ok(*state
            .nodes
            .entry(key)
            .or_insert_with(|| layout::Layout::create(fbb, &args)))
    }
}

impl FlatBufferRoot for DedupLayoutFlatBufferWriter<'_> {}

impl WriteFlatBuffer for DedupLayoutFlatBufferWriter<'_> {
    type Target<'fb> = layout::Layout<'fb>;

    fn write_flatbuffer<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> VortexResult<WIPOffset<Self::Target<'fb>>> {
        let mut state = DedupState::default();
        self.write_node(fbb, &mut state, self.layout, None)
    }
}

#[cfg(test)]
mod tests {
    use flatbuffers::FlatBufferBuilder;
    use flatbuffers::ForwardsUOffset;
    use flatbuffers::Vector;
    use vortex_array::array_session;
    use vortex_array::dtype::DType;
    use vortex_array::dtype::Nullability;
    use vortex_session::registry::ReadContext;

    use super::layout_from_flatbuffer_with_options;
    use crate::LayoutEncodingId;
    use crate::flatbuffers::layout as fbl;
    use crate::session::LayoutSession;

    #[expect(clippy::disallowed_methods, reason = "test-only id")]
    #[test]
    fn unknown_layout_encoding_allow_unknown() {
        let mut fbb = FlatBufferBuilder::new();

        let child_metadata = fbb.create_vector(&[9u8]);
        let child = fbl::Layout::create(
            &mut fbb,
            &fbl::LayoutArgs {
                encoding: 1,
                row_count: 3,
                metadata: Some(child_metadata),
                children: None,
                segments: None,
                segment_map: None,
            },
        );

        let children = fbb.create_vector(&[child]);
        let metadata = fbb.create_vector(&[1u8, 2, 3]);
        let segments = fbb.create_vector(&[7u32]);
        let root = fbl::Layout::create(
            &mut fbb,
            &fbl::LayoutArgs {
                encoding: 0,
                row_count: 10,
                metadata: Some(metadata),
                children: Some(children),
                segments: Some(segments),
                segment_map: None,
            },
        );
        fbb.finish_minimal(root);
        let (buf, start) = fbb.collapse();
        let layout_buffer = vortex_array::flatbuffers::FlatBuffer::align_from(
            vortex_buffer::ByteBuffer::from(buf).slice(start..),
        );

        let layout_ctx = ReadContext::new([
            LayoutEncodingId::new("vortex.test.foreign_layout"),
            LayoutEncodingId::new("vortex.test.foreign_child_layout"),
        ]);
        let array_ctx = ReadContext::new([]);
        let session = array_session().with::<LayoutSession>();

        let layout = layout_from_flatbuffer_with_options(
            layout_buffer,
            &DType::Variant(Nullability::Nullable),
            &layout_ctx,
            &array_ctx,
            &session,
            true,
        )
        .unwrap();

        assert_eq!(layout.encoding_id().as_ref(), "vortex.test.foreign_layout");
        assert_eq!(layout.row_count(), 10);
        assert_eq!(layout.metadata(), vec![1, 2, 3]);
        assert_eq!(layout.segment_ids().len(), 1);
        assert_eq!(*layout.segment_ids()[0], 7);
        assert_eq!(layout.nchildren(), 1);

        let child = layout.slot(0).unwrap().unwrap();
        assert_eq!(
            child.encoding_id().as_ref(),
            "vortex.test.foreign_child_layout"
        );
        assert_eq!(child.metadata(), vec![9]);
    }
}
