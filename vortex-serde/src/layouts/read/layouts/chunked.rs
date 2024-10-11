use std::collections::VecDeque;

use bytes::Bytes;
use itertools::Itertools;
use vortex::IntoArrayVariant;
use vortex_dtype::field::Field;
use vortex_error::{vortex_bail, vortex_err, VortexExpect, VortexResult};
use vortex_flatbuffers::footer;
use vortex_schema::projection::Projection;

use crate::layouts::read::buffered::{BufferedReader, ChunkedFilter, RangedLayoutReader};
use crate::layouts::read::cache::RelativeLayoutCache;
use crate::layouts::read::selection::{RowRange, RowSelector};
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
        length: u64,
        scan: Scan,
        layout_builder: LayoutDeserializer,
        message_cache: RelativeLayoutCache,
    ) -> Box<dyn LayoutReader> {
        Box::new(ChunkedLayout::new(
            fb_bytes,
            fb_loc,
            length,
            scan,
            layout_builder,
            message_cache,
        ))
    }
}

#[derive(Debug)]
pub enum ChunkedLayoutState {
    Init,
    InitFilter,
    InitRead,
    ReadMetadata((Box<dyn LayoutReader>, usize)),
    FilterChunks(ChunkedFilter),
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
    length: u64,
    offset: usize,
    scan: Scan,
    layout_builder: LayoutDeserializer,
    message_cache: RelativeLayoutCache,
    state: ChunkedLayoutState,
    child_ranges: Option<Vec<RowRange>>,
}

impl ChunkedLayout {
    pub fn new(
        fb_bytes: Bytes,
        fb_loc: usize,
        length: u64,
        scan: Scan,
        layout_builder: LayoutDeserializer,
        message_cache: RelativeLayoutCache,
    ) -> Self {
        Self {
            fb_bytes,
            fb_loc,
            length,
            offset: 0,
            scan,
            layout_builder,
            message_cache,
            state: ChunkedLayoutState::Init,
            child_ranges: None,
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

    fn skipped(&self) -> bool {
        self.offset as u64 == self.length
    }

    fn own_range(&self) -> RowSelector {
        RowSelector::new(
            vec![RowRange::new(self.offset, self.length as usize)],
            self.length as usize,
        )
    }

    fn ranged_children(&self) -> VortexResult<VecDeque<RangedLayoutReader>> {
        let Some(ref row_rs) = self.child_ranges else {
            vortex_bail!("Must read metadata before reading children")
        };
        let dtype = self.message_cache.dtype()?;
        self.flatbuffer()
            .children()
            .ok_or_else(|| vortex_err!("Missing children"))?
            .iter()
            .enumerate()
            // Skip over the metadata table of this layout
            .skip(1)
            .zip_eq(row_rs.iter().cloned())
            .skip_while(|(_, rr)| rr.end < self.offset)
            .map(|((i, c), rr)| {
                let mut layout = self.layout_builder.read_layout(
                    self.fb_bytes.clone(),
                    c._tab.loc(),
                    (rr.end - rr.begin) as u64,
                    self.scan.clone(),
                    self.message_cache.relative(i as u16, dtype.clone()),
                )?;
                if self.offset > rr.begin {
                    layout.advance(self.offset - rr.begin)?;
                }
                Ok((rr, layout))
            })
            .collect::<VortexResult<VecDeque<_>>>()
    }
}

impl LayoutReader for ChunkedLayout {
    fn read_next(&mut self, selection: RowSelector) -> VortexResult<Option<ReadResult>> {
        match &mut self.state {
            ChunkedLayoutState::InitRead => {
                self.state = ChunkedLayoutState::ReadChunks(BufferedReader::new(
                    self.ranged_children()?,
                    self.scan.batch_size,
                ));
                self.read_next(selection)
            }
            ChunkedLayoutState::ReadChunks(cr) => cr.read_next_batch(selection),
            _ => vortex_bail!("We are returning chunks"),
        }
    }

    fn read_range(&mut self) -> VortexResult<Option<RangeResult>> {
        if let ChunkedLayoutState::InitFilter = self.state {
            if matches!(self.scan.expr, ScanExpr::Filter(_)) {
                let ranged_children = self.ranged_children()?;
                self.state = ChunkedLayoutState::FilterChunks(ChunkedFilter::new(ranged_children));
                self.read_range()
            } else {
                self.state = ChunkedLayoutState::InitRead;
                Ok(Some(RangeResult::Range(self.own_range())))
            }
        } else if let ChunkedLayoutState::FilterChunks(fr) = &mut self.state {
            match fr.filter_more()? {
                None => {
                    self.state = ChunkedLayoutState::InitRead;
                    Ok(None)
                }
                Some(r) => Ok(Some(r)),
            }
        } else if let ChunkedLayoutState::ReadMetadata((r, nchildren)) = &mut self.state {
            let selector = RowSelector::new(vec![RowRange::new(0, *nchildren)], *nchildren);
            match r.read_next(selector)? {
                None => {
                    self.state = ChunkedLayoutState::InitFilter;
                    return self.read_range();
                }
                Some(rr) => match rr {
                    ReadResult::ReadMore(m) => Ok(Some(RangeResult::ReadMore(m))),
                    ReadResult::Batch(m) => {
                        if self.child_ranges.is_some() {
                            vortex_bail!("Metadata is not chunked for now");
                        } else {
                            let row_offset = m
                                .with_dyn(|a| a.as_struct_array_unchecked().field(0))
                                .ok_or_else(|| {
                                    vortex_err!("must have row_offset metadata column")
                                })?;
                            let primitive_offsets = row_offset.into_primitive()?;
                            self.child_ranges = Some(
                                primitive_offsets
                                    .maybe_null_slice::<u64>()
                                    .iter()
                                    .chain(&[self.length])
                                    .tuple_windows()
                                    .map(|(begin, end)| {
                                        RowRange::new(*begin as usize, *end as usize)
                                    })
                                    .collect::<Vec<_>>(),
                            );
                        }
                        return self.read_range();
                    }
                },
            }
        } else if !matches!(
            self.state,
            ChunkedLayoutState::InitFilter
                | ChunkedLayoutState::FilterChunks(_)
                | ChunkedLayoutState::ReadMetadata(_)
                | ChunkedLayoutState::Init
        ) {
            return Ok(None);
        } else {
            let children = self
                .flatbuffer()
                .children()
                .ok_or_else(|| vortex_err!("Missing children"))?;
            let metadata_child = children.get(0);
            let reader = self.layout_builder.read_layout(
                self.fb_bytes.clone(),
                metadata_child._tab.loc(),
                children.len() as u64,
                Scan {
                    expr: ScanExpr::Projection(Projection::Flat(vec![Field::from("row_offset")])),
                    batch_size: usize::MAX,
                },
                self.message_cache.inlined_schema(0u16),
            )?;
            self.state = ChunkedLayoutState::ReadMetadata((reader, children.len() - 1));
            self.read_range()
        }
    }

    fn advance(&mut self, up_to_row: usize) -> VortexResult<Messages> {
        match &mut self.state {
            ChunkedLayoutState::FilterChunks(fr) => fr.advance(up_to_row),
            ChunkedLayoutState::ReadChunks(br) => br.advance(up_to_row),
            _ => {
                self.offset = up_to_row;
                Ok(vec![])
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::iter;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, RwLock};

    use bytes::Bytes;
    use flatbuffers::{root_unchecked, FlatBufferBuilder};
    use futures_util::TryStreamExt;
    use vortex::array::{ChunkedArray, PrimitiveArray, StructArray};
    use vortex::validity::Validity;
    use vortex::{Array, ArrayDType, IntoArray, IntoArrayVariant};
    use vortex_dtype::PType;
    use vortex_expr::{BinaryExpr, Identity, Literal, Operator};
    use vortex_flatbuffers::{footer, WriteFlatBuffer};
    use vortex_schema::projection::Projection;

    use crate::layouts::read::cache::{LazyDeserializedDType, RelativeLayoutCache};
    use crate::layouts::read::layouts::chunked::ChunkedLayout;
    use crate::layouts::read::selection::RowSelector;
    use crate::layouts::{
        write, ChunkedLayoutSpec, LayoutDeserializer, LayoutMessageCache, LayoutReader,
        RangeResult, ReadResult, RowFilter, Scan, ScanExpr,
    };
    use crate::message_writer::MessageWriter;

    async fn layout_and_bytes(
        cache: Arc<RwLock<LayoutMessageCache>>,
        scan: Scan,
    ) -> (ChunkedLayout, Bytes) {
        let mut writer = MessageWriter::new(Vec::new());
        let array = PrimitiveArray::from((0..100).collect::<Vec<_>>()).into_array();
        let array_dtype = array.dtype().clone();
        let chunked =
            ChunkedArray::try_new(iter::repeat(array).take(5).collect(), array_dtype).unwrap();
        let len = chunked.len();
        let mut byte_offsets = vec![writer.tell()];
        let mut row_offsets = vec![0];
        let mut row_offset = 0;

        let mut chunk_stream = chunked.array_stream();
        while let Some(chunk) = chunk_stream.try_next().await.unwrap() {
            row_offset += chunk.len() as u64;
            row_offsets.push(row_offset);
            writer.write_batch(chunk).await.unwrap();
            byte_offsets.push(writer.tell());
        }
        let mut flat_layouts = byte_offsets
            .iter()
            .zip(byte_offsets.iter().skip(1))
            .map(|(begin, end)| write::Layout::Flat(write::FlatLayout::new(*begin, *end)))
            .collect::<VecDeque<_>>();

        let meta_len = row_offsets.len();
        let metadata_array = StructArray::try_new(
            ["row_offset".into()].into(),
            vec![row_offsets.into_array()],
            meta_len,
            Validity::NonNullable,
        )
        .unwrap();

        let metadata_table_begin = writer.tell();
        writer.write_dtype(metadata_array.dtype()).await.unwrap();
        writer
            .write_batch(metadata_array.into_array())
            .await
            .unwrap();
        flat_layouts.push_front(write::Layout::Flat(write::FlatLayout::new(
            metadata_table_begin,
            writer.tell(),
        )));

        let written = writer.into_inner();

        let mut fb = FlatBufferBuilder::new();
        let chunked_layout = write::Layout::Nested(write::NestedLayout::new(
            flat_layouts,
            ChunkedLayoutSpec::ID,
        ));
        let flat_buf = chunked_layout.write_flatbuffer(&mut fb);
        fb.finish_minimal(flat_buf);
        let fb_bytes = Bytes::copy_from_slice(fb.finished_data());

        let fb_loc = (unsafe { root_unchecked::<footer::Layout>(&fb_bytes) })
            ._tab
            .loc();

        (
            ChunkedLayout::new(
                fb_bytes,
                fb_loc,
                len as u64,
                scan,
                LayoutDeserializer::default(),
                RelativeLayoutCache::new(
                    cache.clone(),
                    LazyDeserializedDType::from_dtype(PType::I32.into()),
                ),
            ),
            Bytes::from(written),
        )
    }

    fn read_layout_ranges(
        layout: &mut dyn LayoutReader,
        cache: Arc<RwLock<LayoutMessageCache>>,
        buf: &Bytes,
    ) -> Vec<RowSelector> {
        let mut s = Vec::new();
        while let Some(rr) = layout.read_range().unwrap() {
            match rr {
                RangeResult::ReadMore(mut m) => {
                    let mut write_cache_guard = cache.write().unwrap();
                    let (id, range) = m.remove(0);
                    write_cache_guard.set(id, buf.slice(range.to_range()));
                }
                RangeResult::Range(r) => s.push(r),
            }
        }
        s
    }

    fn read_layout_data(
        layout: &mut dyn LayoutReader,
        cache: Arc<RwLock<LayoutMessageCache>>,
        buf: &Bytes,
        selector: RowSelector,
    ) -> Vec<Array> {
        let mut arr = Vec::new();
        while let Some(rr) = layout.read_next(selector.clone()).unwrap() {
            match rr {
                ReadResult::ReadMore(mut m) => {
                    let mut write_cache_guard = cache.write().unwrap();
                    let (id, range) = m.remove(0);
                    write_cache_guard.set(id, buf.slice(range.to_range()));
                }
                ReadResult::Batch(a) => arr.push(a),
            }
        }

        arr
    }

    fn read_layout(
        layout: &mut dyn LayoutReader,
        cache: Arc<RwLock<LayoutMessageCache>>,
        buf: &Bytes,
    ) -> Vec<Array> {
        read_layout_ranges(layout, cache.clone(), buf)
            .into_iter()
            .flat_map(|s| read_layout_data(layout, cache.clone(), buf, s))
            .collect()
    }

    #[tokio::test]
    async fn read_range() {
        let cache = Arc::new(RwLock::new(LayoutMessageCache::default()));
        let (mut layout, buf) = layout_and_bytes(
            cache.clone(),
            Scan {
                expr: ScanExpr::Filter(RowFilter::new(Arc::new(BinaryExpr::new(
                    Arc::new(Identity),
                    Operator::Gt,
                    Arc::new(Literal::new(10.into())),
                )))),
                batch_size: 100,
            },
        )
        .await;
        let arr = read_layout(&mut layout, cache, &buf).pop();

        assert!(arr.is_some());
        let arr = arr.unwrap();
        assert_eq!(
            arr.into_primitive().unwrap().maybe_null_slice::<i32>(),
            &(11..100).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn read_range_no_filter() {
        let cache = Arc::new(RwLock::new(LayoutMessageCache::default()));
        let (mut layout, buf) = layout_and_bytes(
            cache.clone(),
            Scan {
                expr: ScanExpr::Projection(Projection::All),
                batch_size: 500,
            },
        )
        .await;
        let arr = read_layout(&mut layout, cache, &buf).pop();

        assert!(arr.is_some());
        let arr = arr.unwrap();
        assert_eq!(
            arr.into_primitive().unwrap().maybe_null_slice::<i32>(),
            iter::repeat(0..100).take(5).flatten().collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn advance_read_range() {
        let cache = Arc::new(RwLock::new(LayoutMessageCache::default()));
        let (mut layout, buf) = layout_and_bytes(
            cache.clone(),
            Scan {
                expr: ScanExpr::Filter(RowFilter::new(Arc::new(BinaryExpr::new(
                    Arc::new(Identity),
                    Operator::Gt,
                    Arc::new(Literal::new(10.into())),
                )))),
                batch_size: 500,
            },
        )
        .await;
        layout.advance(50).unwrap();
        let arr = read_layout(&mut layout, cache, &buf).pop();

        assert!(arr.is_some());
        let arr = arr.unwrap();
        assert_eq!(
            arr.into_primitive().unwrap().maybe_null_slice::<i32>(),
            &(50..100).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn advance_skipped() {
        let cache = Arc::new(RwLock::new(LayoutMessageCache::default()));
        let (mut layout, buf) = layout_and_bytes(
            cache.clone(),
            Scan {
                expr: ScanExpr::Filter(RowFilter::new(Arc::new(BinaryExpr::new(
                    Arc::new(Identity),
                    Operator::Gt,
                    Arc::new(Literal::new(10.into())),
                )))),
                batch_size: 100,
            },
        )
        .await;
        layout.advance(500).unwrap();
        let arr = read_layout(&mut layout, cache, &buf).pop();

        assert!(arr.is_none());
    }

    #[tokio::test]
    async fn batch_size() {
        let cache = Arc::new(RwLock::new(LayoutMessageCache::default()));
        let (mut layout, buf) = layout_and_bytes(
            cache.clone(),
            Scan {
                expr: ScanExpr::Filter(RowFilter::new(Arc::new(BinaryExpr::new(
                    Arc::new(Identity),
                    Operator::Gt,
                    Arc::new(Literal::new(10.into())),
                )))),
                batch_size: 50,
            },
        )
        .await;
        let mut arr = read_layout(&mut layout, cache, &buf);

        assert_eq!(
            arr.remove(0)
                .into_primitive()
                .unwrap()
                .maybe_null_slice::<i32>(),
            &(11..=60).collect::<Vec<_>>()
        );
        assert_eq!(
            arr.remove(0)
                .into_primitive()
                .unwrap()
                .maybe_null_slice::<i32>(),
            &(61..100).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn advance_after_filter() {
        let cache = Arc::new(RwLock::new(LayoutMessageCache::default()));
        let (mut layout, buf) = layout_and_bytes(
            cache.clone(),
            Scan {
                expr: ScanExpr::Filter(RowFilter::new(Arc::new(BinaryExpr::new(
                    Arc::new(Identity),
                    Operator::Gt,
                    Arc::new(Literal::new(10.into())),
                )))),
                batch_size: 50,
            },
        )
        .await;
        let selector = read_layout_ranges(&mut layout, cache.clone(), &buf);
        layout.advance(50).unwrap();
        let mut arr = selector
            .into_iter()
            .flat_map(|s| read_layout_data(&mut layout, cache.clone(), &buf, s))
            .collect::<Vec<_>>();

        assert_eq!(
            arr.remove(0)
                .into_primitive()
                .unwrap()
                .maybe_null_slice::<i32>(),
            &(50..100).collect::<Vec<_>>()
        );
        assert_eq!(
            arr.get(7)
                .unwrap()
                .clone()
                .into_primitive()
                .unwrap()
                .maybe_null_slice::<i32>(),
            &(61..100).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn advance_mid_read() {
        let cache = Arc::new(RwLock::new(LayoutMessageCache::default()));
        let (mut layout, buf) = layout_and_bytes(
            cache.clone(),
            Scan {
                expr: ScanExpr::Filter(RowFilter::new(Arc::new(BinaryExpr::new(
                    Arc::new(Identity),
                    Operator::Gt,
                    Arc::new(Literal::new(10.into())),
                )))),
                batch_size: 50,
            },
        )
        .await;
        let s = read_layout_ranges(&mut layout, cache.clone(), &buf);
        let advanced = AtomicBool::new(false);
        let mut arr = Vec::new();
        for rs in s {
            while let Some(rr) = layout.read_next(rs.clone()).unwrap() {
                match rr {
                    ReadResult::ReadMore(mut m) => {
                        let mut write_cache_guard = cache.write().unwrap();
                        write_cache_guard.set(m.remove(0).0, buf.clone());
                    }
                    ReadResult::Batch(a) => {
                        arr.push(a);
                        if advanced
                            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                            .is_ok()
                        {
                            layout.advance(310).unwrap();
                        }
                    }
                }
            }
        }

        assert_eq!(arr.len(), 5);
        assert_eq!(
            arr.remove(0)
                .into_primitive()
                .unwrap()
                .maybe_null_slice::<i32>(),
            &(11..=60).collect::<Vec<_>>()
        );
        assert_eq!(
            arr.remove(0)
                .into_primitive()
                .unwrap()
                .maybe_null_slice::<i32>(),
            &(11..=60).collect::<Vec<_>>()
        );
        assert_eq!(
            arr.remove(0)
                .into_primitive()
                .unwrap()
                .maybe_null_slice::<i32>(),
            &(61..100).collect::<Vec<_>>()
        );
        assert_eq!(
            arr.remove(0)
                .into_primitive()
                .unwrap()
                .maybe_null_slice::<i32>(),
            &(11..=60).collect::<Vec<_>>()
        );
        assert_eq!(
            arr.remove(0)
                .into_primitive()
                .unwrap()
                .maybe_null_slice::<i32>(),
            &(61..100).collect::<Vec<_>>()
        );
    }
}
