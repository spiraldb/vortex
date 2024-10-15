use std::cmp::{min, PartialEq};
use std::sync::Arc;

use bytes::{Buf, Bytes};
use flatbuffers::root;
use vortex::compute::slice;
use vortex::{Array, Context};
use vortex_error::{vortex_bail, vortex_err, VortexExpect, VortexResult};
use vortex_flatbuffers::{footer, message, ReadFlatBuffer};

use crate::layouts::read::cache::RelativeLayoutCache;
use crate::layouts::read::selection::{RowRange, RowSelector};
use crate::layouts::{
    LayoutDeserializer, LayoutId, LayoutReader, LayoutSpec, Messages, RangeResult, ReadResult,
    Scan, ScanExpr,
};
use crate::message_reader::ArrayBufferReader;
use crate::messages::IPCDType;
use crate::stream_writer::ByteRange;

#[derive(Debug)]
pub struct FlatLayoutSpec;

impl FlatLayoutSpec {
    pub const ID: LayoutId = LayoutId(0);
}

impl LayoutSpec for FlatLayoutSpec {
    fn id(&self) -> LayoutId {
        Self::ID
    }

    fn layout(
        &self,
        fb_bytes: Bytes,
        fb_loc: usize,
        length: u64,
        scan: Scan,
        layout_serde: LayoutDeserializer,
        message_cache: RelativeLayoutCache,
    ) -> Box<dyn LayoutReader> {
        let fb_layout = unsafe {
            let tab = flatbuffers::Table::new(&fb_bytes, fb_loc);
            footer::Layout::init_from_table(tab)
        };
        let flat_fb = fb_layout
            .layout_as_flat_layout()
            .vortex_expect("must be flat layout");

        Box::new(FlatLayout::new(
            flat_fb.begin(),
            flat_fb.end(),
            length,
            scan,
            layout_serde.ctx(),
            message_cache,
        ))
    }
}

#[derive(Debug, PartialEq, Eq)]
enum FlatLayoutState {
    Filter,
    Reading,
    Finished,
}

#[derive(Debug)]
pub struct FlatLayout {
    range: ByteRange,
    length: u64,
    scan: Scan,
    ctx: Arc<Context>,
    cache: RelativeLayoutCache,
    offset: usize,
    state: FlatLayoutState,
    cached_array: Option<Array>,
}

impl FlatLayout {
    pub fn new(
        begin: u64,
        end: u64,
        length: u64,
        scan: Scan,
        ctx: Arc<Context>,
        cache: RelativeLayoutCache,
    ) -> Self {
        Self {
            range: ByteRange { begin, end },
            length,
            scan,
            ctx,
            cache,
            offset: 0,
            state: FlatLayoutState::Filter,
            cached_array: None,
        }
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

    fn own_message(&self) -> Messages {
        vec![(self.cache.absolute_id(&[]), self.range)]
    }

    fn array_from_bytes(&self, mut buf: Bytes) -> VortexResult<Array> {
        let dtype = if self.cache.has_dtype() {
            self.cache.dtype()?
        } else {
            let dtype_fb_length = buf.get_u32_le();
            let dtype_buf = buf.split_to(dtype_fb_length as usize);
            let msg = root::<message::Message>(&dtype_buf)?
                .header_as_schema()
                .ok_or_else(|| vortex_err!("Expected schema message"))?;
            IPCDType::read_flatbuffer(&msg)?.0
        };

        let mut array_reader = ArrayBufferReader::new();
        let mut read_buf = Bytes::new();
        while let Some(u) = array_reader.read(read_buf)? {
            read_buf = buf.split_to(u);
        }
        array_reader.into_array(self.ctx.clone(), dtype)
    }

    fn read_next_internal(
        &mut self,
        selection: RowSelector,
        chunked: bool,
    ) -> VortexResult<Option<ReadResult>> {
        if self.skipped()
            || self.state == FlatLayoutState::Finished
            || selection.length() <= self.offset
        {
            return Ok(None);
        }

        if let Some(array) = self.cached_array.take() {
            let rows_to_read = min(self.scan.batch_size, array.len());
            let array = if array.len() > self.scan.batch_size {
                let taken = slice(&array, 0, rows_to_read)?;
                let leftover = slice(&array, rows_to_read, array.len())?;
                self.cached_array = Some(leftover);
                self.offset += rows_to_read;
                taken
            } else {
                if selection.length() == self.length as usize {
                    self.state = FlatLayoutState::Finished;
                }
                self.offset = selection.length();
                array
            };
            Ok(Some(ReadResult::Batch(array)))
        } else if let Some(buf) = self.cache.get(&[]) {
            let array = self.array_from_bytes(buf)?;
            let selected = selection.offset(self.offset).slice_array(slice(
                &array,
                self.offset,
                selection.length(),
            )?)?;

            if chunked {
                if selection.length() == self.length as usize && selected.is_none() {
                    self.state = FlatLayoutState::Finished;
                }
                self.cached_array = selected;
                self.read_next_internal(selection.clone(), chunked)
            } else {
                if selection.length() == self.length as usize {
                    self.state = FlatLayoutState::Finished;
                }
                Ok(selected.map(ReadResult::Batch))
            }
        } else {
            Ok(Some(ReadResult::ReadMore(self.own_message())))
        }
    }
}

impl LayoutReader for FlatLayout {
    fn read_next(&mut self, selection: RowSelector) -> VortexResult<Option<ReadResult>> {
        self.read_next_internal(selection, true)
    }

    fn read_range(&mut self) -> VortexResult<Option<RangeResult>> {
        if self.state != FlatLayoutState::Filter || self.skipped() {
            Ok(None)
        } else if let ScanExpr::Filter(rf) = self.scan.expr.clone() {
            match self.read_next_internal(self.own_range(), false)? {
                None => {
                    self.state = FlatLayoutState::Reading;
                    Ok(None)
                }
                Some(rr) => match rr {
                    ReadResult::ReadMore(m) => Ok(Some(RangeResult::ReadMore(m))),
                    ReadResult::Batch(b) => {
                        let mask = rf.evaluate(&b)?;
                        let selector = mask.with_dyn(|a| {
                            a.as_bool_array()
                                .ok_or_else(|| vortex_err!("Must be a bool array"))
                                .map(|b| {
                                    RowSelector::from_ranges(
                                        b.maybe_null_slices_iter().map(|(begin, end)| {
                                            RowRange::new(begin + self.offset, end + self.offset)
                                        }),
                                        self.length as usize,
                                    )
                                })
                        })?;
                        self.state = FlatLayoutState::Reading;
                        Ok(Some(RangeResult::Range(selector)))
                    }
                },
            }
        } else {
            self.state = FlatLayoutState::Reading;
            Ok(Some(RangeResult::Range(self.own_range())))
        }
    }

    fn advance(&mut self, up_to_row: usize) -> VortexResult<Messages> {
        if up_to_row < self.offset {
            vortex_bail!("Can't advance backwards")
        }
        if let Some(carr) = self.cached_array.take() {
            if up_to_row > carr.len() {
                self.cached_array = None;
            } else {
                let slice_end = carr.len();
                self.cached_array = Some(slice(
                    carr,
                    slice_end - (self.length as usize - up_to_row),
                    slice_end,
                )?);
            }
        }
        self.offset = up_to_row;
        if self.skipped() {
            Ok(vec![])
        } else {
            Ok(self.own_message())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, RwLock};

    use bytes::Bytes;
    use vortex::array::PrimitiveArray;
    use vortex::{Array, Context, IntoArray, IntoArrayVariant};
    use vortex_dtype::PType;
    use vortex_expr::{BinaryExpr, Identity, Literal, Operator};
    use vortex_schema::projection::Projection;

    use crate::layouts::read::cache::{LazyDeserializedDType, RelativeLayoutCache};
    use crate::layouts::read::layouts::flat::FlatLayout;
    use crate::layouts::read::layouts::test_read::{
        read_layout, read_layout_data, read_layout_ranges,
    };
    use crate::layouts::read::selection::{RowRange, RowSelector};
    use crate::layouts::{LayoutMessageCache, LayoutReader, ReadResult, RowFilter, Scan, ScanExpr};
    use crate::message_writer::MessageWriter;

    async fn layout_and_bytes(
        cache: Arc<RwLock<LayoutMessageCache>>,
        scan: Scan,
    ) -> (FlatLayout, FlatLayout, Bytes) {
        let mut writer = MessageWriter::new(Vec::new());
        let array = PrimitiveArray::from((0..100).collect::<Vec<_>>()).into_array();
        let len = array.len();
        writer.write_batch(array).await.unwrap();
        let written = writer.into_inner();

        let projection_scan = Scan {
            expr: ScanExpr::Projection(Projection::All),
            batch_size: scan.batch_size,
        };

        (
            FlatLayout::new(
                0,
                written.len() as u64,
                len as u64,
                scan,
                Arc::new(Context::default()),
                RelativeLayoutCache::new(
                    cache.clone(),
                    LazyDeserializedDType::from_dtype(PType::I32.into()),
                ),
            ),
            FlatLayout::new(
                0,
                written.len() as u64,
                len as u64,
                projection_scan,
                Arc::new(Context::default()),
                RelativeLayoutCache::new(
                    cache.clone(),
                    LazyDeserializedDType::from_dtype(PType::I32.into()),
                ),
            ),
            Bytes::from(written),
        )
    }

    #[tokio::test]
    async fn read_range() {
        let cache = Arc::new(RwLock::new(LayoutMessageCache::default()));
        let (mut filter_layout, mut projection_layout, buf) = layout_and_bytes(
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
        let arr = read_layout(&mut filter_layout, &mut projection_layout, cache, &buf).pop_front();

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
        let (mut filter_layout, mut projection_layout, buf) = layout_and_bytes(
            cache.clone(),
            Scan {
                expr: ScanExpr::Projection(Projection::All),
                batch_size: 100,
            },
        )
        .await;
        let arr = read_layout(&mut filter_layout, &mut projection_layout, cache, &buf).pop_front();

        assert!(arr.is_some());
        let arr = arr.unwrap();
        assert_eq!(
            arr.into_primitive().unwrap().maybe_null_slice::<i32>(),
            &(0..100).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn read_empty() {
        let cache = Arc::new(RwLock::new(LayoutMessageCache::default()));
        let (mut filter_layout, mut projection_layout, buf) = layout_and_bytes(
            cache.clone(),
            Scan {
                expr: ScanExpr::Filter(RowFilter::new(Arc::new(BinaryExpr::new(
                    Arc::new(Identity),
                    Operator::Gt,
                    Arc::new(Literal::new(101.into())),
                )))),
                batch_size: 100,
            },
        )
        .await;
        let arr = read_layout(&mut filter_layout, &mut projection_layout, cache, &buf).pop_front();

        assert!(arr.is_none());
    }

    #[tokio::test]
    async fn advance_read_range() {
        let cache = Arc::new(RwLock::new(LayoutMessageCache::default()));
        let (mut filter_layout, mut projection_layout, buf) = layout_and_bytes(
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
        filter_layout.advance(50).unwrap();
        let arr = read_layout(&mut filter_layout, &mut projection_layout, cache, &buf).pop_front();

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
        let (mut filter_layout, mut projection_layout, buf) = layout_and_bytes(
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
        filter_layout.advance(100).unwrap();
        let arr = read_layout(&mut filter_layout, &mut projection_layout, cache, &buf).pop_front();

        assert!(arr.is_none());
    }

    #[tokio::test]
    async fn batch_size() {
        let cache = Arc::new(RwLock::new(LayoutMessageCache::default()));
        let (mut filter_layout, mut projection_layout, buf) = layout_and_bytes(
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
        let mut arr = read_layout(&mut filter_layout, &mut projection_layout, cache, &buf);

        assert_eq!(
            arr.pop_front()
                .unwrap()
                .into_primitive()
                .unwrap()
                .maybe_null_slice::<i32>(),
            &(11..=60).collect::<Vec<_>>()
        );
        assert_eq!(
            arr.pop_front()
                .unwrap()
                .into_primitive()
                .unwrap()
                .maybe_null_slice::<i32>(),
            &(61..100).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn read_multiple_selectors() {
        let cache = Arc::new(RwLock::new(LayoutMessageCache::default()));
        let (_, mut projection_layout, buf) = layout_and_bytes(
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
        let mut arr = [
            RowSelector::new(vec![RowRange::new(11, 50)], 50),
            RowSelector::from_ranges(vec![RowRange::new(50, 100)], 100),
        ]
        .into_iter()
        .flat_map(|s| read_layout_data(&mut projection_layout, cache.clone(), &buf, s))
        .collect::<VecDeque<_>>();

        assert_eq!(
            arr.pop_front()
                .unwrap()
                .into_primitive()
                .unwrap()
                .maybe_null_slice::<i32>(),
            &(11..50).collect::<Vec<_>>()
        );
        assert_eq!(
            arr.pop_front()
                .unwrap()
                .into_primitive()
                .unwrap()
                .maybe_null_slice::<i32>(),
            &(50..100).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn advance_after_filter() {
        let cache = Arc::new(RwLock::new(LayoutMessageCache::default()));
        let (mut filter_layout, mut projection_layout, buf) = layout_and_bytes(
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
        let selector = read_layout_ranges(&mut filter_layout, cache.clone(), &buf);
        projection_layout.advance(50).unwrap();
        let mut arr: Vec<Array> = selector
            .into_iter()
            .flat_map(|s| read_layout_data(&mut projection_layout, cache.clone(), &buf, s))
            .collect();

        assert_eq!(
            arr.remove(0)
                .into_primitive()
                .unwrap()
                .maybe_null_slice::<i32>(),
            &(50..100).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn advance_mid_read() {
        let cache = Arc::new(RwLock::new(LayoutMessageCache::default()));
        let (mut filter_layout, mut projection_layout, buf) = layout_and_bytes(
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
        let s = read_layout_ranges(&mut filter_layout, cache.clone(), &buf).pop();
        let mut arr = Vec::new();
        let advanced = AtomicBool::new(false);

        while let Some(rr) = projection_layout.read_next(s.clone().unwrap()).unwrap() {
            match rr {
                ReadResult::ReadMore(m) => {
                    let mut write_cache_guard = cache.write().unwrap();
                    for (id, range) in m {
                        write_cache_guard.set(id, buf.slice(range.to_range()));
                    }
                }
                ReadResult::Batch(a) => {
                    arr.push(a);
                    if advanced
                        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok()
                    {
                        projection_layout.advance(90).unwrap();
                    }
                }
            }
        }
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
            &(90..100).collect::<Vec<_>>()
        );
    }
}
