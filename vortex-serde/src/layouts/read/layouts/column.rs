use bytes::Bytes;
use flatbuffers::{ForwardsUOffset, Vector};
use vortex_dtype::field::Field;
use vortex_dtype::{DType, StructDType};
use vortex_error::{vortex_bail, vortex_err, VortexExpect, VortexResult};
use vortex_flatbuffers::footer;
use vortex_schema::projection::Projection;

use crate::layouts::read::batch::{ColumnBatchFilter, ColumnBatchReader, FilterLayoutReader};
use crate::layouts::read::cache::RelativeLayoutCache;
use crate::layouts::read::selection::{RowRange, RowSelector};
use crate::layouts::{
    LayoutDeserializer, LayoutId, LayoutReader, LayoutSpec, Messages, RangeResult, ReadResult,
    Scan, ScanExpr,
};

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
        length: u64,
        scan: Scan,
        layout_serde: LayoutDeserializer,
        message_cache: RelativeLayoutCache,
    ) -> Box<dyn LayoutReader> {
        Box::new(ColumnLayout::new(
            fb_bytes,
            fb_loc,
            length,
            scan,
            layout_serde,
            message_cache,
        ))
    }
}

#[derive(Debug)]
pub enum ColumnLayoutState {
    Init,
    InitNoFilter,
    InitFilter,
    Filtering(ColumnBatchFilter),
    ReadColumns(ColumnBatchReader),
}

/// In memory representation of Columnar NestedLayout.
///
/// Each child represents a column
#[derive(Debug)]
pub struct ColumnLayout {
    fb_bytes: Bytes,
    fb_loc: usize,
    length: u64,
    scan: Scan,
    layout_serde: LayoutDeserializer,
    message_cache: RelativeLayoutCache,
    state: ColumnLayoutState,
    offset: usize,
}

impl ColumnLayout {
    pub fn new(
        fb_bytes: Bytes,
        fb_loc: usize,
        length: u64,
        scan: Scan,
        layout_serde: LayoutDeserializer,
        message_cache: RelativeLayoutCache,
    ) -> Self {
        let state = if matches!(scan.expr, ScanExpr::Projection(_)) {
            ColumnLayoutState::Init
        } else {
            ColumnLayoutState::InitFilter
        };
        Self {
            fb_bytes,
            fb_loc,
            scan,
            length,
            layout_serde,
            message_cache,
            state,
            offset: 0,
        }
    }

    pub fn flatbuffer(&self) -> footer::NestedLayout {
        let fb_layout = unsafe {
            let tab = flatbuffers::Table::new(&self.fb_bytes, self.fb_loc);
            footer::Layout::init_from_table(tab)
        };
        fb_layout
            .layout_as_nested_layout()
            .vortex_expect("ColumnLayout: Failed to read nested layout from flatbuffer")
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

    fn read_child(
        &self,
        idx: usize,
        children: Vector<ForwardsUOffset<footer::Layout>>,
        dtype: DType,
    ) -> VortexResult<Box<dyn LayoutReader>> {
        let layout = children.get(idx);

        let child_scan = Scan {
            // TODO(robert): Changes this once we support nested projections
            expr: ScanExpr::Projection(Projection::All),
            batch_size: self.scan.batch_size,
        };

        let mut layout = self.layout_serde.read_layout(
            self.fb_bytes.clone(),
            layout._tab.loc(),
            self.length,
            child_scan,
            self.message_cache.relative(idx as u16, dtype),
        )?;
        if self.offset != 0 {
            layout.advance(self.offset)?;
        }
        Ok(layout)
    }

    fn filter_reader(&mut self) -> VortexResult<ColumnBatchFilter> {
        let ScanExpr::Filter(ref rf) = self.scan.expr else {
            vortex_bail!("Asked for filter children when there's no filter")
        };

        let fb_children = self
            .flatbuffer()
            .children()
            .ok_or_else(|| vortex_err!("Missing children"))?;
        let filter_refs = rf.references().into_iter().collect::<Vec<_>>();
        let filter_dtype = self.message_cache.projected_dtype(&filter_refs)?;
        let DType::Struct(s, ..) = filter_dtype else {
            vortex_bail!("Column layout must have struct dtype")
        };

        let mut unhandled_children_names = Vec::new();
        let mut unhandled_children = Vec::new();
        let mut handled_children = Vec::new();

        for (idx, field) in filter_refs.into_iter().enumerate() {
            let resolved_child = self.message_cache.resolve_field(&field)?;
            let child_loc = fb_children.get(resolved_child)._tab.loc();
            let filter = rf.project(&[field]);

            let has_filter = filter.is_some();

            let child_scan = filter
                .map(|cf| Scan {
                    expr: ScanExpr::Filter(cf),
                    batch_size: self.scan.batch_size,
                })
                .unwrap_or_else(|| Scan {
                    expr: ScanExpr::Projection(Projection::All),
                    batch_size: self.scan.batch_size,
                });

            let mut child = self.layout_serde.read_layout(
                self.fb_bytes.clone(),
                child_loc,
                self.length,
                child_scan,
                self.message_cache
                    .relative(idx as u16, s.dtypes()[idx].clone()),
            )?;

            if self.offset != 0 {
                child.advance(self.offset)?;
            }

            if has_filter {
                handled_children.push(child);
            } else {
                unhandled_children.push(child);
                unhandled_children_names.push(s.names()[idx].clone());
            }
        }

        if !unhandled_children_names.is_empty() {
            let Some(prf) = rf.project(
                &unhandled_children_names
                    .iter()
                    .map(|n| Field::from(n.as_ref()))
                    .collect::<Vec<_>>(),
            ) else {
                vortex_bail!("Must be able to project filter into unhandled space")
            };

            handled_children.push(Box::new(FilterLayoutReader::new(
                ColumnBatchReader::new(unhandled_children_names.into(), unhandled_children),
                prf,
                self.offset,
                self.length as usize,
            )));
        }

        Ok(ColumnBatchFilter::new(handled_children))
    }

    fn read_children(&mut self) -> VortexResult<Vec<Box<dyn LayoutReader>>> {
        let s = self.struct_dtype()?;
        let fb_children = self
            .flatbuffer()
            .children()
            .ok_or_else(|| vortex_err!("Missing children"))?;

        if let ScanExpr::Projection(ref p) = self.scan.expr {
            match p {
                Projection::All => (0..fb_children.len())
                    .zip(s.dtypes().iter().cloned())
                    .map(|(idx, dtype)| self.read_child(idx, fb_children, dtype))
                    .collect::<VortexResult<Vec<_>>>(),
                Projection::Flat(v) => v
                    .iter()
                    .map(|f| self.message_cache.resolve_field(f))
                    .zip(s.dtypes().iter().cloned())
                    .map(|(child_idx, dtype)| self.read_child(child_idx?, fb_children, dtype))
                    .collect::<VortexResult<Vec<_>>>(),
            }
        } else {
            vortex_bail!("Not a projection")
        }
    }

    fn struct_dtype(&self) -> VortexResult<StructDType> {
        if let ScanExpr::Projection(proj) = &self.scan.expr {
            let dtype = match proj {
                Projection::All => self.message_cache.dtype()?,
                Projection::Flat(p) => self.message_cache.projected_dtype(p)?,
            };
            let DType::Struct(s, ..) = dtype else {
                vortex_bail!("Column layout must have struct dtype")
            };
            Ok(s)
        } else {
            vortex_bail!("Not a projection")
        }
    }
}

impl LayoutReader for ColumnLayout {
    fn read_next(&mut self, selection: RowSelector) -> VortexResult<Option<ReadResult>> {
        match &mut self.state {
            ColumnLayoutState::Init => {
                self.state = ColumnLayoutState::ReadColumns(ColumnBatchReader::new(
                    self.struct_dtype()?.names().clone(),
                    self.read_children()?,
                ));
                self.read_next(selection)
            }
            ColumnLayoutState::ReadColumns(br) => br.read_next(selection),
            s => vortex_bail!("We are returning batches {s:?}"),
        }
    }

    fn read_range(&mut self) -> VortexResult<Option<RangeResult>> {
        match &mut self.state {
            ColumnLayoutState::Init => {
                self.state = ColumnLayoutState::InitNoFilter;
                Ok(Some(RangeResult::Range(self.own_range())))
            }
            ColumnLayoutState::InitFilter => {
                if let ScanExpr::Filter(_) = &self.scan.expr {
                    self.state = ColumnLayoutState::Filtering(self.filter_reader()?);
                    self.read_range()
                } else {
                    self.state = ColumnLayoutState::Init;
                    Ok(Some(RangeResult::Range(self.own_range())))
                }
            }
            ColumnLayoutState::Filtering(fr) => fr.read_more_ranges(),
            ColumnLayoutState::InitNoFilter => {
                self.state = ColumnLayoutState::Init;
                Ok(None)
            }
            s => vortex_bail!("We are returning ranges {s:?}"),
        }
    }

    fn advance(&mut self, up_to_row: usize) -> VortexResult<Messages> {
        match &mut self.state {
            ColumnLayoutState::Filtering(fr) => fr.advance(up_to_row),
            ColumnLayoutState::ReadColumns(br) => br.advance(up_to_row),
            _ => {
                self.offset = up_to_row;
                Ok(vec![])
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::iter;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, RwLock};

    use bytes::Bytes;
    use vortex::accessor::ArrayAccessor;
    use vortex::array::{ChunkedArray, PrimitiveArray, StructArray, VarBinArray};
    use vortex::validity::Validity;
    use vortex::{ArrayDType, IntoArray, IntoArrayVariant};
    use vortex_dtype::field::Field;
    use vortex_dtype::{DType, Nullability};
    use vortex_expr::{BinaryExpr, Column, Literal, Operator};
    use vortex_schema::projection::Projection;

    use crate::layouts::read::cache::{LazyDeserializedDType, RelativeLayoutCache};
    use crate::layouts::read::footer::FooterReader;
    use crate::layouts::read::layouts::test_read::{
        read_layout, read_layout_data, read_layout_ranges,
    };
    use crate::layouts::{
        LayoutDeserializer, LayoutMessageCache, LayoutReader, LayoutWriter, ReadResult, RowFilter,
        Scan, ScanExpr,
    };

    async fn layout_and_bytes(
        cache: Arc<RwLock<LayoutMessageCache>>,
        scan: Scan,
    ) -> (Box<dyn LayoutReader>, Box<dyn LayoutReader>, Bytes) {
        let int_array = PrimitiveArray::from((0..100).collect::<Vec<_>>()).into_array();
        let int_dtype = int_array.dtype().clone();
        let chunked = ChunkedArray::try_new(iter::repeat(int_array).take(5).collect(), int_dtype)
            .unwrap()
            .into_array();
        let str_array = VarBinArray::from_vec(
            iter::repeat("test text").take(500).collect(),
            DType::Utf8(Nullability::NonNullable),
        )
        .into_array();
        let len = chunked.len();
        let struct_arr = StructArray::try_new(
            vec!["ints".into(), "strs".into()].into(),
            vec![chunked, str_array],
            len,
            Validity::NonNullable,
        )
        .unwrap()
        .into_array();

        let mut writer = LayoutWriter::new(Vec::new());
        writer = writer.write_array_columns(struct_arr).await.unwrap();
        let written = writer.finalize().await.unwrap();

        let footer = FooterReader::new(LayoutDeserializer::default())
            .read_footer(&written, written.len() as u64)
            .await
            .unwrap();

        let projection_scan = Scan {
            expr: ScanExpr::Projection(Projection::All),
            batch_size: scan.batch_size,
        };
        (
            footer
                .layout(
                    scan,
                    RelativeLayoutCache::new(
                        cache.clone(),
                        LazyDeserializedDType::from_bytes(footer.dtype_bytes()),
                    ),
                )
                .unwrap(),
            footer
                .layout(
                    projection_scan,
                    RelativeLayoutCache::new(
                        cache.clone(),
                        LazyDeserializedDType::from_bytes(footer.dtype_bytes()),
                    ),
                )
                .unwrap(),
            Bytes::from(written),
        )
    }

    #[tokio::test]
    async fn read_range() {
        let cache = Arc::new(RwLock::new(LayoutMessageCache::default()));
        let (mut filter_layout, mut project_layout, buf) = layout_and_bytes(
            cache.clone(),
            Scan {
                expr: ScanExpr::Filter(RowFilter::new(Arc::new(BinaryExpr::new(
                    Arc::new(Column::new(Field::from("ints"))),
                    Operator::Gt,
                    Arc::new(Literal::new(10.into())),
                )))),
                batch_size: 100,
            },
        )
        .await;
        let arr =
            read_layout(filter_layout.as_mut(), project_layout.as_mut(), cache, &buf).pop_front();

        assert!(arr.is_some());
        let prim_arr = arr
            .as_ref()
            .unwrap()
            .with_dyn(|a| a.as_struct_array_unchecked().field(0))
            .unwrap()
            .into_primitive()
            .unwrap();
        let str_arr = arr
            .as_ref()
            .unwrap()
            .with_dyn(|a| a.as_struct_array_unchecked().field(1))
            .unwrap()
            .into_varbin()
            .unwrap();
        assert_eq!(
            prim_arr.maybe_null_slice::<i32>(),
            &(11..100).collect::<Vec<_>>()
        );
        assert_eq!(
            str_arr
                .with_iterator(|iter| iter
                    .flatten()
                    .map(|s| unsafe { String::from_utf8_unchecked(s.to_vec()) })
                    .collect::<Vec<_>>())
                .unwrap(),
            iter::repeat("test text").take(89).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn read_range_no_filter() {
        let cache = Arc::new(RwLock::new(LayoutMessageCache::default()));
        let (mut filter_layout, mut project_layout, buf) = layout_and_bytes(
            cache.clone(),
            Scan {
                expr: ScanExpr::Projection(Projection::All),
                batch_size: 500,
            },
        )
        .await;
        let arr =
            read_layout(filter_layout.as_mut(), project_layout.as_mut(), cache, &buf).pop_front();

        assert!(arr.is_some());
        let prim_arr = arr
            .as_ref()
            .unwrap()
            .with_dyn(|a| a.as_struct_array_unchecked().field(0))
            .unwrap()
            .into_primitive()
            .unwrap();
        let str_arr = arr
            .as_ref()
            .unwrap()
            .with_dyn(|a| a.as_struct_array_unchecked().field(1))
            .unwrap()
            .into_varbin()
            .unwrap();
        assert_eq!(
            prim_arr.maybe_null_slice::<i32>(),
            iter::repeat(0..100).take(5).flatten().collect::<Vec<_>>()
        );
        assert_eq!(
            str_arr
                .with_iterator(|iter| iter
                    .flatten()
                    .map(|s| unsafe { String::from_utf8_unchecked(s.to_vec()) })
                    .collect::<Vec<_>>())
                .unwrap(),
            iter::repeat("test text").take(500).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn advance_read_range() {
        let cache = Arc::new(RwLock::new(LayoutMessageCache::default()));
        let (mut filter_layout, mut project_layout, buf) = layout_and_bytes(
            cache.clone(),
            Scan {
                expr: ScanExpr::Filter(RowFilter::new(Arc::new(BinaryExpr::new(
                    Arc::new(Column::new(Field::from("ints"))),
                    Operator::Gt,
                    Arc::new(Literal::new(10.into())),
                )))),
                batch_size: 500,
            },
        )
        .await;
        filter_layout.advance(50).unwrap();
        let arr =
            read_layout(filter_layout.as_mut(), project_layout.as_mut(), cache, &buf).pop_front();

        assert!(arr.is_some());
        let arr = arr
            .unwrap()
            .with_dyn(|a| a.as_struct_array_unchecked().field(0))
            .unwrap()
            .into_primitive()
            .unwrap();
        assert_eq!(
            arr.into_primitive().unwrap().maybe_null_slice::<i32>(),
            &(50..100).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn advance_skipped() {
        let cache = Arc::new(RwLock::new(LayoutMessageCache::default()));
        let (mut filter_layout, mut project_layout, buf) = layout_and_bytes(
            cache.clone(),
            Scan {
                expr: ScanExpr::Filter(RowFilter::new(Arc::new(BinaryExpr::new(
                    Arc::new(Column::new(Field::from("ints"))),
                    Operator::Gt,
                    Arc::new(Literal::new(10.into())),
                )))),
                batch_size: 100,
            },
        )
        .await;
        filter_layout.advance(500).unwrap();
        let arr =
            read_layout(filter_layout.as_mut(), project_layout.as_mut(), cache, &buf).pop_front();

        assert!(arr.is_none());
    }

    #[tokio::test]
    async fn batch_size() {
        let cache = Arc::new(RwLock::new(LayoutMessageCache::default()));
        let (mut filter_layout, mut project_layout, buf) = layout_and_bytes(
            cache.clone(),
            Scan {
                expr: ScanExpr::Filter(RowFilter::new(Arc::new(BinaryExpr::new(
                    Arc::new(Column::new(Field::from("ints"))),
                    Operator::Gt,
                    Arc::new(Literal::new(10.into())),
                )))),
                batch_size: 50,
            },
        )
        .await;
        let arr = read_layout(filter_layout.as_mut(), project_layout.as_mut(), cache, &buf);

        assert_eq!(
            arr.front()
                .unwrap()
                .with_dyn(|a| a.as_struct_array_unchecked().field(0))
                .unwrap()
                .into_primitive()
                .unwrap()
                .maybe_null_slice::<i32>(),
            &(11..=60).collect::<Vec<_>>()
        );
        assert_eq!(
            arr[1]
                .with_dyn(|a| a.as_struct_array_unchecked().field(0))
                .unwrap()
                .into_primitive()
                .unwrap()
                .maybe_null_slice::<i32>(),
            &(61..100).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn advance_after_filter() {
        let cache = Arc::new(RwLock::new(LayoutMessageCache::default()));
        let (mut filter_layout, mut project_layout, buf) = layout_and_bytes(
            cache.clone(),
            Scan {
                expr: ScanExpr::Filter(RowFilter::new(Arc::new(BinaryExpr::new(
                    Arc::new(Column::new(Field::from("ints"))),
                    Operator::Gt,
                    Arc::new(Literal::new(10.into())),
                )))),
                batch_size: 50,
            },
        )
        .await;
        let selector = read_layout_ranges(filter_layout.as_mut(), cache.clone(), &buf);
        project_layout.advance(50).unwrap();
        let arr = selector
            .into_iter()
            .flat_map(|s| read_layout_data(project_layout.as_mut(), cache.clone(), &buf, s))
            .collect::<Vec<_>>();

        assert_eq!(
            arr.first()
                .unwrap()
                .with_dyn(|a| a.as_struct_array_unchecked().field(0))
                .unwrap()
                .into_primitive()
                .unwrap()
                .maybe_null_slice::<i32>(),
            &(50..100).collect::<Vec<_>>()
        );
        assert_eq!(
            arr[8]
                .with_dyn(|a| a.as_struct_array_unchecked().field(0))
                .unwrap()
                .into_primitive()
                .unwrap()
                .maybe_null_slice::<i32>(),
            &(61..100).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn advance_mid_read() {
        let cache = Arc::new(RwLock::new(LayoutMessageCache::default()));
        let (mut filter_layout, mut project_layout, buf) = layout_and_bytes(
            cache.clone(),
            Scan {
                expr: ScanExpr::Filter(RowFilter::new(Arc::new(BinaryExpr::new(
                    Arc::new(Column::new(Field::from("ints"))),
                    Operator::Gt,
                    Arc::new(Literal::new(10.into())),
                )))),
                batch_size: 50,
            },
        )
        .await;
        let s = read_layout_ranges(filter_layout.as_mut(), cache.clone(), &buf);
        let advanced = AtomicBool::new(false);
        let mut arr = Vec::new();
        for rs in s {
            while let Some(rr) = project_layout.read_next(rs.clone()).unwrap() {
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
                            project_layout.advance(310).unwrap();
                        }
                    }
                }
            }
        }

        assert_eq!(arr.len(), 5);
        assert_eq!(
            arr.remove(0)
                .with_dyn(|a| a.as_struct_array_unchecked().field(0))
                .unwrap()
                .into_primitive()
                .unwrap()
                .maybe_null_slice::<i32>(),
            &(11..=60).collect::<Vec<_>>()
        );
        assert_eq!(
            arr.remove(0)
                .with_dyn(|a| a.as_struct_array_unchecked().field(0))
                .unwrap()
                .into_primitive()
                .unwrap()
                .maybe_null_slice::<i32>(),
            &(11..=60).collect::<Vec<_>>()
        );
        assert_eq!(
            arr.remove(0)
                .with_dyn(|a| a.as_struct_array_unchecked().field(0))
                .unwrap()
                .into_primitive()
                .unwrap()
                .maybe_null_slice::<i32>(),
            &(61..100).collect::<Vec<_>>()
        );
        assert_eq!(
            arr.remove(0)
                .with_dyn(|a| a.as_struct_array_unchecked().field(0))
                .unwrap()
                .into_primitive()
                .unwrap()
                .maybe_null_slice::<i32>(),
            &(11..=60).collect::<Vec<_>>()
        );
        assert_eq!(
            arr.remove(0)
                .with_dyn(|a| a.as_struct_array_unchecked().field(0))
                .unwrap()
                .into_primitive()
                .unwrap()
                .maybe_null_slice::<i32>(),
            &(61..100).collect::<Vec<_>>()
        );
    }
}
