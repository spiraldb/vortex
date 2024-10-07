use bytes::Bytes;
use flatbuffers::{ForwardsUOffset, Vector};
use vortex_dtype::{DType, StructDType};
use vortex_error::{vortex_bail, VortexExpect, VortexResult};
use vortex_flatbuffers::footer;

use crate::layouts::read::batch::{BatchFilter, BatchReader};
use crate::layouts::read::cache::RelativeLayoutCache;
use crate::layouts::read::selection::RowSelector;
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
        _length: u64,
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
    InitFilter,
    Filtering(BatchFilter),
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
        let state = if matches!(scan.expr, ScanExpr::Filter(_)) {
            ColumnLayoutState::InitFilter
        } else {
            ColumnLayoutState::Init
        };
        Self {
            fb_bytes,
            fb_loc,
            scan,
            layout_serde,
            message_cache,
            state,
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

    fn read_child(
        &self,
        _idx: usize,
        _children: Vector<ForwardsUOffset<footer::Layout>>,
        _dtype: DType,
    ) -> VortexResult<Box<dyn LayoutReader>> {
        panic!("NOO")
        // let layout = children.get(idx);
        //
        // let child_scan = Scan {
        //     indices: self.scan.indices.clone(),
        //     // TODO(robert): Changes this once we support nested projections
        //     projection: Projection::All,
        //     filter: None,
        //     batch_size: self.scan.batch_size,
        // };
        //
        // self.layout_serde.read_layout(
        //     self.fb_bytes.clone(),
        //     layout._tab.loc(),
        //     // FIXME
        //     0,
        //     child_scan,
        //     self.message_cache.relative(idx as u16, dtype),
        // )
    }

    fn filter_reader(&mut self) -> VortexResult<BatchFilter> {
        panic!("NOO")
        // let Some(ref rf) = self.scan.filter else {
        //     vortex_bail!("Asked for filter children when there's no filter")
        // };
        //
        // let fb_children = self
        //     .flatbuffer()
        //     .children()
        //     .ok_or_else(|| vortex_err!("Missing children"))?;
        // let filter_refs = rf.references().into_iter().collect::<Vec<_>>();
        // let filter_dtype = self.message_cache.projected_dtype(&filter_refs)?;
        // let DType::Struct(s, ..) = filter_dtype else {
        //     vortex_bail!("Column layout must have struct dtype")
        // };
        //
        // let mut unhandled_children_names = Vec::new();
        // let mut unhandled_children = Vec::new();
        // let mut handled_children = Vec::new();
        //
        // for (idx, field) in filter_refs.into_iter().enumerate() {
        //     let resolved_child = self.message_cache.resolve_field(&field)?;
        //     let child_loc = fb_children.get(resolved_child)._tab.loc();
        //     let child_scan = Scan {
        //         indices: self.scan.indices.clone(),
        //         // TODO(robert): Changes this once we support nested projections
        //         projection: Projection::All,
        //         filter: rf.project(&[field]),
        //         batch_size: self.scan.batch_size,
        //     };
        //
        //     let has_filter = child_scan.filter.is_some();
        //
        //     let child = self.layout_serde.read_layout(
        //         self.fb_bytes.clone(),
        //         child_loc,
        //         child_scan,
        //         self.message_cache
        //             .relative(idx as u16, s.dtypes()[idx].clone()),
        //     )?;
        //     if has_filter {
        //         handled_children.push(child);
        //     } else {
        //         unhandled_children.push(child);
        //         unhandled_children_names.push(s.names()[idx].clone());
        //     }
        // }
        //
        // if !unhandled_children_names.is_empty() {
        //     let Some(prf) = rf.project(
        //         &unhandled_children_names
        //             .iter()
        //             .map(|n| Field::from(n.as_ref()))
        //             .collect::<Vec<_>>(),
        //     ) else {
        //         vortex_bail!("Must be able to project filter into unhandled space")
        //     };
        //
        //     handled_children.push(Box::new(FilterLayoutReader::new(
        //         Box::new(BatchReader::new(
        //             unhandled_children_names.into(),
        //             unhandled_children,
        //         )),
        //         prf,
        //     )));
        // }
        //
        // Ok(BatchFilter::new(handled_children))
    }

    fn read_children(&mut self) -> VortexResult<Vec<Box<dyn LayoutReader>>> {
        panic!("NOOO")
        // let s = self.struct_dtype()?;
        // let fb_children = self
        //     .flatbuffer()
        //     .children()
        //     .ok_or_else(|| vortex_err!("Missing children"))?;
        //
        // match self.scan.projection {
        //     Projection::All => (0..fb_children.len())
        //         .zip(s.dtypes().iter().cloned())
        //         .map(|(idx, dtype)| self.read_child(idx, fb_children, dtype))
        //         .collect::<VortexResult<Vec<_>>>(),
        //     Projection::Flat(ref v) => v
        //         .iter()
        //         .map(|f| self.message_cache.resolve_field(f))
        //         .enumerate()
        //         .map(|(idx, child_idx)| {
        //             child_idx
        //                 .and_then(|cid| self.read_child(cid, fb_children, s.dtypes()[idx].clone()))
        //         })
        //         .collect::<VortexResult<Vec<_>>>(),
        // }
    }

    fn struct_dtype(&self) -> VortexResult<StructDType> {
        panic!("NOOO")
        // let dtype = match self.scan.projection {
        //     Projection::All => self.message_cache.dtype()?,
        //     Projection::Flat(ref p) => self.message_cache.projected_dtype(p)?,
        // };
        // let DType::Struct(s, ..) = dtype else {
        //     vortex_bail!("Column layout must have struct dtype")
        // };
        // Ok(s)
    }
}

impl LayoutReader for ColumnLayout {
    fn read_next(&mut self, selection: RowSelector) -> VortexResult<Option<ReadResult>> {
        match &mut self.state {
            ColumnLayoutState::Init => {
                self.state = ColumnLayoutState::ReadColumns(BatchReader::new(
                    self.struct_dtype()?.names().clone(),
                    self.read_children()?,
                    // FIXME
                    0,
                ));
                self.read_next(selection)
            }
            ColumnLayoutState::ReadColumns(br) => br.read_next_batch(selection),
            _ => vortex_bail!("We are returning batches"),
        }
    }

    fn read_range(&mut self) -> VortexResult<Option<RangeResult>> {
        match &mut self.state {
            ColumnLayoutState::InitFilter => {
                self.state = ColumnLayoutState::Filtering(self.filter_reader()?);
                self.read_range()
            }
            ColumnLayoutState::Filtering(fr) => fr.read_more_ranges(),
            _ => vortex_bail!("We are returning ranges"),
        }
    }

    fn advance(&mut self, _up_to_row: usize) -> VortexResult<Messages> {
        todo!()
    }
}
