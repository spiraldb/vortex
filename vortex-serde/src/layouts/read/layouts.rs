use std::collections::VecDeque;
use std::sync::Arc;

use bytes::Bytes;
use flatbuffers::{ForwardsUOffset, Vector};
use vortex::array::BoolArray;
use vortex::compute::and;
use vortex::stats::ArrayStatistics;
use vortex::validity::Validity;
use vortex::{Context, IntoArrayVariant};
use vortex_dtype::field::Field;
use vortex_dtype::DType;
use vortex_error::{vortex_bail, vortex_err, VortexExpect, VortexResult};
use vortex_expr::{eval_binary_expr, BinaryExpr, Literal};
use vortex_flatbuffers::footer as fb;
use vortex_schema::projection::Projection;

use super::RowFilter;
use crate::layouts::read::batch::ColumnsReader;
use crate::layouts::read::buffered::ChunkedReader;
use crate::layouts::read::cache::RelativeLayoutCache;
use crate::layouts::read::context::{LayoutDeserializer, LayoutId, LayoutSpec};
use crate::layouts::read::{LayoutReader, ReadResult, Scan};
use crate::stream_writer::ByteRange;
use crate::ArrayBufferReader;

#[derive(Debug)]
pub struct FlatLayout {
    range: ByteRange,
    ctx: Arc<Context>,
    cache: RelativeLayoutCache,
    scan: Scan,
    selection_read: bool,
    array_read: bool,
}

impl FlatLayout {
    pub fn new(
        begin: u64,
        end: u64,
        ctx: Arc<Context>,
        scan: Scan,
        cache: RelativeLayoutCache,
    ) -> Self {
        Self {
            range: ByteRange { begin, end },
            ctx,
            cache,
            scan,
            selection_read: false,
            array_read: false,
        }
    }
}

impl LayoutReader for FlatLayout {
    fn read_next(&mut self) -> VortexResult<Option<ReadResult>> {
        if self.array_read {
            return Ok(None);
        }

        match self.cache.get(&[]) {
            None => Ok(Some(ReadResult::ReadMore(vec![(
                self.cache.absolute_id(&[]),
                self.range,
            )]))),
            Some(mut buf) => {
                let mut array_reader = ArrayBufferReader::new();
                let mut read_buf = Bytes::new();
                while let Some(u) = array_reader.read(read_buf)? {
                    read_buf = buf.split_to(u);
                }

                let array = array_reader.into_array(self.ctx.clone(), self.cache.dtype())?;

                self.array_read = true;
                Ok(Some(ReadResult::Batch(array)))
            }
        }
    }

    fn eval_selection(
        &mut self,
        base_selection: Option<BoolArray>,
    ) -> VortexResult<Option<ReadResult>> {
        if self.selection_read {
            return Ok(None);
        }
        match self.cache.get(&[]) {
            None => Ok(Some(ReadResult::ReadMore(vec![(
                self.cache.absolute_id(&[]),
                self.range,
            )]))),
            Some(mut buf) => {
                let mut array_reader = ArrayBufferReader::new();
                let mut read_buf = Bytes::new();
                while let Some(u) = array_reader.read(read_buf)? {
                    read_buf = buf.split_to(u);
                }

                let data = array_reader.into_array(self.ctx.clone(), self.cache.dtype())?;

                let mut selection = base_selection.unwrap_or_else(|| {
                    BoolArray::from_vec(vec![true; data.len()], Validity::AllValid)
                });

                if let Some(row_filter) = self.scan.filter.as_ref() {
                    for expr in row_filter.expressions() {
                        if let Some(expr) = expr.as_any().downcast_ref::<BinaryExpr>().cloned() {
                            if let Some(lit) = expr.lhs().as_any().downcast_ref::<Literal>() {
                                let expr_selection =
                                    eval_binary_expr(lit.into_array(data.len()), &data, expr.op())?;
                                selection = and(&selection, expr_selection)?.into_bool()?;
                            }

                            if let Some(lit) = expr.rhs().as_any().downcast_ref::<Literal>() {
                                let expr_selection =
                                    eval_binary_expr(&data, lit.into_array(data.len()), expr.op())?;
                                selection = and(&selection, expr_selection)?.into_bool()?;
                            }
                        }

                        if selection
                            .statistics()
                            .compute_true_count()
                            .unwrap_or_default()
                            == 0
                        {
                            break;
                        }
                    }
                }

                self.selection_read = true;
                Ok(Some(ReadResult::Selection(selection)))
            }
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

    fn build_layout_reader(
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
    array_reader: Option<ColumnsReader>,
    selection_reader: Option<ColumnsReader>,
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
            array_reader: None,
            selection_reader: None,
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

    fn child_filter(field: &Field, row_filter: Option<RowFilter>) -> Option<RowFilter> {
        row_filter
            .as_ref()
            .map(|f| {
                f.expressions()
                    .iter()
                    .filter(|e| e.references().contains(field))
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .filter(|f| f.is_empty())
            .map(|exprs| RowFilter::from_conjunction(exprs))
    }

    fn child_reader(
        &self,
        idx: usize,
        children: Vector<ForwardsUOffset<fb::Layout>>,
        dtype: DType,
        child_filter: Option<RowFilter>,
        message_cache: RelativeLayoutCache,
    ) -> VortexResult<Box<dyn LayoutReader>> {
        let layout = children.get(idx);

        // TODO: Figure out complex nested schema projections
        let mut child_scan = self.scan.clone();
        child_scan.projection = Projection::All;
        child_scan.filter = child_filter;

        self.layout_serde.build_layout_reader(
            self.fb_bytes.clone(),
            layout._tab.loc(),
            child_scan,
            message_cache.relative(idx as u16, dtype),
        )
    }
}

impl LayoutReader for ColumnLayout {
    fn read_next(&mut self) -> VortexResult<Option<ReadResult>> {
        if self.array_reader.is_none() {
            let DType::Struct(top_dtype, ..) = self.message_cache.dtype() else {
                vortex_bail!("Column layout must have struct dtype")
            };

            let fb_children = self
                .flatbuffer()
                .children()
                .ok_or_else(|| vortex_err!("Missing children"))?;

            let column_layouts = match self.scan.projection {
                Projection::All => (0..fb_children.len())
                    .map(|idx| {
                        // let child_filter = Self::child_filter(&Field::Index(idx));
                        self.child_reader(
                            idx,
                            fb_children,
                            top_dtype.dtypes()[idx].clone(),
                            None,
                            self.message_cache.clone(),
                        )
                    })
                    .collect::<VortexResult<Vec<_>>>()?,
                Projection::Flat(ref v) => v
                    .iter()
                    .zip(top_dtype.dtypes().iter().cloned())
                    .map(|(projected_field, dtype)| {
                        // let child_filter = self.child_filter(projected_field);

                        let child_idx = match projected_field {
                            Field::Name(n) => top_dtype.find_name(n.as_ref()).ok_or_else(|| {
                                vortex_err!("Invalid projection, trying to select  {n}")
                            })?,
                            Field::Index(i) => *i,
                        };
                        self.child_reader(
                            child_idx,
                            fb_children,
                            dtype,
                            None,
                            self.message_cache.clone(),
                        )
                    })
                    .collect::<VortexResult<Vec<_>>>()?,
            };

            self.array_reader = Some(ColumnsReader::new(
                top_dtype.names().clone(),
                column_layouts,
            ));
        }

        self.array_reader
            .as_mut()
            .vortex_expect("Missing reader")
            .read()
    }

    fn eval_selection(
        &mut self,
        _base_selection: Option<BoolArray>,
    ) -> VortexResult<Option<ReadResult>> {
        match self.scan.filter_scan.as_ref() {
            None => return Ok(None),
            Some(filter_scan) if self.selection_reader.is_none() => {
                let DType::Struct(ref top_dtype, ..) = filter_scan.dtype else {
                    vortex_bail!("Column layout must have struct dtype")
                };

                let fb_children = self
                    .flatbuffer()
                    .children()
                    .ok_or_else(|| vortex_err!("Missing children"))?;

                let column_layouts = match filter_scan.projection {
                    Projection::All => (0..fb_children.len())
                        .map(|idx| {
                            let child_filter = Self::child_filter(
                                &Field::Index(idx),
                                Some(filter_scan.row_filter.clone()),
                            );
                            self.child_reader(
                                idx,
                                fb_children,
                                top_dtype.dtypes()[idx].clone(),
                                child_filter,
                                filter_scan.message_cache.clone(),
                            )
                        })
                        .collect::<VortexResult<Vec<_>>>()?,
                    Projection::Flat(ref v) => v
                        .iter()
                        .zip(top_dtype.dtypes().iter().cloned())
                        .map(|(projected_field, dtype)| {
                            let child_filter = Self::child_filter(
                                projected_field,
                                Some(filter_scan.row_filter.clone()),
                            );

                            let child_idx = match projected_field {
                                Field::Name(n) => {
                                    top_dtype.find_name(n.as_ref()).ok_or_else(|| {
                                        vortex_err!("Invalid projection, trying to select  {n}")
                                    })?
                                }
                                Field::Index(i) => *i,
                            };
                            self.child_reader(
                                child_idx,
                                fb_children,
                                dtype,
                                child_filter,
                                filter_scan.message_cache.clone(),
                            )
                        })
                        .collect::<VortexResult<Vec<_>>>()?,
                };

                self.selection_reader = Some(ColumnsReader::new(
                    top_dtype.names().clone(),
                    column_layouts,
                ));
            }
            Some(_) => {}
        }

        self.selection_reader
            .as_mut()
            .vortex_expect("Missing reader")
            .eval_selection()
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

    fn build_layout_reader(
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
    buffered_reader: Option<ChunkedReader>,
    selection_reader: Option<ChunkedReader>,
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
            buffered_reader: None,
            selection_reader: None,
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
        if self.buffered_reader.is_none() {
            let children = self
                .flatbuffer()
                .children()
                .ok_or_else(|| vortex_err!("Missing children"))?
                .iter()
                .enumerate()
                // Skip over the metadata table of this layout
                .skip(1)
                .map(|(i, c)| {
                    self.layout_builder.build_layout_reader(
                        self.fb_bytes.clone(),
                        c._tab.loc(),
                        self.scan.clone(),
                        self.message_cache
                            .relative(i as u16, self.message_cache.dtype().clone()),
                    )
                })
                .collect::<VortexResult<VecDeque<_>>>()?;

            self.buffered_reader = Some(ChunkedReader::new(children, self.scan.batch_size));
        }

        self.buffered_reader
            .as_mut()
            .vortex_expect("Missing reader")
            .read_next()
    }

    fn eval_selection(
        &mut self,
        _base_selection: Option<BoolArray>,
    ) -> VortexResult<Option<ReadResult>> {
        self.
    }
}
