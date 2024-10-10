use bytes::Bytes;
use flatbuffers::{ForwardsUOffset, Vector};
use vortex_dtype::field::Field;
use vortex_dtype::{DType, StructDType};
use vortex_error::{vortex_bail, vortex_err, VortexExpect, VortexResult};
use vortex_flatbuffers::footer as fb;
use vortex_schema::projection::Projection;

use crate::layouts::read::batch::{BatchPruner, BatchReader};
use crate::layouts::read::cache::RelativeLayoutCache;
use crate::layouts::read::selection::RowSelector;
use crate::layouts::{
    LayoutDeserializer, LayoutId, LayoutReader, LayoutSpec, PlanResult, PruningScan, ReadResult,
    Scan,
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
    reader: Option<BatchReader>,
    pruner: Option<BatchPruner>,
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
            reader: None,
            pruner: None,
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

    fn read_child(
        &self,
        idx: usize,
        children: Vector<ForwardsUOffset<fb::Layout>>,
        dtype: DType,
    ) -> VortexResult<Box<dyn LayoutReader>> {
        let layout = children.get(idx);

        // TODO: Figure out complex nested schema projections
        let mut child_scan = self.scan.clone();
        child_scan.projection = Projection::All;

        self.layout_serde.read_layout(
            self.fb_bytes.clone(),
            layout._tab.loc(),
            child_scan,
            self.message_cache.relative(idx as u16, dtype),
        )
    }

    fn children_layouts(&self) -> VortexResult<Vec<Box<dyn LayoutReader>>> {
        let DType::Struct(s, ..) = self.message_cache.dtype() else {
            vortex_bail!("Column layout must have struct dtype")
        };

        let fb_children = self
            .flatbuffer()
            .children()
            .ok_or_else(|| vortex_err!("Missing children"))?;

        match self.scan.projection {
            Projection::All => (0..fb_children.len())
                .zip(s.dtypes().iter().cloned())
                .map(|(idx, dtype)| self.read_child(idx, fb_children, dtype))
                .collect::<VortexResult<Vec<_>>>(),
            Projection::Flat(ref v) => v
                .iter()
                .zip(s.dtypes().iter().cloned())
                .map(|(projected_field, dtype)| {
                    let child_idx = match projected_field {
                        Field::Name(n) => s.find_name(n.as_ref()).ok_or_else(|| {
                            vortex_err!("Invalid projection, trying to select  {n}")
                        })?,
                        Field::Index(i) => *i,
                    };
                    self.read_child(child_idx, fb_children, dtype)
                })
                .collect::<VortexResult<Vec<_>>>(),
        }
    }

    fn struct_dtype(&self) -> VortexResult<&StructDType> {
        let DType::Struct(s, ..) = self.message_cache.dtype() else {
            vortex_bail!("Column layout must have struct dtype")
        };
        Ok(s)
    }
}

impl LayoutReader for ColumnLayout {
    fn with_selected_rows(&mut self, row_selector: &RowSelector) {
        assert!(
            self.reader.is_none(),
            "Can only alter row selection if reading hasn't been started"
        );
        self.scan.rows = self
            .scan
            .rows
            .as_ref()
            .map(|rs| rs.intersect(row_selector))
            .or_else(|| Some(row_selector.clone()))
    }

    fn read_next(&mut self) -> VortexResult<Option<ReadResult>> {
        if let Some(br) = &mut self.reader {
            br.read_more()
        } else {
            self.reader = Some(BatchReader::new(
                self.struct_dtype()?.names().clone(),
                self.children_layouts()?,
            ));
            self.read_next()
        }
    }

    fn plan(&mut self, scan: PruningScan) -> VortexResult<Option<PlanResult>> {
        if let Some(pr) = &mut self.pruner {
            pr.plan_more()
        } else {
            self.pruner = Some(BatchPruner::try_new(
                self.struct_dtype()?,
                self.children_layouts()?,
                scan.clone(),
            )?);
            self.plan(scan)
        }
    }
}
