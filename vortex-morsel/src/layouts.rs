// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! The built-in [`LayoutPlanner`]s, one per stored layout kind the executor understands.
//!
//! Each planner answers two questions about its layout: where do fresh stored chunks start, and
//! which nodes execute it. The two answers live side by side so they are reviewed together, and
//! a test checks that the morsel cut they produce and the plan's natural splits agree.

use std::sync::Arc;

use vortex_array::dtype::FieldNames;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_layout::LayoutRef;
use vortex_layout::layouts::chunked::Chunked;
use vortex_layout::layouts::dict::Dict;
use vortex_layout::layouts::flat::Flat;
use vortex_layout::layouts::struct_::Struct;
use vortex_layout::layouts::zoned::LegacyStats;
use vortex_layout::layouts::zoned::Zoned;

use crate::build::LayoutCx;
use crate::build::LayoutPlanner;
use crate::build::SplitCx;
use crate::node::NodeId;
use crate::nodes::ChunkedSpec;
use crate::nodes::DictSpec;
use crate::nodes::FlatSpec;
use crate::nodes::StructSpec;

/// Zoned and legacy-statistics layouts are transparent: the executor does no statistics pruning
/// of its own, so both plan as their data child.
pub struct ZonedPlanner;

impl ZonedPlanner {
    fn data(layout: &LayoutRef) -> VortexResult<LayoutRef> {
        layout
            .slot(0)?
            .ok_or_else(|| vortex_err!("zoned layout has no data child"))
    }
}

impl LayoutPlanner for ZonedPlanner {
    fn handles(&self, layout: &LayoutRef) -> bool {
        layout.is::<Zoned>() || layout.is::<LegacyStats>()
    }

    fn natural_splits(
        &self,
        layout: &LayoutRef,
        root_offset: u64,
        cx: &mut SplitCx<'_>,
    ) -> VortexResult<()> {
        cx.child(&Self::data(layout)?, root_offset)
    }

    fn plan(
        &self,
        layout: &LayoutRef,
        root_offset: u64,
        cx: &mut LayoutCx<'_>,
    ) -> VortexResult<NodeId> {
        cx.child(&Self::data(layout)?, root_offset)
    }
}

/// A flat layout is one stored segment: the only node that reads storage.
pub struct FlatPlanner;

impl LayoutPlanner for FlatPlanner {
    fn handles(&self, layout: &LayoutRef) -> bool {
        layout.is::<Flat>()
    }

    fn natural_splits(
        &self,
        layout: &LayoutRef,
        root_offset: u64,
        cx: &mut SplitCx<'_>,
    ) -> VortexResult<()> {
        cx.split_at(root_offset + layout.row_count());
        Ok(())
    }

    fn plan(
        &self,
        layout: &LayoutRef,
        root_offset: u64,
        cx: &mut LayoutCx<'_>,
    ) -> VortexResult<NodeId> {
        let flat = layout
            .as_opt::<Flat>()
            .ok_or_else(|| vortex_err!("the flat planner was handed {}", layout.encoding_id()))?
            .clone();
        let own_rows = root_offset..root_offset + layout.row_count();
        cx.split_at(own_rows.end);
        let lease_range = cx.lease_range(own_rows);
        Ok(cx.push(Box::new(FlatSpec {
            layout: flat,
            root_offset,
            lease_range,
        })))
    }
}

/// A dictionary layout: its values are used by every morsel of its codes' range, so the values
/// subtree is planned under that lease scope and cuts no morsels of its own.
pub struct DictPlanner;

impl LayoutPlanner for DictPlanner {
    fn handles(&self, layout: &LayoutRef) -> bool {
        layout.is::<Dict>()
    }

    fn natural_splits(
        &self,
        layout: &LayoutRef,
        root_offset: u64,
        cx: &mut SplitCx<'_>,
    ) -> VortexResult<()> {
        let codes = layout
            .slot(1)?
            .ok_or_else(|| vortex_err!("dictionary layout has no codes child"))?;
        cx.child(&codes, root_offset)
    }

    fn plan(
        &self,
        layout: &LayoutRef,
        root_offset: u64,
        cx: &mut LayoutCx<'_>,
    ) -> VortexResult<NodeId> {
        let values_layout = layout
            .slot(0)?
            .ok_or_else(|| vortex_err!("dictionary layout has no values child"))?;
        let codes_layout = layout
            .slot(1)?
            .ok_or_else(|| vortex_err!("dictionary layout has no codes child"))?;
        let values_len = usize::try_from(values_layout.row_count())
            .map_err(|_| vortex_err!("dictionary values row count exceeds usize"))?;
        let logical_range = root_offset..root_offset + layout.row_count();
        let values = cx.child_with_lease(&values_layout, root_offset, logical_range)?;
        let codes = cx.child(&codes_layout, root_offset)?;
        Ok(cx.push(Box::new(DictSpec {
            values,
            codes,
            values_len,
        })))
    }
}

/// A struct layout: one child per field, plus a validity child when the struct is nullable.
pub struct StructPlanner;

impl StructPlanner {
    fn children(
        layout: &LayoutRef,
    ) -> VortexResult<(FieldNames, Option<LayoutRef>, Vec<LayoutRef>)> {
        let fields = layout.dtype().as_struct_fields_opt().ok_or_else(|| {
            vortex_err!("struct layout has a non-struct dtype {}", layout.dtype())
        })?;
        let validity = if layout.dtype().is_nullable() {
            Some(
                layout
                    .slot(0)?
                    .ok_or_else(|| vortex_err!("nullable struct layout has no validity child"))?,
            )
        } else {
            None
        };
        let mut children = Vec::with_capacity(fields.nfields());
        for idx in 0..fields.nfields() {
            children.push(
                layout
                    .slot(idx + 1)?
                    .ok_or_else(|| vortex_err!("struct layout has no child for field {idx}"))?,
            );
        }
        Ok((fields.names().clone(), validity, children))
    }
}

impl LayoutPlanner for StructPlanner {
    fn handles(&self, layout: &LayoutRef) -> bool {
        layout.is::<Struct>()
    }

    fn natural_splits(
        &self,
        layout: &LayoutRef,
        root_offset: u64,
        cx: &mut SplitCx<'_>,
    ) -> VortexResult<()> {
        let (_, validity, children) = Self::children(layout)?;
        if let Some(validity) = validity {
            cx.child(&validity, root_offset)?;
        }
        for child in &children {
            cx.child(child, root_offset)?;
        }
        Ok(())
    }

    fn plan(
        &self,
        layout: &LayoutRef,
        root_offset: u64,
        cx: &mut LayoutCx<'_>,
    ) -> VortexResult<NodeId> {
        let (names, validity, children) = Self::children(layout)?;
        let validity = validity
            .map(|validity| cx.child(&validity, root_offset))
            .transpose()?;
        let children = children
            .iter()
            .map(|child| cx.child(child, root_offset))
            .collect::<VortexResult<Vec<_>>>()?;
        Ok(cx.push(Box::new(StructSpec {
            names,
            children: Arc::from(children),
            validity,
        })))
    }
}

/// A chunked layout: cumulative chunk offsets plus one child per chunk this plan materializes.
///
/// A range-scoped plan skips the chunks no planned range touches; the node remembers the
/// original chunk index of every child it did materialize.
pub struct ChunkedPlanner;

impl LayoutPlanner for ChunkedPlanner {
    fn handles(&self, layout: &LayoutRef) -> bool {
        layout.is::<Chunked>()
    }

    fn natural_splits(
        &self,
        layout: &LayoutRef,
        root_offset: u64,
        cx: &mut SplitCx<'_>,
    ) -> VortexResult<()> {
        let chunked = layout.as_opt::<Chunked>().ok_or_else(|| {
            vortex_err!("the chunked planner was handed {}", layout.encoding_id())
        })?;
        let mut offset = 0;
        for idx in 0..chunked.nchildren() {
            let rows = chunked.child_row_count(idx);
            // An indivisible child has no inner boundaries; its own end is enough.
            if !chunked.children().child_is_indivisible(idx) {
                let child = chunked
                    .slot(idx)?
                    .ok_or_else(|| vortex_err!("chunked layout has no child {idx}"))?;
                cx.child(&child, root_offset + offset)?;
            }
            offset += rows;
            cx.split_at(root_offset + offset);
        }
        Ok(())
    }

    fn plan(
        &self,
        layout: &LayoutRef,
        root_offset: u64,
        cx: &mut LayoutCx<'_>,
    ) -> VortexResult<NodeId> {
        let chunked = layout.as_opt::<Chunked>().ok_or_else(|| {
            vortex_err!("the chunked planner was handed {}", layout.encoding_id())
        })?;
        let nchunks = chunked.nchildren();
        let mut offsets = Vec::with_capacity(nchunks + 1);
        offsets.push(0u64);
        for idx in 0..nchunks {
            offsets.push(offsets[idx] + chunked.child_row_count(idx));
        }

        let mut child_chunks = Vec::with_capacity(nchunks);
        let mut children = Vec::with_capacity(nchunks);
        for idx in 0..nchunks {
            let child_range = root_offset + offsets[idx]..root_offset + offsets[idx + 1];
            if !cx.is_planned(&child_range) {
                continue;
            }
            let child = layout
                .slot(idx)?
                .ok_or_else(|| vortex_err!("chunked layout has no child {idx}"))?;
            child_chunks.push(idx);
            children.push(cx.child(&child, child_range.start)?);
        }
        Ok(cx.push(Box::new(ChunkedSpec {
            chunk_offsets: Arc::from(offsets),
            child_chunks: Arc::from(child_chunks),
            children: Arc::from(children),
            dtype: layout.dtype().clone(),
        })))
    }
}
