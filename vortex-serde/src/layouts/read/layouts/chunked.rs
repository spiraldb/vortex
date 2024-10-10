use std::collections::VecDeque;
use std::sync::Arc;

use ahash::HashSet;
use bytes::Bytes;
use vortex::array::{ConstantArray, StructArray};
use vortex::stats::Stat;
use vortex::variants::StructArrayTrait;
use vortex::{Array, IntoArray, IntoArrayVariant};
use vortex_dtype::field::Field;
use vortex_dtype::{DType, Nullability, PType};
use vortex_error::{vortex_bail, vortex_err, VortexExpect, VortexResult};
use vortex_flatbuffers::footer as fb;
use vortex_scalar::Scalar;
use vortex_schema::projection::Projection;

use crate::layouts::read::buffered::BufferedReader;
use crate::layouts::read::cache::RelativeLayoutCache;
use crate::layouts::read::selection::{RowRange, RowSelector};
use crate::layouts::{
    LayoutDeserializer, LayoutId, LayoutReader, LayoutSpec, PlanResult, PruningScan, ReadResult,
    Scan,
};

#[derive(Debug)]
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
    metadata_reader: Option<Box<dyn LayoutReader>>,
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
            metadata_reader: None,
        }
    }

    fn flatbuffer(&self) -> fb::NestedLayout {
        let fb_layout = unsafe {
            let tab = flatbuffers::Table::new(&self.fb_bytes, self.fb_loc);
            fb::Layout::init_from_table(tab)
        };
        fb_layout
            .layout_as_nested_layout()
            .vortex_expect("ChunkedLayout: Failed to read nested layout from flatbuffer")
    }

    fn children(&self) -> VortexResult<impl Iterator<Item = fb::Layout> + '_> {
        Ok(self
            .flatbuffer()
            .children()
            .ok_or_else(|| vortex_err!("Missing children"))?
            .iter())
    }
}

impl LayoutReader for ChunkedLayout {
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
        if let Some(cr) = &mut self.reader {
            cr.read()
        } else {
            let children = self
                .children()?
                .enumerate()
                // Skip over the metadata table of this layout
                // TODO(robert): Chunked layouts might not have statistics, in this case delegate to children
                .skip(1)
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
            let reader = BufferedReader::new(children, self.scan.batch_size);
            self.reader = Some(reader);
            self.read_next()
        }
    }

    fn plan(&mut self, scan: PruningScan) -> VortexResult<Option<PlanResult>> {
        // TODO(robert): Chunked layouts might not have statistics, in this case delegate to children
        if scan.stats_projection.len() != 1 {
            vortex_bail!("Chunked Layout should only have flat children");
        }
        if let Some(mr) = &mut self.metadata_reader {
            // we assume that metadata isn't chunked
            match mr.read_next()? {
                None => Ok(None),
                Some(rr) => match rr {
                    ReadResult::ReadMore(m) => Ok(Some(PlanResult::ReadMore(m))),
                    ReadResult::Batch(b) => {
                        if let Some(pf) = &scan.filter {
                            let field_prefix = scan
                                .stats_projection
                                .keys()
                                .next()
                                .cloned()
                                .vortex_expect("Must have only single field ref");
                            let field_stats = scan
                                .stats_projection
                                .get(&field_prefix)
                                .vortex_expect("We got the key by iterating keys");
                            let prefix_str = match field_prefix {
                                Field::Name(n) => n.to_string(),
                                Field::Index(i) => i.to_string(),
                            };
                            let expanded_batch = add_missing_columns(
                                b,
                                &prefix_str,
                                field_stats,
                                self.message_cache.dtype(),
                            )?;
                            let mask = pf.evaluate(&expanded_batch)?;
                            let row_offsets = expanded_batch
                                .with_dyn(|a| {
                                    a.as_struct_array()
                                        .ok_or_else(|| vortex_err!("Stats weren't a struct array"))
                                        .map(|s| {
                                            s.field_by_name(&format!("{prefix_str}_row_offset"))
                                        })
                                })?
                                .ok_or_else(|| vortex_err!("Missing row offsets"))?;
                            filter_offsets(row_offsets, mask, scan.row_count as usize)
                                .map(|s| Some(PlanResult::Range(s)))
                        } else {
                            Ok(Some(PlanResult::Batch(b)))
                        }
                    }
                },
            }
        } else {
            let metadata_child = self
                .flatbuffer()
                .children()
                .ok_or_else(|| vortex_err!("Missing children"))?
                .get(0);
            self.metadata_reader = Some(
                self.layout_builder.read_layout(
                    self.fb_bytes.clone(),
                    metadata_child._tab.loc(),
                    Scan {
                        // TODO(robert): This projection is not yet utilized since stats are Flat
                        projection: Projection::Flat(
                            scan.stats_projection
                                .values()
                                .flat_map(|stats| stats.iter().map(|s| Field::Name(s.to_string())))
                                .collect(),
                        ),
                        rows: None,
                        filter: None,
                        batch_size: usize::MAX,
                    },
                    self.message_cache.relative_stored_dtype(0u16),
                )?,
            );
            self.plan(scan)
        }
    }
}

fn filter_offsets(offsets: Array, mask: Array, row_count: usize) -> VortexResult<RowSelector> {
    let primitive_offsets = offsets.into_primitive()?;
    let offsets_slice = primitive_offsets.maybe_null_slice::<u64>();
    let primitive_mask = mask.into_bool()?;
    let mut row_ranges = Vec::new();
    let mut last_included_offset = offsets_slice.len();
    let mut last_excluded_offset = None;
    for (i, b) in primitive_mask.boolean_buffer().iter().enumerate() {
        if !b {
            if last_included_offset + 1 == i {
                row_ranges.push(RowRange::new(
                    offsets_slice[last_excluded_offset.map(|lo| lo + 1).unwrap_or(0)] as usize,
                    offsets_slice[last_included_offset + 1] as usize,
                ));
            }
            last_excluded_offset = Some(i);
        } else {
            last_included_offset = i;
        }
    }

    if last_included_offset + 1 == offsets_slice.len() {
        row_ranges.push(RowRange::new(
            offsets_slice[last_excluded_offset.map(|lo| lo + 1).unwrap_or(0)] as usize,
            row_count,
        ))
    }

    Ok(RowSelector::new(row_ranges))
}

fn add_missing_columns(
    array: Array,
    prefix: impl AsRef<str>,
    required_stats: &HashSet<Stat>,
    data_dtype: &DType,
) -> VortexResult<Array> {
    let st = StructArray::try_from(array)?;
    let prefix_str = prefix.as_ref();
    let missing_stats = required_stats
        .iter()
        .filter(|n| {
            !st.names()
                .iter()
                .any(|f| f.as_ref() == n.to_string().as_str())
        })
        .copied()
        .collect::<Vec<_>>();

    let missing_children = missing_stats
        .iter()
        .map(|s| match s {
            Stat::Max => Ok(
                ConstantArray::new(Scalar::null(data_dtype.as_nullable()), st.len()).into_array(),
            ),
            Stat::Min => Ok(
                ConstantArray::new(Scalar::null(data_dtype.as_nullable()), st.len()).into_array(),
            ),
            Stat::TrueCount => Ok(ConstantArray::new(
                Scalar::null(DType::Primitive(PType::U64, Nullability::Nullable)),
                st.len(),
            )
            .into_array()),
            Stat::NullCount => Ok(ConstantArray::new(
                Scalar::null(DType::Primitive(PType::U64, Nullability::Nullable)),
                st.len(),
            )
            .into_array()),
            s => vortex_bail!("Can't prune on {s}"),
        })
        .collect::<VortexResult<Vec<_>>>()?;

    let new_names = st
        .names()
        .iter()
        .map(|n| Arc::from(format!("{prefix_str}_{n}").as_str()))
        .chain(
            missing_stats
                .iter()
                .map(|s| Arc::from(format!("{prefix_str}_{s}").as_str())),
        )
        .collect();

    let new_children = st.children().chain(missing_children).collect();

    StructArray::try_new(new_names, new_children, st.len(), st.validity()).map(|a| a.into_array())
}

#[cfg(test)]
mod tests {
    use rstest::{fixture, rstest};
    use vortex::array::{BoolArray, PrimitiveArray};
    use vortex::{Array, IntoArray};

    use crate::layouts::read::layouts::chunked::filter_offsets;
    use crate::layouts::read::selection::{RowRange, RowSelector};

    #[fixture]
    pub fn offsets_array() -> (Array, usize) {
        (
            PrimitiveArray::from(vec![0u64, 10, 24, 42, 66, 99]).into_array(),
            121,
        )
    }

    #[rstest]
    #[case(BoolArray::from(vec![false, false, true, true, false, true]).into_array(), RowSelector::new(vec![RowRange::new(24,66), RowRange::new(99, 121)]))]
    #[case(BoolArray::from(vec![true, true, true, true, true, true]).into_array(), RowSelector::new(vec![RowRange::new(0,121)]))]
    #[case(BoolArray::from(vec![true, false, true, false, true, false]).into_array(), RowSelector::new(vec![RowRange::new(0,10), RowRange::new(24, 42), RowRange::new(66, 99)]))]
    #[case(BoolArray::from(vec![false, false, false, false, false, false]).into_array(), RowSelector::new(vec![]))]
    #[case(BoolArray::from(vec![false, false, false, false, false, true]).into_array(), RowSelector::new(vec![RowRange::new(99, 121)]))]
    fn offsets(
        #[from(offsets_array)] offsets_len: (Array, usize),
        #[case] mask: Array,
        #[case] expected: RowSelector,
    ) {
        assert_eq!(
            filter_offsets(offsets_len.0, mask, offsets_len.1).unwrap(),
            expected
        );
    }
}
