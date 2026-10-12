// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! This module defines the file statistics component of the Vortex file footer.
//!
//! File statistics provide metadata about the data in the file, such as min/max values,
//! null counts, and other statistical information that can be used for query optimization
//! and data exploration.
use std::hash::Hash;
use std::sync::Arc;
use std::sync::OnceLock;

use flatbuffers::FlatBufferBuilder;
use flatbuffers::WIPOffset;
use itertools::Itertools;
use vortex_array::aggregate_fn::AggregateFnId;
use vortex_array::aggregate_fn::AggregateFnRef;
use vortex_array::aggregate_fn::session::AggregateFnSessionExt;
use vortex_array::dtype::DType;
use vortex_array::dtype::FieldPath;
use vortex_array::flatbuffers::FlatBufferRoot;
use vortex_array::flatbuffers::WriteFlatBuffer;
use vortex_array::flatbuffers::array::ArrayStats;
use vortex_array::scalar::Scalar;
use vortex_array::scalar::ScalarValue;
use vortex_array::stats::StatsSet;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_ensure;
use vortex_error::vortex_ensure_eq;
use vortex_error::vortex_err;
use vortex_layout::layouts::file_stats::AggregateStat;
use vortex_layout::layouts::file_stats::AggregateStats;
use vortex_layout::layouts::file_stats::postorder_stats_layout;
use vortex_session::VortexSession;
use vortex_utils::aliases::hash_map::HashMap;

use crate::flatbuffers::footer as fb;

/// Contains statistical information about the data in a Vortex file.
///
/// Statistics are held in two views. The nested view has one entry per field of the file in the
/// post-order layout of [`postorder_stats_layout`], addressed by [`FieldPath`] through
/// [`Self::get_by_path`] or by entry index through [`Self::entry`]. The top-level view has one
/// [`StatsSet`] per top-level struct field (or a single one for a non-struct file dtype), as file
/// statistics were exposed before nested fields had statistics, through [`Self::stats_sets`] and
/// [`Self::get`]. These statistics can be used for query optimization and data exploration.
#[derive(Clone, Debug)]
pub struct FileStatistics {
    /// The aggregates recorded for each field or column in the file, following the post-order
    /// nested-struct layout.
    aggregates: Arc<[AggregateStats]>,
    /// The `DType` of each entry in `aggregates`.
    entry_dtypes: Arc<[DType]>,
    /// An array of field paths, one for each field or column in the file. Parallel to `aggregates`
    /// and `entry_dtypes`. For files written before nested field stats, every path has depth 1 (or
    /// is the root path, for a non-struct file dtype).
    paths: Arc<[FieldPath]>,
    /// Maps each entry in `paths` to its index, so [`Self::get_by_path`] doesn't need to scan
    /// `paths` linearly.
    path_index: Arc<HashMap<FieldPath, usize>>,
    /// Top-level-fields-only aggregates, one per top-level struct field (or a single entry for a
    /// non-struct root dtype). Serialized into the legacy `field_stats` for readers that predate
    /// nested field statistics. Empty when the writer excluded them.
    legacy_aggregates: Arc<[AggregateStats]>,
    /// The `DType` of each entry in `legacy_aggregates`.
    legacy_dtypes: Arc<[DType]>,
    /// `legacy_aggregates` as [`StatsSet`]s, built on first use by [`Self::stats_sets`] and shared
    /// by clones.
    legacy_stats_sets: Arc<OnceLock<Arc<[StatsSet]>>>,
}

impl FileStatistics {
    /// Builds `path_index` from `paths` and assembles the final struct. The single place that
    /// constructs a [`FileStatistics`], so `path_index` can't drift out of sync with `paths`.
    ///
    /// # Panics
    ///
    /// Panics if either view's aggregates and dtypes have different lengths, or if `aggregates`
    /// and `paths` do.
    fn from_parts(
        aggregates: Arc<[AggregateStats]>,
        entry_dtypes: Arc<[DType]>,
        paths: Arc<[FieldPath]>,
        legacy_aggregates: Arc<[AggregateStats]>,
        legacy_dtypes: Arc<[DType]>,
    ) -> Self {
        assert_eq!(
            aggregates.len(),
            entry_dtypes.len(),
            "aggregates and dtypes must have the same length"
        );
        assert_eq!(
            aggregates.len(),
            paths.len(),
            "aggregates and paths must have the same length"
        );
        assert_eq!(
            legacy_aggregates.len(),
            legacy_dtypes.len(),
            "top-level aggregates and dtypes must have the same length"
        );
        let path_index = paths
            .iter()
            .cloned()
            .enumerate()
            .map(|(i, p)| (p, i))
            .collect();
        Self {
            aggregates,
            entry_dtypes,
            paths,
            path_index: Arc::new(path_index),
            legacy_aggregates,
            legacy_dtypes,
            legacy_stats_sets: Arc::new(OnceLock::new()),
        }
    }

    /// Creates a new [`FileStatistics`] from the given top-level statistics and data types.
    ///
    /// Without the file dtype the statistics can't be placed in the nested layout, so they are
    /// only reachable through the top-level view ([`Self::stats_sets`], [`Self::get`]), not by
    /// field path. Prefer [`Self::new_with_dtype`] or [`Self::from_entries`].
    ///
    /// # Panics
    ///
    /// Panics if `stats` and `dtypes` have different lengths.
    pub fn new(stats: Arc<[StatsSet]>, dtypes: Arc<[DType]>) -> Self {
        assert_eq!(
            stats.len(),
            dtypes.len(),
            "stats and dtypes must have the same length"
        );
        let legacy_aggregates = stats
            .iter()
            .zip(dtypes.iter())
            .map(|(stats_set, dtype)| aggregates_from_stats_set(stats_set, dtype))
            .collect();
        Self::from_parts(
            Arc::new([]),
            Arc::new([]),
            Arc::new([]),
            legacy_aggregates,
            dtypes,
        )
    }

    /// Creates a new [`FileStatistics`] from the given top-level statistics and file dtype.
    ///
    /// If the [`DType`] of the file is a [`DType::Struct`], then there must be the same number of
    /// statistics as fields in the struct, in field order. Otherwise there must be exactly one.
    /// Nested fields get no statistics; each top-level field's statistics also serve as its entry
    /// in the nested layout.
    ///
    /// # Panics
    ///
    /// Panics if the number of statistics doesn't match the expected number based on the dtype.
    pub fn new_with_dtype(stats: Arc<[StatsSet]>, file_dtype: &DType) -> Self {
        let legacy_dtypes = top_level_dtypes(file_dtype);
        assert_eq!(
            stats.len(),
            legacy_dtypes.len(),
            "stats length must match number of struct fields"
        );
        let legacy_aggregates: Arc<[AggregateStats]> = stats
            .iter()
            .zip(legacy_dtypes.iter())
            .map(|(stats_set, dtype)| aggregates_from_stats_set(stats_set, dtype))
            .collect();

        let (paths, entry_dtypes): (Vec<FieldPath>, Vec<DType>) =
            postorder_stats_layout(file_dtype).into_iter().unzip();
        let aggregates = paths
            .iter()
            .map(|path| {
                top_level_index(path, file_dtype)
                    .map(|index| legacy_aggregates[index].clone())
                    .unwrap_or_else(|| AggregateStats::new(Vec::new()))
            })
            .collect();
        Self::from_parts(
            aggregates,
            entry_dtypes.into(),
            paths.into(),
            legacy_aggregates,
            legacy_dtypes,
        )
    }

    /// Creates a new [`FileStatistics`] from the given aggregates, data types, and field paths.
    ///
    /// The top-level view is made of the entries at depth-1 paths (or at the root path, for a
    /// non-struct file dtype), in order.
    ///
    /// # Panics
    ///
    /// Panics if `aggregates`, `dtypes`, and `paths` have different lengths.
    pub fn from_entries(
        aggregates: Arc<[AggregateStats]>,
        dtypes: Arc<[DType]>,
        paths: Arc<[FieldPath]>,
    ) -> Self {
        assert_eq!(
            aggregates.len(),
            paths.len(),
            "aggregates and paths must have the same length"
        );
        let (legacy_aggregates, legacy_dtypes): (Vec<AggregateStats>, Vec<DType>) = paths
            .iter()
            .enumerate()
            .filter(|(_, path)| path.is_root() || path.parts().len() == 1)
            .map(|(index, _)| (aggregates[index].clone(), dtypes[index].clone()))
            .unzip();
        Self::from_parts(
            aggregates,
            dtypes,
            paths,
            legacy_aggregates.into(),
            legacy_dtypes.into(),
        )
    }

    /// Creates a new [`FileStatistics`] from the given nested aggregates, top-level-only
    /// aggregates, and file dtype.
    ///
    /// `aggregates` must follow the post-order nested-struct layout produced by
    /// [`postorder_stats_layout`] for `file_dtype`. `legacy_aggregates` must have one entry per
    /// top-level struct field (or a single entry for a non-struct root dtype), or be empty to
    /// write no legacy `ArrayStats` for readers that predate nested field statistics.
    ///
    /// # Panics
    ///
    /// Panics if the number of entries doesn't match the expected number based on the dtype.
    pub fn from_postorder(
        aggregates: Arc<[AggregateStats]>,
        legacy_aggregates: Arc<[AggregateStats]>,
        file_dtype: &DType,
    ) -> Self {
        let layout = postorder_stats_layout(file_dtype);
        assert_eq!(
            aggregates.len(),
            layout.len(),
            "aggregates length must match the post-order stats layout for the file dtype"
        );
        let legacy_dtypes = if legacy_aggregates.is_empty() {
            Arc::new([])
        } else {
            top_level_dtypes(file_dtype)
        };

        let (paths, dtypes): (Vec<FieldPath>, Vec<DType>) = layout.into_iter().unzip();
        Self::from_parts(
            aggregates,
            dtypes.into(),
            paths.into(),
            legacy_aggregates,
            legacy_dtypes,
        )
    }

    /// Creates [`FileStatistics`] from a flatbuffers [`fb::FileStatistics<'a>`].
    ///
    /// Aggregates the session doesn't know are skipped: file statistics are optional, so a reader
    /// without an aggregate loses only what that aggregate could tell it.
    pub fn from_flatbuffer<'a>(
        fb: &fb::FileStatistics<'a>,
        file_dtype: &DType,
        session: &VortexSession,
    ) -> VortexResult<Self> {
        let legacy_dtypes = top_level_dtypes(file_dtype);
        let field_stats = fb.field_stats().unwrap_or_default();
        let array_stats: Vec<ArrayStats> = field_stats.iter().collect();

        if let Some(entry_sets) = fb.entry_sets() {
            let aggregate_fns: Vec<Option<AggregateFnRef>> = fb
                .aggregate_specs()
                .iter()
                .flat_map(|specs| specs.iter())
                .map(|spec| aggregate_fn_from_spec(&spec, session))
                .try_collect()?;
            let aggregate_sets: Vec<Vec<u16>> = fb
                .aggregate_sets()
                .iter()
                .flat_map(|sets| sets.iter())
                .map(|set| {
                    set.specs()
                        .iter()
                        .map(|spec| {
                            vortex_ensure!(
                                usize::from(spec) < aggregate_fns.len(),
                                "aggregate spec index {spec} out of range for {} specs",
                                aggregate_fns.len()
                            );
                            Ok(spec)
                        })
                        .collect()
                })
                .try_collect()?;
            let layout = postorder_stats_layout(file_dtype);
            vortex_ensure_eq!(entry_sets.len(), layout.len());

            let mut partials =
                PartialsReader::new(fb.partials().map(|p| p.bytes()).unwrap_or_default());
            let mut aggregates = Vec::with_capacity(layout.len());
            let mut dtypes = Vec::with_capacity(layout.len());
            let mut paths = Vec::with_capacity(layout.len());
            for (set, (path, dtype)) in entry_sets.iter().zip(layout) {
                let set = aggregate_sets.get(usize::from(set)).ok_or_else(|| {
                    vortex_err!(
                        "aggregate set index {set} out of range for {} sets",
                        aggregate_sets.len()
                    )
                })?;
                aggregates.push(aggregate_stats_from_partials(
                    set,
                    &aggregate_fns,
                    &mut partials,
                    &dtype,
                    session,
                )?);
                dtypes.push(dtype);
                paths.push(path);
            }
            partials.finish()?;

            // The top-level view comes from the legacy `field_stats` when the writer emitted
            // them, since that is what readers without nested statistics see; otherwise from the
            // entries at the top-level fields' own paths.
            let legacy_aggregates: Arc<[AggregateStats]> =
                if array_stats.len() == legacy_dtypes.len() {
                    array_stats
                        .iter()
                        .zip(legacy_dtypes.iter())
                        .map(|(array_stat, dtype)| {
                            aggregates_from_array_stats(array_stat, dtype, session)
                        })
                        .try_collect()?
                } else {
                    let entry_index: HashMap<&FieldPath, usize> = paths
                        .iter()
                        .enumerate()
                        .map(|(index, path)| (path, index))
                        .collect();
                    top_level_paths(file_dtype)
                        .iter()
                        .map(|path| {
                            entry_index
                                .get(path)
                                .map(|&index| aggregates[index].clone())
                                .unwrap_or_else(|| AggregateStats::new(Vec::new()))
                        })
                        .collect()
                };

            return Ok(Self::from_parts(
                aggregates.into(),
                dtypes.into(),
                paths.into(),
                legacy_aggregates,
                legacy_dtypes,
            ));
        }

        // Legacy (pre-nested-stats) layout: top-level struct fields only, or a single entry for a
        // non-struct root dtype. Those entries serve as both views.
        vortex_ensure_eq!(array_stats.len(), legacy_dtypes.len());
        let aggregates: Arc<[AggregateStats]> = array_stats
            .iter()
            .zip(legacy_dtypes.iter())
            .map(|(array_stat, dtype)| aggregates_from_array_stats(array_stat, dtype, session))
            .try_collect()?;

        Ok(Self::from_parts(
            Arc::clone(&aggregates),
            Arc::clone(&legacy_dtypes),
            top_level_paths(file_dtype),
            aggregates,
            legacy_dtypes,
        ))
    }

    /// Returns a reference to the aggregates recorded for each entry of the nested layout.
    pub fn aggregates(&self) -> &Arc<[AggregateStats]> {
        &self.aggregates
    }

    /// Returns `true` if there is no statistical information at all, in either the nested or the
    /// top-level view.
    ///
    /// These can disagree: a non-nullable struct field whose entire subtree is unsupported dtypes
    /// (e.g. all-`Variant`) contributes no entries to the nested post-order layout (every leaf is
    /// skipped, and there's no nullable struct along the way to emit an own entry), but still gets
    /// a top-level entry with a real `NullCount`, since that stat is dtype-agnostic.
    pub fn is_empty(&self) -> bool {
        self.aggregates.is_empty() && self.legacy_aggregates.is_empty()
    }

    /// Returns a reference to the data type of each entry of the nested layout.
    pub fn entry_dtypes(&self) -> &Arc<[DType]> {
        &self.entry_dtypes
    }

    /// Returns a reference to the field paths of the nested layout.
    pub fn paths(&self) -> &Arc<[FieldPath]> {
        &self.paths
    }

    /// Returns the aggregates and data type of an entry of the nested layout.
    ///
    /// # Panics
    ///
    /// Panics if `entry_idx` is out of bounds.
    pub fn entry(&self, entry_idx: usize) -> (&AggregateStats, &DType) {
        (&self.aggregates[entry_idx], &self.entry_dtypes[entry_idx])
    }

    /// Returns the aggregates and data type for the field at the given path, if present.
    pub fn get_by_path(&self, path: &FieldPath) -> Option<(&AggregateStats, &DType)> {
        self.path_index
            .get(path)
            .map(|&idx| (&self.aggregates[idx], &self.entry_dtypes[idx]))
    }

    /// Returns the top-level aggregates, one per top-level struct field (or a single entry for a
    /// non-struct file dtype).
    pub fn legacy_aggregates(&self) -> &Arc<[AggregateStats]> {
        &self.legacy_aggregates
    }

    /// Returns the top-level statistics as [`StatsSet`]s, one per top-level struct field (or a
    /// single entry for a non-struct file dtype).
    ///
    /// Nested fields are reachable by path through [`Self::get_by_path`] instead.
    pub fn stats_sets(&self) -> &Arc<[StatsSet]> {
        self.legacy_stats_sets.get_or_init(|| {
            self.legacy_aggregates
                .iter()
                .map(AggregateStats::to_stats_set)
                .collect()
        })
    }

    /// Returns a reference to the data types of the top-level statistics.
    pub fn dtypes(&self) -> &Arc<[DType]> {
        &self.legacy_dtypes
    }

    /// Returns the statistics and data type for a specific top-level field.
    ///
    /// # Panics
    ///
    /// Panics if `field_idx` is out of bounds.
    pub fn get(&self, field_idx: usize) -> (&StatsSet, &DType) {
        (
            &self.stats_sets()[field_idx],
            &self.legacy_dtypes[field_idx],
        )
    }
}

/// The dtypes of the top-level statistics of a file of `file_dtype`: its struct fields' dtypes, or
/// the dtype itself if it is not a struct.
fn top_level_dtypes(file_dtype: &DType) -> Arc<[DType]> {
    match file_dtype.as_struct_fields_opt() {
        Some(struct_fields) => struct_fields.fields().collect(),
        None => Arc::new([file_dtype.clone()]),
    }
}

/// The paths of the top-level statistics of a file of `file_dtype`: its struct fields' names, or
/// the root path if it is not a struct.
fn top_level_paths(file_dtype: &DType) -> Arc<[FieldPath]> {
    match file_dtype.as_struct_fields_opt() {
        Some(struct_fields) => struct_fields
            .names()
            .iter()
            .map(|name| FieldPath::from_name(name.clone()))
            .collect(),
        None => Arc::new([FieldPath::root()]),
    }
}

/// The index of the top-level statistics that `path` addresses, if it is a top-level field of
/// `file_dtype` (or the root of a non-struct one).
fn top_level_index(path: &FieldPath, file_dtype: &DType) -> Option<usize> {
    match file_dtype.as_struct_fields_opt() {
        Some(struct_fields) => match path.parts() {
            [field] => struct_fields.find(field.as_name()?),
            _ => None,
        },
        None => path.is_root().then_some(0),
    }
}

/// The aggregates a legacy [`ArrayStats`] over a field of `dtype` holds, for the stats that have an
/// aggregate function.
fn aggregates_from_array_stats(
    array_stats: &ArrayStats<'_>,
    dtype: &DType,
    session: &VortexSession,
) -> VortexResult<AggregateStats> {
    let stats_set = StatsSet::from_flatbuffer(array_stats, dtype, session)?;
    Ok(aggregates_from_stats_set(&stats_set, dtype))
}

/// The aggregates a [`StatsSet`] over a field of `dtype` holds, for the stats that have an
/// aggregate function.
fn aggregates_from_stats_set(stats_set: &StatsSet, dtype: &DType) -> AggregateStats {
    AggregateStats::new(
        stats_set
            .iter()
            .filter_map(|(stat, value)| {
                let aggregate_fn = stat.aggregate_fn()?;
                let stat_dtype = stat.dtype(dtype)?;
                let value = value
                    .clone()
                    .map(|value| Scalar::try_new(stat_dtype.clone(), Some(value)))
                    .transpose()
                    .ok()?;
                Some(AggregateStat::from_value(aggregate_fn, value))
            })
            .collect(),
    )
}

/// Resolves an [`fb::AggregateSpec`]'s aggregate function, or `None` if the session doesn't
/// know it.
fn aggregate_fn_from_spec(
    spec: &fb::AggregateSpec<'_>,
    session: &VortexSession,
) -> VortexResult<Option<AggregateFnRef>> {
    #[expect(clippy::disallowed_methods, reason = "interning a dynamic id")]
    let id = AggregateFnId::new(spec.id());
    let Some(plugin) = session.aggregate_fns().find_plugin(&id) else {
        return Ok(None);
    };
    let options = spec
        .options()
        .map(|options| options.bytes())
        .unwrap_or_default();
    let aggregate_fn = plugin.deserialize(options, session)?;
    if aggregate_fn.id() != id {
        vortex_bail!(
            "Aggregate function ID mismatch: expected {}, got {}",
            id,
            aggregate_fn.id()
        );
    }
    Ok(Some(aggregate_fn))
}

/// Reads one entry's partial states, one per spec in `set`, from `partials`.
///
/// `set`'s spec indices must already be checked against `aggregate_fns`.
fn aggregate_stats_from_partials(
    set: &[u16],
    aggregate_fns: &[Option<AggregateFnRef>],
    partials: &mut PartialsReader<'_>,
    dtype: &DType,
    session: &VortexSession,
) -> VortexResult<AggregateStats> {
    let mut aggregates = Vec::with_capacity(set.len());
    for &spec in set {
        // Read the partial even for an unknown aggregate, to stay in step with the stream.
        let bytes = partials.next_partial()?;
        let Some(aggregate_fn) = &aggregate_fns[usize::from(spec)] else {
            continue;
        };
        let partial_dtype = aggregate_fn.state_dtype(dtype).ok_or_else(|| {
            vortex_err!("aggregate {aggregate_fn} does not support field dtype {dtype}")
        })?;
        let partial = ScalarValue::from_proto_bytes(bytes, &partial_dtype, session)?;
        let partial = Scalar::try_new(partial_dtype, partial)?;
        aggregates.push(AggregateStat::try_from_partial(
            aggregate_fn.clone(),
            partial,
            dtype,
        )?);
    }
    Ok(AggregateStats::new(aggregates))
}

/// The maximum length of an unsigned LEB128 encoding of a `u64`.
const MAX_VARINT_LEN: usize = 10;

/// Appends `value` to `out` as an unsigned LEB128 varint.
fn write_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

/// Reads the length-prefixed partial states of `FileStatistics.partials` in order.
struct PartialsReader<'a> {
    remaining: &'a [u8],
}

impl<'a> PartialsReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { remaining: bytes }
    }

    /// Returns the bytes of the next partial state.
    fn next_partial(&mut self) -> VortexResult<&'a [u8]> {
        let len = self.next_varint()?;
        let len = usize::try_from(len)
            .ok()
            .filter(|&len| len <= self.remaining.len())
            .ok_or_else(|| {
                vortex_err!(
                    "file statistics partial of {len} bytes exceeds the {} remaining bytes",
                    self.remaining.len()
                )
            })?;
        let (partial, rest) = self.remaining.split_at(len);
        self.remaining = rest;
        Ok(partial)
    }

    fn next_varint(&mut self) -> VortexResult<u64> {
        let mut value = 0u64;
        for (i, &byte) in self.remaining.iter().take(MAX_VARINT_LEN).enumerate() {
            let bits = u64::from(byte & 0x7f);
            // The tenth byte holds only the top bit of a `u64`.
            vortex_ensure!(
                i < MAX_VARINT_LEN - 1 || bits <= 1,
                "file statistics partial length overflows u64"
            );
            value |= bits << (7 * i);
            if byte & 0x80 == 0 {
                self.remaining = &self.remaining[i + 1..];
                return Ok(value);
            }
        }
        vortex_bail!("truncated or overlong file statistics partial length")
    }

    /// Fails unless every partial state has been read.
    fn finish(self) -> VortexResult<()> {
        vortex_ensure!(
            self.remaining.is_empty(),
            "{} unread bytes after the last file statistics partial",
            self.remaining.len()
        );
        Ok(())
    }
}

/// Assigns each distinct value a `u16` index in order of first appearance.
struct Dictionary<K> {
    values: Vec<K>,
    indices: HashMap<K, u16>,
    kind: &'static str,
}

impl<K: Clone + Eq + Hash> Dictionary<K> {
    fn new(kind: &'static str) -> Self {
        Self {
            values: Vec::new(),
            indices: HashMap::default(),
            kind,
        }
    }

    fn intern(&mut self, value: K) -> VortexResult<u16> {
        if let Some(&index) = self.indices.get(&value) {
            return Ok(index);
        }
        let index = u16::try_from(self.values.len()).map_err(|_| {
            vortex_err!("file statistics exceed {} distinct {}", u16::MAX, self.kind)
        })?;
        self.values.push(value.clone());
        self.indices.insert(value, index);
        Ok(index)
    }
}

/// Iterates the top-level statistics and their dtypes; see [`FileStatistics::stats_sets`].
impl<'a> IntoIterator for &'a FileStatistics {
    type Item = (&'a StatsSet, &'a DType);
    type IntoIter = std::iter::Zip<std::slice::Iter<'a, StatsSet>, std::slice::Iter<'a, DType>>;

    fn into_iter(self) -> Self::IntoIter {
        self.stats_sets().iter().zip(self.legacy_dtypes.iter())
    }
}

impl FlatBufferRoot for FileStatistics {}

impl WriteFlatBuffer for FileStatistics {
    type Target<'a> = fb::FileStatistics<'a>;

    fn write_flatbuffer<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> VortexResult<WIPOffset<Self::Target<'fb>>> {
        // The legacy `ArrayStats` format has a fixed slot per stat rather than open-ended
        // aggregates.
        let field_stats = self
            .legacy_aggregates
            .iter()
            .map(|aggregates| aggregates.to_stats_set().write_flatbuffer(fbb))
            .collect::<VortexResult<Vec<_>>>()?;
        let field_stats = fbb.create_vector(field_stats.as_slice());

        // Aggregate functions and the lists of them recorded per entry are dictionary-encoded, so
        // each distinct function and list is written once however many entries use it.
        let mut specs = Dictionary::<&AggregateFnRef>::new("aggregate functions");
        let mut sets = Dictionary::<Vec<u16>>::new("aggregate sets");
        let mut entry_sets = Vec::with_capacity(self.aggregates.len());
        let mut partials = Vec::new();
        for field_aggregates in self.aggregates.iter() {
            let mut set = Vec::new();
            // Values without a partial state (e.g. from legacy statistics) can't be written.
            for stat in field_aggregates.iter() {
                let Some(partial) = stat.partial() else {
                    continue;
                };
                set.push(specs.intern(stat.aggregate_fn())?);
                let partial = ScalarValue::to_proto_bytes::<Vec<u8>>(partial.value());
                write_varint(&mut partials, partial.len() as u64);
                partials.extend_from_slice(&partial);
            }
            entry_sets.push(sets.intern(set)?);
        }

        let aggregate_specs = specs
            .values
            .into_iter()
            .map(|aggregate_fn| {
                let options = aggregate_fn.options().serialize()?.ok_or_else(|| {
                    vortex_err!(
                        "Aggregate function '{}' is not serializable",
                        aggregate_fn.id()
                    )
                })?;
                let id = fbb.create_string(aggregate_fn.id().as_ref());
                let options = (!options.is_empty()).then(|| fbb.create_vector(options.as_slice()));
                Ok(fb::AggregateSpec::create(
                    fbb,
                    &fb::AggregateSpecArgs {
                        id: Some(id),
                        options,
                    },
                ))
            })
            .collect::<VortexResult<Vec<_>>>()?;
        let aggregate_specs = fbb.create_vector(aggregate_specs.as_slice());
        let aggregate_sets = sets
            .values
            .iter()
            .map(|set| {
                let specs = fbb.create_vector(set.as_slice());
                fb::AggregateSet::create(fbb, &fb::AggregateSetArgs { specs: Some(specs) })
            })
            .collect::<Vec<_>>();
        let aggregate_sets = fbb.create_vector(aggregate_sets.as_slice());
        let entry_sets = fbb.create_vector(entry_sets.as_slice());
        let partials = fbb.create_vector(partials.as_slice());

        // Statistics built from values rather than accumulated, such as from top-level
        // `StatsSet`s or a file without nested statistics, have no partial states to write.
        // Leaving `entry_sets` absent then sends readers to `field_stats`, which holds every
        // value, instead of to nested entries that would hold none of them.
        let has_partials = self
            .aggregates
            .iter()
            .flat_map(AggregateStats::iter)
            .any(|stat| stat.partial().is_some());
        Ok(fb::FileStatistics::create(
            fbb,
            &fb::FileStatisticsArgs {
                field_stats: Some(field_stats),
                aggregate_specs: has_partials.then_some(aggregate_specs),
                aggregate_sets: has_partials.then_some(aggregate_sets),
                entry_sets: has_partials.then_some(entry_sets),
                partials: has_partials.then_some(partials),
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use flatbuffers::FlatBufferBuilder;
    use rstest::rstest;
    use vortex_array::IntoArray;
    use vortex_array::VortexSessionExecute;
    use vortex_array::aggregate_fn::AggregateFnVTableExt;
    use vortex_array::aggregate_fn::EmptyOptions;
    use vortex_array::aggregate_fn::NumericalAggregateOpts;
    use vortex_array::aggregate_fn::fns::bounded_max::BoundedMax;
    use vortex_array::aggregate_fn::fns::bounded_max::BoundedMaxOptions;
    use vortex_array::aggregate_fn::fns::max::Max;
    use vortex_array::aggregate_fn::fns::min::Min;
    use vortex_array::aggregate_fn::fns::null_count::NullCount;
    use vortex_array::array_session;
    use vortex_array::arrays::VarBinViewArray;
    use vortex_array::dtype::FieldPath;
    use vortex_array::dtype::Nullability;
    use vortex_array::dtype::PType;
    use vortex_array::expr::stats::Precision;
    use vortex_array::expr::stats::Stat;
    use vortex_array::flatbuffers::WriteFlatBufferExt;
    use vortex_array::scalar::ScalarValue;
    use vortex_error::VortexExpect;

    use super::*;

    fn i32_dtype() -> DType {
        DType::Primitive(PType::I32, Nullability::NonNullable)
    }

    /// An aggregate over a field of `dtype` whose partial state is `value` itself, as for `Min`
    /// and `NullCount`.
    fn stat(aggregate_fn: AggregateFnRef, value: Scalar, dtype: &DType) -> AggregateStat {
        let partial_dtype = aggregate_fn
            .state_dtype(dtype)
            .vortex_expect("aggregate supports dtype");
        let partial = value.cast(&partial_dtype).vortex_expect("castable partial");
        AggregateStat::try_from_partial(aggregate_fn, partial, dtype).vortex_expect("valid partial")
    }

    fn min_fn() -> AggregateFnRef {
        Min.bind(NumericalAggregateOpts::skip_nans())
    }

    /// The value `aggregates` resolves for `aggregate_fn`.
    fn value(aggregates: &AggregateStats, aggregate_fn: &AggregateFnRef) -> Precision<ScalarValue> {
        aggregates.get(aggregate_fn).and_then(Scalar::into_value)
    }

    fn min(value: i32) -> AggregateStat {
        stat(min_fn(), Scalar::from(value), &i32_dtype())
    }

    fn null_count(value: u64, dtype: &DType) -> AggregateStat {
        stat(NullCount.bind(EmptyOptions), Scalar::from(value), dtype)
    }

    fn read_back(file_stats: &FileStatistics, file_dtype: &DType) -> VortexResult<FileStatistics> {
        let bytes = file_stats.write_flatbuffer_bytes()?;
        let fb = flatbuffers::root::<fb::FileStatistics>(bytes.as_ref())
            .vortex_expect("valid flatbuffer");
        FileStatistics::from_flatbuffer(&fb, file_dtype, &array_session())
    }

    #[test]
    fn nested_round_trip_resolves_by_path() -> VortexResult<()> {
        let inner = DType::struct_([("b", i32_dtype())], Nullability::Nullable);
        let file_dtype = DType::struct_([("a", inner.clone())], Nullability::NonNullable);

        // Layout: [a.b, a] (a's own null-count entry trails its child).
        let file_stats = FileStatistics::from_postorder(
            Arc::from([
                AggregateStats::new(vec![min(1)]),
                AggregateStats::new(vec![null_count(1, &inner)]),
            ]),
            Arc::from([AggregateStats::default()]),
            &file_dtype,
        );

        let read_back = read_back(&file_stats, &file_dtype)?;

        let (b, _) = read_back
            .get_by_path(&FieldPath::from_name("a").push("b"))
            .expect("a.b stats");
        assert_eq!(
            value(b, &min_fn()),
            Precision::exact(ScalarValue::from(1i32))
        );

        let (a, _) = read_back
            .get_by_path(&FieldPath::from_name("a"))
            .expect("a's own null-count stats");
        assert_eq!(
            value(a, &NullCount.bind(EmptyOptions)),
            Precision::exact(ScalarValue::from(1u64))
        );

        assert!(read_back.get_by_path(&FieldPath::root()).is_none());

        Ok(())
    }

    #[test]
    fn top_level_view_round_trips_for_nested_dtype() -> VortexResult<()> {
        let inner = DType::struct_([("b", i32_dtype())], Nullability::Nullable);
        let file_dtype = DType::struct_(
            [("a", inner.clone()), ("c", i32_dtype())],
            Nullability::NonNullable,
        );

        // Layout: [a.b, a, c]; the top-level view has one entry per top-level field.
        let file_stats = FileStatistics::from_postorder(
            Arc::from([
                AggregateStats::new(vec![min(1)]),
                AggregateStats::new(vec![null_count(1, &inner)]),
                AggregateStats::new(vec![min(5)]),
            ]),
            Arc::from([
                AggregateStats::new(vec![null_count(1, &inner)]),
                AggregateStats::new(vec![min(5)]),
            ]),
            &file_dtype,
        );

        for stats in [&file_stats, &read_back(&file_stats, &file_dtype)?] {
            assert_eq!(stats.stats_sets().len(), 2);
            assert_eq!(stats.dtypes().as_ref(), &[inner.clone(), i32_dtype()]);
            assert_eq!(stats.into_iter().count(), 2);
            let (a, _) = stats.get(0);
            assert_eq!(
                a.get(Stat::NullCount),
                Precision::exact(ScalarValue::from(1u64))
            );
            let (c, _) = stats.get(1);
            assert_eq!(c.get(Stat::Min), Precision::exact(ScalarValue::from(5i32)));
            assert_eq!(stats.aggregates().len(), 3);
        }
        Ok(())
    }

    #[test]
    fn top_level_view_is_derived_when_legacy_stats_are_excluded() -> VortexResult<()> {
        let inner = DType::struct_([("b", i32_dtype())], Nullability::Nullable);
        let file_dtype = DType::struct_(
            [("a", inner.clone()), ("c", i32_dtype())],
            Nullability::NonNullable,
        );
        let file_stats = FileStatistics::from_postorder(
            Arc::from([
                AggregateStats::new(vec![min(1)]),
                AggregateStats::new(vec![null_count(1, &inner)]),
                AggregateStats::new(vec![min(5)]),
            ]),
            Arc::from([]),
            &file_dtype,
        );
        assert!(file_stats.stats_sets().is_empty());

        // Without `field_stats` in the file, the top-level fields' own entries stand in.
        let read_back = read_back(&file_stats, &file_dtype)?;
        assert_eq!(read_back.stats_sets().len(), 2);
        let (a, _) = read_back.get(0);
        assert_eq!(
            a.get(Stat::NullCount),
            Precision::exact(ScalarValue::from(1u64))
        );
        let (c, _) = read_back.get(1);
        assert_eq!(c.get(Stat::Min), Precision::exact(ScalarValue::from(5i32)));
        Ok(())
    }

    #[test]
    fn stats_set_constructors_round_trip() -> VortexResult<()> {
        let file_dtype = DType::struct_([("c", i32_dtype())], Nullability::NonNullable);
        let mut stats_set = StatsSet::default();
        stats_set.set(Stat::Min, Precision::exact(ScalarValue::from(5i32)));

        // With the file dtype, the top-level stats also fill the nested layout. They have no
        // partial states, so the file carries them as `field_stats`, which the reader turns back
        // into top-level entries.
        let file_stats =
            FileStatistics::new_with_dtype(Arc::from([stats_set.clone()]), &file_dtype);
        let bytes = file_stats.write_flatbuffer_bytes()?;
        let fb = flatbuffers::root::<fb::FileStatistics>(bytes.as_ref())
            .vortex_expect("valid flatbuffer");
        assert!(fb.entry_sets().is_none());
        for stats in [&file_stats, &read_back(&file_stats, &file_dtype)?] {
            let (c, _) = stats
                .get_by_path(&FieldPath::from_name("c"))
                .expect("c stats");
            assert_eq!(
                value(c, &min_fn()),
                Precision::exact(ScalarValue::from(5i32))
            );
            assert_eq!(
                stats.get(0).0.get(Stat::Min),
                Precision::exact(ScalarValue::from(5i32))
            );
        }

        // Without it, only the top-level view is populated, and the file is written in the
        // layout readers without nested statistics use.
        let file_stats = FileStatistics::new(Arc::from([stats_set]), Arc::from([i32_dtype()]));
        assert!(file_stats.aggregates().is_empty());
        assert!(!file_stats.is_empty());
        assert!(file_stats.get_by_path(&FieldPath::from_name("c")).is_none());
        let bytes = file_stats.write_flatbuffer_bytes()?;
        let fb = flatbuffers::root::<fb::FileStatistics>(bytes.as_ref())
            .vortex_expect("valid flatbuffer");
        assert!(fb.entry_sets().is_none());
        let read_back = FileStatistics::from_flatbuffer(&fb, &file_dtype, &array_session())?;
        let (c, _) = read_back
            .get_by_path(&FieldPath::from_name("c"))
            .expect("c stats");
        assert_eq!(
            value(c, &min_fn()),
            Precision::exact(ScalarValue::from(5i32))
        );
        Ok(())
    }

    #[test]
    fn specs_and_sets_are_written_once() -> VortexResult<()> {
        let file_dtype = DType::struct_(
            [("a", i32_dtype()), ("b", i32_dtype()), ("c", i32_dtype())],
            Nullability::NonNullable,
        );
        let min_including_nans = Min.bind(NumericalAggregateOpts::include_nans());
        let file_stats = FileStatistics::from_postorder(
            Arc::from([
                AggregateStats::new(vec![min(1), null_count(0, &i32_dtype())]),
                AggregateStats::new(vec![min(2), null_count(0, &i32_dtype())]),
                // Same ID as `min`, different options: a spec of its own.
                AggregateStats::new(vec![stat(
                    min_including_nans.clone(),
                    Scalar::from(3i32),
                    &i32_dtype(),
                )]),
            ]),
            Arc::from([
                AggregateStats::default(),
                AggregateStats::default(),
                AggregateStats::default(),
            ]),
            &file_dtype,
        );

        let bytes = file_stats.write_flatbuffer_bytes()?;
        let fb = flatbuffers::root::<fb::FileStatistics>(bytes.as_ref())
            .vortex_expect("valid flatbuffer");
        let specs = fb
            .aggregate_specs()
            .vortex_expect("aggregate specs")
            .iter()
            .map(|spec| {
                (
                    spec.id().to_string(),
                    spec.options()
                        .map(|options| options.bytes().to_vec())
                        .unwrap_or_default(),
                )
            })
            .collect::<Vec<_>>();
        let options = |aggregate_fn: &AggregateFnRef| -> VortexResult<Vec<u8>> {
            Ok(aggregate_fn.options().serialize()?.unwrap_or_default())
        };
        assert_eq!(
            specs,
            [
                ("vortex.min".to_string(), options(&min_fn())?),
                (
                    "vortex.null_count".to_string(),
                    options(&NullCount.bind(EmptyOptions))?
                ),
                ("vortex.min".to_string(), options(&min_including_nans)?),
            ]
        );

        // "a" and "b" record the same aggregates, so they share a set.
        let sets = fb
            .aggregate_sets()
            .vortex_expect("aggregate sets")
            .iter()
            .map(|set| set.specs().iter().collect::<Vec<_>>())
            .collect::<Vec<_>>();
        assert_eq!(sets, [vec![0, 1], vec![2]]);
        let entry_sets = fb
            .entry_sets()
            .vortex_expect("entry sets")
            .iter()
            .collect::<Vec<_>>();
        assert_eq!(entry_sets, [0, 0, 1]);

        let read_back = read_back(&file_stats, &file_dtype)?;
        let (b, _) = read_back
            .get_by_path(&FieldPath::from_name("b"))
            .expect("b stats");
        assert_eq!(
            value(b, &min_fn()),
            Precision::exact(ScalarValue::from(2i32))
        );
        let (c, _) = read_back
            .get_by_path(&FieldPath::from_name("c"))
            .expect("c stats");
        assert_eq!(
            c.iter().next().map(AggregateStat::aggregate_fn),
            Some(&min_including_nans)
        );
        Ok(())
    }

    #[test]
    fn bounded_aggregate_round_trips_as_an_approximate_max() -> VortexResult<()> {
        let file_dtype = DType::struct_(
            [("s", DType::Utf8(Nullability::NonNullable))],
            Nullability::NonNullable,
        );
        let field_dtype = DType::Utf8(Nullability::NonNullable);
        let bounded_max = BoundedMax.bind(BoundedMaxOptions {
            max_bytes: NonZeroUsize::new(4).vortex_expect("non-zero"),
        });
        let mut accumulator = bounded_max.accumulator(&field_dtype)?;
        accumulator.accumulate(
            &VarBinViewArray::from_iter_str(["a value past the bound"]).into_array(),
            &mut array_session().create_execution_ctx(),
        )?;
        let partial = accumulator.partial_scalar()?;
        let file_stats = FileStatistics::from_postorder(
            Arc::from([AggregateStats::new(vec![AggregateStat::try_from_partial(
                bounded_max.clone(),
                partial,
                &field_dtype,
            )?])]),
            Arc::from([AggregateStats::default()]),
            &file_dtype,
        );

        let read_back = read_back(&file_stats, &file_dtype)?;
        let path = FieldPath::from_name("s");
        let (aggregates, _) = read_back.get_by_path(&path).expect("s aggregates");
        assert!(
            aggregates
                .iter()
                .all(|stat| stat.aggregate_fn() == &bounded_max)
        );
        assert_eq!(
            value(aggregates, &Max.bind(NumericalAggregateOpts::skip_nans())),
            Precision::inexact(ScalarValue::from("a vb"))
        );
        Ok(())
    }

    /// Hand-assembles the aggregate-based sections of a `FileStatistics` flatbuffer, so tests can
    /// write footers this writer never would.
    fn raw_file_stats(
        specs: &[(&str, &[u8])],
        sets: &[&[u16]],
        entry_sets: &[u16],
        partials: &[u8],
    ) -> Vec<u8> {
        let mut fbb = FlatBufferBuilder::new();
        let specs = specs
            .iter()
            .map(|(id, options)| {
                let id = fbb.create_string(id);
                let options = fbb.create_vector(options);
                fb::AggregateSpec::create(
                    &mut fbb,
                    &fb::AggregateSpecArgs {
                        id: Some(id),
                        options: Some(options),
                    },
                )
            })
            .collect::<Vec<_>>();
        let specs = fbb.create_vector(&specs);
        let sets = sets
            .iter()
            .map(|set| {
                let specs = fbb.create_vector(set);
                fb::AggregateSet::create(&mut fbb, &fb::AggregateSetArgs { specs: Some(specs) })
            })
            .collect::<Vec<_>>();
        let sets = fbb.create_vector(&sets);
        let entry_sets = fbb.create_vector(entry_sets);
        let partials = fbb.create_vector(partials);
        let root = fb::FileStatistics::create(
            &mut fbb,
            &fb::FileStatisticsArgs {
                field_stats: None,
                aggregate_specs: Some(specs),
                aggregate_sets: Some(sets),
                entry_sets: Some(entry_sets),
                partials: Some(partials),
            },
        );
        fbb.finish_minimal(root);
        fbb.finished_data().to_vec()
    }

    fn read_raw(bytes: &[u8], file_dtype: &DType) -> VortexResult<FileStatistics> {
        let fb = flatbuffers::root::<fb::FileStatistics>(bytes).vortex_expect("valid flatbuffer");
        FileStatistics::from_flatbuffer(&fb, file_dtype, &array_session())
    }

    /// The options of `min` and its partial state for `value`, length-prefixed as in `partials`.
    fn min_spec_and_partial(value: i32) -> VortexResult<(Vec<u8>, Vec<u8>)> {
        let known = min(value);
        let options = known
            .aggregate_fn()
            .options()
            .serialize()?
            .vortex_expect("serializable options");
        let partial = ScalarValue::to_proto_bytes::<Vec<u8>>(
            known.partial().vortex_expect("has a partial").value(),
        );
        let mut prefixed = Vec::new();
        write_varint(&mut prefixed, partial.len() as u64);
        prefixed.extend_from_slice(&partial);
        Ok((options, prefixed))
    }

    #[test]
    fn unknown_aggregates_are_skipped_by_length() -> VortexResult<()> {
        let file_dtype = DType::struct_([("col", i32_dtype())], Nullability::NonNullable);
        let (options, known_partial) = min_spec_and_partial(7)?;

        // Spec 0 is an aggregate no session knows, recorded before the known `min`. Its partial is
        // bytes this reader can't interpret, which it has to skip to reach `min`'s.
        let mut partials = vec![3, 0xde, 0xad, 0xbe];
        partials.extend_from_slice(&known_partial);
        let bytes = raw_file_stats(
            &[("test.unknown", &[]), ("vortex.min", &options)],
            &[&[0, 1]],
            &[0],
            &partials,
        );

        let read_back = read_raw(&bytes, &file_dtype)?;
        let (aggregates, _) = read_back
            .get_by_path(&FieldPath::from_name("col"))
            .expect("col aggregates");
        assert_eq!(aggregates.iter().count(), 1);
        assert_eq!(
            value(aggregates, &min_fn()),
            Precision::exact(ScalarValue::from(7i32))
        );
        Ok(())
    }

    #[rstest]
    #[case::set_index_out_of_range(vec![vec![0]], vec![1], None, "aggregate set index 1 out of range")]
    #[case::spec_index_out_of_range(vec![vec![1]], vec![0], None, "aggregate spec index 1 out of range")]
    #[case::wrong_entry_count(vec![vec![0]], vec![0, 0], None, "")]
    #[case::partial_missing(vec![vec![0]], vec![0], Some(vec![]), "truncated or overlong")]
    #[case::truncated_varint(vec![vec![0]], vec![0], Some(vec![0x80]), "truncated or overlong")]
    #[case::overlong_varint(vec![vec![0]], vec![0], Some(vec![0x80; 11]), "truncated or overlong")]
    #[case::varint_overflows(
        vec![vec![0]],
        vec![0],
        Some(vec![0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02]),
        "overflows u64"
    )]
    #[case::length_past_end(vec![vec![0]], vec![0], Some(vec![5, 0]), "exceeds the 1 remaining bytes")]
    #[case::trailing_bytes(vec![vec![]], vec![0], Some(vec![0]), "1 unread bytes")]
    fn malformed_aggregate_statistics_are_rejected(
        #[case] sets: Vec<Vec<u16>>,
        #[case] entry_sets: Vec<u16>,
        #[case] partials: Option<Vec<u8>>,
        #[case] message: &str,
    ) -> VortexResult<()> {
        let file_dtype = DType::struct_([("col", i32_dtype())], Nullability::NonNullable);
        let (options, valid_partial) = min_spec_and_partial(7)?;
        let sets = sets.iter().map(Vec::as_slice).collect::<Vec<_>>();
        let bytes = raw_file_stats(
            &[("vortex.min", &options)],
            &sets,
            &entry_sets,
            &partials.unwrap_or(valid_partial),
        );

        let error = read_raw(&bytes, &file_dtype)
            .err()
            .vortex_expect("malformed statistics must be rejected");
        assert!(
            error.to_string().contains(message),
            "unexpected error: {error}"
        );
        Ok(())
    }

    #[rstest]
    #[case(0)]
    #[case(1)]
    #[case(127)]
    #[case(128)]
    #[case(300)]
    #[case(u64::from(u32::MAX))]
    #[case(u64::MAX)]
    fn varints_round_trip(#[case] value: u64) -> VortexResult<()> {
        let mut bytes = Vec::new();
        write_varint(&mut bytes, value);
        assert!(bytes.len() <= MAX_VARINT_LEN);
        let mut reader = PartialsReader::new(&bytes);
        assert_eq!(reader.next_varint()?, value);
        reader.finish()
    }

    #[test]
    fn get_by_path_resolves_a_later_sibling_past_a_nullable_struct_field() {
        // Regression test: a nullable struct field ("s") contributes both an `s.b` entry and a
        // trailing entry for its own null count to the nested (post-order) layout, so that layout
        // has one more entry (3) than there are top-level fields (2: "s", "c"). Resolving the
        // later top-level field "c" by path must not be thrown off by "s"'s extra own-entry.
        let s_dtype = DType::struct_([("b", i32_dtype())], Nullability::Nullable);
        let file_dtype = DType::struct_(
            [("s", s_dtype.clone()), ("c", i32_dtype())],
            Nullability::NonNullable,
        );

        // Post-order layout: [s.b, s, c].
        let file_stats = FileStatistics::from_postorder(
            Arc::from([
                AggregateStats::default(),
                AggregateStats::new(vec![null_count(0, &s_dtype)]),
                AggregateStats::new(vec![min(42)]),
            ]),
            Arc::from([AggregateStats::default(), AggregateStats::default()]),
            &file_dtype,
        );

        let (c, _) = file_stats
            .get_by_path(&FieldPath::from_name("c"))
            .expect("c stats");
        assert_eq!(
            value(c, &min_fn()),
            Precision::exact(ScalarValue::from(42i32))
        );
    }

    #[test]
    fn legacy_non_nested_footer_still_parses() -> VortexResult<()> {
        // Simulates a footer written before aggregate-based stats existed: `entry_sets` is
        // absent, and `field_stats` holds one entry per top-level
        // struct field.
        let session = array_session();
        let file_dtype = DType::struct_([("col", i32_dtype())], Nullability::NonNullable);

        let mut stats = StatsSet::default();
        stats.set(Stat::Min, Precision::exact(ScalarValue::from(7i32)));

        let mut fbb = FlatBufferBuilder::new();
        let array_stats = stats.write_flatbuffer(&mut fbb)?;
        let field_stats = fbb.create_vector(&[array_stats]);
        let root = fb::FileStatistics::create(
            &mut fbb,
            &fb::FileStatisticsArgs {
                field_stats: Some(field_stats),
                ..Default::default()
            },
        );
        fbb.finish_minimal(root);
        let bytes = fbb.finished_data().to_vec();

        let fb = flatbuffers::root::<fb::FileStatistics>(&bytes).vortex_expect("valid flatbuffer");
        assert!(fb.entry_sets().is_none());

        let read_back = FileStatistics::from_flatbuffer(&fb, &file_dtype, &session)?;
        let path = FieldPath::from_name("col");
        let (col, _) = read_back.get_by_path(&path).expect("col aggregates");
        assert_eq!(
            value(col, &min_fn()),
            Precision::exact(ScalarValue::from(7i32))
        );

        Ok(())
    }

    #[test]
    fn writer_still_emits_legacy_field_stats_matching_top_level_field_count() -> VortexResult<()> {
        // Regression test: an old reader (pre-nested-stats) only ever looks at `field_stats` and
        // requires its length to match the number of top-level struct fields. A nested/nullable
        // struct schema's post-order layout has a different length, so `field_stats` must keep
        // carrying the legacy top-level-only shape, not the nested one.
        let inner = DType::struct_([("b", i32_dtype())], Nullability::Nullable);
        let file_dtype =
            DType::struct_([("a", inner), ("c", i32_dtype())], Nullability::NonNullable);
        let struct_fields = file_dtype
            .as_struct_fields_opt()
            .vortex_expect("file_dtype is a struct");

        // Nested (post-order) layout: [a.b, a, c] - 3 entries, differs from the 2 top-level fields.
        let nested: Arc<[AggregateStats]> = Arc::from([
            AggregateStats::default(),
            AggregateStats::default(),
            AggregateStats::default(),
        ]);
        // Legacy layout: one entry per top-level field ("a", "c").
        let legacy: Arc<[AggregateStats]> =
            Arc::from([AggregateStats::default(), AggregateStats::default()]);

        let file_stats = FileStatistics::from_postorder(nested, legacy, &file_dtype);
        let bytes = file_stats.write_flatbuffer_bytes()?;
        let fb = flatbuffers::root::<fb::FileStatistics>(bytes.as_ref())
            .vortex_expect("valid flatbuffer");

        let field_stats_len = fb.field_stats().map_or(0, |field_stats| field_stats.len());
        assert_eq!(field_stats_len, struct_fields.nfields());

        Ok(())
    }

    #[test]
    fn is_empty_considers_legacy_stats_too() {
        // Regression test: the nested (post-order) layout can be empty while the legacy layout
        // still carries real content, e.g. a non-nullable struct field whose entire subtree is
        // unsupported dtypes contributes no nested entries, but still gets a legacy NullCount
        // (dtype-agnostic). `is_empty` must not report "nothing to write" in that case, or the
        // caller (the footer serializer) would silently drop the legacy stats too.
        // A non-nullable struct field whose only child is an unsupported dtype (`Variant`)
        // contributes zero entries to the nested post-order layout: the leaf is skipped, and the
        // struct itself is non-nullable so it gets no own entry either.
        let file_dtype = DType::struct_(
            [("a", DType::Variant(Nullability::NonNullable))],
            Nullability::NonNullable,
        );
        assert_eq!(postorder_stats_layout(&file_dtype), Vec::new());
        let legacy = AggregateStats::new(vec![null_count(
            0,
            &DType::Variant(Nullability::NonNullable),
        )]);

        let file_stats =
            FileStatistics::from_postorder(Arc::from([]), Arc::from([legacy]), &file_dtype);

        assert!(!file_stats.is_empty());
    }
}
