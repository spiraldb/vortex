// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! This module defines the file statistics component of the Vortex file footer.
//!
//! File statistics provide metadata about the data in the file, such as min/max values,
//! null counts, and other statistical information that can be used for query optimization
//! and data exploration.
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;
use std::sync::OnceLock;

use flatbuffers::FlatBufferBuilder;
use flatbuffers::WIPOffset;
use vortex_array::dtype::DType;
use vortex_array::flatbuffers::FlatBuffer;
use vortex_array::flatbuffers::FlatBufferRoot;
use vortex_array::flatbuffers::WriteFlatBuffer;
use vortex_array::flatbuffers::root_verified;
use vortex_array::stats::StatsSet;
use vortex_error::VortexError;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_ensure_eq;
use vortex_session::VortexSession;

use crate::flatbuffers::footer as fb;

/// Contains statistical information about the data in a Vortex file.
///
/// This struct wraps an array of `StatsSet` objects, each containing statistics
/// for a field or column in the file. These statistics can be used for query
/// optimization and data exploration.
#[derive(Clone)]
pub struct FileStatistics(Arc<FileStatisticsInner>);

struct FileStatisticsInner {
    /// An array of statistics sets, one for each field or column in the file.
    stats: OnceLock<Arc<[StatsSet]>>,
    /// An array of `DType`s, one for each field or column in the file.
    dtypes: OnceLock<Arc<[DType]>>,
    /// The serialized statistics and the file dtype, for statistics read from a file and parsed
    /// on first use.
    source: Option<(FlatBuffer, DType, VortexSession)>,
}

impl Debug for FileStatistics {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileStatistics")
            .field("stats", &self.0.stats.get())
            .field("dtypes", &self.0.dtypes.get())
            .finish()
    }
}

impl FileStatistics {
    fn parsed(stats: Arc<[StatsSet]>, dtypes: Arc<[DType]>) -> Self {
        Self(Arc::new(FileStatisticsInner {
            stats: OnceLock::from(stats),
            dtypes: OnceLock::from(dtypes),
            source: None,
        }))
    }

    /// Creates a new [`FileStatistics`] from the given statistics and data types.
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

        Self::parsed(stats, dtypes)
    }

    /// Creates a new [`FileStatistics`] from the given statistics and file dtype.
    ///
    /// If the [`DType`] of the file is a [`DType::Struct`], then there must be the same number of
    /// stats as struct fields. Otherwise, there must be only 1 statistic.
    ///
    /// # Panics
    ///
    /// Panics if the number of stats doesn't match the expected number based on the dtype.
    pub fn new_with_dtype(stats: Arc<[StatsSet]>, file_dtype: &DType) -> Self {
        if let DType::Struct(struct_fields, _) = file_dtype {
            assert_eq!(
                stats.len(),
                struct_fields.nfields(),
                "stats length must match number of struct fields"
            );
        } else {
            assert_eq!(
                stats.len(),
                1,
                "non-struct dtype must have exactly 1 statistic"
            );
        }
        Self::parsed(stats, field_dtypes(file_dtype))
    }

    /// Creates [`FileStatistics`] from a flatbuffers [`fb::FileStatistics<'a>`].
    ///
    /// If the [`DType`] of the file is a [`DType::Struct`], then there must be the same number of
    /// file stats in the flatbuffer. Otherwise, there must be only 1 statistic.
    pub fn from_flatbuffer<'a>(
        fb: &fb::FileStatistics<'a>,
        file_dtype: &DType,
        session: &VortexSession,
    ) -> VortexResult<Self> {
        let dtypes = field_dtypes(file_dtype);
        let stats = parse_stats_sets(fb, &dtypes, session)?;
        Ok(Self::parsed(stats, dtypes))
    }

    /// Creates [`FileStatistics`] from serialized statistics, verified and parsed on first use.
    ///
    /// A scan that filters nothing never reads the statistics, and parsing them for every column
    /// of a wide file dominated opening it. Statistics that fail to parse are treated as absent:
    /// they only enable pruning, so the file still reads correctly without them.
    pub(crate) fn from_serialized(
        buffer: FlatBuffer,
        file_dtype: DType,
        session: VortexSession,
    ) -> Self {
        Self(Arc::new(FileStatisticsInner {
            stats: OnceLock::new(),
            dtypes: OnceLock::new(),
            source: Some((buffer, file_dtype, session)),
        }))
    }

    /// Returns a reference to the statistics sets.
    pub fn stats_sets(&self) -> &Arc<[StatsSet]> {
        self.0.stats.get_or_init(|| {
            let (buffer, _, session) = self
                .0
                .source
                .as_ref()
                .vortex_expect("statistics without a source are parsed on creation");
            let dtypes = self.dtypes();
            root_verified::<fb::FileStatistics>(buffer)
                .map_err(VortexError::from)
                .and_then(|fb| parse_stats_sets(&fb, dtypes, session))
                .unwrap_or_else(|err| {
                    tracing::warn!("Ignoring file statistics that failed to parse: {err}");
                    dtypes.iter().map(|_| StatsSet::default()).collect()
                })
        })
    }

    /// Returns a reference to the data types.
    pub fn dtypes(&self) -> &Arc<[DType]> {
        self.0.dtypes.get_or_init(|| {
            let (_, file_dtype, _) = self
                .0
                .source
                .as_ref()
                .vortex_expect("statistics without a source are parsed on creation");
            field_dtypes(file_dtype)
        })
    }

    /// Returns the statistics and data type for a specific field.
    ///
    /// # Panics
    ///
    /// Panics if `field_idx` is out of bounds.
    pub fn get(&self, field_idx: usize) -> (&StatsSet, &DType) {
        (&self.stats_sets()[field_idx], &self.dtypes()[field_idx])
    }
}

/// The dtype of each field the statistics describe: the struct fields, or the file dtype itself.
fn field_dtypes(file_dtype: &DType) -> Arc<[DType]> {
    match file_dtype {
        DType::Struct(struct_fields, _) => struct_fields.fields().collect(),
        _ => Arc::new([file_dtype.clone()]),
    }
}

fn parse_stats_sets(
    fb: &fb::FileStatistics<'_>,
    dtypes: &[DType],
    session: &VortexSession,
) -> VortexResult<Arc<[StatsSet]>> {
    let field_stats = fb.field_stats().unwrap_or_default();
    vortex_ensure_eq!(field_stats.len(), dtypes.len());

    // A wide file has a statistics set per column, so collect into a vector sized up front
    // rather than growing one.
    let mut stats_sets = Vec::with_capacity(dtypes.len());
    for (array_stat, field_dtype) in field_stats.into_iter().zip(dtypes.iter()) {
        stats_sets.push(StatsSet::from_flatbuffer(
            &array_stat,
            field_dtype,
            session,
        )?);
    }
    Ok(stats_sets.into())
}

impl<'a> IntoIterator for &'a FileStatistics {
    type Item = (&'a StatsSet, &'a DType);
    type IntoIter = std::iter::Zip<std::slice::Iter<'a, StatsSet>, std::slice::Iter<'a, DType>>;

    fn into_iter(self) -> Self::IntoIter {
        self.stats_sets().iter().zip(self.dtypes().iter())
    }
}

impl FlatBufferRoot for FileStatistics {}

impl WriteFlatBuffer for FileStatistics {
    type Target<'a> = fb::FileStatistics<'a>;

    fn write_flatbuffer<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> VortexResult<WIPOffset<Self::Target<'fb>>> {
        let field_stats = self
            .stats_sets()
            .iter()
            .map(|s| s.write_flatbuffer(fbb))
            .collect::<VortexResult<Vec<_>>>()?;
        let field_stats = fbb.create_vector(field_stats.as_slice());

        Ok(fb::FileStatistics::create(
            fbb,
            &fb::FileStatisticsArgs {
                field_stats: Some(field_stats),
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use vortex_array::array_session;
    use vortex_array::dtype::Nullability;
    use vortex_array::dtype::PType;
    use vortex_array::dtype::StructFields;
    use vortex_array::expr::stats::Precision;
    use vortex_array::expr::stats::Stat;
    use vortex_array::flatbuffers::WriteFlatBufferExt;
    use vortex_array::scalar::ScalarValue;

    use super::*;

    fn two_column_dtype() -> DType {
        let i64 = DType::Primitive(PType::I64, Nullability::NonNullable);
        DType::Struct(
            StructFields::from_iter([("a", i64.clone()), ("b", i64)]),
            Nullability::NonNullable,
        )
    }

    #[test]
    fn serialized_statistics_parse_on_first_use() -> VortexResult<()> {
        let dtype = two_column_dtype();
        let stats: Arc<[StatsSet]> = [3i64, 7]
            .into_iter()
            .map(|max| {
                let mut stats = StatsSet::default();
                stats.set(Stat::Max, Precision::exact(ScalarValue::from(max)));
                stats
            })
            .collect();
        let bytes = FileStatistics::new_with_dtype(stats, &dtype).write_flatbuffer_bytes()?;

        let lazy = FileStatistics::from_serialized(bytes, dtype.clone(), array_session());
        assert_eq!(lazy.dtypes().as_ref(), field_dtypes(&dtype).as_ref());
        let maxes = lazy
            .stats_sets()
            .iter()
            .map(|stats| stats.get(Stat::Max))
            .collect::<Vec<_>>();
        assert_eq!(
            maxes,
            [3i64, 7].map(|max| Precision::exact(ScalarValue::from(max)))
        );
        Ok(())
    }

    #[test]
    fn corrupt_serialized_statistics_are_absent() {
        let lazy = FileStatistics::from_serialized(
            FlatBuffer::copy_from(b"not a flatbuffer".as_slice()),
            two_column_dtype(),
            array_session(),
        );
        assert_eq!(lazy.stats_sets().len(), 2);
        assert!(lazy.stats_sets().iter().all(StatsSet::is_empty));
    }
}
