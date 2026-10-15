// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! An alternative scan executor, selected by callers when [`enabled`] returns true.
//!
//! The executor has its own [`ScanBuilder`], a copy of the default one that also takes the
//! [`ScanFile`] the reader was opened over.
//!
//! Callers that configure a default [`ScanBuilder`](crate::scan::scan_builder::ScanBuilder) can
//! instead swap only the final call: [`prepare`] for its `prepare`, and [`into_stream`] for its
//! `into_stream`. Both copy the default builder into this executor's and return the same types as
//! the calls they replace.
//!
//! Each split runs as a [`FilterPlanner`](crate::scan::planning::FilterPlanner), which hands the
//! rows that survive to a [`ProjectionPlanner`](crate::scan::planning::ProjectionPlanner) and its
//! morsel. Both execute physical plans lowered from the file's layout, on the planning driver, and
//! the segment IO service the file is registered with serves their reads.
//!
//! The driver runs inside the split's future, on whichever thread polls it, and the future awaits
//! the segment reads it registers. Every split announces the segments its plans are likely to read
//! when it starts, so the service can coalesce them.
//!
//! A caller that drives the planning protocol itself takes the splits as [`SplitPlan`]s from
//! [`RepeatedScanV2::split_plans`] and admits them to its own run, reading through a session of
//! [`RepeatedScanV2::io`].

mod conjuncts;
mod file;
mod lower;
pub(crate) mod prefetch;
mod pruning;
mod repeated_scan;
mod scan_builder;
mod share;
mod split;
pub(crate) mod splits;
mod stream;

use std::env;
use std::sync::Arc;
use std::sync::LazyLock;

pub use file::FilePlans;
pub use repeated_scan::RepeatedScanV2;
pub use repeated_scan::prepare;
pub use scan_builder::ScanBuilder;
pub use split::SplitPlan;
pub use stream::into_stream;
use vortex_array::dtype::DType;
use vortex_io::segments::RegisteredSource;

use crate::LayoutRef;
use crate::scan::planning::SegmentLocation;
use crate::scan::scan_builder::ScanBuilder as DefaultScanBuilder;

/// The environment variable that selects this executor when set to `1`.
pub const ENV_VAR: &str = "VORTEX_SCAN_V2";

static ENABLED: LazyLock<bool> = LazyLock::new(|| env::var(ENV_VAR).is_ok_and(|v| v == "1"));

/// Whether [`ENV_VAR`] selects this executor. Read once per process.
pub fn enabled() -> bool {
    *ENABLED
}

/// Whether this executor can scan `file`.
///
/// Physical plans run on pipelines, which are compiled before any read, so a list's element
/// range, known only once its offsets are read, cannot be compiled: files with list columns are
/// scanned by the default executor.
pub fn supports(file: &ScanFile) -> bool {
    !has_list(file.layout.dtype())
}

/// Whether this executor can run `builder`'s scan over `file`: it supports the file, and the scan
/// does not combine a filter with a row limit.
pub fn supports_scan<A: 'static + Send>(builder: &DefaultScanBuilder<A>, file: &ScanFile) -> bool {
    supports(file) && !builder.has_filter_and_limit()
}

fn has_list(dtype: &DType) -> bool {
    match dtype {
        DType::List(..) => true,
        DType::FixedSizeList(element, ..) => has_list(element),
        DType::Struct(fields, _) => fields.fields().any(|field| has_list(&field)),
        DType::Extension(ext) => has_list(ext.storage_dtype()),
        _ => false,
    }
}

/// The file a scan reads: its root layout, where each segment lives, and its registration with the
/// segment IO service that reads it.
#[derive(Clone)]
pub struct ScanFile {
    /// The root layout the scan builder's reader was opened over.
    pub layout: LayoutRef,
    /// The location of every segment, indexed by segment id.
    pub locations: Arc<[SegmentLocation]>,
    /// The file's registration with the service that reads its segments.
    pub io: RegisteredSource,
    /// Retains the lowered layout plans for an explicitly cached reader. No decoded data is
    /// retained here.
    /// Only share this state between scans over the same layout and segment source.
    pub plans: Option<Arc<FilePlans>>,
}

#[cfg(test)]
mod tests;
