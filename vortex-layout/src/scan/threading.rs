// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use vortex_utils::parallelism::get_available_parallelism;

/// Who drives the parallelism of a scan.
///
/// A scan stream keeps a number of splits in flight at once. When the scan owns the machine it
/// scales that number by the available parallelism so every core has work. When a query engine
/// such as DataFusion or DuckDB already partitions the work across its own threads, each scan is
/// one of many running at once, and scaling by the core count again would multiply the in-flight
/// splits by the core count a second time.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum ScanThreading {
    /// The scan is the only consumer of the machine and keeps `concurrency` splits in flight per
    /// available core.
    #[default]
    Internal,
    /// An external engine runs many scans in parallel, so each scan keeps only `concurrency`
    /// splits in flight.
    External,
}

impl ScanThreading {
    /// The number of splits to keep in flight for the given per-thread `concurrency`.
    pub(crate) fn splits_in_flight(self, concurrency: usize) -> usize {
        match self {
            ScanThreading::Internal => concurrency * get_available_parallelism().unwrap_or(1),
            ScanThreading::External => concurrency,
        }
    }
}

#[cfg(test)]
mod tests {
    use vortex_utils::parallelism::get_available_parallelism;

    use super::ScanThreading;

    #[test]
    fn external_threading_does_not_scale_by_cores() {
        assert_eq!(ScanThreading::External.splits_in_flight(4), 4);
        assert_eq!(
            ScanThreading::Internal.splits_in_flight(4),
            4 * get_available_parallelism().unwrap_or(1)
        );
    }
}
