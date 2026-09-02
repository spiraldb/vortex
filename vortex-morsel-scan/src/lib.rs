// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

#![deny(missing_docs)]

//! Neutral selection and construction facade for the experimental morsel scan executors.
//!
//! The pull and push implementations remain independent crates. SQL integrations use this crate
//! so implementation-specific dependencies and backend-selection policy do not leak into them.

use std::str::FromStr;
use std::sync::Arc;

use vortex_error::VortexResult;
use vortex_error::vortex_bail;
mod scan_builder;

pub use scan_builder::MorselScanBuilder;

const DEFAULT_THREADS: usize = 4;

/// Scan implementation selected by SQL and benchmark integrations.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ScanBackend {
    /// The original asynchronous `LayoutReader` implementation.
    #[default]
    V1,
    /// The optimized recursive pull-morsel implementation.
    Pull,
    /// The independently imported physical push-morsel implementation.
    Push,
}

impl FromStr for ScanBackend {
    type Err = vortex_error::VortexError;

    fn from_str(value: &str) -> VortexResult<Self> {
        match value {
            "v1" => Ok(Self::V1),
            "pull" | "morsel-pull" => Ok(Self::Pull),
            "push" | "morsel-push" => Ok(Self::Push),
            _ => vortex_bail!("scan backend must be v1, pull, or push; received {value:?}"),
        }
    }
}

/// Read [`ScanBackend`] from `VORTEX_SCAN_BACKEND`, defaulting to the established V1 path.
pub fn scan_backend_from_env() -> VortexResult<ScanBackend> {
    match std::env::var("VORTEX_SCAN_BACKEND") {
        Ok(value) => value.parse(),
        Err(std::env::VarError::NotPresent) => Ok(ScanBackend::default()),
        Err(err) => vortex_bail!("VORTEX_SCAN_BACKEND is not valid Unicode: {err}"),
    }
}

/// Engine-specific settings used when constructing a morsel scan executor.
#[derive(Clone)]
pub struct ScanExecutorOptions {
    threads: usize,
    external_driver: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl Default for ScanExecutorOptions {
    fn default() -> Self {
        Self {
            threads: DEFAULT_THREADS,
            external_driver: None,
        }
    }
}

impl ScanExecutorOptions {
    /// Set the number of workers used by either morsel implementation.
    pub fn with_threads(mut self, threads: usize) -> Self {
        self.threads = threads.max(1);
        self
    }

    /// Let the execution engine's calling threads drive returned morsel futures directly.
    /// `driver` must advance the engine's async runtime by one cooperative turn while a morsel is
    /// waiting for segment I/O.
    pub fn with_external_threads(mut self, driver: impl Fn() + Send + Sync + 'static) -> Self {
        self.external_driver = Some(Arc::new(driver));
        self
    }

    /// Create and retain a persistent pull-worker pool of the requested size.
    ///
    /// Push scans continue to use the same requested thread count without sharing this pool.
    pub fn with_persistent_pull_workers(mut self, threads: usize) -> VortexResult<Self> {
        self.threads = threads.max(1);
        Ok(self)
    }

    /// The configured number of executor workers.
    pub fn threads(&self) -> usize {
        self.threads
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use vortex_error::VortexResult;

    use super::ScanBackend;
    use super::ScanExecutorOptions;

    #[test]
    fn parses_stable_backend_labels() -> VortexResult<()> {
        assert_eq!(ScanBackend::from_str("v1")?, ScanBackend::V1);
        assert_eq!(ScanBackend::from_str("pull")?, ScanBackend::Pull);
        assert_eq!(ScanBackend::from_str("morsel-pull")?, ScanBackend::Pull);
        assert_eq!(ScanBackend::from_str("push")?, ScanBackend::Push);
        assert_eq!(ScanBackend::from_str("morsel-push")?, ScanBackend::Push);
        assert!(ScanBackend::from_str("other").is_err());
        Ok(())
    }

    #[test]
    fn preserves_thread_defaults_and_clamps_zero() {
        assert_eq!(ScanExecutorOptions::default().threads(), 4);
        assert_eq!(ScanExecutorOptions::default().with_threads(0).threads(), 1);
        assert_eq!(ScanExecutorOptions::default().with_threads(7).threads(), 7);
    }

    #[test]
    fn persistent_pool_controls_pull_and_push_thread_count() -> VortexResult<()> {
        let options = ScanExecutorOptions::default().with_persistent_pull_workers(3)?;
        assert_eq!(options.threads(), 3);
        Ok(())
    }

    #[test]
    fn defaults_to_v1() {
        assert_eq!(ScanBackend::default(), ScanBackend::V1);
    }
}
