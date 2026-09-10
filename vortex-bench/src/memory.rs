// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Memory measurement and reclamation utilities for benchmarks

use parking_lot::Mutex;
use sysinfo::ProcessRefreshKind;
use sysinfo::ProcessesToUpdate;
use sysinfo::RefreshKind;
use sysinfo::System;

/// Memory statistics for a process
#[derive(Debug, Clone, Copy)]
pub struct MemoryStats {
    /// Physical memory usage in bytes
    pub physical_memory: u64,
    /// Virtual memory usage in bytes
    pub virtual_memory: u64,
}

impl MemoryStats {
    pub fn new(physical_memory: u64, virtual_memory: u64) -> Self {
        Self {
            physical_memory,
            virtual_memory,
        }
    }

    /// Calculate the difference between two memory measurements
    pub fn diff(&self, other: &MemoryStats) -> MemoryStatsDiff {
        MemoryStatsDiff {
            physical_memory_delta: self.physical_memory as i64 - other.physical_memory as i64,
            virtual_memory_delta: self.virtual_memory as i64 - other.virtual_memory as i64,
        }
    }
}

/// Memory usage difference between two measurements
#[derive(Debug, Clone, Copy)]
pub struct MemoryStatsDiff {
    /// Change in physical memory usage in bytes (can be negative)
    pub physical_memory_delta: i64,
    /// Change in virtual memory usage in bytes (can be negative)
    pub virtual_memory_delta: i64,
}

/// Thread-safe memory tracker using sysinfo
#[derive(Default)]
pub struct MemoryTracker {
    system: Mutex<System>,
    pid: u32,
    peak_physical_memory: Mutex<u64>,
    peak_virtual_memory: Mutex<u64>,
}

impl MemoryTracker {
    /// Create a new memory tracker for the current process
    pub fn new() -> Self {
        let mut system = System::new_with_specifics(
            RefreshKind::default().with_processes(ProcessRefreshKind::nothing().with_memory()),
        );
        let pid = std::process::id();

        // Initial refresh to populate process info
        system.refresh_processes(
            ProcessesToUpdate::Some(&[sysinfo::Pid::from_u32(pid)]),
            true,
        );

        // Get initial memory usage as baseline for peak tracking
        let initial_memory = if let Some(process) = system.process(sysinfo::Pid::from_u32(pid)) {
            MemoryStats::new(process.memory(), process.virtual_memory())
        } else {
            MemoryStats::new(0, 0)
        };

        Self {
            system: Mutex::new(system),
            pid,
            peak_physical_memory: Mutex::new(initial_memory.physical_memory),
            peak_virtual_memory: Mutex::new(initial_memory.virtual_memory),
        }
    }

    /// Get current memory usage for the tracked process and update peak tracking
    pub fn current_memory(&self) -> Option<MemoryStats> {
        let mut system = self.system.lock();
        system.refresh_processes(
            ProcessesToUpdate::Some(&[sysinfo::Pid::from_u32(self.pid)]),
            true,
        );

        if let Some(process) = system.process(sysinfo::Pid::from_u32(self.pid)) {
            let current_stats = MemoryStats::new(process.memory(), process.virtual_memory());

            // Update peak memory if current usage is higher
            {
                let mut peak_physical = self.peak_physical_memory.lock();
                if current_stats.physical_memory > *peak_physical {
                    *peak_physical = current_stats.physical_memory;
                }
            }

            {
                let mut peak_virtual = self.peak_virtual_memory.lock();
                if current_stats.virtual_memory > *peak_virtual {
                    *peak_virtual = current_stats.virtual_memory;
                }
            }

            Some(current_stats)
        } else {
            None
        }
    }

    /// Get the peak memory usage recorded so far
    pub fn peak_memory(&self) -> MemoryStats {
        let peak_physical = *self.peak_physical_memory.lock();
        let peak_virtual = *self.peak_virtual_memory.lock();
        MemoryStats::new(peak_physical, peak_virtual)
    }

    /// Reset peak memory tracking to current usage
    pub fn reset_peak(&self) {
        if let Some(current) = self.current_memory() {
            *self.peak_physical_memory.lock() = current.physical_memory;
            *self.peak_virtual_memory.lock() = current.virtual_memory;
        }
    }
}

/// Memory measurement guard that tracks memory usage before and after an operation
pub struct MemoryMeasurement {
    tracker: MemoryTracker,
    before: Option<MemoryStats>,
}

/// Comprehensive memory measurement result containing all memory metrics
#[derive(Debug, Clone)]
pub struct MemoryMeasurementResult {
    /// Memory change during the operation (can be negative)
    pub physical_memory_delta: i64,
    pub virtual_memory_delta: i64,
    /// Peak resident-set bytes during the query. On Linux this is the kernel
    /// `VmHWM` high-water mark, reset at query start; elsewhere it is the
    /// maximum of the values sampled at query start and end.
    pub peak_physical_memory: u64,
    /// Maximum virtual-memory bytes sampled at query start and end.
    pub peak_virtual_memory: u64,
}

/// Simplified interface for memory tracking with a global tracker
pub struct BenchmarkMemoryTracker {
    global_tracker: MemoryTracker,
    baseline_memory: Option<MemoryStats>,
    hwm_reset: bool,
}

impl Default for BenchmarkMemoryTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl BenchmarkMemoryTracker {
    /// Create a new benchmark memory tracker
    pub fn new() -> Self {
        Self {
            global_tracker: MemoryTracker::new(),
            baseline_memory: None,
            hwm_reset: false,
        }
    }

    /// Mark the start of a query execution - should be called before running a query
    pub fn start_query(&mut self) {
        self.hwm_reset = reset_peak_rss();
        self.global_tracker.reset_peak();
        self.baseline_memory = self.global_tracker.current_memory();
    }

    /// Mark the end of a query execution and collect memory measurements
    /// Returns None if no baseline was set or memory tracking failed
    pub fn end_query(&self) -> Option<MemoryMeasurementResult> {
        let baseline = self.baseline_memory?;
        let after_memory = self.global_tracker.current_memory()?;
        let usage_diff = after_memory.diff(&baseline);

        let sampled_peak = self.global_tracker.peak_memory();
        let peak_physical_memory = if self.hwm_reset {
            peak_rss().unwrap_or(sampled_peak.physical_memory)
        } else {
            sampled_peak.physical_memory
        };

        Some(MemoryMeasurementResult {
            physical_memory_delta: usage_diff.physical_memory_delta,
            virtual_memory_delta: usage_diff.virtual_memory_delta,
            peak_physical_memory,
            peak_virtual_memory: sampled_peak.virtual_memory,
        })
    }

    /// Get the current peak memory without ending a query
    pub fn peak_memory(&self) -> MemoryStats {
        self.global_tracker.peak_memory()
    }
}

impl MemoryMeasurement {
    /// Start a memory measurement
    pub fn start() -> Self {
        let tracker = MemoryTracker::new();
        let before = tracker.current_memory();

        Self { tracker, before }
    }

    /// End the measurement and return the memory usage difference
    pub fn end(self) -> Option<MemoryStatsDiff> {
        let after = self.tracker.current_memory()?;
        let before = self.before?;

        Some(after.diff(&before))
    }
}

/// Peak resident-set size (`VmHWM`) of the current process.
#[cfg(target_os = "linux")]
fn peak_rss() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    status
        .lines()
        .find_map(|line| line.strip_prefix("VmHWM:"))
        .and_then(parse_status_kib)
}

/// Reset the kernel `VmHWM` high-water mark to the current RSS, so subsequent
/// [`peak_rss`] reads report the peak since this call. Returns whether the
/// reset took effect.
#[cfg(target_os = "linux")]
fn reset_peak_rss() -> bool {
    std::fs::write("/proc/self/clear_refs", "5").is_ok()
}

#[cfg(target_os = "linux")]
fn parse_status_kib(field: &str) -> Option<u64> {
    field
        .split_whitespace()
        .next()?
        .parse::<u64>()
        .ok()
        .map(|kib| kib * 1024)
}

#[cfg(not(target_os = "linux"))]
fn peak_rss() -> Option<u64> {
    None
}

#[cfg(not(target_os = "linux"))]
fn reset_peak_rss() -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_tracker() {
        let tracker = MemoryTracker::new();
        let memory = tracker.current_memory();
        assert!(memory.is_some());

        if let Some(stats) = memory {
            assert!(stats.physical_memory > 0);
            assert!(stats.virtual_memory > 0);
        }
    }

    #[test]
    fn test_memory_measurement() {
        let measurement = MemoryMeasurement::start();

        // Allocate some memory
        let _data: Vec<u8> = vec![0; 1024 * 1024]; // 1MB

        let diff = measurement.end();
        assert!(diff.is_some());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn peak_memory() {
        assert!(peak_rss().unwrap() > 0);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn per_query_peak() -> anyhow::Result<()> {
        const BIG: usize = 256 << 20;

        // Anonymous mmap instead of Vec: unmapping bypasses mimalloc, so the
        // pages verifiably leave RSS between the two queries.
        fn touched_map(len: usize) -> anyhow::Result<memmap2::MmapMut> {
            let mut map = memmap2::MmapMut::map_anon(len)?;
            for page in map.chunks_mut(4096) {
                page[0] = 7;
            }
            std::hint::black_box(&mut map);
            Ok(map)
        }

        let mut tracker = BenchmarkMemoryTracker::new();

        tracker.start_query();
        let big = touched_map(BIG)?;
        let first = tracker.end_query().unwrap();
        drop(big);

        tracker.start_query();
        let _small = touched_map(1 << 20)?;
        let second = tracker.end_query().unwrap();

        assert!(first.peak_physical_memory >= BIG as u64);
        assert!(first.physical_memory_delta > 0);
        assert!(second.peak_physical_memory < first.peak_physical_memory - (BIG as u64 / 2));
        Ok(())
    }
}
