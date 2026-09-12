// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Helpers for extracting Vortex scan metrics from DataFusion execution plans.
use std::sync::Arc;
use std::time::Duration;

use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::source::DataSourceExec;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::ExecutionPlanVisitor;
use datafusion_physical_plan::Metric as DatafusionMetric;
use datafusion_physical_plan::accept;
use datafusion_physical_plan::metrics::Count;
use datafusion_physical_plan::metrics::Gauge;
use datafusion_physical_plan::metrics::Label as DatafusionLabel;
use datafusion_physical_plan::metrics::MetricValue as DatafusionMetricValue;
use datafusion_physical_plan::metrics::MetricsSet;
use datafusion_physical_plan::metrics::Time;
use vortex::error::VortexExpect;
use vortex::metrics::Label;
use vortex::metrics::Metric;
use vortex::metrics::MetricValue;

use crate::persistent::source::VortexSource;

pub(crate) static PARTITION_LABEL: &str = "partition";
pub(crate) static PATH_LABEL: &str = "file_path";

/// Walks a physical plan and returns one [`MetricsSet`] per
/// [`DataSourceExec`].
///
/// For Vortex-backed scans, the returned metrics include both the metrics
/// already attached to the `DataSourceExec` and the Vortex metrics accumulated
/// in [`VortexSource::metrics_registry`].
///
/// This helper exists because the Vortex read path records most scan metrics in
/// a Vortex [`MetricsRegistry`] rather than in DataFusion's native metrics set.
/// Push-morsel scan metrics exported through that shared registry are additive
/// counters and, when diagnostics are enabled, max gauges, so DataFusion's name-based
/// aggregation is well-defined. Each execution-partition opener lazily registers fixed
/// handle sets and reuses them across every file and range it completes.
///
/// # Example
///
/// ```no_run
/// # use std::sync::Arc;
/// use datafusion_physical_plan::ExecutionPlan;
/// use vortex_datafusion::metrics::VortexMetricsFinder;
///
/// # let plan: Arc<dyn ExecutionPlan> = todo!();
/// for metrics in VortexMetricsFinder::find_all(plan.as_ref()) {
///     for metric in metrics.aggregate_by_name().sorted_for_display().iter() {
///         println!("{metric}");
///     }
/// }
/// ```
///
/// [`DataSourceExec`]: datafusion_datasource::source::DataSourceExec
/// [`VortexSource::metrics_registry`]: crate::VortexSource::metrics_registry
/// [`MetricsRegistry`]: vortex::metrics::MetricsRegistry
pub struct VortexMetricsFinder {
    metric_sets: Vec<MetricsSet>,
    include_native: bool,
}

impl VortexMetricsFinder {
    /// Collects metrics for each `DataSourceExec` in `plan`, augmenting any
    /// Vortex-backed scan with the attached Vortex registry snapshot.
    pub fn find_all(plan: &dyn ExecutionPlan) -> Vec<MetricsSet> {
        let mut finder = Self {
            metric_sets: Vec::new(),
            include_native: true,
        };
        match accept(plan, &mut finder) {
            Ok(()) => finder.metric_sets,
            Err(_) => Vec::new(),
        }
    }

    /// Collect only Vortex registry metrics for each Vortex-backed `DataSourceExec`.
    ///
    /// Native execution metrics are intentionally excluded so callers that already walk the
    /// execution tree do not report those metrics twice. Non-Vortex data sources are omitted.
    pub fn find_vortex_registry(plan: &dyn ExecutionPlan) -> Vec<MetricsSet> {
        let mut finder = Self {
            metric_sets: Vec::new(),
            include_native: false,
        };
        match accept(plan, &mut finder) {
            Ok(()) => finder.metric_sets,
            Err(_) => Vec::new(),
        }
    }
}

impl ExecutionPlanVisitor for VortexMetricsFinder {
    type Error = std::convert::Infallible;
    fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
        if let Some(exec) = plan.downcast_ref::<DataSourceExec>() {
            let vortex_source = exec
                .data_source()
                .downcast_ref::<FileScanConfig>()
                .and_then(|file_scan| file_scan.file_source.downcast_ref::<VortexSource>());
            if self.include_native {
                let mut set = exec.metrics().unwrap_or_default();
                if let Some(scan) = vortex_source {
                    for metric in scan
                        .metrics_registry()
                        .snapshot()
                        .iter()
                        .flat_map(metric_to_datafusion)
                    {
                        set.push(Arc::new(metric));
                    }
                }
                self.metric_sets.push(set);
            } else if let Some(scan) = vortex_source {
                let mut set = MetricsSet::new();
                for metric in scan
                    .metrics_registry()
                    .snapshot()
                    .iter()
                    .flat_map(metric_to_datafusion)
                {
                    set.push(Arc::new(metric));
                }
                self.metric_sets.push(set);
            }

            Ok(false)
        } else {
            Ok(true)
        }
    }
}

fn metric_to_datafusion(metric: &Metric) -> impl Iterator<Item = DatafusionMetric> {
    let (partition, labels) = labels_to_datafusion(metric.labels());
    metric_value_to_datafusion(metric.name(), metric.value())
        .into_iter()
        .map(move |metric_value| {
            DatafusionMetric::new_with_labels(metric_value, partition, labels.clone())
        })
}

fn labels_to_datafusion(tags: &[Label]) -> (Option<usize>, Vec<DatafusionLabel>) {
    tags.iter()
        .fold((None, Vec::new()), |(mut partition, mut labels), metric| {
            if metric.key() == PARTITION_LABEL {
                partition = metric.value().parse().ok();
            } else {
                labels.push(DatafusionLabel::new(
                    metric.key().to_string(),
                    metric.value().to_string(),
                ));
            }
            (partition, labels)
        })
}

fn metric_value_to_datafusion(name: &str, metric: &MetricValue) -> Vec<DatafusionMetricValue> {
    // DataFusion stores integer metrics as usize. On 32-bit targets values outside that range are
    // omitted rather than truncated; the Vortex registry retains the original value.
    match metric {
        MetricValue::Counter(counter) => counter
            .value()
            .try_into()
            .into_iter()
            .map(|count| df_counter(name.to_string(), count))
            .collect(),
        MetricValue::Histogram(hist) => {
            let mut res = Vec::new();

            res.push(df_counter(format!("{name}_count"), hist.count()));

            if !hist.is_empty() {
                if let Some(max) = f_to_u(hist.quantile(1.0).vortex_expect("must not be empty")) {
                    res.push(df_gauge(format!("{name}_max"), max));
                }

                if let Some(min) = f_to_u(hist.quantile(0.0).vortex_expect("must not be empty")) {
                    res.push(df_gauge(format!("{name}_min"), min));
                }

                if let Some(p95) = f_to_u(hist.quantile(0.95).vortex_expect("must not be empty")) {
                    res.push(df_gauge(format!("{name}_p95"), p95));
                }
                if let Some(p99) = f_to_u(hist.quantile(0.99).vortex_expect("must not be empty")) {
                    res.push(df_gauge(format!("{name}_p99"), p99));
                }
            }

            res
        }
        MetricValue::Timer(timer) => {
            let mut res = Vec::new();
            res.push(df_counter(format!("{name}_count"), timer.count()));

            if !timer.is_empty() {
                let max = timer.quantile(1.0).vortex_expect("must not be empty");
                res.push(df_timer(format!("{name}_max"), max));

                let min = timer.quantile(0.0).vortex_expect("must not be empty");
                res.push(df_timer(format!("{name}_min"), min));

                let p95 = timer.quantile(0.95).vortex_expect("must not be empty");
                res.push(df_timer(format!("{name}_p95"), p95));

                let p99 = timer.quantile(0.99).vortex_expect("must not be empty");
                res.push(df_timer(format!("{name}_p99"), p99));
            }

            res
        }
        MetricValue::Gauge(gauge) => f_to_u(gauge.value())
            .into_iter()
            .map(|value| df_gauge(name.to_string(), value))
            .collect(),
    }
}

fn df_counter(name: String, value: usize) -> DatafusionMetricValue {
    let count = Count::new();
    count.add(value);
    DatafusionMetricValue::Count {
        name: name.into(),
        count,
    }
}

fn df_gauge(name: String, value: usize) -> DatafusionMetricValue {
    let gauge = Gauge::new();
    gauge.set(value);
    DatafusionMetricValue::Gauge {
        name: name.into(),
        gauge,
    }
}

fn df_timer(name: String, value: Duration) -> DatafusionMetricValue {
    let time = Time::new();
    time.add_duration(value);
    DatafusionMetricValue::Time {
        name: name.into(),
        time,
    }
}

#[expect(
    clippy::cast_possible_truncation,
    reason = "truncation is checked before cast"
)]
fn f_to_u(f: f64) -> Option<usize> {
    (f.is_finite() && f >= usize::MIN as f64 && f <= usize::MAX as f64).then(||
        // After the range check, truncation is guaranteed to keep the value in usize bounds.
        f.trunc() as usize)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion_datasource::source::DataSourceExec;
    use datafusion_physical_plan::ExecutionPlanVisitor;
    use datafusion_physical_plan::accept;
    use datafusion_physical_plan::metrics::MetricValue as DatafusionMetricValue;
    use datafusion_physical_plan::metrics::MetricsSet;
    use vortex::metrics::DefaultMetricsRegistry;
    use vortex::metrics::MetricBuilder as VortexMetricBuilder;
    use vortex::metrics::MetricsRegistry;

    use super::VortexMetricsFinder;
    use super::metric_to_datafusion;
    use crate::common_tests::TestSessionContext;

    /// Counts the number of DataSourceExec nodes in a plan.
    struct DataSourceExecCounter(usize);

    #[test]
    fn converts_vortex_gauge_without_counter_semantics() {
        let registry = DefaultMetricsRegistry::default();
        VortexMetricBuilder::new(&registry)
            .gauge("retained_bytes_peak")
            .set(42.0);
        let metric = registry
            .snapshot()
            .into_iter()
            .next()
            .expect("registered gauge must be present");
        let converted = metric_to_datafusion(&metric).collect::<Vec<_>>();
        assert_eq!(converted.len(), 1);
        assert!(matches!(
            converted[0].value(),
            DatafusionMetricValue::Gauge { name, gauge }
                if name.as_ref() == "retained_bytes_peak" && gauge.value() == 42
        ));
    }

    #[test]
    fn same_partition_scan_counters_remain_visible_and_aggregate_by_sum() {
        let registry = DefaultMetricsRegistry::default();
        for value in [3, 5] {
            VortexMetricBuilder::new(&registry)
                .add_label("partition", "2")
                .counter("morsel_scan.output.rows_before_map")
                .add(value);
        }

        let converted = registry
            .snapshot()
            .iter()
            .flat_map(metric_to_datafusion)
            .collect::<Vec<_>>();
        assert_eq!(converted.len(), 2);
        assert!(converted.iter().all(|metric| metric.partition() == Some(2)));
        assert!(converted.iter().all(|metric| matches!(
            metric.value(),
            DatafusionMetricValue::Count { name, .. }
                if name.as_ref() == "morsel_scan.output.rows_before_map"
        )));

        let mut set = MetricsSet::new();
        for metric in converted {
            set.push(Arc::new(metric));
        }
        let aggregated = set.aggregate_by_name();
        assert!(aggregated.iter().any(|metric| matches!(
            metric.value(),
            DatafusionMetricValue::Count { name, count }
                if name.as_ref() == "morsel_scan.output.rows_before_map" && count.value() == 8
        )));
    }

    impl ExecutionPlanVisitor for DataSourceExecCounter {
        type Error = std::convert::Infallible;
        fn pre_visit(
            &mut self,
            plan: &dyn datafusion_physical_plan::ExecutionPlan,
        ) -> Result<bool, Self::Error> {
            if plan.downcast_ref::<DataSourceExec>().is_some() {
                self.0 += 1;
                Ok(false)
            } else {
                Ok(true)
            }
        }
    }

    #[tokio::test]
    async fn metrics_finder_returns_one_set_per_data_source_exec() -> anyhow::Result<()> {
        let ctx = TestSessionContext::default();

        ctx.session
            .sql(
                "CREATE EXTERNAL TABLE my_tbl \
                (c1 VARCHAR NOT NULL, c2 INT NOT NULL) \
                STORED AS vortex \
                LOCATION 'files/'",
            )
            .await?;

        ctx.session
            .sql("INSERT INTO my_tbl VALUES ('a', 1), ('b', 2)")
            .await?
            .collect()
            .await?;

        let df = ctx.session.sql("SELECT * FROM my_tbl").await?;
        let (state, plan) = df.into_parts();
        let physical_plan = state.create_physical_plan(&plan).await?;

        // Count DataSourceExec nodes
        let mut counter = DataSourceExecCounter(0);
        accept(physical_plan.as_ref(), &mut counter)?;

        // Get metrics sets
        let metrics_sets = VortexMetricsFinder::find_all(physical_plan.as_ref());
        let registry_sets = VortexMetricsFinder::find_vortex_registry(physical_plan.as_ref());

        assert!(!metrics_sets.is_empty());
        assert_eq!(
            metrics_sets.len(),
            counter.0,
            "Expected one MetricsSet per DataSourceExec, got {} sets for {} DataSourceExec nodes",
            metrics_sets.len(),
            counter.0
        );
        assert_eq!(registry_sets.len(), counter.0);

        Ok(())
    }
}
