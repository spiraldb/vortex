// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

#[cfg(test)]
mod tests {
    use anyhow::anyhow;
    use datafusion::prelude::SessionConfig;
    use datafusion_bench::build_benchmark_runtime;
    use datafusion_bench::format_to_df_format;
    use datafusion_bench::get_session_context;
    use vortex_bench::Format;
    use vortex_datafusion::VortexFormat;

    #[test]
    fn explicit_threads_reach_datafusion_and_vortex() -> anyhow::Result<()> {
        let runtime = build_benchmark_runtime(Some(7))?;
        assert_eq!(runtime.metrics().num_workers(), 7);

        let session = get_session_context(Some(7));
        assert_eq!(session.state().config().target_partitions(), 7);

        let format = format_to_df_format(Format::OnDiskVortex, Some(7));
        let vortex_format = format
            .downcast_ref::<VortexFormat>()
            .ok_or_else(|| anyhow!("expected a Vortex file format"))?;
        assert_eq!(vortex_format.options().scan_concurrency, Some(7));

        Ok(())
    }

    #[test]
    fn runtime_rejects_zero_workers() {
        assert!(build_benchmark_runtime(Some(0)).is_err());
    }

    #[test]
    fn absent_threads_preserve_vortex_scan_default() -> anyhow::Result<()> {
        let mut default_builder = tokio::runtime::Builder::new_multi_thread();
        default_builder.enable_all();
        let expected_runtime = default_builder.build()?;
        let actual_runtime = build_benchmark_runtime(None)?;
        assert_eq!(
            actual_runtime.metrics().num_workers(),
            expected_runtime.metrics().num_workers()
        );

        let expected_target_partitions = SessionConfig::from_env()?.target_partitions();
        assert_eq!(
            get_session_context(None)
                .state()
                .config()
                .target_partitions(),
            expected_target_partitions
        );

        let format = format_to_df_format(Format::OnDiskVortex, None);
        let vortex_format = format
            .downcast_ref::<VortexFormat>()
            .ok_or_else(|| anyhow!("expected a Vortex file format"))?;
        assert_eq!(vortex_format.options().scan_concurrency, None);

        Ok(())
    }
}
