// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::Arc;

    use datafusion::arrow::array::ArrayRef;
    use datafusion::arrow::array::Float64Array;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::array::RecordBatch;
    use datafusion::arrow::array::RunArray;
    use datafusion::arrow::array::StringArray;
    use datafusion::arrow::array::StringDictionaryBuilder;
    use datafusion::arrow::array::UInt32Array;
    use datafusion::arrow::array::types::Int32Type;
    use datafusion::arrow::array::types::UInt8Type;
    use datafusion::arrow::datatypes::DataType;
    use datafusion::arrow::datatypes::Field;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::arrow::datatypes::SchemaRef;
    use datafusion_bench::result_artifacts::EffectiveScanBackend;
    use datafusion_bench::result_artifacts::ResultArtifactOperation;
    use datafusion_bench::result_artifacts::ResultOrderPolicy;
    use datafusion_bench::result_artifacts::canonical_result_artifact;
    use datafusion_bench::result_artifacts::effective_scan_backend;
    use datafusion_bench::result_artifacts::validate_result_artifact_execution;
    use datafusion_bench::result_artifacts::verify_result_artifact;
    use datafusion_bench::result_artifacts::write_result_artifact;
    use tempfile::tempdir;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("value", DataType::Float64, false),
            Field::new("label", DataType::Utf8, false),
        ]))
    }

    fn batch(schema: SchemaRef, rows: &[(u32, u64, &str)]) -> anyhow::Result<RecordBatch> {
        let ids = UInt32Array::from_iter_values(rows.iter().map(|row| row.0));
        let values = Float64Array::from_iter_values(rows.iter().map(|row| f64::from_bits(row.1)));
        let labels = StringArray::from_iter_values(rows.iter().map(|row| row.2));
        let columns: Vec<ArrayRef> = vec![Arc::new(ids), Arc::new(values), Arc::new(labels)];
        Ok(RecordBatch::try_new(schema, columns)?)
    }

    fn single_column_batch(array: ArrayRef) -> anyhow::Result<(SchemaRef, RecordBatch)> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            array.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![array])?;
        Ok((schema, batch))
    }

    fn dictionary_batch(values: &[&str]) -> anyhow::Result<(SchemaRef, RecordBatch)> {
        let mut builder = StringDictionaryBuilder::<UInt8Type>::new();
        for value in values {
            builder.append(*value)?;
        }
        single_column_batch(Arc::new(builder.finish()))
    }

    #[test]
    fn reordered_duplicates_require_explicit_multiset_policy() -> anyhow::Result<()> {
        let schema = schema();
        let nan_a = f64::NAN.to_bits();
        let nan_b = f64::from_bits(nan_a + 1).to_bits();
        let rows = [
            (2, nan_a, "beta"),
            (1, (-0.0_f64).to_bits(), "duplicate"),
            (1, (-0.0_f64).to_bits(), "duplicate"),
            (3, nan_b, "gamma"),
        ];

        let one_batch = vec![batch(Arc::clone(&schema), &rows)?];
        let split_and_reordered = vec![
            batch(Arc::clone(&schema), &[rows[3], rows[1]])?,
            batch(Arc::clone(&schema), &[rows[0], rows[2]])?,
        ];

        let directory = tempdir()?;
        let path = directory.path().join("result.arrow");
        write_result_artifact(&path, Arc::clone(&schema), &one_batch)?;
        verify_result_artifact(
            &path,
            Arc::clone(&schema),
            &split_and_reordered,
            ResultOrderPolicy::Multiset,
        )?;
        let Err(error) = verify_result_artifact(
            &path,
            schema,
            &split_and_reordered,
            ResultOrderPolicy::Ordered,
        ) else {
            anyhow::bail!("reordered duplicate rows unexpectedly passed ordered verification");
        };
        assert!(format!("{error:#}").contains("row-sequence mismatch"));
        Ok(())
    }

    #[test]
    fn canonical_artifact_preserves_duplicates_and_float_bits() -> anyhow::Result<()> {
        let result_schema = schema();
        let duplicate = (1, 0.0_f64.to_bits(), "same");
        let one_row = vec![batch(Arc::clone(&result_schema), &[duplicate])?];
        let two_rows = vec![batch(Arc::clone(&result_schema), &[duplicate, duplicate])?];
        assert_ne!(
            canonical_result_artifact(Arc::clone(&result_schema), &one_row)?,
            canonical_result_artifact(Arc::clone(&result_schema), &two_rows)?
        );

        let negative_zero = vec![batch(
            Arc::clone(&result_schema),
            &[(1, (-0.0_f64).to_bits(), "same")],
        )?];
        assert_ne!(
            canonical_result_artifact(Arc::clone(&result_schema), &one_row)?,
            canonical_result_artifact(result_schema, &negative_zero)?
        );

        let nan_payload_a = vec![batch(schema(), &[(1, f64::NAN.to_bits(), "same")])?];
        let nan_payload_b = vec![batch(schema(), &[(1, f64::NAN.to_bits() + 1, "same")])?];
        assert_ne!(
            canonical_result_artifact(schema(), &nan_payload_a)?,
            canonical_result_artifact(schema(), &nan_payload_b)?
        );
        Ok(())
    }

    #[test]
    fn canonical_artifact_includes_the_schema() -> anyhow::Result<()> {
        let first_schema = schema();
        let renamed_schema = Arc::new(Schema::new(vec![
            Field::new("renamed_id", DataType::UInt32, false),
            Field::new("value", DataType::Float64, false),
            Field::new("label", DataType::Utf8, false),
        ]));
        let rows = [(1, 1.0_f64.to_bits(), "value")];

        assert_ne!(
            canonical_result_artifact(Arc::clone(&first_schema), &[batch(first_schema, &rows)?])?,
            canonical_result_artifact(
                Arc::clone(&renamed_schema),
                &[batch(renamed_schema, &rows)?]
            )?
        );
        Ok(())
    }

    #[test]
    fn canonical_artifact_handles_dictionary_columns() -> anyhow::Result<()> {
        let (first_schema, first_batch) = dictionary_batch(&["beta", "alpha", "beta", "alpha"])?;
        let (second_schema, second_batch) = dictionary_batch(&["alpha", "beta", "alpha", "beta"])?;

        assert_eq!(first_schema, second_schema);
        let directory = tempdir()?;
        let path = directory.path().join("result.arrow");
        write_result_artifact(&path, first_schema, &[first_batch])?;
        verify_result_artifact(
            &path,
            second_schema,
            &[second_batch],
            ResultOrderPolicy::Multiset,
        )?;
        Ok(())
    }

    #[test]
    fn canonical_artifact_handles_run_end_encoded_columns() -> anyhow::Result<()> {
        let first_run_ends = Int32Array::from(vec![2, 4]);
        let first_values = StringArray::from(vec!["alpha", "beta"]);
        let first = RunArray::<Int32Type>::try_new(&first_run_ends, &first_values)?;

        let second_run_ends = Int32Array::from(vec![1, 2, 4]);
        let second_values = StringArray::from(vec!["alpha", "alpha", "beta"]);
        let second = RunArray::<Int32Type>::try_new(&second_run_ends, &second_values)?;

        let (first_schema, first_batch) = single_column_batch(Arc::new(first))?;
        let (second_schema, second_batch) = single_column_batch(Arc::new(second))?;
        assert_eq!(first_schema, second_schema);
        assert_eq!(
            canonical_result_artifact(first_schema, &[first_batch])?,
            canonical_result_artifact(second_schema, &[second_batch])?
        );
        Ok(())
    }

    #[test]
    fn write_refuses_to_overwrite_a_v1_baseline() -> anyhow::Result<()> {
        let directory = tempdir()?;
        let path = directory.path().join("result.arrow");
        let result_schema = schema();
        let rows = [(1, 1.0_f64.to_bits(), "original")];
        let batches = vec![batch(Arc::clone(&result_schema), &rows)?];
        write_result_artifact(&path, Arc::clone(&result_schema), &batches)?;
        verify_result_artifact(
            &path,
            Arc::clone(&result_schema),
            &batches,
            ResultOrderPolicy::Ordered,
        )?;
        let original = fs::read(&path)?;

        let Err(error) = write_result_artifact(&path, result_schema, &batches) else {
            anyhow::bail!("overwriting an existing V1 baseline unexpectedly succeeded");
        };
        assert!(error.to_string().contains("refusing to overwrite"));
        assert_eq!(fs::read(path)?, original);
        Ok(())
    }

    #[test]
    fn provenance_must_be_v1_and_well_formed() -> anyhow::Result<()> {
        const MAGIC_LEN: usize = b"VORTEX-RESULT\0".len();
        const PRODUCER_OFFSET: usize = MAGIC_LEN + 4 + 8;
        const VERIFIER_OFFSET: usize = PRODUCER_OFFSET + 2 + 8;

        let directory = tempdir()?;
        let path = directory.path().join("result.arrow");
        let result_schema = schema();
        let rows = [(1, 1.0_f64.to_bits(), "value")];
        let batches = vec![batch(Arc::clone(&result_schema), &rows)?];

        let mut wrong_backend = canonical_result_artifact(Arc::clone(&result_schema), &batches)?;
        wrong_backend[PRODUCER_OFFSET..PRODUCER_OFFSET + 2].copy_from_slice(b"xx");
        fs::write(&path, wrong_backend)?;
        let Err(error) = verify_result_artifact(
            &path,
            Arc::clone(&result_schema),
            &batches,
            ResultOrderPolicy::Multiset,
        ) else {
            anyhow::bail!("non-V1 artifact provenance unexpectedly verified");
        };
        assert!(format!("{error:#}").contains("expected a v1 baseline"));

        let mut wrong_verifier = canonical_result_artifact(Arc::clone(&result_schema), &batches)?;
        wrong_verifier[VERIFIER_OFFSET..VERIFIER_OFFSET + 13].copy_from_slice(b"morsel-push!!");
        fs::write(&path, wrong_verifier)?;
        let Err(error) = verify_result_artifact(
            &path,
            Arc::clone(&result_schema),
            &batches,
            ResultOrderPolicy::Multiset,
        ) else {
            anyhow::bail!("wrong verifier provenance unexpectedly verified");
        };
        assert!(format!("{error:#}").contains("expected push-frontier"));

        let mut malformed = canonical_result_artifact(Arc::clone(&result_schema), &batches)?;
        malformed[MAGIC_LEN..MAGIC_LEN + 4].copy_from_slice(&99_u32.to_le_bytes());
        fs::write(&path, malformed)?;
        let Err(error) =
            verify_result_artifact(&path, result_schema, &batches, ResultOrderPolicy::Multiset)
        else {
            anyhow::bail!("malformed artifact provenance unexpectedly verified");
        };
        assert!(format!("{error:#}").contains("unsupported canonical result format version"));
        Ok(())
    }

    #[test]
    fn artifact_execution_requires_the_intended_scan_path() -> anyhow::Result<()> {
        let default_backend = effective_scan_backend(None)?;
        assert_eq!(default_backend, EffectiveScanBackend::V1);
        validate_result_artifact_execution(ResultArtifactOperation::Write, default_backend, false)?;
        assert!(
            validate_result_artifact_execution(
                ResultArtifactOperation::Verify,
                default_backend,
                false
            )
            .is_err()
        );

        let explicit_v1 = effective_scan_backend(Some("v1"))?;
        assert_eq!(explicit_v1, EffectiveScanBackend::V1);
        validate_result_artifact_execution(ResultArtifactOperation::Write, explicit_v1, false)?;
        assert!(
            validate_result_artifact_execution(ResultArtifactOperation::Verify, explicit_v1, false)
                .is_err()
        );

        let push = effective_scan_backend(Some("push"))?;
        assert_eq!(push, EffectiveScanBackend::Push);
        assert!(
            validate_result_artifact_execution(ResultArtifactOperation::Write, push, false)
                .is_err()
        );
        assert!(
            validate_result_artifact_execution(ResultArtifactOperation::Verify, push, false)
                .is_err()
        );

        let frontier = effective_scan_backend(Some("push-frontier"))?;
        assert_eq!(frontier, EffectiveScanBackend::PushFrontier);
        validate_result_artifact_execution(ResultArtifactOperation::Verify, frontier, false)?;
        assert!(
            validate_result_artifact_execution(ResultArtifactOperation::Write, frontier, false)
                .is_err()
        );

        assert!(
            validate_result_artifact_execution(
                ResultArtifactOperation::Write,
                EffectiveScanBackend::V1,
                true
            )
            .is_err()
        );
        assert!(
            validate_result_artifact_execution(
                ResultArtifactOperation::Verify,
                EffectiveScanBackend::PushFrontier,
                true
            )
            .is_err()
        );
        Ok(())
    }
}
