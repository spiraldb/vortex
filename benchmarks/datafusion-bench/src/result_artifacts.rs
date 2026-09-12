// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::fs;
use std::fs::OpenOptions;
use std::io::Cursor;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use anyhow::bail;
use arrow_row::RowConverter;
use arrow_row::SortField;
use clap::ValueEnum;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::reader::FileReader;
use datafusion::arrow::ipc::writer::FileWriter;

const ARTIFACT_MAGIC: &[u8] = b"VORTEX-RESULT\0";
const CANONICAL_FORMAT_VERSION: u32 = 3;
const V1_BACKEND: &str = "v1";
const PUSH_FRONTIER_BACKEND: &str = "push-frontier";

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
pub enum ResultOrderPolicy {
    /// Compare the logical SQL row multiset, ignoring physical output order.
    #[default]
    Multiset,
    /// Also require the final Arrow row sequence to match exactly.
    Ordered,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResultArtifactOperation {
    Write,
    Verify,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EffectiveScanBackend {
    V1,
    Push,
    PushFrontier,
}

pub fn effective_scan_backend(value: Option<&str>) -> anyhow::Result<EffectiveScanBackend> {
    match value {
        None | Some("v1") => Ok(EffectiveScanBackend::V1),
        Some("push" | "morsel-push") => Ok(EffectiveScanBackend::Push),
        Some("push-frontier" | "morsel-push-frontier") => Ok(EffectiveScanBackend::PushFrontier),
        Some(value) => {
            bail!("scan backend must be v1, push, or push-frontier; received {value:?}")
        }
    }
}

pub fn validate_result_artifact_execution(
    operation: ResultArtifactOperation,
    backend: EffectiveScanBackend,
    use_scan_api: bool,
) -> anyhow::Result<()> {
    if use_scan_api {
        bail!(
            "exact result artifacts require the persistent scan path; unset VORTEX_USE_SCAN_API or set it to a value other than 1"
        );
    }

    match (operation, backend) {
        (ResultArtifactOperation::Write, EffectiveScanBackend::V1)
        | (ResultArtifactOperation::Verify, EffectiveScanBackend::PushFrontier) => Ok(()),
        (ResultArtifactOperation::Write, backend) => {
            bail!("result artifact writes require the v1 backend, got {backend:?}")
        }
        (ResultArtifactOperation::Verify, backend) => {
            bail!(
                "result artifact verification requires the push-frontier backend, got {backend:?}"
            )
        }
    }
}

/// Serialize a V1 query result with schema-aware sequence and multiset views.
///
/// The artifact contains a deterministic schema-only Arrow IPC file, the original
/// length-prefixed Arrow row sequence, and a sorted copy of the same row encodings.
/// Keeping the row encodings instead of decoding them back into arrays preserves
/// Dictionary and RunEndEncoded result schemas while still comparing their logical
/// scalar values. Duplicate rows and exact IEEE-754 representations, including NaN
/// payloads and signed zeroes, are retained.
pub fn canonical_result_artifact(
    schema: SchemaRef,
    batches: &[RecordBatch],
) -> anyhow::Result<Vec<u8>> {
    serialize_result_artifact(schema, batches)
}

fn serialize_result_artifact(
    schema: SchemaRef,
    batches: &[RecordBatch],
) -> anyhow::Result<Vec<u8>> {
    for (batch_idx, batch) in batches.iter().enumerate() {
        if batch.schema().as_ref() != schema.as_ref() {
            bail!(
                "record batch {batch_idx} schema does not match the query result schema: \
                 batch={:?}, result={:?}",
                batch.schema(),
                schema
            );
        }
    }

    let row_count = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
    let mut encoded_rows = if schema.fields().is_empty() {
        vec![Vec::new(); row_count]
    } else {
        let sort_fields = schema
            .fields()
            .iter()
            .map(|field| SortField::new(field.data_type().clone()))
            .collect();
        let converter = RowConverter::new(sort_fields)?;
        let mut rows = converter.empty_rows(row_count, 0);
        for batch in batches {
            converter.append(&mut rows, batch.columns())?;
        }

        rows.iter()
            .map(|row| row.data().to_vec())
            .collect::<Vec<_>>()
    };

    let schema_ipc = serialize_schema(&schema)?;
    let mut artifact = Vec::new();
    artifact.extend_from_slice(ARTIFACT_MAGIC);
    artifact.extend_from_slice(&CANONICAL_FORMAT_VERSION.to_le_bytes());
    append_len_prefixed(&mut artifact, V1_BACKEND.as_bytes())?;
    append_len_prefixed(&mut artifact, PUSH_FRONTIER_BACKEND.as_bytes())?;
    append_len_prefixed(&mut artifact, &schema_ipc)?;
    artifact.extend_from_slice(&u64::try_from(row_count)?.to_le_bytes());
    for row in &encoded_rows {
        append_len_prefixed(&mut artifact, row)?;
    }
    encoded_rows.sort_unstable();
    for row in &encoded_rows {
        append_len_prefixed(&mut artifact, row)?;
    }
    Ok(artifact)
}

fn serialize_schema(schema: &SchemaRef) -> anyhow::Result<Vec<u8>> {
    let mut schema_ipc = Vec::new();
    {
        let mut writer = FileWriter::try_new(&mut schema_ipc, schema.as_ref())?;
        writer.finish()?;
    }
    Ok(schema_ipc)
}

fn append_len_prefixed(output: &mut Vec<u8>, bytes: &[u8]) -> anyhow::Result<()> {
    output.extend_from_slice(&u64::try_from(bytes.len())?.to_le_bytes());
    output.extend_from_slice(bytes);
    Ok(())
}

/// Write a canonical V1 result artifact without overwriting an existing baseline.
pub fn write_result_artifact(
    path: &Path,
    schema: SchemaRef,
    batches: &[RecordBatch],
) -> anyhow::Result<()> {
    let artifact = serialize_result_artifact(schema, batches)?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create result artifact directory {parent:?}"))?;
    }

    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .with_context(|| {
            format!(
                "refusing to overwrite result artifact {}; use a new directory or remove the old baseline explicitly",
                path.display()
            )
        })?;
    file.write_all(&artifact)
        .with_context(|| format!("failed to write result artifact {}", path.display()))
}

/// Verify a query result exactly against a V1 artifact.
pub fn verify_result_artifact(
    path: &Path,
    schema: SchemaRef,
    batches: &[RecordBatch],
    order_policy: ResultOrderPolicy,
) -> anyhow::Result<()> {
    let expected = fs::read(path)
        .with_context(|| format!("failed to read result artifact {}", path.display()))?;
    let metadata = parse_result_artifact(&expected)
        .with_context(|| format!("invalid result artifact {}", path.display()))?;

    if metadata.producer_backend != V1_BACKEND {
        bail!(
            "result artifact {} was produced by backend {:?}, expected a v1 baseline",
            path.display(),
            metadata.producer_backend
        );
    }
    if metadata.verifier_backend != PUSH_FRONTIER_BACKEND {
        bail!(
            "result artifact {} expects verifier backend {:?}, expected push-frontier",
            path.display(),
            metadata.verifier_backend
        );
    }
    if metadata.schema.as_ref() != schema.as_ref() {
        bail!(
            "result schema mismatch for {}: expected {:?}, got {:?}",
            path.display(),
            metadata.schema,
            schema
        );
    }

    let actual = canonical_result_artifact(Arc::clone(&schema), batches)?;
    let actual_metadata = parse_result_artifact(&actual)?;
    if metadata.multiset_rows != actual_metadata.multiset_rows {
        let actual_rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
        let first_difference = metadata
            .multiset_rows
            .iter()
            .zip(&actual_metadata.multiset_rows)
            .position(|(expected, actual)| expected != actual)
            .unwrap_or_else(|| {
                metadata
                    .multiset_rows
                    .len()
                    .min(actual_metadata.multiset_rows.len())
            });
        let expected_row = metadata.multiset_rows.get(first_difference);
        let actual_row = actual_metadata.multiset_rows.get(first_difference);
        bail!(
            "result row-multiset mismatch for {}: expected {} rows, got {actual_rows} rows; \
             first difference at canonical row {first_difference}: expected bytes {expected_row:?}, \
             got {actual_row:?}",
            path.display(),
            metadata.row_count
        );
    }
    if order_policy == ResultOrderPolicy::Ordered
        && metadata.sequence_rows != actual_metadata.sequence_rows
    {
        bail!("result row-sequence mismatch for {}", path.display());
    }

    Ok(())
}

struct ArtifactMetadata<'a> {
    producer_backend: String,
    verifier_backend: String,
    schema: SchemaRef,
    row_count: usize,
    sequence_rows: Vec<&'a [u8]>,
    multiset_rows: Vec<&'a [u8]>,
}

fn parse_result_artifact(artifact: &[u8]) -> anyhow::Result<ArtifactMetadata<'_>> {
    let mut decoder = ArtifactDecoder::new(artifact);
    if decoder.take(ARTIFACT_MAGIC.len())? != ARTIFACT_MAGIC {
        bail!("invalid artifact magic");
    }

    let version = decoder.read_u32()?;
    if version != CANONICAL_FORMAT_VERSION {
        bail!(
            "unsupported canonical result format version {version}; expected {CANONICAL_FORMAT_VERSION}"
        );
    }

    let producer_backend = std::str::from_utf8(decoder.read_len_prefixed()?)
        .context("producer backend provenance is not valid UTF-8")?
        .to_owned();
    let verifier_backend = std::str::from_utf8(decoder.read_len_prefixed()?)
        .context("verifier backend provenance is not valid UTF-8")?
        .to_owned();
    let schema_ipc = decoder.read_len_prefixed()?;
    let mut schema_reader = FileReader::try_new(Cursor::new(schema_ipc), None)
        .context("schema provenance is not valid Arrow IPC")?;
    let schema = schema_reader.schema();
    if schema_reader.next().is_some() {
        bail!("schema provenance unexpectedly contains record batches");
    }

    let row_count = usize::try_from(decoder.read_u64()?)?;
    let mut sequence_rows = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        sequence_rows.push(decoder.read_len_prefixed()?);
    }
    let mut multiset_rows = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        multiset_rows.push(decoder.read_len_prefixed()?);
    }
    if !multiset_rows.is_sorted() {
        bail!("artifact multiset rows are not in canonical order");
    }
    if !decoder.is_empty() {
        bail!("artifact contains trailing bytes");
    }

    Ok(ArtifactMetadata {
        producer_backend,
        verifier_backend,
        schema,
        row_count,
        sequence_rows,
        multiset_rows,
    })
}

struct ArtifactDecoder<'a> {
    remaining: &'a [u8],
}

impl<'a> ArtifactDecoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { remaining: bytes }
    }

    fn take(&mut self, len: usize) -> anyhow::Result<&'a [u8]> {
        if len > self.remaining.len() {
            bail!(
                "truncated artifact: requested {len} bytes with only {} remaining",
                self.remaining.len()
            );
        }
        let (taken, remaining) = self.remaining.split_at(len);
        self.remaining = remaining;
        Ok(taken)
    }

    fn read_u32(&mut self) -> anyhow::Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into()?))
    }

    fn read_u64(&mut self) -> anyhow::Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into()?))
    }

    fn read_len_prefixed(&mut self) -> anyhow::Result<&'a [u8]> {
        let len = usize::try_from(self.read_u64()?)?;
        self.take(len)
    }

    fn is_empty(&self) -> bool {
        self.remaining.is_empty()
    }
}
