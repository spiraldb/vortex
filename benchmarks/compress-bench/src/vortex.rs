// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::io::Cursor;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use anyhow::Result;
use anyhow::bail;
use async_trait::async_trait;
use bytes::Bytes;
use clap::ValueEnum;
use futures::StreamExt;
use futures::pin_mut;
use tempfile::NamedTempFile;
use vortex::array::IntoArray;
use vortex::dtype::FieldNames;
use vortex::expr::root;
use vortex::expr::select;
use vortex::file::OpenOptionsSessionExt;
use vortex::file::VortexFile;
use vortex::file::WriteOptionsSessionExt;
use vortex::io::session::RuntimeSessionExt;
use vortex::layout::scan::v2::ScanBuilder;
use vortex_arrow::ArrowSessionExt;
use vortex_bench::Format;
use vortex_bench::SESSION;
use vortex_bench::compress::Compressed;
use vortex_bench::compress::CompressedData;
use vortex_bench::compress::Compressor;
use vortex_bench::compress::Uncompressed;
use vortex_bench::compress::read_projection;
use vortex_bench::conversions::parquet_to_vortex_chunks;
use vortex_bench::runtime::open_local;

/// Where Vortex decompression reads the compressed file from.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, ValueEnum)]
pub enum VortexSource {
    /// The in-memory file image, as Parquet decompression reads its own.
    #[default]
    Memory,
    /// A temporary file on local disk, read with io_uring on the io_uring runtime and on the
    /// blocking pool otherwise. After the first iteration the reads hit the page cache.
    File,
}

/// Compressor implementation for Vortex format.
pub struct VortexCompressor {
    source: VortexSource,
}

impl VortexCompressor {
    /// A compressor whose decompression reads from `source`.
    pub fn new(source: VortexSource) -> Self {
        Self { source }
    }
}

#[async_trait]
impl Compressor for VortexCompressor {
    fn format(&self) -> Format {
        Format::OnDiskVortex
    }

    async fn load(&self, parquet_path: &Path) -> Result<Uncompressed> {
        let chunks = parquet_to_vortex_chunks(parquet_path.to_path_buf()).await?;
        Ok(Uncompressed::Vortex(chunks.into_array()))
    }

    async fn compress(&self, input: &Uncompressed) -> Result<Compressed> {
        let array = input.vortex()?.clone();

        // Run the timed work as a task on the session's runtime so it uses only that runtime's
        // worker threads: the Tokio pool the Parquet compressor runs on, or as many io_uring
        // workers.
        let start = Instant::now();
        let buf = SESSION
            .handle()
            .spawn(async move {
                let mut buf = Vec::new();
                SESSION
                    .write_options()
                    .write(&mut Cursor::new(&mut buf), array.to_array_stream())
                    .await?;
                anyhow::Ok(buf)
            })
            .await?;
        let elapsed = start.elapsed();

        let size = buf.len() as u64;
        let data = match self.source {
            VortexSource::Memory => CompressedData::Bytes(Bytes::from(buf)),
            VortexSource::File => {
                let mut file = NamedTempFile::new()?;
                file.write_all(&buf)?;
                file.flush()?;
                CompressedData::File(file)
            }
        };
        Ok(Compressed {
            size,
            data,
            elapsed,
        })
    }

    async fn decompress(&self, compressed: &Compressed) -> Result<Duration> {
        let source = match &compressed.data {
            CompressedData::Bytes(data) => Source::Memory(data.clone()),
            CompressedData::File(file) => Source::File(file.path().to_path_buf()),
            CompressedData::Dir(_) => bail!("Vortex decompression expects a single file"),
        };

        let start = Instant::now();
        SESSION
            .handle()
            .spawn(vortex_decompress_read(source))
            .await?;
        Ok(start.elapsed())
    }
}

/// A compressed Vortex file to scan.
enum Source {
    Memory(Bytes),
    File(PathBuf),
}

impl Source {
    async fn open(self) -> Result<VortexFile> {
        Ok(match self {
            Source::Memory(data) => SESSION.open_options().open_buffer(data)?,
            Source::File(path) => SESSION.open_options().open(open_local(path)?).await?,
        })
    }
}

/// Scan every record batch out of a Vortex file.
async fn vortex_decompress_read(source: Source) -> Result<()> {
    let file = source.open().await?;
    let mut scan = ScanBuilder::new(SESSION.clone(), file.scan_file());
    let source_dtype = scan.dtype().clone();
    let root_columns = source_dtype
        .as_struct_fields_opt()
        .map_or(0, |fields| fields.nfields());
    if let Some(cols) = read_projection(root_columns) {
        // Columns are named "0".."num_columns-1"; project the given subset.
        let names: FieldNames = cols.iter().map(|i| i.to_string()).collect();
        let projection = select(names, root())
            .bind(&source_dtype)?
            .optimize_recursive()?;
        scan = scan.with_projection(projection);
    }
    let schema = Arc::new(SESSION.arrow().to_arrow_schema(scan.dtype())?);

    let stream = scan.into_record_batch_stream(schema)?;
    pin_mut!(stream);

    while let Some(batch) = stream.next().await {
        let _batch = batch?;
    }
    Ok(())
}
