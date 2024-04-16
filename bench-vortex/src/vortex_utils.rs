use std::fs::File;
use std::os::unix::prelude::MetadataExt;
use std::path::Path;

use vortex::array::downcast::DowncastArrayBuiltin;
use vortex::array::Array;
use vortex_error::VortexResult;
use vortex_schema::DType;

use crate::data_downloads::FileType;
use crate::reader::open_vortex;
use crate::CompressionRunStats;

pub fn vortex_chunk_sizes(path: &Path) -> VortexResult<CompressionRunStats> {
    let file = File::open(path)?;
    let total_compressed_size = file.metadata()?.size();
    let vortex = open_vortex(path)?;
    let DType::Struct(ns, _) = vortex.dtype() else {
        unreachable!()
    };

    let mut compressed_sizes = vec![0; ns.len()];
    for chunk in vortex.as_chunked().chunks() {
        for (i, f) in chunk.as_struct().fields().iter().enumerate() {
            compressed_sizes[i] += f.nbytes() as u64;
        }
    }

    let stats = CompressionRunStats {
        schema: vortex.dtype().clone(),
        file_type: FileType::Vortex,
        total_compressed_size: Some(total_compressed_size),
        compressed_sizes,
        file_name: path
            .with_extension("")
            .file_name()
            .unwrap()
            .to_string_lossy()
            .to_string(),
    };
    Ok(stats)
}
