use std::fs::File;
use std::os::unix::prelude::MetadataExt;
use std::path::Path;

use vortex::array::chunked::ChunkedArray;
use vortex::array::struct_::StructArray;
use vortex::ArrayDType;
use vortex_dtype::DType;
use vortex_error::VortexResult;

use crate::data_downloads::FileType;
use crate::reader::open_vortex;
use crate::CompressionRunStats;

pub fn vortex_chunk_sizes(path: &Path) -> VortexResult<CompressionRunStats> {
    let file = File::open(path)?;
    let total_compressed_size = file.metadata()?.size();
    let vortex = open_vortex(path)?;
    let DType::Struct(st, _) = vortex.dtype() else {
        unreachable!()
    };

    let mut compressed_sizes = vec![0; st.names().len()];
    let chunked_array = ChunkedArray::try_from(vortex).unwrap();
    for chunk in chunked_array.chunks() {
        let struct_arr = StructArray::try_from(chunk).unwrap();
        for (i, f) in (0..struct_arr.nfields()).map(|i| (i, struct_arr.field(i).unwrap())) {
            compressed_sizes[i] += f.nbytes() as u64;
        }
    }

    let stats = CompressionRunStats {
        schema: chunked_array.dtype().clone(),
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
