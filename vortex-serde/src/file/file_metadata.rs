use crate::file::column_metadata::ColumnMetadata;

#[allow(dead_code)]
#[derive(Debug)]
pub struct FileMetadata {
    column_metas: Vec<ColumnMetadata>,
    row_boundaries: Vec<u64>,
}

impl FileMetadata {}
