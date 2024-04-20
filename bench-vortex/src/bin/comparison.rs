use bench_vortex::data_downloads::{BenchmarkDataset, FileType};
use bench_vortex::parquet_utils::sum_column_chunk_sizes;
use bench_vortex::public_bi_data::BenchmarkDatasets::PBI;
use bench_vortex::public_bi_data::PBIDataset;
use bench_vortex::setup_logger;
use bench_vortex::vortex_utils::vortex_chunk_sizes;
use csv::Writer;
use itertools::Itertools;
use log::LevelFilter;

pub fn main() {
    setup_logger(LevelFilter::Info);
    export_comparison_info(PBIDataset::Medicare1);
}

fn export_comparison_info(which_pbi: PBIDataset) {
    let dataset = PBI(which_pbi);
    dataset.write_as_vortex();
    dataset.write_as_parquet();
    let comparison = dataset
        .list_files(FileType::Vortex)
        .into_iter()
        .flat_map(|file| {
            vortex_chunk_sizes(file.as_path())
                .unwrap()
                .to_results(which_pbi.dataset_name().to_string())
        })
        .chain(
            dataset
                .list_files(FileType::Parquet)
                .into_iter()
                .flat_map(|file| {
                    sum_column_chunk_sizes(file.as_path())
                        .unwrap()
                        .to_results(which_pbi.dataset_name().to_string())
                }),
        )
        .collect_vec();

    let mut writer =
        Writer::from_path(dataset.directory_location().join("comparison_results.csv")).unwrap();
    writer
        .write_record([
            "dataset_name",
            "file",
            "file_type",
            "column",
            "column_type",
            "total_compressed_size",
            "column_compressed_size",
        ])
        .unwrap();

    for result in comparison {
        let record: Vec<String> = vec![
            result.dataset_name,
            result.file_name,
            result.file_type.to_string(),
            result.column_name,
            result.column_type,
            result.total_compressed_size.unwrap_or(0).to_string(),
            result.compressed_size.to_string(),
        ];
        writer.write_record(&record).unwrap();
    }
}
