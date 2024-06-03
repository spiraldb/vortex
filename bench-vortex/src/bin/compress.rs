use std::os::unix::prelude::MetadataExt;
use std::path::PathBuf;

use bench_vortex::data_downloads::BenchmarkDataset;
use bench_vortex::public_bi_data::BenchmarkDatasets::PBI;
use bench_vortex::public_bi_data::PBIDataset;
use bench_vortex::reader::{open_vortex_async, rewrite_parquet_as_vortex};
use bench_vortex::taxi_data::taxi_data_parquet;
use bench_vortex::{setup_logger, IdempotentPath};
use log::{info, LevelFilter};
use tokio::fs::File;
use vortex_error::VortexResult;

#[tokio::main]
pub async fn main() {
    setup_logger(LevelFilter::Info);
    // compress_pbi(PBIDataset::Medicare1);
    compress_taxi().await.unwrap();
}

async fn compress_taxi() -> VortexResult<()> {
    let path: PathBuf = "taxi_data.vortex".to_data_path();
    let output_file = File::create(&path).await?;
    rewrite_parquet_as_vortex(taxi_data_parquet(), output_file).await?;

    let taxi_vortex = open_vortex_async(&path).await?;
    info!("{}", taxi_vortex.tree_display());

    let pq_size = taxi_data_parquet().metadata().unwrap().size();
    let vx_size = taxi_vortex.nbytes();

    info!("Parquet size: {}, Vortex size: {}", pq_size, vx_size);
    info!("Compression ratio: {}", vx_size as f32 / pq_size as f32);

    Ok(())
}

#[allow(dead_code)]
fn compress_pbi(which_pbi: PBIDataset) {
    let dataset = PBI(which_pbi);
    dataset.write_as_vortex();
    dataset.write_as_parquet();
    dataset.write_as_lance();
}
