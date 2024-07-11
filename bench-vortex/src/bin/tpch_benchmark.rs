use std::path::PathBuf;
use std::time::SystemTime;

use bench_vortex::tpch;
use bench_vortex::tpch::dbgen::{DBGen, DBGenOptions};
use bench_vortex::tpch::{load_datasets, Format};

async fn q1_csv(base_dir: &PathBuf) -> anyhow::Result<()> {
    let ctx = load_datasets(base_dir, Format::Csv).await?;

    println!("BEGIN: Q1(CSV)");

    let start = SystemTime::now();
    ctx.sql(tpch::query::Q1).await?.show().await?;
    let elapsed = start.elapsed()?.as_millis();
    println!("END CSV: {elapsed}ms");

    Ok(())
}

async fn q1_arrow(base_dir: &PathBuf) -> anyhow::Result<()> {
    let ctx = load_datasets(base_dir, Format::Arrow).await?;

    println!("BEGIN: Q1(ARROW)");
    let start = SystemTime::now();

    ctx.sql(tpch::query::Q1).await?.show().await?;
    let elapsed = start.elapsed()?.as_millis();

    println!("END ARROW: {elapsed}ms");

    Ok(())
}

async fn q1_vortex(base_dir: &PathBuf) -> anyhow::Result<()> {
    let ctx = load_datasets(base_dir, Format::VortexUncompressed).await?;

    println!("BEGIN: Q1(VORTEX)");
    let start = SystemTime::now();

    ctx.sql(tpch::query::Q1).await?.show().await?;

    let elapsed = start.elapsed()?.as_millis();
    println!("END VORTEX: {elapsed}ms");

    Ok(())
}

#[tokio::main]
async fn main() {
    // Run TPC-H data gen.
    let data_dir = DBGen::new(DBGenOptions::default()).generate().unwrap();

    q1_csv(&data_dir).await.unwrap();
    q1_arrow(&data_dir).await.unwrap();
    q1_vortex(&data_dir).await.unwrap();
}
