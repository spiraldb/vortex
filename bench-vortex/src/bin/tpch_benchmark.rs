#![allow(dead_code)]
use std::path::PathBuf;
use std::time::SystemTime;

use bench_vortex::tpch::dbgen::{DBGen, DBGenOptions};
use bench_vortex::tpch::{load_datasets, tpch_query, Format};

async fn q1_csv(base_dir: &PathBuf) -> anyhow::Result<()> {
    let ctx = load_datasets(base_dir, Format::Csv).await?;
    let q1 = tpch_query(1);

    println!("BEGIN: Q1(CSV)");

    let start = SystemTime::now();
    ctx.sql(&q1).await?.show().await?;
    let elapsed = start.elapsed()?.as_millis();
    println!("END CSV: {elapsed}ms");

    Ok(())
}

async fn q1_arrow(base_dir: &PathBuf) -> anyhow::Result<()> {
    let ctx = load_datasets(base_dir, Format::Arrow).await?;
    let q1 = tpch_query(1);

    println!("BEGIN: Q1(ARROW)");
    let start = SystemTime::now();

    ctx.sql(&q1).await?.show().await?;
    let elapsed = start.elapsed()?.as_millis();

    println!("END ARROW: {elapsed}ms");

    Ok(())
}

async fn q1_vortex(base_dir: &PathBuf) -> anyhow::Result<()> {
    let ctx = load_datasets(
        base_dir,
        Format::Vortex {
            disable_pushdown: true,
        },
    )
    .await?;
    let q1 = tpch_query(1);

    println!("BEGIN: Q1(VORTEX)");
    let start = SystemTime::now();

    ctx.sql(&q1).await?.show().await?;

    let elapsed = start.elapsed()?.as_millis();
    println!("END VORTEX: {elapsed}ms");

    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    // uncomment the below to enable trace logging of datafusion execution
    // setup_logger(LevelFilter::Trace);

    // Run TPC-H data gen.
    let data_dir = DBGen::new(DBGenOptions::default()).generate().unwrap();

    q1_csv(&data_dir).await.unwrap();
    q1_arrow(&data_dir).await.unwrap();
    q1_vortex(&data_dir).await.unwrap();
}
