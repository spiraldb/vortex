#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use arrow_array::StructArray;
use arrow_schema::Schema;
use bench_vortex::tpch;
use bench_vortex::tpch::dbgen::{DBGen, DBGenOptions};
use datafusion::datasource::MemTable;
use datafusion::prelude::{CsvReadOptions, SessionContext};
use vortex::array::chunked::ChunkedArray;
use vortex::arrow::FromArrowArray;
use vortex::{Array, ArrayDType, ArrayData, IntoArray, IntoCanonical};
use vortex_datafusion::SessionContextExt;

enum Format {
    Csv,
    Arrow,
    VortexUncompressed,
}

// Generate table dataset.
async fn load_datasets<P: AsRef<Path>>(
    base_dir: P,
    format: Format,
) -> anyhow::Result<SessionContext> {
    let context = SessionContext::new();
    let base_dir = base_dir.as_ref();

    let customer = base_dir.join("customer.tbl");
    let lineitem = base_dir.join("lineitem.tbl");
    let nation = base_dir.join("nation.tbl");
    let orders = base_dir.join("orders.tbl");
    let part = base_dir.join("part.tbl");
    let partsupp = base_dir.join("partsupp.tbl");
    let region = base_dir.join("region.tbl");
    let supplier = base_dir.join("supplier.tbl");

    macro_rules! register_table {
        ($name:ident, $schema:expr) => {
            match format {
                Format::Csv => register_csv(&context, stringify!($name), &$name, $schema).await,
                Format::Arrow => register_arrow(&context, stringify!($name), &$name, $schema).await,
                Format::VortexUncompressed => {
                    register_vortex(&context, stringify!($name), &$name, $schema).await
                }
            }
        };
    }

    register_table!(customer, &tpch::schema::CUSTOMER)?;
    register_table!(lineitem, &tpch::schema::LINEITEM)?;
    register_table!(nation, &tpch::schema::NATION)?;
    register_table!(orders, &tpch::schema::ORDERS)?;
    register_table!(part, &tpch::schema::PART)?;
    register_table!(partsupp, &tpch::schema::PARTSUPP)?;
    register_table!(region, &tpch::schema::REGION)?;
    register_table!(supplier, &tpch::schema::SUPPLIER)?;

    Ok(context)
}

async fn register_csv(
    session: &SessionContext,
    name: &str,
    file: &Path,
    schema: &Schema,
) -> anyhow::Result<()> {
    session
        .register_csv(
            name,
            file.to_str().unwrap(),
            CsvReadOptions::default()
                .delimiter(b'|')
                .has_header(false)
                .file_extension("tbl")
                .schema(schema),
        )
        .await?;

    Ok(())
}

async fn register_arrow(
    session: &SessionContext,
    name: &str,
    file: &Path,
    schema: &Schema,
) -> anyhow::Result<()> {
    // Read CSV file into a set of Arrow RecordBatch.
    let record_batches = session
        .read_csv(
            file.to_str().unwrap(),
            CsvReadOptions::default()
                .delimiter(b'|')
                .has_header(false)
                .file_extension("tbl")
                .schema(schema),
        )
        .await?
        .collect()
        .await?;

    let mem_table = MemTable::try_new(Arc::new(schema.clone()), vec![record_batches])?;
    session.register_table(name, Arc::new(mem_table))?;

    Ok(())
}

async fn register_vortex(
    session: &SessionContext,
    name: &str,
    file: &Path,
    schema: &Schema,
    // TODO(aduffy): add compression option
) -> anyhow::Result<()> {
    let record_batches = session
        .read_csv(
            file.to_str().unwrap(),
            CsvReadOptions::default()
                .delimiter(b'|')
                .has_header(false)
                .file_extension("tbl")
                .schema(schema),
        )
        .await?
        .collect()
        .await?;

    // Create a ChunkedArray from the set of chunks.
    let chunks: Vec<Array> = record_batches
        .iter()
        .cloned()
        .map(StructArray::from)
        .map(|struct_array| ArrayData::from_arrow(&struct_array, false).into_array())
        .collect();

    let dtype = chunks[0].dtype().clone();
    let chunked_array = ChunkedArray::try_new(chunks, dtype)?
        .into_canonical()?
        .into_array();

    session.register_vortex(name, chunked_array)?;

    Ok(())
}

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
