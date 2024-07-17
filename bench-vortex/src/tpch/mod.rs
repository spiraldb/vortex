use std::fs;
use std::path::Path;
use std::sync::Arc;

use arrow_array::StructArray;
use arrow_schema::Schema;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::datasource::MemTable;
use datafusion::prelude::{CsvReadOptions, ParquetReadOptions, SessionContext};
use futures::executor::block_on;
use vortex::array::chunked::ChunkedArray;
use vortex::arrow::FromArrowArray;
use vortex::{Array, ArrayDType, ArrayData, IntoArray};
use vortex_datafusion::{SessionContextExt, VortexMemTableOptions};

use crate::idempotent;

pub mod dbgen;
pub mod schema;

#[derive(Clone, Copy, Debug)]
pub enum Format {
    Csv,
    Arrow,
    Parquet,
    Vortex { disable_pushdown: bool },
}

// Generate table dataset.
pub async fn load_datasets<P: AsRef<Path>>(
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
                Format::Parquet => {
                    register_parquet(&context, stringify!($name), &$name, $schema).await
                }
                Format::Vortex {
                    disable_pushdown, ..
                } => {
                    register_vortex(
                        &context,
                        stringify!($name),
                        &$name,
                        $schema,
                        disable_pushdown,
                    )
                    .await
                }
            }
        };
    }

    register_table!(customer, &schema::CUSTOMER)?;
    register_table!(lineitem, &schema::LINEITEM)?;
    register_table!(nation, &schema::NATION)?;
    register_table!(orders, &schema::ORDERS)?;
    register_table!(part, &schema::PART)?;
    register_table!(partsupp, &schema::PARTSUPP)?;
    register_table!(region, &schema::REGION)?;
    register_table!(supplier, &schema::SUPPLIER)?;

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

    // Convert to StringView for all of the relevant columns
    let mem_table = MemTable::try_new(Arc::new(schema.clone()), vec![record_batches])?;
    session.register_table(name, Arc::new(mem_table))?;

    Ok(())
}

async fn register_parquet(
    session: &SessionContext,
    name: &str,
    file: &Path,
    schema: &Schema,
) -> anyhow::Result<()> {
    // Idempotent conversion from TPCH CSV to Parquet.
    let pq_file = idempotent(
        &file.with_extension("").with_extension("parquet"),
        |pq_file| {
            let df = block_on(
                session.read_csv(
                    file.to_str().unwrap(),
                    CsvReadOptions::default()
                        .delimiter(b'|')
                        .has_header(false)
                        .file_extension("tbl")
                        .schema(schema),
                ),
            )
            .unwrap();

            block_on(df.write_parquet(
                pq_file.as_os_str().to_str().unwrap(),
                DataFrameWriteOptions::default(),
                None,
            ))
        },
    )
    .unwrap();

    Ok(session
        .register_parquet(
            name,
            pq_file.as_os_str().to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await?)
}

async fn register_vortex(
    session: &SessionContext,
    name: &str,
    file: &Path,
    schema: &Schema,
    disable_pushdown: bool,
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
    let chunked_array = ChunkedArray::try_new(chunks, dtype)?.into_array();

    session.register_vortex_opts(
        name,
        chunked_array,
        VortexMemTableOptions::default().with_disable_pushdown(disable_pushdown),
    )?;

    Ok(())
}

pub fn tpch_queries() -> impl Iterator<Item = (usize, String)> {
    (1..=22)
        .filter(|q| {
            // Query 15 has multiple SQL statements so doesn't yet run in DataFusion.
            *q != 15
        })
        .map(|q| (q, tpch_query(q)))
}

pub fn tpch_query(query_idx: usize) -> String {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tpch")
        .join(format!("q{}.sql", query_idx));
    fs::read_to_string(manifest_dir).unwrap()
}

#[cfg(test)]
mod test {
    use crate::tpch::{load_datasets, tpch_query, Format};

    #[tokio::test]
    async fn test_schema() {
        println!("TEST START");
        let session_ctx = load_datasets(
            "/Volumes/Code/vortex/bench-vortex/data/tpch/1",
            Format::Arrow,
        )
        .await
        .unwrap();
        println!("DATA LOADED");

        // get access to session context
        let logical_plan = session_ctx
            .state()
            .create_logical_plan(&tpch_query(7))
            .await
            .unwrap();
        println!("LOGICAL PLAN: {}", logical_plan.display_indent());
        session_ctx
            .sql(&tpch_query(1))
            .await
            .unwrap()
            .show()
            .await
            .unwrap();
    }
}
