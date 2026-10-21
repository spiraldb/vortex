// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! TPC-DS data generation using the pure-Rust `tpcdsgen` crate.

use std::fs;
use std::fs::File;
use std::path::Path;
use std::path::PathBuf;

use anyhow::Result;
use arrow_array::RecordBatchReader;
use futures::StreamExt;
use futures::stream;
use parquet::arrow::ArrowWriter;
use tpcdsgen::config::CompatMode;
use tpcdsgen::config::Session;
use tpcdsgen::config::SessionBuilder;
use tpcdsgen_arrow::CallCenterArrow;
use tpcdsgen_arrow::CatalogPageArrow;
use tpcdsgen_arrow::CatalogReturnsArrow;
use tpcdsgen_arrow::CatalogSalesArrow;
use tpcdsgen_arrow::CustomerAddressArrow;
use tpcdsgen_arrow::CustomerArrow;
use tpcdsgen_arrow::CustomerDemographicsArrow;
use tpcdsgen_arrow::DateDimArrow;
use tpcdsgen_arrow::HouseholdDemographicsArrow;
use tpcdsgen_arrow::IncomeBandArrow;
use tpcdsgen_arrow::InventoryArrow;
use tpcdsgen_arrow::ItemArrow;
use tpcdsgen_arrow::PromotionArrow;
use tpcdsgen_arrow::ReasonArrow;
use tpcdsgen_arrow::ShipModeArrow;
use tpcdsgen_arrow::StoreArrow;
use tpcdsgen_arrow::StoreReturnsArrow;
use tpcdsgen_arrow::StoreSalesArrow;
use tpcdsgen_arrow::TimeDimArrow;
use tpcdsgen_arrow::WarehouseArrow;
use tpcdsgen_arrow::WebPageArrow;
use tpcdsgen_arrow::WebReturnsArrow;
use tpcdsgen_arrow::WebSalesArrow;
use tpcdsgen_arrow::WebSiteArrow;
use tracing::info;
use vortex::utils::parallelism::get_available_parallelism;

use crate::Format;
use crate::utils::file::idempotent;
use crate::utils::parquet::parquet_writer_properties;

const BATCH_SIZE: usize = 8192 * 16;

/// Matches the row group size of DuckDB's Parquet writer, which previously generated this data.
const ROW_GROUP_SIZE: usize = 122_880;

/// Generate every TPC-DS table as a single Parquet file per table under `base_dir/parquet`.
///
/// The files hold the same data and schema as `tpcgen-cli tpcds parquet` of the same `tpcdsgen`
/// version, which generates the SQL logic test data. Each table is generated on its own blocking
/// thread. Already generated tables are skipped.
pub async fn generate_tpcds(base_dir: PathBuf, scale_factor: String) -> Result<PathBuf> {
    let output_dir = base_dir.join(Format::Parquet.name());
    fs::create_dir_all(&output_dir)?;

    let scale_factor: f64 = scale_factor.parse()?;
    // Follow the C `dsdgen` reference rather than the Trino Java port, like `tpcgen-cli` does.
    let session = SessionBuilder::new()
        .with_scale_factor(scale_factor)
        .with_compat_mode(CompatMode::C)
        .build()?;

    let parallelism = get_available_parallelism().unwrap_or(1);
    let mut tasks = stream::iter(TABLES.map(|table| {
        let session = session.clone();
        let path = output_dir.join(format!("{table}.{}", Format::Parquet.ext()));
        tokio::task::spawn_blocking(move || {
            idempotent(&path, |tmp| {
                info!(table, scale_factor, "Generating TPC-DS table");
                write_parquet(tmp, table_batches(table, session))
            })
        })
    }))
    .buffer_unordered(parallelism);

    while let Some(result) = tasks.next().await {
        result??;
    }
    drop(tasks);

    Ok(output_dir)
}

const TABLES: [&str; 24] = [
    "call_center",
    "catalog_page",
    "catalog_returns",
    "catalog_sales",
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "household_demographics",
    "income_band",
    "inventory",
    "item",
    "promotion",
    "reason",
    "ship_mode",
    "store",
    "store_returns",
    "store_sales",
    "time_dim",
    "warehouse",
    "web_page",
    "web_returns",
    "web_sales",
    "web_site",
];

fn table_batches(table: &str, session: Session) -> Box<dyn RecordBatchReader + Send> {
    match table {
        "call_center" => Box::new(CallCenterArrow::new(session).with_batch_size(BATCH_SIZE)),
        "catalog_page" => Box::new(CatalogPageArrow::new(session).with_batch_size(BATCH_SIZE)),
        "catalog_returns" => {
            Box::new(CatalogReturnsArrow::new(session).with_batch_size(BATCH_SIZE))
        }
        "catalog_sales" => Box::new(CatalogSalesArrow::new(session).with_batch_size(BATCH_SIZE)),
        "customer" => Box::new(CustomerArrow::new(session).with_batch_size(BATCH_SIZE)),
        "customer_address" => {
            Box::new(CustomerAddressArrow::new(session).with_batch_size(BATCH_SIZE))
        }
        "customer_demographics" => {
            Box::new(CustomerDemographicsArrow::new(session).with_batch_size(BATCH_SIZE))
        }
        "date_dim" => Box::new(DateDimArrow::new(session).with_batch_size(BATCH_SIZE)),
        "household_demographics" => {
            Box::new(HouseholdDemographicsArrow::new(session).with_batch_size(BATCH_SIZE))
        }
        "income_band" => Box::new(IncomeBandArrow::new(session).with_batch_size(BATCH_SIZE)),
        "inventory" => Box::new(InventoryArrow::new(session).with_batch_size(BATCH_SIZE)),
        "item" => Box::new(ItemArrow::new(session).with_batch_size(BATCH_SIZE)),
        "promotion" => Box::new(PromotionArrow::new(session).with_batch_size(BATCH_SIZE)),
        "reason" => Box::new(ReasonArrow::new(session).with_batch_size(BATCH_SIZE)),
        "ship_mode" => Box::new(ShipModeArrow::new(session).with_batch_size(BATCH_SIZE)),
        "store" => Box::new(StoreArrow::new(session).with_batch_size(BATCH_SIZE)),
        "store_returns" => Box::new(StoreReturnsArrow::new(session).with_batch_size(BATCH_SIZE)),
        "store_sales" => Box::new(StoreSalesArrow::new(session).with_batch_size(BATCH_SIZE)),
        "time_dim" => Box::new(TimeDimArrow::new(session).with_batch_size(BATCH_SIZE)),
        "warehouse" => Box::new(WarehouseArrow::new(session).with_batch_size(BATCH_SIZE)),
        "web_page" => Box::new(WebPageArrow::new(session).with_batch_size(BATCH_SIZE)),
        "web_returns" => Box::new(WebReturnsArrow::new(session).with_batch_size(BATCH_SIZE)),
        "web_sales" => Box::new(WebSalesArrow::new(session).with_batch_size(BATCH_SIZE)),
        "web_site" => Box::new(WebSiteArrow::new(session).with_batch_size(BATCH_SIZE)),
        _ => unreachable!("unknown TPC-DS table {table}"),
    }
}

fn write_parquet(path: &Path, batches: Box<dyn RecordBatchReader + Send>) -> Result<()> {
    let props = parquet_writer_properties()
        .set_max_row_group_row_count(Some(ROW_GROUP_SIZE))
        .build();
    let mut writer = ArrowWriter::try_new(File::create(path)?, batches.schema(), Some(props))?;
    for batch in batches {
        writer.write(&batch?)?;
    }
    writer.close()?;
    Ok(())
}
