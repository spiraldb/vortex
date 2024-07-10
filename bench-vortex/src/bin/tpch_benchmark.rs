use std::sync::Arc;

use arrow_array::StructArray;
use datafusion::datasource::MemTable;
use datafusion::prelude::{CsvReadOptions, SessionContext};
use vortex::array::chunked::ChunkedArray;
use vortex::arrow::FromArrowArray;
use vortex::{Array, ArrayDType, ArrayData, IntoArray, IntoCanonical};
use vortex_datafusion::SessionContextExt;

/// Create a new TPC-H benchmark.

mod tpch {
    pub mod schemas {
        use arrow_schema::{DataType, Field, Schema};
        use lazy_static::lazy_static;

        lazy_static! {
            pub static ref CUSTOMER: Schema = Schema::new(vec![
                Field::new("c_custkey", DataType::Int64, false),
                Field::new("c_name", DataType::Utf8, false),
                Field::new("c_address", DataType::Int64, false),
                Field::new("c_nationkey", DataType::Int64, false),
                Field::new("c_phone", DataType::Utf8, false),
                Field::new("c_acctbal", DataType::Float64, false),
                Field::new("c_mktsegment", DataType::Utf8, false),
                Field::new("c_comment", DataType::Utf8, false),
            ]);

            pub static ref LINEITEM: Schema = Schema::new(vec![
                Field::new("l_orderkey", DataType::Int64, false),
                Field::new("l_partkey", DataType::Int64, false),
                Field::new("l_suppkey", DataType::Int64, false),
                Field::new("l_linenumber", DataType::Int64, false),
                Field::new("l_quantity", DataType::Float64, false),
                Field::new("l_extendedprice", DataType::Float64, false),
                Field::new("l_discount", DataType::Float64, false),
                Field::new("l_tax", DataType::Float64, false),
                Field::new("l_returnflag", DataType::Utf8, false),
                Field::new("l_linestatus", DataType::Utf8, false),
                // NOTE: Arrow doesn't have a DATE type, but YYYY-MM-DD is lexicographically ordered
                //  so we can just use Utf8 and adjust any queries that rely on date functions.
                Field::new("l_shipdate", DataType::Utf8, false),
                Field::new("l_commitdate", DataType::Utf8, false),
                Field::new("l_receiptdate", DataType::Utf8, false),
                Field::new("l_shipinstruct", DataType::Utf8, false),
                Field::new("l_shipmode", DataType::Utf8, false),
                Field::new("l_comment", DataType::Utf8, false),
            ]);

            pub static ref NATION_SCHEMA: Schema = Schema::new(vec![
                Field::new("n_nationkey", DataType::Int64, false),
                Field::new("n_name", DataType::Utf8, false),
                Field::new("n_regionkey", DataType::Int64, false),
                Field::new("n_comment", DataType::Utf8, true),
            ]);
        }
    }

    pub mod queries {
        pub const Q1: &'static str = r#"
            select
                l_returnflag,
                l_linestatus,
                sum(l_quantity) as sum_qty,
                sum(l_extendedprice) as sum_base_price,
                sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
                sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
                avg(l_quantity) as avg_qty,
                avg(l_extendedprice) as avg_price,
                avg(l_discount) as avg_disc,
                count(*) as count_order
            from
                lineitem
            where
                l_shipdate <= '1998-11-30'
            group by
                l_returnflag,
                l_linestatus
            order by
                l_returnflag,
                l_linestatus
            LIMIT 1;
            "#;
    }
}

async fn q1_csv() {
    let ctx = SessionContext::new();

    println!("loading CSV");
    ctx.register_csv(
        "lineitem",
        "bench-vortex/data/tpch/lineitem.tbl",
        CsvReadOptions::default()
            .file_extension("tbl")
            .has_header(false)
            .delimiter(b'|')
            .schema(&tpch::schemas::LINEITEM),
    )
    .await
    .unwrap();

    println!("BEGIN: Q1(CSV)");
    ctx.sql(tpch::queries::Q1)
        .await
        .unwrap()
        .show()
        .await
        .unwrap();
}

async fn q1_arrow() {
    let ctx = SessionContext::new();

    println!("reading CSV");
    let batches = ctx
        .read_csv(
            "bench-vortex/data/tpch/lineitem.tbl",
            CsvReadOptions::default()
                .file_extension("tbl")
                .has_header(false)
                .delimiter(b'|')
                .schema(&tpch::schemas::LINEITEM),
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    println!("loading from CSV to arrow batches");

    let arrow_table =
        MemTable::try_new(Arc::new(tpch::schemas::LINEITEM.clone()), vec![batches]).unwrap();

    println!("registering table");
    // Read the lineitem table directly into memory here.
    ctx.register_table("lineitem", Arc::new(arrow_table))
        .unwrap();

    println!("BEGIN: Q1(VORTEX)");
    ctx.sql(tpch::queries::Q1)
        .await
        .unwrap()
        .show()
        .await
        .unwrap();
}

async fn q1_vortex() {
    let ctx = SessionContext::new();

    println!("reading CSV");
    let batches = ctx
        .read_csv(
            "bench-vortex/data/tpch/lineitem.tbl",
            CsvReadOptions::default()
                .file_extension("tbl")
                .has_header(false)
                .delimiter(b'|')
                .schema(&tpch::schemas::LINEITEM),
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    println!("loading from CSV to arrow batches");
    let arrays = batches
        .iter()
        .map(|batch| ArrayData::from_arrow(&StructArray::from(batch.clone()), false).into_array())
        .collect::<Vec<Array>>();

    let dtype = arrays[0].dtype().clone();
    let array = ChunkedArray::try_new(arrays, dtype)
        .unwrap()
        .into_canonical()
        .unwrap()
        .into_array();

    println!("registering table");
    // Read the lineitem table directly into memory here.
    ctx.register_vortex("lineitem", array).unwrap();

    println!("BEGIN: Q1(VORTEX)");
    ctx.sql(tpch::queries::Q1)
        .await
        .unwrap()
        .show()
        .await
        .unwrap();
}

#[tokio::main]
async fn main() {
    let csv_start = std::time::SystemTime::now();
    q1_csv().await;
    let csv_duration = csv_start.elapsed().unwrap().as_secs();
    println!("CSV: {csv_duration}s");

    let arrow_start = std::time::SystemTime::now();
    q1_arrow().await;
    let arrow_duration = arrow_start.elapsed().unwrap().as_secs();
    println!("ARROW: {arrow_duration}s");

    let vortex_start = std::time::SystemTime::now();
    q1_vortex().await;
    let vortex_duration = vortex_start.elapsed().unwrap().as_secs();
    println!("VORTEX: {vortex_duration}s");
}
