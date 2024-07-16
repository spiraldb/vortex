use std::time::SystemTime;

use bench_vortex::tpch::dbgen::{DBGen, DBGenOptions};
use bench_vortex::tpch::{load_datasets, tpch_query, Format};
use indicatif::ProgressBar;
use prettytable::{Cell, Row, Table};

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    // uncomment the below to enable trace logging of datafusion execution
    // setup_logger(LevelFilter::Trace);

    // Run TPC-H data gen.
    let data_dir = DBGen::new(DBGenOptions::default()).generate().unwrap();

    let formats = [
        Format::Csv,
        Format::Arrow,
        Format::Vortex {
            disable_pushdown: false,
        },
        Format::Vortex {
            disable_pushdown: true,
        },
    ];

    // Set up a results table
    let mut table = Table::new();
    let mut cells = vec![Cell::new("Query")];
    cells.extend(formats.iter().map(|f| Cell::new(&format!("{:?}", f))));
    table.add_row(Row::new(cells));

    // Setup a progress bar
    let progress = ProgressBar::new(22 * formats.len() as u64);

    for i in 1..=22 {
        // Skip query 15 as it is not supported by DataFusion
        if i == 15 {
            continue;
        }

        let query = tpch_query(i);
        let mut cells = Vec::with_capacity(formats.len());
        cells.push(Cell::new(&format!("Q{}", i)));
        for format in formats.iter() {
            let ctx = load_datasets(&data_dir, *format).await.unwrap();
            let start = SystemTime::now();
            ctx.sql(&query)
                .await
                .map_err(|e| println!("Failed to run {} {:?}: {}", i, format, e))
                .unwrap()
                .collect()
                .await
                .map_err(|e| println!("Failed to collect {} {:?}: {}", i, format, e))
                .unwrap();
            let elapsed = start.elapsed().unwrap();
            progress.inc(1);
            cells.push(Cell::new(&format!("{} us", elapsed.as_micros())));
        }
        table.add_row(Row::new(cells));
    }
    progress.clone().finish();
    table.printstd();
}
