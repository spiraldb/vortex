use std::time::SystemTime;

use bench_vortex::tpch::dbgen::{DBGen, DBGenOptions};
use bench_vortex::tpch::{load_datasets, tpch_query, Format};
use futures::future::join_all;
use indicatif::ProgressBar;
use itertools::Itertools;
use prettytable::{Cell, Row, Table};

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    // uncomment the below to enable trace logging of datafusion execution
    // setup_logger(LevelFilter::Trace);

    // Run TPC-H data gen.
    let data_dir = DBGen::new(DBGenOptions::default()).generate().unwrap();

    // The formats to run against (vs the baseline)
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

    // Load datasets
    let ctxs = join_all(
        formats
            .iter()
            .map(|format| load_datasets(&data_dir, *format)),
    )
    .await
    .into_iter()
    .map(|r| r.unwrap())
    .collect_vec();

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
        for (ctx, format) in ctxs.iter().zip(formats.iter()) {
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
