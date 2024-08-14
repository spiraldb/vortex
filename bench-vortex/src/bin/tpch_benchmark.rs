use std::collections::HashMap;
use std::process::ExitCode;
use std::sync;
use std::time::SystemTime;

use bench_vortex::tpch::dbgen::{DBGen, DBGenOptions};
use bench_vortex::tpch::{load_datasets, tpch_queries, Format, EXPECTED_ROW_COUNTS};
use clap::Parser;
use futures::future::try_join_all;
use indicatif::ProgressBar;
use itertools::Itertools;
use prettytable::{Cell, Row, Table};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, value_delimiter = ',')]
    queries: Option<Vec<usize>>,
    #[arg(short, long)]
    threads: Option<usize>,
}

fn main() -> ExitCode {
    let args = Args::parse();

    let runtime = match args.threads {
        Some(0) => panic!("Can't use 0 threads for runtime"),
        Some(1) => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build(),
        Some(n) => tokio::runtime::Builder::new_multi_thread()
            .worker_threads(n)
            .enable_all()
            .build(),
        None => tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build(),
    }
    .expect("Failed building the Runtime");

    runtime.block_on(bench_main(args.queries))
}

async fn bench_main(queries: Option<Vec<usize>>) -> ExitCode {
    // uncomment the below to enable trace logging of datafusion execution
    // setup_logger(LevelFilter::Trace);

    // Run TPC-H data gen.
    let data_dir = DBGen::new(DBGenOptions::default()).generate().unwrap();

    // The formats to run against (vs the baseline)
    let formats = [
        Format::Arrow,
        Format::Parquet,
        Format::InMemoryVortex {
            enable_pushdown: true,
        },
        Format::OnDiskVortex {
            enable_compression: true,
        },
        Format::OnDiskVortex {
            enable_compression: false,
        },
    ];

    // Load datasets
    let ctxs = try_join_all(formats.map(|format| load_datasets(&data_dir, format)))
        .await
        .unwrap();

    // Set up a results table
    let mut table = Table::new();
    {
        let mut cells = vec![Cell::new("Query")];
        cells.extend(formats.iter().map(|f| Cell::new(&format!("{:?}", f))));
        table.add_row(Row::new(cells));
    }

    let query_count = queries.as_ref().map_or(21, |c| c.len());

    // Setup a progress bar
    let progress = ProgressBar::new((query_count * formats.len()) as u64);

    // Send back a channel with the results of Row.
    let (rows_tx, rows_rx) = sync::mpsc::channel();
    let (row_count_tx, row_count_rx) = sync::mpsc::channel();
    for (q, query) in tpch_queries() {
        if let Some(queries) = queries.as_ref() {
            if !queries.contains(&q) {
                continue;
            }
        }
        let ctxs = ctxs.clone();
        let tx = rows_tx.clone();
        let count_tx = row_count_tx.clone();
        let progress = progress.clone();
        rayon::spawn_fifo(move || {
            let mut cells = Vec::with_capacity(formats.len());
            cells.push(Cell::new(&format!("Q{}", q)));

            let mut elapsed_us = Vec::new();
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            for (ctx, format) in ctxs.iter().zip(formats.iter()) {
                for i in 0..3 {
                    // warmup
                    let row_count: usize = rt.block_on(async {
                        ctx.sql(&query)
                            .await
                            .map_err(|e| println!("Failed to run {} {:?}: {}", q, format, e))
                            .unwrap()
                            .collect()
                            .await
                            .map_err(|e| println!("Failed to collect {} {:?}: {}", q, format, e))
                            .unwrap()
                            .iter()
                            .map(|r| r.num_rows())
                            .sum()
                    });
                    if i == 0 {
                        count_tx.send((q, *format, row_count)).unwrap();
                    }
                }
                let mut measure = Vec::new();
                for _ in 0..10 {
                    let start = SystemTime::now();
                    rt.block_on(async {
                        ctx.sql(&query)
                            .await
                            .map_err(|e| println!("Failed to run {} {:?}: {}", q, format, e))
                            .unwrap()
                            .collect()
                            .await
                            .map_err(|e| println!("Failed to collect {} {:?}: {}", q, format, e))
                            .unwrap();
                    });
                    let elapsed = start.elapsed().unwrap();
                    measure.push(elapsed);
                }
                let fastest = measure.iter().cloned().min().unwrap();
                elapsed_us.push(fastest);

                progress.inc(1);
            }

            let baseline = elapsed_us.first().unwrap();
            // yellow: 10% slower than baseline
            let yellow = baseline.as_micros() + (baseline.as_micros() / 10);
            // red: 50% slower than baseline
            let red = baseline.as_micros() + (baseline.as_micros() / 2);
            cells.push(Cell::new(&format!("{} us", baseline.as_micros())).style_spec("b"));
            for measure in elapsed_us.iter().skip(1) {
                let style_spec = if measure.as_micros() > red {
                    "bBr"
                } else if measure.as_micros() > yellow {
                    "bFdBy"
                } else {
                    "bFdBG"
                };
                cells.push(
                    Cell::new(&format!(
                        "{} us ({:.2})",
                        measure.as_micros(),
                        measure.as_micros() as f64 / baseline.as_micros() as f64
                    ))
                    .style_spec(style_spec),
                );
            }

            tx.send((q, Row::new(cells))).unwrap();
        });
    }

    // delete parent handle to tx
    drop(rows_tx);
    drop(row_count_tx);

    let mut format_row_counts: HashMap<Format, Vec<usize>> = HashMap::new();
    while let Ok((idx, format, row_count)) = row_count_rx.recv() {
        format_row_counts
            .entry(format)
            .or_insert_with(|| vec![0; EXPECTED_ROW_COUNTS.len()])[idx] = row_count;
    }

    let mut rows = vec![];
    while let Ok((idx, row)) = rows_rx.recv() {
        rows.push((idx, row));
    }
    rows.sort_by(|(idx0, _), (idx1, _)| idx0.cmp(idx1));
    for (_, row) in rows {
        table.add_row(row);
    }

    progress.finish();
    table.printstd();

    let mut mismatched = false;
    for (format, row_counts) in format_row_counts {
        row_counts
            .into_iter()
            .zip_eq(EXPECTED_ROW_COUNTS)
            .enumerate()
            .filter(|(idx, _)| queries.as_ref().map(|q| q.contains(idx)).unwrap_or(true))
            .for_each(|(idx, (row_count, expected_row_count))| {
                if row_count != expected_row_count {
                    println!("Mismatched row count {row_count} instead of {expected_row_count} in query {idx} for format {format:?}");
                    mismatched = true;
                }
            })
    }
    if mismatched {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}
