// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Hash-like columns: Dict (codes by first occurrence, entropy-bins codes, raw values) against
//! Dict with codes replaced by their move-to-front recency rank, a lookback encoding (distance
//! to the previous occurrence or 0 for a literal, entropy-bins distances, raw literals), and
//! vortex-pco. Sizes only.
//!
//! Usage: hashexp <parquet>... -- <column>...
#![allow(deprecated, clippy::all)]

use std::collections::HashMap;
use std::fs::File;
use std::path::Path;

use arrow_array::Array as _;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::PrimitiveArray;
use vortex_entropy_bins::EntropyBins;
use vortex_entropy_bins::EntropyBinsConfig;

fn eb_bytes(v: Vec<u32>) -> usize {
    let prim = PrimitiveArray::from_iter(v);
    let plan = EntropyBins::plan(prim.as_view(), &EntropyBinsConfig::BALANCED).unwrap();
    EntropyBins::from_primitive(prim.as_view(), 8, plan.options).unwrap().into_array().nbytes() as usize
}

fn eb_bytes64(v: Vec<u64>) -> usize {
    let prim = PrimitiveArray::from_iter(v);
    let plan = EntropyBins::plan(prim.as_view(), &EntropyBinsConfig::BALANCED).unwrap();
    EntropyBins::from_primitive(prim.as_view(), 8, plan.options).unwrap().into_array().nbytes() as usize
}

fn per_value_ns(n: usize, mut f: impl FnMut()) -> f64 {
    let mut best = f64::MAX;
    for _ in 0..5 {
        let t = std::time::Instant::now();
        f();
        best = best.min(t.elapsed().as_nanos() as f64);
    }
    best / n as f64
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let split = args.iter().position(|a| a == "--").unwrap();
    let files = &args[1..split];
    let cols = &args[split + 1..];
    let session = vortex_array::array_session();
    vortex_file::register_default_encodings(&session);
    let mut ctx = session.create_execution_ctx();
    let mut tot = [0usize; 6];
    let mut tot2 = [0usize; 2];
    let mut ns = [0f64; 2];
    println!(
        "{:28} {:>7} {:>7} | {:>9} {:>9} {:>9} {:>9} {:>9}",
        "column", "n", "unique", "dict", "dict+mtf", "lookback", "pco", "dict vals"
    );
    for file in files {
        let stem = Path::new(file).file_stem().unwrap().to_string_lossy().to_string();
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(file).unwrap())
            .unwrap()
            .with_batch_size(131072)
            .build()
            .unwrap();
        for (bi, batch) in reader.enumerate().filter(|(i, _)| *i == 0 || *i == 4) {
            let batch = batch.unwrap();
            for name in cols {
                let Ok(idx) = batch.schema().index_of(name) else { continue };
                let col = batch.column(idx);
                let col = col.slice(0, (131072).min(col.len()));
                let array = vortex_arrow::convert::from_arrow_dyn(&*col, true).unwrap();
                let prim = array.execute::<PrimitiveArray>(&mut ctx).unwrap();
                let v: Vec<i64> = prim.as_slice::<i64>().to_vec();
                let n = v.len();
                // Dict with codes in first-occurrence order.
                let mut code_of: HashMap<i64, u32> = HashMap::new();
                let mut codes = Vec::with_capacity(n);
                for &x in &v {
                    let next = code_of.len() as u32;
                    codes.push(*code_of.entry(x).or_insert(next));
                }
                let uniq = code_of.len();
                let values_bytes = uniq * 8;
                let dict = values_bytes + eb_bytes(codes.clone());
                // Move-to-front: each code becomes its recency rank; a new code is coded as its
                // rank among codes seen so far plus one (the decoder appends it at the front).
                let mut mtf: Vec<u32> = Vec::new();
                let mut ranks = Vec::with_capacity(n);
                for &c in &codes {
                    match mtf.iter().position(|&m| m == c) {
                        Some(p) => {
                            ranks.push(p as u32 + 1);
                            mtf.remove(p);
                        }
                        None => ranks.push(0),
                    }
                    mtf.insert(0, c);
                }
                let dict_mtf = values_bytes + eb_bytes(ranks.clone());
                // Lookback: distance to the previous occurrence, 0 for a literal.
                let mut last: HashMap<i64, usize> = HashMap::new();
                let mut dist = Vec::with_capacity(n);
                let mut literals = 0usize;
                for (i, &x) in v.iter().enumerate() {
                    match last.insert(x, i) {
                        Some(p) => dist.push((i - p) as u32),
                        None => {
                            dist.push(0);
                            literals += 1;
                        }
                    }
                }
                let lookback = literals * 8 + eb_bytes(dist.clone());
                // Sorted dictionary values as entropy-bins coded gaps (the codes are remapped to
                // the sorted order; recency ranks do not change).
                let mut sorted: Vec<u64> = code_of.keys().map(|&x| x as u64 ^ (1 << 63)).collect();
                sorted.sort_unstable();
                let gaps: Vec<u64> = sorted.iter().scan(0u64, |p, &x| { let g = x - *p; *p = x; Some(g) }).collect();
                let sorted_vals = eb_bytes64(gaps);
                // Decode costs: lookback is one gather per value; move-to-front keeps a list.
                let lb_ns = per_value_ns(n, || {
                    let mut out = vec![0u32; n];
                    let mut next = 0u32;
                    for i in 0..n {
                        let d = dist[i] as usize;
                        out[i] = if d == 0 { next += 1; next - 1 } else { out[i - d] };
                    }
                    std::hint::black_box(&out);
                });
                let mtf_ns = per_value_ns(n, || {
                    let mut list: Vec<u32> = Vec::new();
                    let mut out = vec![0u32; n];
                    let mut next = 0u32;
                    for (i, &r) in ranks.iter().enumerate() {
                        let c = if r == 0 { next += 1; next - 1 } else { list.remove(r as usize - 1) };
                        list.insert(0, c);
                        out[i] = c;
                    }
                    std::hint::black_box(&out);
                });
                tot2[0] += sorted_vals; tot2[1] += values_bytes;
                ns[0] += lb_ns * n as f64; ns[1] += mtf_ns * n as f64;
                let pco = vortex_pco::Pco::from_primitive(prim.as_view(), pco::DEFAULT_COMPRESSION_LEVEL, 8192, &mut ctx)
                    .unwrap()
                    .into_array()
                    .nbytes() as usize;
                for (t, x) in tot.iter_mut().zip([n, uniq, dict, dict_mtf, lookback, pco]) {
                    *t += x;
                }
                println!(
                    "{:28} {:>7} {:>7} | {:>8.0}K {:>8.0}K {:>8.0}K {:>8.0}K {:>8.0}K",
                    format!("{stem}.{name}.{bi}"),
                    n,
                    uniq,
                    dict as f64 / 1e3,
                    dict_mtf as f64 / 1e3,
                    lookback as f64 / 1e3,
                    pco as f64 / 1e3,
                    values_bytes as f64 / 1e3
                );
            }
        }
    }
    println!(
        "{:28} {:>7} {:>7} | {:>8.0}K {:>8.0}K {:>8.0}K {:>8.0}K",
        "TOTAL", tot[0], tot[1], tot[2] as f64 / 1e3, tot[3] as f64 / 1e3, tot[4] as f64 / 1e3, tot[5] as f64 / 1e3
    );
    println!(
        "dict values raw {:.0}K -> sorted + entropy-bins gaps {:.0}K; decode lookback {:.1} ns/value, move-to-front {:.1} ns/value",
        tot2[1] as f64 / 1e3,
        tot2[0] as f64 / 1e3,
        ns[0] / tot[0] as f64,
        ns[1] / tot[0] as f64
    );
}
