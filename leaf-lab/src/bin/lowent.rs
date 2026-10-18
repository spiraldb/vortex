// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Where entropy-bins' bytes go on low-entropy integer columns, against pco: ideal id entropy,
//! the loss from 128-state probabilities, offset bits, and what is left (per-block overhead).
//!
//! Usage: lowent <parquet> <column>...
#![allow(deprecated, clippy::all)]

use std::fs::File;

use arrow_array::Array as _;
use leaf_lab::bins::Bins;
use leaf_lab::bins::latent;
use leaf_lab::entropy_bits;
use leaf_lab::normalize;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use pco::ChunkConfig;
use pco::PagingSpec;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::match_each_integer_ptype;
use vortex_entropy_bins::EntropyBins;
use vortex_entropy_bins::EntropyBinsConfig;
use vortex_entropy_bins::EntropyBinsOptions;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let session = vortex_array::array_session();
    vortex_file::register_default_encodings(&session);
    let mut ctx = session.create_execution_ctx();
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&args[1]).unwrap())
        .unwrap()
        .with_batch_size(131072)
        .build()
        .unwrap();
    let batch = reader.skip(4).next().unwrap().unwrap();
    println!(
        "{:22} {:>7} {:>5} | {:>7} {:>7} {:>7} {:>7} | {:>7} {:>7} {:>7} {:>7} | {:>7}",
        "column", "n", "bins", "H0", "q128", "offs", "ideal", "eb", "ovh", "eb16k8", "ovh", "pco"
    );
    for name in &args[2..] {
        let idx = batch.schema().index_of(name).unwrap();
        let col = batch.column(idx);
        let width = col.data_type().primitive_width().unwrap();
        let col = col.slice(0, ((1 << 20) / width).min(col.len()));
        let array = vortex_arrow::convert::from_arrow_dyn(&*col, true).unwrap();
        let prim = array.execute::<PrimitiveArray>(&mut ctx).unwrap();
        let ptype = prim.ptype();
        let (latents, pco_bytes): (Vec<u64>, usize) = match_each_integer_ptype!(ptype, |T| {
            let v = prim.as_slice::<T>();
            let pco_cfg = ChunkConfig::default().with_paging_spec(PagingSpec::EqualPagesUpTo(8192));
            (
                v.iter().map(|&x| latent(x as i128, ptype.is_signed_int())).collect(),
                pco::standalone::simple_compress(v, &pco_cfg).unwrap().len(),
            )
        });
        let (mut h0, mut q, mut offs, mut nbins, mut meta) = (0f64, 0f64, 0u64, 0, 0usize);
        for c in latents.chunks(1 << 18) {
            let bins = Bins::train(c, 8);
            let mut counts = vec![0u32; bins.len()];
            for &v in c {
                let b = bins.bin_of(v);
                counts[b] += 1;
                offs += u64::from(bins.widths[b]);
            }
            h0 += entropy_bits(&counts);
            let norm = normalize(&counts, 7);
            q += counts
                .iter()
                .zip(&norm)
                .filter(|(c, _)| **c > 0)
                .map(|(&c, &f)| -f64::from(c) * (f64::from(f) / 128.0).log2())
                .sum::<f64>();
            nbins = nbins.max(bins.len());
            meta += bins.metadata_bytes() + bins.len();
        }
        let ideal = ((q + offs as f64) / 8.0) as usize + meta;
        let plan = EntropyBins::plan(prim.as_view(), &EntropyBinsConfig::BALANCED).unwrap();
        let eb = EntropyBins::from_primitive(prim.as_view(), 8, plan.options).unwrap().into_array();
        let big = EntropyBinsOptions::new(plan.options.lag, 16384).with_word_bits(8);
        let eb16 = EntropyBins::from_primitive(prim.as_view(), 8, big).unwrap().into_array();
        let ebb = eb.nbytes() as usize + meta;
        let eb16b = eb16.nbytes() as usize + meta;
        println!(
            "{:22} {:>7} {:>5} | {:>6.1}K {:>6.1}K {:>6.1}K {:>6.1}K | {:>6.1}K {:>6.1}K {:>6.1}K {:>6.1}K | {:>6.1}K   lag{} block{} w{}",
            name,
            latents.len(),
            nbins,
            h0 / 8e3,
            q / 8e3,
            offs as f64 / 8e3,
            ideal as f64 / 1e3,
            ebb as f64 / 1e3,
            (ebb as f64 - ideal as f64) / 1e3,
            eb16b as f64 / 1e3,
            (eb16b as f64 - ideal as f64) / 1e3,
            pco_bytes as f64 / 1e3,
            plan.options.lag,
            plan.options.block_values,
            plan.options.word_bits,
        );
    }
}
