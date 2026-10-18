// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Print the BtrBlocks tree (default and with entropy-bins) for one column slice.
//!
//! Usage: tree <parquet> <batch index> <column>
#![allow(deprecated, clippy::all)]

use std::fs::File;

use arrow_array::Array as _;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use vortex_btrblocks::BtrBlocksCompressorBuilder;
use vortex_btrblocks::schemes::integer::EntropyBinsScheme;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let session = vortex_array::array_session();
    vortex_file::register_default_encodings(&session);
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&args[1]).unwrap())
        .unwrap()
        .with_batch_size(131072)
        .build()
        .unwrap();
    let batch = reader.skip(args[2].parse().unwrap()).next().unwrap().unwrap();
    let col = batch.column(batch.schema().index_of(&args[3]).unwrap());
    let width = col.data_type().primitive_width().unwrap();
    let col = col.slice(0, ((1 << 20) / width).min(col.len()));
    let array = vortex_arrow::convert::from_arrow_dyn(&*col, true).unwrap();
    {
        let mut ctx = vortex_array::VortexSessionExecute::create_execution_ctx(&session);
        let prim = array.clone().execute::<vortex_array::arrays::PrimitiveArray>(&mut ctx).unwrap();
        if prim.ptype().is_int() {
            let plan = vortex_entropy_bins::EntropyBins::plan(prim.as_view(), &vortex_entropy_bins::EntropyBinsConfig::BALANCED).unwrap();
            let runs = vortex_array::match_each_integer_ptype!(prim.ptype(), |T| {
                let v = prim.as_slice::<T>();
                1 + v.windows(2).filter(|w| w[0] != w[1]).count()
            });
            println!("eb plan {} bytes (lag {}), runs {} (avg run {:.1}), runend ~{} bytes", plan.nbytes, plan.options.lag, runs, prim.len() as f64 / runs as f64, runs * 3 / 2);
        }
    }
    let base = || BtrBlocksCompressorBuilder::from_session(&session).unrestricted();
    for (label, c) in [
        ("default", base().build()),
        ("+entropy-bins", base().with_new_scheme(&EntropyBinsScheme::BALANCED).build()),
    ] {
        let mut ctx = vortex_array::VortexSessionExecute::create_execution_ctx(&session);
        let out = c.compress(&array, &mut ctx).unwrap();
        println!("== {label}: {} bytes\n{}", out.nbytes(), out.display_tree_encodings_only());
    }
}
