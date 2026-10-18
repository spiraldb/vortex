// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Why pco beats BtrBlocks + entropy-bins: per numeric column slice, pco's chosen mode and
//! delta, pco with each feature switched off, and entropy-bins on the raw column.
//!
//! Usage: pcoexplain <out.jsonl> <parquet>...
#![allow(deprecated, clippy::all)]

use std::fs::File;
use std::io::Write;
use std::path::Path;

use arrow_array::Array as _;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use pco::ChunkConfig;
use pco::DeltaSpec;
use pco::ModeSpec;
use pco::PagingSpec;
use pco::data_types::Number;
use pco::wrapped::FileCompressor;
use serde_json::json;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::dtype::DType;
use vortex_array::dtype::PType;
use vortex_entropy_bins::EntropyBins;
use vortex_entropy_bins::EntropyBinsConfig;

const BATCH: usize = 131072;

fn pco_size<T: Number>(v: &[T], config: &ChunkConfig) -> usize {
    let config = config.clone().with_paging_spec(PagingSpec::EqualPagesUpTo(8192));
    pco::standalone::simple_compress(v, &config).map_or(usize::MAX, |b| b.len())
}

fn describe<T: Number>(v: &[T]) -> (String, String, usize) {
    let fc = FileCompressor::default();
    let config = ChunkConfig::default();
    let n = v.len().min(1 << 18);
    let Ok(cc) = fc.chunk_compressor(&v[..n], &config) else {
        return ("?".into(), "?".into(), 0);
    };
    let meta = cc.meta();
    let bins = match &meta.per_latent_var.primary.bins {
        pco::metadata::DynBins::U16(b) => b.len(),
        pco::metadata::DynBins::U32(b) => b.len(),
        pco::metadata::DynBins::U64(b) => b.len(),
        _ => 0,
    };
    (format!("{:?}", meta.mode), format!("{:?}", meta.delta_encoding), bins)
}

fn analyze<T: Number>(v: &[T]) -> serde_json::Value {
    let dflt = ChunkConfig::default();
    let (mode, delta, bins) = describe(v);
    json!({
        "pco": pco_size(v, &dflt),
        "pco_nodelta": pco_size(v, &dflt.clone().with_delta_spec(DeltaSpec::NoOp)),
        "pco_classic": pco_size(v, &dflt.clone().with_mode_spec(ModeSpec::Classic)),
        "pco_leaf": pco_size(v, &dflt.clone().with_mode_spec(ModeSpec::Classic).with_delta_spec(DeltaSpec::NoOp)),
        "pco_delta1": pco_size(v, &dflt.clone().with_mode_spec(ModeSpec::Classic).with_delta_spec(DeltaSpec::TryConsecutive(1))),
        "mode": mode, "delta": delta, "bins": bins,
    })
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut out = File::create(&args[1]).unwrap();
    let session = vortex_array::array_session();
    vortex_file::register_default_encodings(&session);
    let mut ctx = session.create_execution_ctx();
    for file in &args[2..] {
        let stem = Path::new(file).file_stem().unwrap().to_string_lossy().to_string();
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(file).unwrap())
            .unwrap()
            .with_batch_size(BATCH)
            .build()
            .unwrap();
        for (bi, batch) in reader.enumerate().filter(|(i, _)| *i == 0 || *i == 4) {
            let batch = batch.unwrap();
            let schema = batch.schema();
            for (ci, col) in batch.columns().iter().enumerate() {
                let name = schema.field(ci).name().clone();
                let Some(width) = col.data_type().primitive_width() else { continue };
                let col = col.slice(0, ((1 << 20) / width).min(col.len()));
                let Ok(array) = vortex_arrow::convert::from_arrow_dyn(&*col, true) else { continue };
                let DType::Primitive(ptype, _) = array.dtype() else { continue };
                let ptype = *ptype;
                let prim = array.clone().execute::<PrimitiveArray>(&mut ctx).unwrap();
                macro_rules! go {
                    ($t:ty) => {{
                        let v = prim.as_slice::<$t>();
                        analyze(v)
                    }};
                }
                let mut row = match ptype {
                    PType::U16 => go!(u16),
                    PType::I16 => go!(i16),
                    PType::U32 => go!(u32),
                    PType::I32 => go!(i32),
                    PType::U64 => go!(u64),
                    PType::I64 => go!(i64),
                    PType::F32 => go!(f32),
                    PType::F64 => go!(f64),
                    _ => continue,
                };
                if ptype.is_int() {
                    let plan = EntropyBins::plan(prim.as_view(), &EntropyBinsConfig::BALANCED).unwrap();
                    let eb = EntropyBins::from_primitive(prim.as_view(), 8, plan.options).unwrap();
                    row["eb_raw"] = json!(eb.into_array().nbytes());
                    row["eb_raw_lag"] = json!(plan.options.lag);
                }
                row["col"] = json!(format!("{stem}.{name}"));
                row["batch"] = json!(bi);
                row["ptype"] = json!(ptype.to_string());
                writeln!(out, "{row}").unwrap();
            }
        }
    }
}
