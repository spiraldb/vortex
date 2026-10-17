// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Whole-column comparison: BtrBlocks default, with the entropy-bins scheme, the compact (pco)
//! preset, and pco alone. One JSON line per column slice with bytes, compress and decompress time
//! and the encodings each tree used.
//!
//! Usage: columns <out.jsonl> <parquet>...
#![allow(deprecated, clippy::all)]

use std::collections::BTreeMap;
use std::fs::File;
use std::hint::black_box;
use std::io::Write;
use std::path::Path;
use std::time::Duration;
use std::time::Instant;

use arrow_array::Array as _;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::json;
use vortex_array::ArrayRef;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::dtype::DType;
use vortex_array::dtype::PType;
use vortex_btrblocks::BtrBlocksCompressor;
use vortex_btrblocks::BtrBlocksCompressorBuilder;
use vortex_btrblocks::schemes::integer::EntropyBinsScheme;

const BATCH: usize = 131072;

fn best_of(mut f: impl FnMut()) -> Duration {
    let mut best = Duration::MAX;
    let mut total = Duration::ZERO;
    let mut runs = 0;
    while runs < 3 || (total < Duration::from_millis(100) && runs < 20) {
        let t = Instant::now();
        f();
        let e = t.elapsed();
        best = best.min(e);
        total += e;
        runs += 1;
    }
    best
}

fn encodings(a: &ArrayRef, out: &mut BTreeMap<String, u64>) {
    let children = a.children();
    let own = a.nbytes() - children.iter().map(|c| c.nbytes()).sum::<u64>();
    *out.entry(a.encoding_id().to_string()).or_default() += own;
    for c in &children {
        encodings(c, out);
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut out = File::create(&args[1]).unwrap();
    let session = vortex_array::array_session();
    vortex_file::register_default_encodings(&session);
    let mut ctx = session.create_execution_ctx();
    let base = || BtrBlocksCompressorBuilder::from_session(&session).unrestricted();
    let setups: Vec<(&str, BtrBlocksCompressor)> = vec![
        ("btr", base().build()),
        ("btr+eb", base().with_new_scheme(&EntropyBinsScheme::BALANCED).build()),
        ("btr+eb-smallest", base().with_new_scheme(&EntropyBinsScheme::SMALLEST).build()),
        ("compact", base().with_compact().build()),
        ("compact+eb", base().with_compact().with_new_scheme(&EntropyBinsScheme::BALANCED).build()),
    ];

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
                let mut row = serde_json::Map::new();
                row.insert("col".into(), json!(format!("{stem}.{name}")));
                row.insert("batch".into(), json!(bi));
                row.insert("ptype".into(), json!(ptype.to_string()));
                row.insert("n".into(), json!(array.len()));
                row.insert("raw".into(), json!(array.nbytes()));
                let expected = array.clone().execute::<PrimitiveArray>(&mut session.create_execution_ctx()).unwrap();
                let valid = expected
                    .validity()
                    .unwrap()
                    .to_mask(expected.len(), &mut session.create_execution_ctx())
                    .unwrap();
                let mut run = |label: &str, compress: &mut dyn FnMut() -> ArrayRef| {
                    let compressed = compress();
                    let t_enc = best_of(|| {
                        black_box(compress());
                    });
                    let back = compressed.clone().execute::<PrimitiveArray>(&mut ctx).unwrap();
                    assert_eq!(back.len(), array.len());
                    vortex_array::match_each_native_ptype!(ptype, |T| {
                        let a = back.as_slice::<T>();
                        let b = expected.as_slice::<T>();
                        let valid = &valid;
                        for i in 0..a.len() {
                            if valid.value(i) {
                                assert!(a[i].to_le_bytes() == b[i].to_le_bytes(), "{label} mismatch at {i}");
                            }
                        }
                    });
                    let t_dec = best_of(|| {
                        black_box(compressed.clone().execute::<PrimitiveArray>(&mut ctx).unwrap());
                    });
                    let mut enc = BTreeMap::new();
                    encodings(&compressed, &mut enc);
                    row.insert(
                        label.into(),
                        json!({"bytes": compressed.nbytes(), "enc_ns": t_enc.as_nanos(),
                               "dec_ns": t_dec.as_nanos(), "encodings": enc}),
                    );
                };
                for (label, compressor) in &setups {
                    let mut c = session.create_execution_ctx();
                    run(label, &mut || compressor.compress(&array, &mut c).unwrap());
                }
                if !matches!(ptype, PType::U8 | PType::I8) {
                    let prim = array.clone().execute::<PrimitiveArray>(&mut session.create_execution_ctx()).unwrap();
                    let mut c = session.create_execution_ctx();
                    run("pco", &mut || {
                        vortex_pco::Pco::from_primitive(prim.as_view(), pco::DEFAULT_COMPRESSION_LEVEL, 8192, &mut c)
                            .unwrap()
                            .into_array()
                    });
                }
                writeln!(out, "{}", serde_json::Value::Object(row)).unwrap();
                eprintln!("{stem}.{name}.{bi}");
            }
        }
    }
}
