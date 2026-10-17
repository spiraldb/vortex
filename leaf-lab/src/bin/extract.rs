// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Compress parquet columns with the default BtrBlocks cascade and dump every integer leaf the
//! cascade ended on (BitPacked or uncompressed Primitive), with its path through the tree.
//!
//! Usage: extract <out dir> <parquet>...
#![allow(deprecated, clippy::all)]

use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::Path;

use arrow_array::Array as _;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use vortex_array::ArrayRef;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::dtype::DType;
use vortex_array::match_each_integer_ptype;
use vortex_btrblocks::BtrBlocksCompressorBuilder;
use vortex_session::VortexSession;

const BATCH: usize = 131072;
const MIN_LEAF: usize = 4096;

struct Leaf {
    path: String,
    encoding: String,
    nbytes: u64,
    array: ArrayRef,
}

fn walk(a: &ArrayRef, path: String, out: &mut Vec<Leaf>) {
    let enc = a.encoding_id().to_string();
    let is_int = matches!(a.dtype(), DType::Primitive(p, _) if p.is_int());
    if is_int && (enc == "vortex.primitive" || enc.contains("bitpacked")) {
        out.push(Leaf {
            path: format!("{path}/{enc}"),
            encoding: enc,
            nbytes: a.nbytes(),
            array: a.clone(),
        });
        return;
    }
    for (name, child) in a.children_names().into_iter().zip(a.children()) {
        if name.contains("validity") {
            continue;
        }
        walk(&child, format!("{path}/{enc}.{name}"), out);
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let out = Path::new(&args[1]);
    fs::create_dir_all(out.join("data")).unwrap();
    let mut manifest = File::create(out.join("manifest.jsonl")).unwrap();
    let mut columns = File::create(out.join("columns.jsonl")).unwrap();

    let session: VortexSession = vortex_array::array_session();
    vortex_file::register_default_encodings(&session);
    let compressor = BtrBlocksCompressorBuilder::from_session(&session)
        .unrestricted()
        .build();
    let mut ctx = session.create_execution_ctx();

    for file in &args[2..] {
        let stem = Path::new(file).file_stem().unwrap().to_string_lossy().to_string();
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(file).unwrap())
            .unwrap()
            .with_batch_size(BATCH)
            .build()
            .unwrap();
        // Two batches per file, far apart.
        for (bi, batch) in reader.enumerate().filter(|(i, _)| *i == 0 || *i == 4) {
            let batch = batch.unwrap();
            let schema = batch.schema();
            for (ci, col) in batch.columns().iter().enumerate() {
                let name = schema.field(ci).name().clone();
                let width = col.data_type().primitive_width().unwrap_or(16);
                let rows = (1 << 20) / width.max(1);
                let col = col.slice(0, rows.min(col.len()));
                let Ok(array) = vortex_arrow::convert::from_arrow_dyn(&*col, true) else {
                    eprintln!("skip {name}: {:?}", col.data_type());
                    continue;
                };
                let raw = array.nbytes();
                let compressed = match compressor.compress(&array, &mut ctx) {
                    Ok(c) => c,
                    Err(e) => {
                        eprintln!("compress {name}: {e}");
                        continue;
                    }
                };
                let mut leaves = Vec::new();
                walk(&compressed, String::new(), &mut leaves);
                writeln!(
                    columns,
                    "{{\"src\":\"{stem}.{name}\",\"batch\":{bi},\"dtype\":\"{}\",\"rows\":{},\"raw\":{raw},\"compressed\":{},\"tree\":\"{}\"}}",
                    array.dtype(),
                    array.len(),
                    compressed.nbytes(),
                    compressed.encoding_id()
                )
                .unwrap();
                for (li, leaf) in leaves.into_iter().enumerate() {
                    if leaf.array.len() < MIN_LEAF {
                        continue;
                    }
                    let prim = leaf.array.clone().execute::<PrimitiveArray>(&mut ctx).unwrap();
                    let ptype = prim.ptype();
                    let id = format!("{stem}.{name}.{bi}.{li}");
                    let mut bytes = Vec::new();
                    match_each_integer_ptype!(ptype, |T| {
                        for v in prim.as_slice::<T>() {
                            bytes.extend_from_slice(&v.to_le_bytes());
                        }
                    });
                    fs::write(out.join("data").join(format!("{id}.bin")), &bytes).unwrap();
                    writeln!(
                        manifest,
                        "{{\"id\":\"{id}\",\"src\":\"{stem}.{name}\",\"dtype\":\"{ptype}\",\"n\":{},\"leaf_bytes\":{},\"encoding\":\"{}\",\"path\":\"{}\",\"col_compressed\":{}}}",
                        prim.len(),
                        leaf.nbytes,
                        leaf.encoding,
                        leaf.path,
                        compressed.nbytes()
                    )
                    .unwrap();
                }
            }
        }
        eprintln!("done {file}");
    }
}
