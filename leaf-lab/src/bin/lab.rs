// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Runs every leaf coder over the extracted corpus and writes one JSON line per leaf with each
//! coder's serialized bytes and single-threaded full-decode time.
//!
//! Usage: lab <corpus dir> [id substring]
#![allow(clippy::all)]

use std::fs;
use std::hint::black_box;
use std::io::BufRead;
use std::io::Write;
use std::path::Path;
use std::time::Duration;
use std::time::Instant;

use leaf_lab::SymbolCoder;
use leaf_lab::bins::Bins;
use leaf_lab::bins::latent;
use leaf_lab::bits::pad;
use leaf_lab::entropy_bits;
use leaf_lab::freq_table_bytes;
use leaf_lab::golomb;
use leaf_lab::huff::Huffman;
use leaf_lab::normalize;
use leaf_lab::rans::Rans;
use leaf_lab::rc::AdaptiveBinary;
use leaf_lab::tunstall::Tunstall;
use serde_json::Value;
use serde_json::json;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::match_each_integer_ptype;
use vortex_entropy_bins::EntropyBins;
use vortex_entropy_bins::EntropyBinsConfig;
use vortex_entropy_bins::EntropyBinsOptions;

const BLOCK: usize = 4096;
const CHUNK: usize = 1 << 18;
/// Per-block start offset kept for random access by every blocked coder.
const BLOCK_INDEX_BYTES: usize = 4;

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

type Factory = Box<dyn Fn() -> Box<dyn SymbolCoder>>;

struct Out {
    bytes: usize,
    decode_ns: Option<u128>,
    ids_ns: Option<u128>,
    enc_ns: Option<u128>,
}

/// Code symbol blocks with one model per chunk; returns bytes and decode time (ids only, plus
/// `merge` per block when given).
fn run_symbols(
    factory: &Factory,
    chunks: &[(Vec<u32>, Vec<Vec<u16>>)],
    mut merge: impl FnMut(usize, usize, &[u16]),
) -> Out {
    let t_enc = best_of(|| {
        for (counts, blocks) in chunks {
            let mut c = factory();
            c.prepare(counts);
            if counts.iter().filter(|&&n| n > 0).count() == 1 {
                continue;
            }
            for b in blocks {
                black_box(c.encode(b));
            }
        }
    });
    let mut bytes = 0;
    let mut coded: Vec<(Box<dyn SymbolCoder>, Vec<Vec<u8>>)> = Vec::new();
    for (counts, blocks) in chunks {
        let mut c = factory();
        bytes += c.prepare(counts);
        // A chunk with one live symbol needs no id stream in any coder.
        let uniform = counts.iter().filter(|&&n| n > 0).count() == 1;
        let mut encoded = Vec::new();
        for b in blocks {
            if uniform {
                bytes += BLOCK_INDEX_BYTES;
                encoded.push(Vec::new());
                continue;
            }
            let e = c.encode(b);
            bytes += e.len() + BLOCK_INDEX_BYTES;
            let e = pad(e);
            let mut back = vec![0u16; b.len()];
            c.decode(&e, &mut back);
            assert!(back == *b, "{} roundtrip failed", c.name());
            encoded.push(e);
        }
        coded.push((c, encoded));
    }
    let mut buf = vec![0u16; BLOCK];
    let mut decode = |buf: &mut [u16], with_merge: bool, merge: &mut dyn FnMut(usize, usize, &[u16])| {
        for (ci, ((c, encoded), (counts, blocks))) in coded.iter().zip(chunks).enumerate() {
            for (bi, (e, b)) in encoded.iter().zip(blocks).enumerate() {
                let out = &mut buf[..b.len()];
                if e.is_empty() {
                    out.fill(counts.iter().position(|&n| n > 0).unwrap() as u16);
                } else {
                    c.decode(e, out);
                }
                if with_merge {
                    merge(ci, bi, out);
                }
            }
        }
        black_box(&*buf);
    };
    let t = best_of(|| decode(&mut buf, true, &mut merge));
    let t_ids = best_of(|| decode(&mut buf, false, &mut merge));
    Out { bytes, decode_ns: Some(t.as_nanos()), ids_ns: Some(t_ids.as_nanos()), enc_ns: Some(t_enc.as_nanos()) }
}

/// pco on the leaf values (u8/i8 widened to 16 bits, which pco requires), pages of 8192 values.
fn pco_run<T: pco::data_types::Number + PartialEq + std::fmt::Debug>(vals: &[T], config: &pco::ChunkConfig) -> Out {
    let config = config.clone().with_paging_spec(pco::PagingSpec::EqualPagesUpTo(8192));
    let bytes = pco::standalone::simple_compress(vals, &config).unwrap();
    let back: Vec<T> = pco::standalone::simple_decompress(&bytes).unwrap();
    assert!(back == vals, "pco roundtrip");
    let t_enc = best_of(|| {
        black_box(pco::standalone::simple_compress(vals, &config).unwrap());
    });
    let mut out = vec![vals[0]; vals.len()];
    let t = best_of(|| {
        pco::standalone::simple_decompress_into(&bytes, &mut out).unwrap();
        black_box(&out);
    });
    Out { bytes: bytes.len(), decode_ns: Some(t.as_nanos()), ids_ns: None, enc_ns: Some(t_enc.as_nanos()) }
}

fn put(row: &mut serde_json::Map<String, Value>, name: &str, o: Out) {
    row.insert(name.to_string(), json!({"bytes": o.bytes, "ns": o.decode_ns, "ids_ns": o.ids_ns, "enc_ns": o.enc_ns}));
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let dir = Path::new(&args[1]);
    let filter = args.get(2).cloned().unwrap_or_default();
    let session = vortex_array::array_session();
    vortex_file::register_default_encodings(&session);
    let mut ctx = session.create_execution_ctx();
    let mut results = fs::File::create(dir.join("results.jsonl")).unwrap();

    let binned_coders: Vec<Factory> = vec![
        Box::new(|| Box::new(Huffman::new(16, 12))),
        Box::new(|| Box::new(Rans::new(16, 12))),
        Box::new(|| Box::new(Huffman::new(32, 12))),
        Box::new(|| Box::new(Rans::new(32, 12))),
        Box::new(|| Box::new(Tunstall::new(12))),
        Box::new(|| Box::new(Tunstall::new(16))),
        Box::new(|| Box::new(AdaptiveBinary::new(false, 5))),
        Box::new(|| Box::new(AdaptiveBinary::new(true, 5))),
    ];
    let direct_coders: Vec<Factory> = vec![
        Box::new(|| Box::new(Huffman::new(16, 15))),
        Box::new(|| Box::new(Rans::new(16, 15))),
        Box::new(|| Box::new(Tunstall::new(16))),
        Box::new(|| Box::new(AdaptiveBinary::new(false, 5))),
    ];

    let manifest = fs::File::open(dir.join("manifest.jsonl")).unwrap();
    for line in std::io::BufReader::new(manifest).lines() {
        let m: Value = serde_json::from_str(&line.unwrap()).unwrap();
        let id = m["id"].as_str().unwrap().to_string();
        if !id.contains(&filter) {
            continue;
        }
        let dtype = m["dtype"].as_str().unwrap().to_string();
        let raw = fs::read(dir.join("data").join(format!("{id}.bin"))).unwrap();
        macro_rules! parse {
            ($t:ty) => {{
                let n = std::mem::size_of::<$t>();
                let v: Vec<$t> =
                    raw.chunks_exact(n).map(|c| <$t>::from_le_bytes(c.try_into().unwrap())).collect();
                let wide: Vec<i128> = v.iter().map(|&x| x as i128).collect();
                (PrimitiveArray::from_iter(v), wide, n, <$t>::MIN != 0)
            }};
        }
        let (prim, wide, width, signed) = match dtype.as_str() {
            "u8" => parse!(u8),
            "u16" => parse!(u16),
            "u32" => parse!(u32),
            "u64" => parse!(u64),
            "i8" => parse!(i8),
            "i16" => parse!(i16),
            "i32" => parse!(i32),
            "i64" => parse!(i64),
            _ => continue,
        };
        let n = wide.len();
        let latents: Vec<u64> = wide.iter().map(|&v| latent(v, signed)).collect();
        let mut row = serde_json::Map::new();
        put(&mut row, "cascade", Out { bytes: m["leaf_bytes"].as_u64().unwrap() as usize, decode_ns: None, ids_ns: None, enc_ns: None });
        put(&mut row, "raw", Out { bytes: n * width, decode_ns: None, ids_ns: None, enc_ns: None });

        // BitPacked re-encoded at its best width (with patches), for decode speed.
        if let Ok(bp) = vortex_fastlanes::bitpack_compress::bitpack_to_best_bit_width(&prim, &mut ctx) {
            let bp = bp.into_array();
            let bytes = bp.nbytes() as usize;
            let t_enc = best_of(|| {
                black_box(vortex_fastlanes::bitpack_compress::bitpack_to_best_bit_width(&prim, &mut ctx).unwrap());
            });
            let t = best_of(|| {
                black_box(bp.clone().execute::<PrimitiveArray>(&mut ctx).unwrap());
            });
            put(&mut row, "bitpacked", Out { bytes, decode_ns: Some(t.as_nanos()), ids_ns: None, enc_ns: Some(t_enc.as_nanos()) });
        }

        // vortex-entropy-bins itself, planner-chosen and forced to lag 0 (a pure leaf).
        let lag0 = EntropyBinsConfig { lags: &[0], ..EntropyBinsConfig::BALANCED };
        for (name, config, forced) in [
            ("eb", EntropyBinsConfig::BALANCED, None),
            ("eb-lag0", lag0, None),
            ("eb-4k", lag0, Some(EntropyBinsOptions::new(0, BLOCK))),
        ] {
            let mut plan = EntropyBins::plan(prim.as_view(), &config).unwrap();
            if let Some(o) = forced {
                plan.options = o;
            }
            let eb = EntropyBins::from_primitive(prim.as_view(), config.level, plan.options).unwrap();
            let t_enc = best_of(|| {
                let mut plan = EntropyBins::plan(prim.as_view(), &config).unwrap();
                if let Some(o) = forced {
                    plan.options = o;
                }
                black_box(EntropyBins::from_primitive(prim.as_view(), config.level, plan.options).unwrap());
            });
            let eb = eb.into_array();
            let back = eb.clone().execute::<PrimitiveArray>(&mut ctx).unwrap();
            match_each_integer_ptype!(prim.ptype(), |T| {
                assert_eq!(back.as_slice::<T>(), prim.as_slice::<T>(), "eb roundtrip");
            });
            let t = best_of(|| {
                black_box(eb.clone().execute::<PrimitiveArray>(&mut ctx).unwrap());
            });
            // Buffers only; add the per-chunk bins and weights it stores in metadata.
            let meta: usize = latents
                .chunks(CHUNK)
                .map(|c| {
                    let bins = Bins::train(c, config.level);
                    bins.metadata_bytes() + bins.len()
                })
                .sum();
            put(&mut row, name, Out { bytes: eb.nbytes() as usize + meta, decode_ns: Some(t.as_nanos()), ids_ns: None, enc_ns: Some(t_enc.as_nanos()) });
            row.insert(format!("{name}_lag"), json!(plan.options.lag));
            row.insert(format!("{name}_block"), json!(plan.options.block_values));
        }

        // pco: its default (chooses delta and int-mult modes itself) and leaf-only (plain bins).
        let pco_default = pco::ChunkConfig::default();
        let pco_leaf = pco::ChunkConfig::default()
            .with_mode_spec(pco::ModeSpec::Classic)
            .with_delta_spec(pco::DeltaSpec::NoOp);
        for (name, config) in [("pco", &pco_default), ("pco-leaf", &pco_leaf)] {
            let o = match dtype.as_str() {
                "u8" => pco_run(&wide.iter().map(|&v| v as u16).collect::<Vec<_>>(), config),
                "i8" => pco_run(&wide.iter().map(|&v| v as i16).collect::<Vec<_>>(), config),
                "u16" => pco_run(&wide.iter().map(|&v| v as u16).collect::<Vec<_>>(), config),
                "i16" => pco_run(&wide.iter().map(|&v| v as i16).collect::<Vec<_>>(), config),
                "u32" => pco_run(&wide.iter().map(|&v| v as u32).collect::<Vec<_>>(), config),
                "i32" => pco_run(&wide.iter().map(|&v| v as i32).collect::<Vec<_>>(), config),
                "u64" => pco_run(&wide.iter().map(|&v| v as u64).collect::<Vec<_>>(), config),
                _ => pco_run(&wide.iter().map(|&v| v as i64).collect::<Vec<_>>(), config),
            };
            put(&mut row, name, o);
        }

        // Binned family: identical bins and offsets, only the id coder changes.
        let t_bins = best_of(|| {
            for c in latents.chunks(CHUNK) {
                let bins = Bins::train(c, 8);
                for b in c.chunks(BLOCK) {
                    black_box(bins.split(b));
                }
            }
        });
        row.insert("bins_train_ns".into(), json!(t_bins.as_nanos()));
        let mut chunks = Vec::new();
        let mut chunk_bins = Vec::new();
        let mut offsets: Vec<Vec<Vec<u8>>> = Vec::new();
        let mut fixed = 0usize;
        let mut h0 = 0f64;
        for c in latents.chunks(CHUNK) {
            let bins = Bins::train(c, 8);
            let mut counts = vec![0u32; bins.len()];
            let mut blocks = Vec::new();
            let mut offs = Vec::new();
            for b in c.chunks(BLOCK) {
                let (ids, off) = bins.split(b);
                for &i in &ids {
                    counts[i as usize] += 1;
                }
                fixed += off.len();
                blocks.push(ids);
                offs.push(pad(off));
            }
            fixed += bins.metadata_bytes();
            h0 += entropy_bits(&counts);
            chunks.push((counts, blocks));
            chunk_bins.push(bins);
            offsets.push(offs);
        }
        let n_bins: usize = chunk_bins.iter().map(Bins::len).max().unwrap_or(0);
        row.insert("bins".into(), json!(n_bins));
        let offset_bytes: usize = offsets.iter().flatten().map(|o| o.len() - 16).sum();
        row.insert("offset_bytes".into(), json!(offset_bytes));
        let ids_table: usize = chunks.iter().map(|(c, _)| freq_table_bytes(&normalize(c, 12))).sum();
        put(&mut row, 
            "bins+H0",
            Out { bytes: fixed + ids_table + (h0 / 8.0).ceil() as usize, decode_ns: None, ids_ns: None, enc_ns: None },
        );
        let mut vals = vec![0u64; BLOCK];
        for f in &binned_coders {
            let name = format!("bins+{}", f().name());
            let mut o = run_symbols(f, &chunks, |ci, bi, ids| {
                chunk_bins[ci].merge(ids, &offsets[ci][bi], &mut vals[..ids.len()]);
                black_box(&vals);
            });
            o.bytes += fixed;
            put(&mut row, &name, o);
        }

        // Direct symbols when the value range is a small alphabet (dict codes and the like).
        let lo = *latents.iter().min().unwrap();
        let hi = *latents.iter().max().unwrap();
        if hi - lo < 4096 {
            let alphabet = (hi - lo + 1) as usize;
            let mut chunks = Vec::new();
            let mut h0 = 0f64;
            for c in latents.chunks(CHUNK) {
                let mut counts = vec![0u32; alphabet];
                let blocks: Vec<Vec<u16>> = c
                    .chunks(BLOCK)
                    .map(|b| b.iter().map(|&v| (v - lo) as u16).collect())
                    .collect();
                for &v in c {
                    counts[(v - lo) as usize] += 1;
                }
                h0 += entropy_bits(&counts);
                chunks.push((counts, blocks));
            }
            let table: usize = chunks.iter().map(|(c, _)| freq_table_bytes(&normalize(c, 15))).sum();
            put(&mut row, "sym+H0", Out { bytes: 8 + table + (h0 / 8.0).ceil() as usize, decode_ns: None, ids_ns: None, enc_ns: None });
            for f in &direct_coders {
                let name = format!("sym+{}", f().name());
                let mut o = run_symbols(f, &chunks, |_, _, ids| {
                    for (v, &s) in vals.iter_mut().zip(ids) {
                        *v = lo + u64::from(s);
                    }
                    black_box(&vals);
                });
                o.bytes += 8;
                put(&mut row, &name, o);
            }
        }

        // Parametric codes straight on the values.
        for (name, kind) in [("rice", golomb::Kind::Rice), ("expgolomb", golomb::Kind::ExpGolomb)] {
            let blocks: Vec<Vec<u8>> = latents.chunks(BLOCK).map(|b| pad(golomb::encode(kind, b))).collect();
            let bytes: usize = blocks.iter().map(|b| b.len() - 16 + BLOCK_INDEX_BYTES).sum();
            let t_enc = best_of(|| {
                for b in latents.chunks(BLOCK) {
                    black_box(golomb::encode(kind, b));
                }
            });
            for (b, src) in blocks.iter().zip(latents.chunks(BLOCK)) {
                let mut back = vec![0u64; src.len()];
                golomb::decode(kind, b, &mut back);
                assert!(back == src, "{name} roundtrip");
            }
            let t = best_of(|| {
                for (b, src) in blocks.iter().zip(latents.chunks(BLOCK)) {
                    golomb::decode(kind, b, &mut vals[..src.len()]);
                    black_box(&vals);
                }
            });
            put(&mut row, name, Out { bytes, decode_ns: Some(t.as_nanos()), ids_ns: None, enc_ns: Some(t_enc.as_nanos()) });
        }

        let mut line = serde_json::Map::new();
        for k in ["id", "src", "dtype", "n", "path", "encoding", "col_compressed"] {
            line.insert(k.into(), m[k].clone());
        }
        line.insert("width".into(), json!(width));
        line.insert("coders".into(), Value::Object(row));
        writeln!(results, "{}", Value::Object(line)).unwrap();
        eprintln!("{id}");
    }
}
