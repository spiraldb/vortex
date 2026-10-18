// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Block size and refill word sweep for entropy-bins over the corpus leaves: bytes (buffers plus
//! per-chunk bins metadata), full-scan decode and cold single-row lookup latency, for all leaves
//! and for a low-entropy subset. Each leaf keeps the lag its BALANCED plan picks. The model chunk
//! size is the build's `CHUNK_VALUES`.
//!
//! Usage: blocksweep <corpus dir> <low-entropy id list>
#![allow(clippy::all)]

use std::collections::HashSet;
use std::fs;
use std::hint::black_box;
use std::io::BufRead;
use std::path::Path;
use std::time::Duration;
use std::time::Instant;

use leaf_lab::bins::Bins;
use leaf_lab::bins::latent;
use vortex_array::ArrayRef;
use vortex_array::ExecutionCtx;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::match_each_integer_ptype;
use vortex_entropy_bins::CHUNK_VALUES;
use vortex_entropy_bins::EntropyBins;
use vortex_entropy_bins::EntropyBinsConfig;
use vortex_entropy_bins::EntropyBinsOptions;

const LOOKUPS: usize = 500;

fn best_of(mut f: impl FnMut()) -> Duration {
    let mut best = Duration::MAX;
    let mut total = Duration::ZERO;
    let mut runs = 0;
    while runs < 5 || (total < Duration::from_millis(30) && runs < 50) {
        let t = Instant::now();
        f();
        let e = t.elapsed();
        best = best.min(e);
        total += e;
        runs += 1;
    }
    best
}

#[derive(Default, Clone, Copy)]
struct Acc {
    n: usize,
    bytes: usize,
    dec: Duration,
    lookups: usize,
    look: Duration,
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let dir = Path::new(&args[1]);
    let low: HashSet<String> = fs::read_to_string(&args[2]).unwrap().lines().map(String::from).collect();
    let session = vortex_array::array_session();
    vortex_file::register_default_encodings(&session);
    let mut ctx = session.create_execution_ctx();
    let configs: Vec<(usize, u32)> = [1024, 2048, 4096, 16384]
        .into_iter()
        .flat_map(|b| [(b, 16), (b, 8)])
        .collect();
    let mut all = vec![Acc::default(); configs.len() + 1];
    let mut sub = vec![Acc::default(); configs.len() + 1];
    let mut rng = 0x2545_f491_4f6c_dd1du64;
    let manifest = fs::File::open(dir.join("manifest.jsonl")).unwrap();
    for line in std::io::BufReader::new(manifest).lines() {
        let m: serde_json::Value = serde_json::from_str(&line.unwrap()).unwrap();
        let id = m["id"].as_str().unwrap().to_string();
        let data = fs::read(dir.join("data").join(format!("{id}.bin"))).unwrap();
        macro_rules! parse {
            ($t:ty) => {{
                let w = std::mem::size_of::<$t>();
                PrimitiveArray::from_iter(data.chunks_exact(w).map(|c| <$t>::from_le_bytes(c.try_into().unwrap())))
            }};
        }
        let prim = match m["dtype"].as_str().unwrap() {
            "u8" => parse!(u8),
            "u16" => parse!(u16),
            "u32" => parse!(u32),
            "u64" => parse!(u64),
            "i8" => parse!(i8),
            "i16" => parse!(i16),
            "i32" => parse!(i32),
            _ => parse!(i64),
        };
        let n = prim.len();
        let plan = EntropyBins::plan(prim.as_view(), &EntropyBinsConfig::BALANCED).unwrap();
        let lag = plan.options.lag;
        // Per-chunk bins and weights as serialized metadata (same for every block size).
        let latents: Vec<u64> = match_each_integer_ptype!(prim.ptype(), |T| {
            prim.as_slice::<T>().iter().map(|&v| latent(v as i128, prim.ptype().is_signed_int())).collect()
        });
        let meta: usize = if lag == 0 {
            latents.chunks(CHUNK_VALUES).map(|c| {
                let b = Bins::train(c, 8);
                b.metadata_bytes() + b.len()
            }).sum()
        } else {
            // Difference latents train their own bins; estimate their metadata from the diffs.
            let diffs: Vec<u64> = (0..n).map(|i| if i >= lag { latents[i].wrapping_sub(latents[i - lag]) ^ (1 << 63) } else { 1 << 63 }).collect();
            diffs.chunks(CHUNK_VALUES).map(|c| {
                let b = Bins::train(c, 8);
                b.metadata_bytes() + b.len()
            }).sum()
        };
        let rows: Vec<usize> = (0..LOOKUPS)
            .map(|_| {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                (rng % n as u64) as usize
            })
            .collect();
        let is_low = low.contains(&id);
        let mut measure = |ctx: &mut ExecutionCtx, k: usize, array: &ArrayRef, bytes: usize| {
            let dec = best_of(|| {
                black_box(array.clone().execute::<PrimitiveArray>(ctx).unwrap());
            });
            let t = Instant::now();
            for &r in &rows {
                black_box(array.execute_scalar(r, ctx).unwrap());
            }
            let look = t.elapsed();
            for acc in [&mut all[k]].into_iter().chain(if is_low { Some(&mut sub[k]) } else { None }) {
                acc.n += n;
                acc.bytes += bytes;
                acc.dec += dec;
                acc.lookups += rows.len();
                acc.look += look;
            }
        };
        for (k, &(block, words)) in configs.iter().enumerate() {
            let options = EntropyBinsOptions::new(lag, block).with_word_bits(words);
            let eb = EntropyBins::from_primitive(prim.as_view(), 8, options).unwrap().into_array();
            let back = eb.clone().execute::<PrimitiveArray>(&mut ctx).unwrap();
            match_each_integer_ptype!(prim.ptype(), |T| {
                assert_eq!(back.as_slice::<T>(), prim.as_slice::<T>(), "{id} roundtrip");
            });
            measure(&mut ctx, k, &eb, eb.nbytes() as usize + meta);
        }
        // The planner's own choice, for reference.
        let eb = EntropyBins::from_primitive(prim.as_view(), 8, plan.options).unwrap().into_array();
        measure(&mut ctx, configs.len(), &eb, eb.nbytes() as usize + meta);
    }
    let print = |name: &str, accs: &[Acc]| {
        let base = accs[configs.len()].bytes as f64;
        println!("{name}: {:.1}M values, chunk 2^{}", accs[0].n as f64 / 1e6, CHUNK_VALUES.trailing_zeros());
        for (k, a) in accs.iter().enumerate() {
            let label = if k < configs.len() {
                format!("{:>5} values, {:>2}-bit words", configs[k].0, configs[k].1)
            } else {
                "BALANCED planner".to_string()
            };
            println!(
                "  {label:28} {:>8.3} MB {:>6.1}%  scan {:>5.0} Mv/s  lookup {:>6.2} us",
                a.bytes as f64 / 1e6,
                a.bytes as f64 / base * 100.0,
                a.n as f64 / a.dec.as_secs_f64() / 1e6,
                a.look.as_secs_f64() / a.lookups as f64 * 1e6,
            );
        }
    };
    print("all leaves", &all);
    print("low-entropy leaves (bound < 2 bits/value)", &sub);
}
