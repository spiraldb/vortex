// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! entropy-bins encode and decode speed over the corpus leaves, at the kernel level chosen by
//! `VORTEX_ENTROPY_BINS_ISA` (scalar, avx2, avx512).
//!
//! Usage: ebbench <corpus dir> [config: balanced|fast|smallest] [id substring]
#![allow(clippy::all)]

use std::fs;
use std::hint::black_box;
use std::io::BufRead;
use std::path::Path;
use std::time::Duration;
use std::time::Instant;

use serde_json::Value;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::match_each_integer_ptype;
use vortex_entropy_bins::EntropyBins;
use vortex_entropy_bins::EntropyBinsConfig;

fn best_of(mut f: impl FnMut()) -> Duration {
    let mut best = Duration::MAX;
    let mut total = Duration::ZERO;
    let mut runs = 0;
    while runs < 5 || (total < Duration::from_millis(50) && runs < 50) {
        let t = Instant::now();
        f();
        let e = t.elapsed();
        best = best.min(e);
        total += e;
        runs += 1;
    }
    best
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let dir = Path::new(&args[1]);
    let config = match args.get(2).map(String::as_str) {
        Some("fast") => EntropyBinsConfig::FAST,
        Some("smallest") => EntropyBinsConfig::SMALLEST,
        Some("lag0") => EntropyBinsConfig { lags: &[0], ..EntropyBinsConfig::BALANCED },
        _ => EntropyBinsConfig::BALANCED,
    };
    let filter = args.get(3).cloned().unwrap_or_default();
    let session = vortex_array::array_session();
    vortex_file::register_default_encodings(&session);
    let mut ctx = session.create_execution_ctx();
    let (mut n, mut raw, mut bytes) = (0usize, 0usize, 0usize);
    let (mut plan_t, mut enc_t, mut dec_t) = (Duration::ZERO, Duration::ZERO, Duration::ZERO);
    let (mut n64, mut dec64) = (0usize, Duration::ZERO);
    // On the leaves FastLanes BitPacked can encode: [values, raw, eb bytes, bp bytes] and
    // [eb encode, eb decode, bp encode, bp decode].
    let mut sub = [0usize; 4];
    let mut sub_t = [Duration::ZERO; 4];
    let manifest = fs::File::open(dir.join("manifest.jsonl")).unwrap();
    for line in std::io::BufReader::new(manifest).lines() {
        let m: Value = serde_json::from_str(&line.unwrap()).unwrap();
        let id = m["id"].as_str().unwrap();
        if !id.contains(&filter) {
            continue;
        }
        let data = fs::read(dir.join("data").join(format!("{id}.bin"))).unwrap();
        macro_rules! parse {
            ($t:ty) => {{
                let w = std::mem::size_of::<$t>();
                PrimitiveArray::from_iter(
                    data.chunks_exact(w).map(|c| <$t>::from_le_bytes(c.try_into().unwrap())),
                )
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
        let plan = EntropyBins::plan(prim.as_view(), &config).unwrap();
        let eb = EntropyBins::from_primitive(prim.as_view(), config.level, plan.options)
            .unwrap()
            .into_array();
        let back = eb.clone().execute::<PrimitiveArray>(&mut ctx).unwrap();
        let check = std::env::var_os("EB_SKIP_MERGE").is_none();
        match_each_integer_ptype!(prim.ptype(), |T| {
            if check {
                assert_eq!(back.as_slice::<T>(), prim.as_slice::<T>(), "{id} roundtrip");
            }
        });
        let eb_enc_before = plan_t + enc_t;
        plan_t += best_of(|| {
            black_box(EntropyBins::plan(prim.as_view(), &config).unwrap());
        });
        enc_t += best_of(|| {
            black_box(EntropyBins::from_primitive(prim.as_view(), config.level, plan.options).unwrap());
        });
        let d = best_of(|| {
            black_box(eb.clone().execute::<PrimitiveArray>(&mut ctx).unwrap());
        });
        dec_t += d;
        let e = plan_t + enc_t;
        if let Ok(bp) = vortex_fastlanes::bitpack_compress::bitpack_to_best_bit_width(&prim, &mut ctx) {
            let bp = bp.into_array();
            let back = bp.clone().execute::<PrimitiveArray>(&mut ctx).unwrap();
            match_each_integer_ptype!(prim.ptype(), |T| {
                assert_eq!(back.as_slice::<T>(), prim.as_slice::<T>(), "{id} bitpacked roundtrip");
            });
            sub_t[0] += e - eb_enc_before;
            sub_t[1] += d;
            sub_t[2] += best_of(|| {
                black_box(vortex_fastlanes::bitpack_compress::bitpack_to_best_bit_width(&prim, &mut ctx).unwrap());
            });
            sub_t[3] += best_of(|| {
                black_box(bp.clone().execute::<PrimitiveArray>(&mut ctx).unwrap());
            });
            sub[0] += prim.len();
            sub[1] += prim.nbytes() as usize;
            sub[2] += eb.nbytes() as usize;
            sub[3] += bp.nbytes() as usize;
        }
        n += prim.len();
        raw += prim.nbytes() as usize;
        bytes += eb.nbytes() as usize;
        if prim.ptype().byte_width() == 8 {
            n64 += prim.len();
            dec64 += d;
        }
    }
    let mvs = |n: usize, t: Duration| n as f64 / t.as_secs_f64() / 1e6;
    println!(
        "isa={} values={:.1}M ratio={:.3} plan={:.0}Mv/s encode={:.0}Mv/s plan+encode={:.0}Mv/s decode={:.0}Mv/s ({:.2} GB/s) decode64={:.0}Mv/s decode-narrow={:.0}Mv/s",
        std::env::var("VORTEX_ENTROPY_BINS_ISA").unwrap_or_else(|_| "native".into()),
        n as f64 / 1e6,
        raw as f64 / bytes as f64,
        mvs(n, plan_t),
        mvs(n, enc_t),
        mvs(n, plan_t + enc_t),
        mvs(n, dec_t),
        raw as f64 / dec_t.as_secs_f64() / 1e9,
        mvs(n64, dec64),
        mvs(n - n64, dec_t - dec64),
    );
    println!(
        "  bitpackable leaves {:.1}M values: eb ratio={:.3} enc={:.0}Mv/s dec={:.0}Mv/s ({:.2} GB/s) | fastlanes ratio={:.3} enc={:.0}Mv/s dec={:.0}Mv/s ({:.2} GB/s)",
        sub[0] as f64 / 1e6,
        sub[1] as f64 / sub[2] as f64,
        mvs(sub[0], sub_t[0]),
        mvs(sub[0], sub_t[1]),
        sub[1] as f64 / sub_t[1].as_secs_f64() / 1e9,
        sub[1] as f64 / sub[3] as f64,
        mvs(sub[0], sub_t[2]),
        mvs(sub[0], sub_t[3]),
        sub[1] as f64 / sub_t[3].as_secs_f64() / 1e9,
    );
}
