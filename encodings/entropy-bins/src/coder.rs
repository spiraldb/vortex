// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Bins, tANS tables and the block encoder.
//!
//! Block segment layout, all little-endian:
//!
//! ```text
//! uniform:           flags: u8 (bit 0 set: every id is one bin), bin id: u8
//! otherwise:         u16 header: bit 0 clear, bit 1 = stop field present, word count above;
//!                    lane states: 16 x S bits, stop field: 16 x 3 bits (if present),
//!                    id words: W bits x word count
//! offsets:           each value's offset, LSB first, at its bin's width
//! ```
//!
//! The id stream is a 16-lane tANS stream (value `i` belongs to lane `i % 16`). Every `R = W / S`
//! steps each lane with fewer than `W` buffered bits takes the next `W`-bit word, so the decoder
//! checks for refills only once per `R` steps. The word width `W` (8 or 16) is set per array:
//! 8-bit words waste fewer bits where lanes end mid-word, 16-bit words refill half as often. Near the end of a block a lane may have no words
//! left; the optional stop field records the round from which each lane stops refilling.

// Single-letter names follow the ANS literature: x is a coder state, s the state log, l = 2^s.
#![allow(clippy::many_single_char_names)]

use pco::ChunkConfig;
use pco::DeltaSpec;
use pco::ModeSpec;
use pco::metadata::DynBins;
use pco::wrapped::FileCompressor;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_err;

use crate::EntropyBinsChunk;

/// Values per independently decodable block.
pub const BLOCK_VALUES: usize = 1024;
/// Most values per block; arrays whose blocks would be dominated by their fixed per-block costs
/// use larger blocks, up to this.
pub const MAX_BLOCK_VALUES: usize = 16384;
/// Values per chunk sharing one set of bins.
pub const CHUNK_VALUES: usize = 1 << 16;
/// Interleaved tANS lanes.
pub(crate) const LANES: usize = 16;
/// Largest row distance a block may code differences against.
pub const MAX_LAG: usize = 8;
/// Largest tANS state log; keeps both decode tables in two registers.
const MAX_S: u32 = 7;
/// How many rounds before the end a lane may stop refilling (3 bits per lane).
const STOP_WINDOW: usize = 7;
/// Zero bytes after the last block so vector loads may read past it. The id kernel reads at most
/// one unneeded word per lane past a block's words (32 bytes of 16-bit words) plus a 32-byte load,
/// and the merges load a 64-byte window starting no later than the end of the offsets.
pub(crate) const TAIL_PADDING: usize = 64;

pub(crate) const FLAG_UNIFORM: u8 = 1;
pub(crate) const FLAG_STOP: u8 = 2;

/// Most bins the vector decoder keeps in registers.
const MAX_FAST_BINS: usize = 64;

/// Train the bins for one chunk, lowering pco's compression level (which caps the bin count)
/// until the chunk has at most [`MAX_FAST_BINS`] bins.
pub(crate) fn train_bins(latents: &[u64], level: usize) -> VortexResult<EntropyBinsChunk> {
    let mut lvl = level;
    loop {
        let chunk = train_bins_at(latents, lvl)?;
        if chunk.lowers.len() <= MAX_FAST_BINS || lvl == 0 {
            return Ok(chunk);
        }
        lvl -= 1;
    }
}

fn train_bins_at(latents: &[u64], level: usize) -> VortexResult<EntropyBinsChunk> {
    let config = ChunkConfig::default()
        .with_compression_level(level)
        .with_mode_spec(ModeSpec::Classic)
        .with_delta_spec(DeltaSpec::NoOp);
    let compressor = FileCompressor::default()
        .chunk_compressor(latents, &config)
        .map_err(|e| vortex_err!("pco bin training failed: {}", e.message))?;
    let var = &compressor.meta().per_latent_var.primary;
    let DynBins::U64(bins) = &var.bins else {
        vortex_bail!("pco returned non-u64 bins for u64 latents");
    };
    let mut bins: Vec<(u64, u32, u32)> = bins
        .iter()
        .map(|b| (b.lower, b.offset_bits, b.weight))
        .collect();
    bins.sort_unstable_by_key(|b| b.0);
    Ok(EntropyBinsChunk {
        n_values: u32::try_from(latents.len())?,
        ans_log: var.ans_size_log,
        lowers: bins.iter().map(|b| b.0).collect(),
        widths: bins.iter().map(|b| b.1).collect(),
        weights: bins.iter().map(|b| b.2).collect(),
    })
}

/// Index of the bin holding `v`, or `None` if no bin does. Cheap on misses, unlike [`bin_of`].
fn bin_index(chunk: &EntropyBinsChunk, v: u64) -> Option<usize> {
    let bin = chunk.lowers.partition_point(|&l| l <= v).checked_sub(1)?;
    let width = chunk.widths[bin];
    (width >= 64 || v - chunk.lowers[bin] < 1u64 << width).then_some(bin)
}

/// Index of the bin holding `v`: the last bin whose lower bound is at most `v`.
pub(crate) fn bin_of(chunk: &EntropyBinsChunk, v: u64) -> VortexResult<usize> {
    let idx = chunk.lowers.partition_point(|&l| l <= v);
    if idx == 0 {
        vortex_bail!("value {v} is below every bin");
    }
    let bin = idx - 1;
    let width = chunk.widths[bin];
    if width < 64 && v - chunk.lowers[bin] >= 1u64 << width {
        vortex_bail!("value {v} is outside its bin");
    }
    Ok(bin)
}

/// [`bin_of`] for a block of values, without building an error per value: the error is built
/// only if some value falls outside every bin.
fn bin_ids(chunk: &EntropyBinsChunk, latents: &[u64]) -> VortexResult<Vec<u8>> {
    let lowers = &chunk.lowers;
    let widths = &chunk.widths;
    let last = lowers.len().saturating_sub(1);
    let mut ids = Vec::with_capacity(latents.len());
    let mut ok = !lowers.is_empty();
    for &v in latents {
        // The last bin whose lower bound is at most `v`. Real columns are clustered, so the
        // search's branches predict well.
        let b = lowers
            .partition_point(|&l| l <= v)
            .saturating_sub(1)
            .min(last);
        let w = widths[b];
        let in_bin = v >= lowers[b] && (w >= 64 || v - lowers[b] < 1u64 << w);
        ok &= in_bin;
        // At most `MAX_FAST_BINS` bins, so the index fits a byte.
        #[allow(clippy::cast_possible_truncation)]
        ids.push(b as u8);
    }
    if !ok {
        for &v in latents {
            bin_of(chunk, v)?;
        }
        vortex_bail!("a value is outside every bin");
    }
    Ok(ids)
}

/// tANS tables for the bin ids of one chunk (absent when the chunk has a single bin).
#[derive(Clone, Debug)]
pub(crate) struct IdTable {
    /// State log, at most 7.
    pub(crate) s: u32,
    /// Steps between refill checks.
    pub(crate) r: usize,
    /// Bits per refill word: 8 or 16.
    pub(crate) word_bits: u32,
    freq: Vec<u32>,
    cum: Vec<u32>,
    enc_states: Vec<u32>,
    /// Encoding symbol `s` from state `x` emits `nb_base[s] - (x < nb_thresh[s])` bits: the
    /// smallest `nb` with `x >> nb < 2 freq[s]`.
    nb_base: Vec<u32>,
    nb_thresh: Vec<u32>,
    /// Decode tables indexed by `state & 127` for states in `[2^s, 2^(s+1))`.
    pub(crate) sym_tab: [u8; 128],
    pub(crate) xs_tab: [u8; 128],
    /// The same tables for gathers: `xs | nb << 8 | sym << 16`, where `nb` is the bits the step
    /// reads (`s - floor(log2(xs))`).
    pub(crate) dec32: [u32; 128],
}

impl IdTable {
    /// Requantize the chunk's weights to at most `2^7` states and build the tables.
    pub(crate) fn new(chunk: &EntropyBinsChunk, word_bits: u32) -> VortexResult<Option<Self>> {
        if word_bits != 8 && word_bits != 16 {
            vortex_bail!("refill words must be 8 or 16 bits, got {word_bits}");
        }
        let k = chunk.weights.len();
        if k <= 1 {
            return Ok(None);
        }
        if k > 1 << MAX_S {
            vortex_bail!("entropy bins support at most {} bins, got {k}", 1 << MAX_S);
        }
        let s_in = chunk.ans_log;
        if s_in > 32 {
            vortex_bail!("tANS table log {s_in} exceeds 32");
        }
        let s = s_in
            .min(MAX_S)
            .max(usize::BITS - (k - 1).leading_zeros())
            .max(1);
        let l = 1u32 << s;
        let half = if s_in == 0 { 0 } else { 1u64 << (s_in - 1) };
        let mut freq: Vec<u32> = chunk
            .weights
            .iter()
            .map(|&w| {
                // Capping each weight at the table size bounds the requantizing loop below.
                u32::try_from(((u64::from(w) << s) + half) >> s_in)
                    .unwrap_or(l)
                    .clamp(1, l)
            })
            .collect();
        loop {
            let sum: u32 = freq.iter().sum();
            if sum == l {
                break;
            }
            if sum > l {
                let Some(i) = (0..k).filter(|&i| freq[i] > 1).max_by_key(|&i| freq[i]) else {
                    vortex_bail!("cannot requantize {k} bins to {l} states");
                };
                freq[i] -= 1;
            } else {
                let Some(i) = (0..k).max_by_key(|&i| freq[i]) else {
                    vortex_bail!("no bins");
                };
                freq[i] += 1;
            }
        }
        let lu = l as usize;
        let mut spread = vec![0usize; lu];
        let step = if lu < 16 {
            1
        } else {
            (lu >> 1) + (lu >> 3) + 3
        };
        let mut pos = 0usize;
        for (sym, &f) in freq.iter().enumerate() {
            for _ in 0..f {
                spread[pos] = sym;
                pos = (pos + step) & (lu - 1);
            }
        }
        let mut cum = vec![0u32; k];
        for i in 1..k {
            cum[i] = cum[i - 1] + freq[i - 1];
        }
        let mut next = freq.clone();
        let mut seen = vec![0u32; k];
        let mut enc_states = vec![0u32; lu];
        let mut sym_tab = [0u8; 128];
        let mut xs_tab = [0u8; 128];
        for (u, &sym) in spread.iter().enumerate() {
            let xs = next[sym];
            next[sym] += 1;
            let x = u + lu;
            sym_tab[x & 127] = u8::try_from(sym)?;
            xs_tab[x & 127] = u8::try_from(xs)?;
            enc_states[(cum[sym] + seen[sym]) as usize] = u32::try_from(x)?;
            seen[sym] += 1;
        }
        let mut dec32 = [0u32; 128];
        for (i, e) in dec32.iter_mut().enumerate() {
            let xs = u32::from(xs_tab[i]);
            if xs > 0 {
                *e = xs | ((s - xs.ilog2()) << 8) | (u32::from(sym_tab[i]) << 16);
            }
        }
        let (nb_base, nb_thresh) = freq
            .iter()
            .map(|&f| {
                let b = s - f.ilog2();
                (b, f << b)
            })
            .unzip();
        Ok(Some(Self {
            s,
            r: (word_bits as usize / s as usize).min(8),
            word_bits,
            freq,
            cum,
            enc_states,
            nb_base,
            nb_thresh,
            sym_tab,
            xs_tab,
            dec32,
        }))
    }
}

/// Little-endian LSB-first bit writer.
struct BitWriter {
    bytes: Vec<u8>,
    acc: u64,
    n: u32,
}

impl BitWriter {
    fn new() -> Self {
        Self {
            bytes: Vec::new(),
            acc: 0,
            n: 0,
        }
    }

    fn put(&mut self, v: u64, w: u32) {
        if w == 0 {
            return;
        }
        let v = if w == 64 { v } else { v & ((1u64 << w) - 1) };
        self.acc |= v << self.n;
        let total = self.n + w;
        if total >= 64 {
            self.bytes.extend_from_slice(&self.acc.to_le_bytes());
            self.acc = if self.n == 0 { 0 } else { v >> (64 - self.n) };
            self.n = total - 64;
        } else {
            self.n = total;
        }
    }

    fn finish(mut self) -> Vec<u8> {
        let tail = self.n.div_ceil(8) as usize;
        self.bytes
            .extend_from_slice(&self.acc.to_le_bytes()[..tail]);
        self.bytes
    }
}

/// Append one block (`latents.len() <= MAX_BLOCK_VALUES`) to `out`.
pub(crate) fn encode_block(
    chunk: &EntropyBinsChunk,
    table: Option<&IdTable>,
    latents: &[u64],
    out: &mut Vec<u8>,
) -> VortexResult<()> {
    let ids = bin_ids(chunk, latents)?;
    match table {
        Some(t) if ids.iter().any(|&s| s != ids[0]) => encode_ids(t, &ids, out)?,
        _ => {
            out.push(FLAG_UNIFORM);
            out.push(ids.first().copied().unwrap_or(0));
        }
    }
    let mut w = BitWriter::new();
    for (&v, &id) in latents.iter().zip(&ids) {
        let id = usize::from(id);
        w.put(v - chunk.lowers[id], chunk.widths[id]);
    }
    out.extend_from_slice(&w.finish());
    Ok(())
}

/// The interleaved word stream as the decoder takes it, built round by round.
#[derive(Clone)]
struct Interleave {
    words: Vec<u16>,
    /// Length up to the last real (non-padding) word.
    real_end: usize,
    /// Length and real end the stream would have without a stop field.
    len_without: usize,
    real_end_without: usize,
    next: [usize; LANES],
    avail: [i64; LANES],
    stop: [u16; LANES],
}

impl Interleave {
    fn new(rounds: u16) -> Self {
        Self {
            words: Vec::new(),
            real_end: 0,
            len_without: 0,
            real_end_without: 0,
            next: [0; LANES],
            avail: [0; LANES],
            stop: [rounds; LANES],
        }
    }

    /// Take the words of `range`; a lane that runs out within the last `window` rounds stops
    /// instead of taking zero words.
    fn run(
        &mut self,
        lanes: &[&[u16]; LANES],
        round_bits: &[[u16; LANES]],
        range: std::ops::Range<usize>,
        wb: u32,
        rounds: usize,
        window: usize,
    ) -> VortexResult<()> {
        let wbi = i64::from(wb);
        for round in range {
            let consumed = &round_bits[round];
            for lane in 0..LANES {
                if self.avail[lane] < wbi && round < usize::from(self.stop[lane]) {
                    match lanes[lane].get(self.next[lane]) {
                        Some(&w) => {
                            self.words.push(w);
                            self.real_end = self.words.len();
                            self.len_without += 1;
                            self.real_end_without = self.len_without;
                        }
                        None if window > 0 && round + window >= rounds => {
                            self.stop[lane] = u16::try_from(round)?;
                            // Without a stop field the lane keeps taking zero words.
                            let mut a = self.avail[lane];
                            for later in &round_bits[round..] {
                                if a < wbi {
                                    self.len_without += 1;
                                    a += wbi;
                                }
                                a -= i64::from(later[lane]);
                            }
                            self.avail[lane] = a;
                            continue;
                        }
                        None => {
                            self.words.push(0);
                            self.len_without += 1;
                        }
                    }
                    self.next[lane] += 1;
                    self.avail[lane] += wbi;
                }
                self.avail[lane] -= i64::from(consumed[lane]);
            }
        }
        Ok(())
    }
}

fn encode_ids(t: &IdTable, ids: &[u8], out: &mut Vec<u8>) -> VortexResult<()> {
    let n = ids.len();
    let n_pad = n.div_ceil(LANES) * LANES;
    let steps = n_pad / LANES;
    let sym = |i: usize| usize::from(ids[i.min(n - 1)]);
    let l = 1u32 << t.s;
    // Encode the lanes backwards, all 16 per step so their state chains overlap, to get every
    // value's state bits. A lane's last symbol needs no bits: the state the decoder would
    // rebuild after it is never used, so the lane starts in the smallest state of that symbol
    // instead (zstd's `FSE_initCState2`).
    let mut bit_value = vec![0u8; n_pad];
    let mut bit_count = vec![0u8; n_pad];
    let mut x = [0u32; LANES];
    for (lane, xl) in x.iter_mut().enumerate() {
        let s_last = sym(n_pad - LANES + lane);
        let first = t.cum[s_last] as usize;
        *xl = t.enc_states[first..first + t.freq[s_last] as usize]
            .iter()
            .copied()
            .min()
            .unwrap_or(l);
    }
    for step in (0..steps - 1).rev() {
        for (lane, xl) in x.iter_mut().enumerate() {
            let i = step * LANES + lane;
            let s = sym(i);
            let nb = t.nb_base[s] - u32::from(*xl < t.nb_thresh[s]);
            // `nb <= s_log <= 7`, so both fit a byte.
            #[allow(clippy::cast_possible_truncation)]
            {
                bit_value[i] = (*xl & ((1u32 << nb) - 1)) as u8;
                bit_count[i] = nb as u8;
            }
            *xl = t.enc_states[(t.cum[s] + (*xl >> nb) - t.freq[s]) as usize];
        }
    }
    let mut states = [0u8; LANES];
    for (state, &xl) in states.iter_mut().zip(&x) {
        *state = u8::try_from(xl - l)?;
    }
    // Per-lane word streams.
    let wb = t.word_bits;
    let word_mask = (1u64 << wb) - 1;
    // A lane reads at most `s <= 7` bits per step, so it fills at most this many words.
    let lane_cap = steps * t.s as usize / wb as usize + 2;
    let mut lane_words = vec![0u16; LANES * lane_cap];
    let mut lane_len = [0usize; LANES];
    let mut acc = [0u64; LANES];
    let mut nacc = [0u32; LANES];
    for (values, counts) in bit_value[..n_pad]
        .as_chunks::<LANES>()
        .0
        .iter()
        .zip(bit_count[..n_pad].as_chunks::<LANES>().0)
    {
        for lane in 0..LANES {
            acc[lane] |= u64::from(values[lane]) << nacc[lane];
            nacc[lane] += u32::from(counts[lane]);
            if nacc[lane] >= wb {
                // Masked to `wb <= 16` bits.
                #[allow(clippy::cast_possible_truncation)]
                {
                    lane_words[lane * lane_cap + lane_len[lane]] = (acc[lane] & word_mask) as u16;
                }
                lane_len[lane] += 1;
                acc[lane] >>= wb;
                nacc[lane] -= wb;
            }
        }
    }
    for lane in 0..LANES {
        if nacc[lane] > 0 {
            lane_words[lane * lane_cap + lane_len[lane]] = u16::try_from(acc[lane] & word_mask)?;
            lane_len[lane] += 1;
        }
    }
    let lanes: [&[u16]; LANES] =
        std::array::from_fn(|lane| &lane_words[lane * lane_cap..lane * lane_cap + lane_len[lane]]);
    // Interleave the words in the order the decoder takes them, with and without a stop field.
    let r = t.r;
    let rounds = (n_pad / LANES).div_ceil(r);
    // Bits each lane consumes per round: refills happen only at round starts.
    let mut round_bits = vec![[0u16; LANES]; rounds];
    for (step, counts) in bit_count[..n_pad].as_chunks::<LANES>().0.iter().enumerate() {
        let consumed = &mut round_bits[step / r];
        for (c, &w) in consumed.iter_mut().zip(counts) {
            *c += u16::from(w);
        }
    }
    // Rounds before the last `STOP_WINDOW` interleave the same way with or without a stop field,
    // so they run once; the window then runs with a stop field (counting how long the stream
    // without one would be) and, only if that loses, again from a snapshot without one.
    let split = rounds.saturating_sub(STOP_WINDOW);
    let mut head = Interleave::new(u16::try_from(rounds)?);
    head.run(&lanes, &round_bits, 0..split, wb, rounds, 0)?;
    let mut with_stop = head.clone();
    with_stop.run(&lanes, &round_bits, split..rounds, wb, rounds, STOP_WINDOW)?;
    let word_bytes = wb as usize / 8;
    let has_stop = with_stop.real_end * word_bytes + 6 < with_stop.real_end_without * word_bytes;
    let (words, stop) = if has_stop {
        let mut words = with_stop.words;
        // Trailing words a lane never needs are served by the zero padding.
        words.truncate(with_stop.real_end);
        (words, with_stop.stop)
    } else {
        let mut without = head;
        without.run(&lanes, &round_bits, split..rounds, wb, rounds, 0)?;
        let mut words = without.words;
        words.truncate(without.real_end);
        debug_assert_eq!(words.len(), with_stop.real_end_without);
        (words, without.stop)
    };

    let flags = if has_stop { FLAG_STOP } else { 0 };
    let header = u16::try_from(words.len() << 2)? | u16::from(flags);
    out.extend_from_slice(&header.to_le_bytes());
    let mut sw = BitWriter::new();
    for &st in &states {
        sw.put(u64::from(st), t.s);
    }
    out.extend_from_slice(&sw.finish());
    if has_stop {
        let mut stw = BitWriter::new();
        for &st in &stop {
            stw.put((rounds - usize::from(st)) as u64, 3);
        }
        out.extend_from_slice(&stw.finish());
    }
    for w in words {
        out.extend_from_slice(&w.to_le_bytes()[..word_bytes]);
    }
    Ok(())
}

/// Estimated bits per value when bins trained on `train` encode `test`. A test value outside
/// every trained bin costs a full-range escape, so data whose support drifts (sorted, trending)
/// is not mistaken for data that compresses.
pub(crate) fn held_out_bits(
    train: &[u64],
    test: &[u64],
    level: usize,
) -> VortexResult<(f64, usize)> {
    if test.is_empty() {
        return Ok((0.0, 1));
    }
    let chunk = train_bins(train, level)?;
    let total_weight = f64::from(chunk.weights.iter().sum::<u32>().max(1));
    let (lo, hi) = test
        .iter()
        .chain(train)
        .fold((u64::MAX, 0u64), |(lo, hi), &v| (lo.min(v), hi.max(v)));
    let n_bins = chunk.weights.len().max(1);
    let escape = f64::from(64 - (hi - lo).leading_zeros()) + (n_bins as f64).log2() + 1.0;
    let bits: f64 = test
        .iter()
        .map(|&v| match bin_index(&chunk, v) {
            Some(b) => {
                -(f64::from(chunk.weights[b].max(1)) / total_weight).log2()
                    + f64::from(chunk.widths[b])
            }
            None => escape,
        })
        .sum();
    Ok((bits / test.len() as f64, n_bins))
}
