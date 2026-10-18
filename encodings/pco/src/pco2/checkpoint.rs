// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Mid-page checkpoints for pco pages.
//!
//! A pco page is a single bitstream: a page header holding, for each latent variable, the delta
//! state and the four interleaved tANS decoder states, followed by batches of 256 numbers. The
//! decoder carries exactly that header-shaped state from batch to batch, so recording it (plus
//! the bit position) at a batch boundary is enough to resume decoding there.
//!
//! pco keeps its decoder state private, so [`PageWalker`] re-derives it from the public chunk
//! metadata: it rebuilds the tANS decode table from the bin weights exactly as pco does, reads
//! tANS symbols and offset bits batch by batch, and replays consecutive delta decoding to track
//! the delta moments. Modes never need replaying because pco applies them after delta decoding.
//! A correct walk is self-checking: it must end on the page's last byte with every tANS lane back
//! in the encoder's initial state, so a format drift in pco fails compression rather than
//! corrupting reads.
//!
//! To resume at a checkpoint, [`decode_blocks`] hands pco's ordinary public page decompressor a
//! synthetic page: the recorded state, serialized in pco's own page-header layout, followed by the
//! original bitstream from the recorded bit position, shifted onto a byte boundary.
//!
//! Lookback and convolution delta encodings carry state proportional to their window, so chunks
//! using them are not walkable; [`PageWalker::try_new`] returns `None` and the caller falls back
//! to short pages.

use std::ops::Range;

use pco::FULL_BATCH_N;
use pco::data_types::Latent;
use pco::data_types::Number;
use pco::match_latent_enum;
use pco::metadata::Bin;
use pco::metadata::ChunkLatentVarMeta;
use pco::metadata::ChunkMeta;
use pco::metadata::DeltaEncoding;
use pco::metadata::DynBins;
use pco::wrapped::ChunkDecompressor;
use vortex_error::VortexResult;
use vortex_error::vortex_ensure;
use vortex_error::vortex_ensure_eq;
use vortex_error::vortex_err;

use crate::array::vortex_err_from_pco;

/// The number of interleaved tANS decoders per latent variable in pco's format.
const ANS_INTERLEAVING: usize = 4;

/// Bytes pco's bit reader asks to have readable per latent variable per batch
/// (`MAX_BATCH_LATENT_VAR_SIZE` in pco). Padding a synthetic page by this much keeps pco reading
/// in place instead of copying the tail of the page into a scratch buffer.
const READ_PADDING: usize = FULL_BATCH_N * (16 + 2) + 9;

/// Size of the bit position stored at the front of each checkpoint record.
const BIT_POSITION_BYTES: usize = size_of::<u32>();

/// Least-significant-bit-first reader matching pco's bit order.
struct BitReader<'a> {
    src: &'a [u8],
    bit_idx: usize,
}

impl<'a> BitReader<'a> {
    fn new(src: &'a [u8]) -> Self {
        Self { src, bit_idx: 0 }
    }

    /// Loads 8 bytes starting at `byte_idx`, treating bytes past the end as zero.
    fn word_at(&self, byte_idx: usize) -> u64 {
        match self.src.get(byte_idx..byte_idx + 8) {
            Some(bytes) => {
                let mut word = [0u8; 8];
                word.copy_from_slice(bytes);
                u64::from_le_bytes(word)
            }
            None => {
                let mut word = [0u8; 8];
                if let Some(tail) = self.src.get(byte_idx..) {
                    word[..tail.len()].copy_from_slice(tail);
                }
                u64::from_le_bytes(word)
            }
        }
    }

    /// Reads `n <= 64` bits.
    fn read(&mut self, n: u32) -> u64 {
        if n == 0 {
            return 0;
        }
        let byte_idx = self.bit_idx / 8;
        let shift = sub_byte_shift(self.bit_idx);
        let mut value = self.word_at(byte_idx) >> shift;
        if n > 64 - shift {
            value |= self.word_at(byte_idx + 8) << (64 - shift);
        }
        self.bit_idx += n as usize;
        if n == 64 {
            value
        } else {
            value & ((1u64 << n) - 1)
        }
    }

    /// Reads `n <= 32` bits.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "a read of at most 32 bits fits u32"
    )]
    fn read_u32(&mut self, n: u32) -> u32 {
        debug_assert!(n <= 32);
        self.read(n) as u32
    }

    fn align_to_byte(&mut self) {
        self.bit_idx = self.bit_idx.next_multiple_of(8);
    }
}

/// The bit offset of `bit_idx` within its byte.
#[expect(clippy::cast_possible_truncation, reason = "the value is below 8")]
fn sub_byte_shift(bit_idx: usize) -> u32 {
    (bit_idx % 8) as u32
}

/// Least-significant-bit-first writer matching pco's bit order.
#[derive(Default)]
struct BitWriter {
    bytes: Vec<u8>,
    bit_idx: usize,
}

impl BitWriter {
    /// Writes the low `n <= 64` bits of `value`.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "each step writes at most the bits left in one byte"
    )]
    fn write(&mut self, mut value: u64, mut n: u32) {
        while n > 0 {
            let byte_idx = self.bit_idx / 8;
            if byte_idx == self.bytes.len() {
                self.bytes.push(0);
            }
            let shift = sub_byte_shift(self.bit_idx);
            let take = n.min(8 - shift);
            let mask = (1u64 << take) - 1;
            self.bytes[byte_idx] |= ((value & mask) << shift) as u8;
            value = value.checked_shr(take).unwrap_or(0);
            n -= take;
            self.bit_idx += take as usize;
        }
    }
}

/// One entry of pco's tANS decode table.
struct Node {
    next_state_idx_base: u32,
    bits_to_read: u32,
    offset_bits: u32,
}

/// The decoder of one latent variable: the tANS table and offsets of its bins, plus the state
/// that pco carries between batches.
trait LatentVarWalker {
    /// Bits this variable occupies in a page header.
    fn state_bits(&self) -> usize;
    /// Reads this variable's state from a page header.
    fn read_state(&mut self, reader: &mut BitReader<'_>);
    /// Writes this variable's current state in page header layout.
    fn write_state(&self, writer: &mut BitWriter);
    /// Consumes one batch, given the count of numbers left in the page at its start.
    fn read_batch(&mut self, reader: &mut BitReader<'_>, n_remaining: usize) -> VortexResult<()>;
    /// Whether every tANS lane is back in the encoder's initial state.
    fn ans_drained(&self) -> bool;
}

struct VarWalker<L: Latent> {
    ans_size_log: u32,
    n_bins: usize,
    nodes: Vec<Node>,
    lowers: Vec<L>,
    only_offset_bits: u32,
    delta_order: usize,
    ans_states: [u32; ANS_INTERLEAVING],
    moments: Vec<L>,
    // Like pco's scratch, this persists across batches: the delta moments after the last full
    // batch depend on whatever it holds past the final pre-delta latent.
    latents: Vec<L>,
    offset_bits: Vec<u32>,
}

/// pco's tANS stride: an odd number near 3/5 of the table size.
fn choose_stride(table_size: u32) -> u32 {
    let stride = (3 * table_size) / 5;
    if stride.is_multiple_of(2) {
        stride + 1
    } else {
        stride
    }
}

impl<L: Latent> VarWalker<L> {
    fn new(bins: &[Bin<L>], ans_size_log: u32, delta_order: usize) -> VortexResult<Self> {
        // Mirrors pco's `Spec::from_weights` and `Decoder::new`, which must stay backward
        // compatible with every pco file ever written.
        let weights = if bins.is_empty() {
            vec![1]
        } else {
            bins.iter().map(|bin| bin.weight).collect::<Vec<_>>()
        };
        vortex_ensure!(ans_size_log < 32, "invalid tANS size log {ans_size_log}");
        let table_size = weights
            .iter()
            .try_fold(0u32, |acc, &weight| acc.checked_add(weight))
            .ok_or_else(|| vortex_err!("tANS weights overflow"))?;
        vortex_ensure_eq!(
            table_size,
            1u32 << ans_size_log,
            "tANS weights do not sum to the table size"
        );

        let mut state_symbols = vec![0u32; table_size as usize];
        let stride = choose_stride(table_size);
        let mask = table_size - 1;
        let mut step = 0u32;
        for (symbol, &weight) in weights.iter().enumerate() {
            for _ in 0..weight {
                state_symbols[(stride.wrapping_mul(step) & mask) as usize] = u32::try_from(symbol)?;
                step += 1;
            }
        }

        let mut symbol_x_s = weights;
        let mut nodes = Vec::with_capacity(state_symbols.len());
        let mut lowers = Vec::with_capacity(state_symbols.len());
        for &symbol in &state_symbols {
            let symbol = symbol as usize;
            let next_state_base = symbol_x_s[symbol];
            let bits_to_read = next_state_base.leading_zeros() - table_size.leading_zeros();
            nodes.push(Node {
                next_state_idx_base: (next_state_base << bits_to_read) - table_size,
                bits_to_read,
                offset_bits: bins.get(symbol).map_or(0, |bin| bin.offset_bits),
            });
            lowers.push(bins.get(symbol).map_or(L::ZERO, |bin| bin.lower));
            symbol_x_s[symbol] += 1;
        }

        Ok(Self {
            ans_size_log,
            n_bins: bins.len(),
            nodes,
            lowers,
            only_offset_bits: match bins {
                [bin] => bin.offset_bits,
                _ => 0,
            },
            delta_order,
            ans_states: [0; ANS_INTERLEAVING],
            moments: Vec::with_capacity(delta_order),
            latents: vec![L::ZERO; FULL_BATCH_N],
            offset_bits: vec![0; FULL_BATCH_N],
        })
    }
}

impl<L: Latent> LatentVarWalker for VarWalker<L> {
    fn state_bits(&self) -> usize {
        self.delta_order * L::BITS as usize + ANS_INTERLEAVING * self.ans_size_log as usize
    }

    fn read_state(&mut self, reader: &mut BitReader<'_>) {
        self.moments.clear();
        for _ in 0..self.delta_order {
            self.moments.push(L::from_u64(reader.read(L::BITS)));
        }
        for state in &mut self.ans_states {
            *state = reader.read_u32(self.ans_size_log);
        }
    }

    fn write_state(&self, writer: &mut BitWriter) {
        for &moment in &self.moments {
            writer.write(moment.to_u64(), L::BITS);
        }
        for &state in &self.ans_states {
            writer.write(u64::from(state), self.ans_size_log);
        }
    }

    fn read_batch(&mut self, reader: &mut BitReader<'_>, n_remaining: usize) -> VortexResult<()> {
        let pre_delta_len = FULL_BATCH_N.min(n_remaining.saturating_sub(self.delta_order));
        if pre_delta_len > 0 {
            if self.n_bins > 1 {
                for i in 0..pre_delta_len {
                    let lane = i % ANS_INTERLEAVING;
                    let state_idx = self.ans_states[lane] as usize;
                    let node = self
                        .nodes
                        .get(state_idx)
                        .ok_or_else(|| vortex_err!("tANS state {state_idx} out of range"))?;
                    let ans_val = reader.read_u32(node.bits_to_read);
                    self.latents[i] = self.lowers[state_idx];
                    self.offset_bits[i] = node.offset_bits;
                    self.ans_states[lane] = node.next_state_idx_base + ans_val;
                }
            } else {
                self.latents[..pre_delta_len].fill(self.lowers[0]);
                self.offset_bits[..pre_delta_len].fill(self.only_offset_bits);
            }
            for i in 0..pre_delta_len {
                let offset = reader.read(self.offset_bits[i]);
                self.latents[i] = self.latents[i].wrapping_add(L::from_u64(offset));
            }
        }

        if self.delta_order > 0 {
            let latents = &mut self.latents[..n_remaining.min(FULL_BATCH_N)];
            for latent in latents.iter_mut() {
                *latent = latent.toggle_center();
            }
            for moment in self.moments.iter_mut().rev() {
                for latent in latents.iter_mut() {
                    let delta = *latent;
                    *latent = *moment;
                    *moment = moment.wrapping_add(delta);
                }
            }
        }
        Ok(())
    }

    fn ans_drained(&self) -> bool {
        self.n_bins <= 1 || self.ans_states.iter().all(|&state| state == 0)
    }
}

fn var_walker(
    meta: &ChunkLatentVarMeta,
    delta_order: usize,
) -> VortexResult<Box<dyn LatentVarWalker>> {
    Ok(match_latent_enum!(
        &meta.bins,
        DynBins<L>(bins) => {
            Box::new(VarWalker::<L>::new(bins, meta.ans_size_log, delta_order)?)
        }
    ))
}

/// Re-derives pco's decoder state while walking the pages of one chunk.
pub(crate) struct PageWalker {
    vars: Vec<Box<dyn LatentVarWalker>>,
}

impl PageWalker {
    /// Returns `None` when the chunk's delta encoding carries state too large to checkpoint.
    pub(crate) fn try_new(meta: &ChunkMeta) -> VortexResult<Option<Self>> {
        let (primary_order, secondary_order) = match &meta.delta_encoding {
            DeltaEncoding::NoOp => (0, 0),
            DeltaEncoding::Consecutive {
                order,
                secondary_uses_delta,
            } => (*order, if *secondary_uses_delta { *order } else { 0 }),
            _ => return Ok(None),
        };
        if meta.per_latent_var.delta.is_some() {
            return Ok(None);
        }

        let mut vars = vec![var_walker(&meta.per_latent_var.primary, primary_order)?];
        if let Some(secondary) = &meta.per_latent_var.secondary {
            vars.push(var_walker(secondary, secondary_order)?);
        }
        Ok(Some(Self { vars }))
    }

    /// Bytes of one checkpoint record: the bit position followed by a pco page header.
    pub(crate) fn record_bytes(&self) -> usize {
        let header_bits = self.vars.iter().map(|var| var.state_bits()).sum::<usize>();
        BIT_POSITION_BYTES + header_bits.div_ceil(8)
    }

    /// Walks one page and appends a checkpoint record for every `interval` numbers after the
    /// first, i.e. `(page_n - 1) / interval` records.
    pub(crate) fn checkpoint_page(
        &mut self,
        page: &[u8],
        page_n: usize,
        interval: usize,
        out: &mut Vec<u8>,
    ) -> VortexResult<()> {
        vortex_ensure!(
            interval > 0 && interval.is_multiple_of(FULL_BATCH_N),
            "checkpoint interval {interval} must be a positive multiple of {FULL_BATCH_N}"
        );
        let record_bytes = self.record_bytes();
        let batches_per_checkpoint = interval / FULL_BATCH_N;

        let mut reader = BitReader::new(page);
        for var in &mut self.vars {
            var.read_state(&mut reader);
        }
        reader.align_to_byte();

        for batch_idx in 0..page_n.div_ceil(FULL_BATCH_N) {
            if batch_idx > 0 && batch_idx.is_multiple_of(batches_per_checkpoint) {
                let record_start = out.len();
                out.extend_from_slice(&u32::try_from(reader.bit_idx)?.to_le_bytes());
                let mut writer = BitWriter::default();
                for var in &self.vars {
                    var.write_state(&mut writer);
                }
                out.extend_from_slice(&writer.bytes);
                vortex_ensure_eq!(out.len() - record_start, record_bytes);
            }
            let n_remaining = page_n - batch_idx * FULL_BATCH_N;
            for var in &mut self.vars {
                var.read_batch(&mut reader, n_remaining)?;
            }
        }

        vortex_ensure_eq!(
            reader.bit_idx.div_ceil(8),
            page.len(),
            "pco page walk ended at bit {} of a {}-byte page",
            reader.bit_idx,
            page.len()
        );
        vortex_ensure!(
            self.vars.iter().all(|var| var.ans_drained()),
            "pco page walk ended with undrained tANS states"
        );
        Ok(())
    }
}

/// Appends the bits `start_bit..end_bit` of `src` to `dst`, shifted to start on a byte boundary.
/// Bits past the end of `src` read as zero.
fn append_shifted_bits(dst: &mut Vec<u8>, src: &[u8], start_bit: usize, end_bit: usize) {
    let src = &src[start_bit / 8..];
    let n_bytes = (end_bit - start_bit).div_ceil(8);
    let shift = sub_byte_shift(start_bit);
    if shift == 0 {
        dst.extend_from_slice(&src[..n_bytes]);
        return;
    }

    dst.reserve(n_bytes);
    let mut i = 0;
    while i + 8 <= n_bytes && i + 9 <= src.len() {
        let mut word = [0u8; 8];
        word.copy_from_slice(&src[i..i + 8]);
        let shifted = (u64::from_le_bytes(word) >> shift) | (u64::from(src[i + 8]) << (64 - shift));
        dst.extend_from_slice(&shifted.to_le_bytes());
        i += 8;
    }
    while i < n_bytes {
        let high = src.get(i + 1).map_or(0, |&byte| byte << (8 - shift));
        dst.push((src[i] >> shift) | high);
        i += 1;
    }
}

/// The checkpoint records of one page.
#[derive(Clone, Copy)]
pub(crate) struct PageCheckpoints<'a> {
    pub(crate) records: &'a [u8],
    pub(crate) record_bytes: usize,
    pub(crate) interval: usize,
}

impl PageCheckpoints<'_> {
    /// The bit position in the page where block `block` (`>= 1`) begins.
    fn bit_position(&self, block: usize) -> usize {
        let start = (block - 1) * self.record_bytes;
        let mut bytes = [0u8; BIT_POSITION_BYTES];
        bytes.copy_from_slice(&self.records[start..start + BIT_POSITION_BYTES]);
        u32::from_le_bytes(bytes) as usize
    }

    /// The pco page header that resumes decoding at block `block` (`>= 1`).
    fn page_header(&self, block: usize) -> &[u8] {
        let start = (block - 1) * self.record_bytes;
        &self.records[start + BIT_POSITION_BYTES..start + self.record_bytes]
    }
}

/// Decodes the numbers of checkpoint blocks `blocks` of one page into `dst`.
///
/// Block `b` holds numbers `b * interval..min((b + 1) * interval, page_n)`; `dst` must hold
/// exactly the numbers of `blocks`. Block 0 decodes from the page itself; any other start
/// decodes from a synthetic page built from its checkpoint, using `scratch` for the bytes.
#[allow(clippy::too_many_arguments)]
pub(crate) fn decode_blocks<T: Number>(
    cd: &mut ChunkDecompressor<T>,
    page: &[u8],
    page_n: usize,
    checkpoints: PageCheckpoints<'_>,
    blocks: Range<usize>,
    dst: &mut [T],
    scratch: &mut Vec<u8>,
) -> VortexResult<()> {
    let interval = checkpoints.interval;
    let value_start = blocks.start * interval;
    let value_end = (blocks.end * interval).min(page_n);
    vortex_ensure!(
        value_start < value_end,
        "empty checkpoint block range {blocks:?} for a page of {page_n} numbers"
    );
    vortex_ensure_eq!(dst.len(), value_end - value_start);

    if blocks.start == 0 {
        let mut pd = cd
            .page_decompressor(page, page_n)
            .map_err(vortex_err_from_pco)?;
        pd.read(dst).map_err(vortex_err_from_pco)?;
        return Ok(());
    }

    let start_bit = checkpoints.bit_position(blocks.start);
    let end_bit = if value_end < page_n {
        checkpoints.bit_position(blocks.end)
    } else {
        page.len() * 8
    };
    vortex_ensure!(
        start_bit <= end_bit && end_bit <= page.len() * 8,
        "checkpoint bit range {start_bit}..{end_bit} exceeds a {}-byte page",
        page.len()
    );

    scratch.clear();
    scratch.extend_from_slice(checkpoints.page_header(blocks.start));
    append_shifted_bits(scratch, page, start_bit, end_bit);
    scratch.resize(scratch.len() + READ_PADDING, 0);

    let mut pd = cd
        .page_decompressor(scratch.as_slice(), page_n - value_start)
        .map_err(vortex_err_from_pco)?;
    pd.read(dst).map_err(vortex_err_from_pco)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case(0, 64)]
    #[case(3, 64)]
    #[case(7, 61)]
    #[case(5, 13)]
    fn bit_reader_matches_writer(#[case] lead_bits: u32, #[case] width: u32) {
        let mut writer = BitWriter::default();
        writer.write(0b101, lead_bits);
        let value = 0xdead_beef_cafe_f00d_u64 & (u64::MAX >> (64 - width));
        writer.write(value, width);
        writer.write(1, 1);

        let mut reader = BitReader::new(&writer.bytes);
        assert_eq!(reader.read(lead_bits), 0b101 & ((1 << lead_bits) - 1));
        assert_eq!(reader.read(width), value);
        assert_eq!(reader.read(1), 1);
    }

    #[rstest]
    #[case(0, 80)]
    #[case(3, 80)]
    #[case(13, 140)]
    #[case(5, 6)]
    fn shifted_bits_round_trip(#[case] start_bit: usize, #[case] end_bit: usize) {
        let src = (0u8..32)
            .map(|byte| byte.wrapping_mul(37))
            .collect::<Vec<_>>();
        let mut shifted = Vec::new();
        append_shifted_bits(&mut shifted, &src, start_bit, end_bit);
        assert_eq!(shifted.len(), (end_bit - start_bit).div_ceil(8));

        let mut expected = BitReader::new(&src);
        expected.bit_idx = start_bit;
        let mut actual = BitReader::new(&shifted);
        for _ in start_bit..end_bit {
            assert_eq!(actual.read(1), expected.read(1));
        }
    }
}
