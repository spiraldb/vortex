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
//! A chunk's records are stored as a [`CheckpointTable`]: one column per field (the bit position,
//! each delta moment, each tANS state), each holding bit-packed residuals from a line through its
//! values. Bit positions and moments grow steadily from block to block, so their residuals need
//! far fewer bits than the values, and any record still decodes in constant time.
//!
//! Lookback and convolution delta encodings carry state proportional to their window, so chunks
//! using them are not walkable; [`PageWalker::try_new`] returns `None` and the caller falls back
//! to short pages.

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

/// Bits of the bit position column of a checkpoint table. Pages hold at most 2^24 numbers of at
/// most 64 bits per latent, so positions fit.
const POSITION_BITS: u32 = 32;

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
    /// Appends the bits of each field of this variable's page header state, in header order.
    fn state_bits(&self, out: &mut Vec<u32>);
    /// Reads this variable's state from a page header.
    fn read_state(&mut self, reader: &mut BitReader<'_>);
    /// Appends each field of this variable's current state, in header order.
    fn state_values(&self, out: &mut Vec<u64>);
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
    fn state_bits(&self, out: &mut Vec<u32>) {
        out.extend(std::iter::repeat_n(L::BITS, self.delta_order));
        out.extend([self.ans_size_log; ANS_INTERLEAVING]);
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

    fn state_values(&self, out: &mut Vec<u64>) {
        out.extend(self.moments.iter().map(|moment| moment.to_u64()));
        out.extend(self.ans_states.iter().map(|&state| u64::from(state)));
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

    /// An empty set of records for this chunk.
    pub(crate) fn new_records(&self) -> Records {
        let mut bits = vec![POSITION_BITS];
        for var in &self.vars {
            var.state_bits(&mut bits);
        }
        Records {
            columns: vec![Vec::new(); bits.len()],
            bits,
            page_records: Vec::new(),
        }
    }

    /// Walks one page and appends a checkpoint record for every `interval` numbers after the
    /// first, i.e. `(page_n - 1) / interval` records.
    pub(crate) fn checkpoint_page(
        &mut self,
        page: &[u8],
        page_n: usize,
        interval: usize,
        records: &mut Records,
    ) -> VortexResult<()> {
        vortex_ensure!(
            interval > 0 && interval.is_multiple_of(FULL_BATCH_N),
            "checkpoint interval {interval} must be a positive multiple of {FULL_BATCH_N}"
        );
        let batches_per_checkpoint = interval / FULL_BATCH_N;

        let mut reader = BitReader::new(page);
        for var in &mut self.vars {
            var.read_state(&mut reader);
        }
        reader.align_to_byte();

        let mut record = Vec::with_capacity(records.bits.len());
        records.page_records.push((page_n - 1) / interval);
        for batch_idx in 0..page_n.div_ceil(FULL_BATCH_N) {
            if batch_idx > 0 && batch_idx.is_multiple_of(batches_per_checkpoint) {
                record.clear();
                record.push(u64::try_from(reader.bit_idx)?);
                for var in &self.vars {
                    var.state_values(&mut record);
                }
                vortex_ensure_eq!(record.len(), records.columns.len());
                for (column, &value) in records.columns.iter_mut().zip(&record) {
                    column.push(value);
                }
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

/// The checkpoint records of one chunk, gathered field by field.
pub(crate) struct Records {
    /// Bits of each field: the bit position, then the page header fields in header order.
    bits: Vec<u32>,
    columns: Vec<Vec<u64>>,
    /// The number of records of each checkpointed page, in order.
    page_records: Vec<usize>,
}

impl Records {
    pub(crate) fn is_empty(&self) -> bool {
        self.columns[0].is_empty()
    }

    /// The records of an interval `factor` times longer: every `factor`-th record of each page.
    pub(crate) fn coarsen(&self, factor: usize) -> Self {
        let mut columns = vec![Vec::new(); self.columns.len()];
        let mut page_records = Vec::with_capacity(self.page_records.len());
        let mut start = 0;
        for &n in &self.page_records {
            let kept = (start + factor - 1..start + n).step_by(factor);
            for (coarse, column) in columns.iter_mut().zip(&self.columns) {
                coarse.extend(kept.clone().map(|record| column[record]));
            }
            page_records.push(kept.len());
            start += n;
        }
        Self {
            bits: self.bits.clone(),
            columns,
            page_records,
        }
    }

    /// Serializes the records as a [`CheckpointTable`].
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        write_varint(&mut out, self.columns.len() as u64);
        let columns = self
            .columns
            .iter()
            .zip(&self.bits)
            .map(|(values, &bits)| fit_column(values, bits))
            .collect::<Vec<_>>();
        for (column, &value_bits) in columns.iter().zip(&self.bits) {
            write_varint(&mut out, u64::from(value_bits));
            write_varint(&mut out, u64::from(column.width));
            write_varint(&mut out, column.base);
            write_varint(&mut out, zigzag(column.slope));
            write_varint(&mut out, zigzag(column.offset));
        }
        let mut writer = BitWriter::default();
        for column in &columns {
            for &residual in &column.residuals {
                writer.write(residual, column.width);
            }
        }
        out.extend_from_slice(&writer.bytes);
        out
    }
}

/// A column's values as residuals from the line `base + slope * record`, offset to be
/// non-negative. Arithmetic wraps at the column's bit width.
struct FittedColumn {
    width: u32,
    base: u64,
    slope: u64,
    offset: u64,
    residuals: Vec<u64>,
}

/// Fits whichever of a flat line and the line through the first and last values packs into
/// fewer bits.
fn fit_column(values: &[u64], bits: u32) -> FittedColumn {
    let mask = bit_mask(bits);
    let base = values.first().map_or(0, |&value| value & mask);
    let through_ends = match values {
        [_, .., last] => {
            let rise = sign_extend(last.wrapping_sub(base) & mask, bits);
            // `values` has at least two elements here.
            (rise / (values.len() as i64 - 1)) as u64
        }
        _ => 0,
    };
    let fit = |slope: u64| {
        let signed = values
            .iter()
            .zip(0u64..)
            .map(|(&value, record)| {
                let predicted = base.wrapping_add(slope.wrapping_mul(record));
                sign_extend(value.wrapping_sub(predicted) & mask, bits)
            })
            .collect::<Vec<_>>();
        let offset = signed.iter().copied().min().unwrap_or(0);
        let residuals = signed
            .iter()
            .map(|&residual| residual.wrapping_sub(offset) as u64)
            .collect::<Vec<_>>();
        let max = residuals.iter().copied().max().unwrap_or(0);
        FittedColumn {
            width: u64::BITS - max.leading_zeros(),
            base,
            slope,
            offset: offset as u64,
            residuals,
        }
    };
    let flat = fit(0);
    if through_ends == 0 {
        return flat;
    }
    let sloped = fit(through_ends);
    if sloped.width < flat.width {
        sloped
    } else {
        flat
    }
}

/// The low `bits` bits set.
fn bit_mask(bits: u32) -> u64 {
    u64::MAX.checked_shr(u64::BITS - bits).unwrap_or(0)
}

/// Interprets the low `bits` bits of `value` as a two's complement integer.
fn sign_extend(value: u64, bits: u32) -> i64 {
    if bits == 0 {
        return 0;
    }
    let shift = u64::BITS - bits;
    ((value << shift) as i64) >> shift
}

fn zigzag(value: u64) -> u64 {
    let value = value as i64;
    ((value << 1) ^ (value >> 63)) as u64
}

fn unzigzag(value: u64) -> u64 {
    (value >> 1) ^ (value & 1).wrapping_neg()
}

#[expect(
    clippy::cast_possible_truncation,
    reason = "each byte takes at most the low 7 bits"
)]
fn write_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value & 0x7f) as u8 | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn read_varint(src: &[u8], pos: &mut usize) -> VortexResult<u64> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = *src
            .get(*pos)
            .ok_or_else(|| vortex_err!("Pco checkpoint table is truncated"))?;
        *pos += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte < 0x80 {
            return Ok(value);
        }
    }
    Err(vortex_err!("Pco checkpoint table has an overlong varint"))
}

/// One field of a [`CheckpointTable`].
#[derive(Clone, Debug)]
struct TableColumn {
    value_bits: u32,
    width: u32,
    base: u64,
    slope: u64,
    offset: u64,
    /// Bit offset of the column's first residual in the packed data.
    start_bit: usize,
}

/// The layout of one chunk's checkpoint buffer.
///
/// The buffer starts with a varint column count and, for each column, varints of its value bits,
/// residual width, base, zigzagged slope and zigzagged offset. The packed residuals follow, column
/// after column, least significant bit first. Record `k` of a column is
/// `base + slope * k + offset + residual[k]`, wrapping at the value bits. Column 0 is the bit
/// position in the page; the others are the fields of the pco page header that resumes there.
#[derive(Clone, Debug)]
pub(crate) struct CheckpointTable {
    columns: Vec<TableColumn>,
    data_start: usize,
}

impl CheckpointTable {
    /// Parses a checkpoint buffer of `n_records` records, checking its size.
    pub(crate) fn parse(buffer: &[u8], n_records: usize) -> VortexResult<Self> {
        let mut pos = 0;
        let n_columns = read_varint(buffer, &mut pos)?;
        vortex_ensure!(
            n_columns >= 1 && n_columns <= buffer.len() as u64,
            "Pco checkpoint table has an invalid column count {n_columns}"
        );
        let mut columns = Vec::with_capacity(usize::try_from(n_columns)?);
        let mut start_bit = 0usize;
        for _ in 0..n_columns {
            let value_bits = read_varint(buffer, &mut pos)?;
            let width = read_varint(buffer, &mut pos)?;
            vortex_ensure!(
                value_bits <= 64 && width <= value_bits,
                "Pco checkpoint column of {value_bits} bits has an invalid width {width}"
            );
            let column = TableColumn {
                value_bits: u32::try_from(value_bits)?,
                width: u32::try_from(width)?,
                base: read_varint(buffer, &mut pos)?,
                slope: unzigzag(read_varint(buffer, &mut pos)?),
                offset: unzigzag(read_varint(buffer, &mut pos)?),
                start_bit,
            };
            start_bit = n_records
                .checked_mul(column.width as usize)
                .and_then(|bits| bits.checked_add(start_bit))
                .ok_or_else(|| vortex_err!("Pco checkpoint table size overflows usize"))?;
            columns.push(column);
        }
        vortex_ensure_eq!(
            buffer.len() - pos,
            start_bit.div_ceil(8),
            "Pco checkpoint table does not match its {n_records} records"
        );
        Ok(Self {
            columns,
            data_start: pos,
        })
    }

    fn value(&self, buffer: &[u8], column: usize, record: usize) -> u64 {
        let column = &self.columns[column];
        let mut reader = BitReader::new(&buffer[self.data_start..]);
        reader.bit_idx = column.start_bit + record * column.width as usize;
        let residual = reader.read(column.width);
        column
            .base
            .wrapping_add(column.slope.wrapping_mul(record as u64))
            .wrapping_add(column.offset)
            .wrapping_add(residual)
            & bit_mask(column.value_bits)
    }

    /// The bit position in the page where record `record` resumes decoding.
    fn bit_position(&self, buffer: &[u8], record: usize) -> usize {
        // The value has at most 64 bits, and a bad one fails the page bounds check.
        usize::try_from(self.value(buffer, 0, record)).unwrap_or(usize::MAX)
    }

    /// Appends the pco page header that resumes decoding at record `record`.
    fn append_page_header(&self, buffer: &[u8], record: usize, out: &mut Vec<u8>) {
        let mut writer = BitWriter::default();
        for (idx, column) in self.columns.iter().enumerate().skip(1) {
            writer.write(self.value(buffer, idx, record), column.value_bits);
        }
        out.extend_from_slice(&writer.bytes);
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
    /// The chunk's checkpoint table and buffer, if the page has checkpoints.
    pub(crate) table: Option<(&'a CheckpointTable, &'a [u8])>,
    /// The record of the page's block 1 in the chunk's table.
    pub(crate) first_record: usize,
    /// Values per block: the checkpoint interval, or the whole page without checkpoints.
    pub(crate) block_len: usize,
}

impl PageCheckpoints<'_> {
    fn table(&self) -> VortexResult<(&CheckpointTable, &[u8])> {
        self.table
            .ok_or_else(|| vortex_err!("Pco page has no checkpoints"))
    }

    /// The bit position in the page where block `block` (`>= 1`) begins.
    fn bit_position(&self, block: usize) -> VortexResult<usize> {
        let (table, buffer) = self.table()?;
        Ok(table.bit_position(buffer, self.first_record + block - 1))
    }

    /// Appends the pco page header that resumes decoding at block `block` (`>= 1`).
    fn append_page_header(&self, block: usize, out: &mut Vec<u8>) -> VortexResult<()> {
        let (table, buffer) = self.table()?;
        table.append_page_header(buffer, self.first_record + block - 1, out);
        Ok(())
    }
}

/// Decodes `dst.len()` numbers of one page into `dst`, starting at the first number of block
/// `start_block`.
///
/// Block `b` holds numbers `b * block_len..min((b + 1) * block_len, page_n)`. `dst` must end on a
/// batch boundary or at the end of the page, so a read can stop early. Block 0 decodes from the
/// page itself; any other start decodes from a synthetic page built from its checkpoint, using
/// `scratch` for the bytes.
pub(crate) fn decode_blocks<T: Number>(
    cd: &mut ChunkDecompressor<T>,
    page: &[u8],
    page_n: usize,
    checkpoints: PageCheckpoints<'_>,
    start_block: usize,
    dst: &mut [T],
    scratch: &mut Vec<u8>,
) -> VortexResult<()> {
    let block_len = checkpoints.block_len;
    let value_start = start_block * block_len;
    let value_end = value_start + dst.len();
    vortex_ensure!(
        value_start < value_end
            && value_end <= page_n
            && (value_end == page_n || dst.len().is_multiple_of(FULL_BATCH_N)),
        "invalid decode of numbers {value_start}..{value_end} of a page of {page_n} numbers"
    );

    if start_block == 0 {
        let mut pd = cd
            .page_decompressor(page, page_n)
            .map_err(vortex_err_from_pco)?;
        pd.read(dst).map_err(vortex_err_from_pco)?;
        return Ok(());
    }

    let start_bit = checkpoints.bit_position(start_block)?;
    let end_block = (value_end - 1) / block_len + 1;
    let end_bit = if end_block * block_len < page_n {
        checkpoints.bit_position(end_block)?
    } else {
        page.len() * 8
    };
    vortex_ensure!(
        start_bit <= end_bit && end_bit <= page.len() * 8,
        "checkpoint bit range {start_bit}..{end_bit} exceeds a {}-byte page",
        page.len()
    );

    scratch.clear();
    checkpoints.append_page_header(start_block, scratch)?;
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

    /// Every record of a table decodes back to its values, whatever their shape.
    #[rstest]
    #[case::steady(32, (0..300u64).map(|k| 1000 + 97 * k + (k * 7919) % 13).collect())]
    #[case::wrapping(64, (0..50u64).map(|k| (u64::MAX - 3).wrapping_add(k << 40)).collect())]
    #[case::falling(64, (0..80u64).map(|k| 5_000_000u64.wrapping_sub(k * 33)).collect())]
    #[case::random(11, (0..64u64).map(|k| k.wrapping_mul(0x9e37_79b9) % 2048).collect())]
    #[case::constant(16, vec![7; 40])]
    #[case::zero_bits(0, vec![0; 9])]
    #[case::single(64, vec![u64::MAX])]
    fn checkpoint_table_round_trips(
        #[case] bits: u32,
        #[case] values: Vec<u64>,
    ) -> VortexResult<()> {
        let records = Records {
            bits: vec![POSITION_BITS, bits],
            columns: vec![(0..values.len() as u64).collect(), values.clone()],
            page_records: vec![values.len()],
        };
        let buffer = records.encode();
        let table = CheckpointTable::parse(&buffer, values.len())?;
        for (record, &value) in values.iter().enumerate() {
            assert_eq!(table.bit_position(&buffer, record), record);
            assert_eq!(table.value(&buffer, 1, record), value);
        }
        Ok(())
    }

    #[test]
    fn checkpoint_table_rejects_wrong_sizes() {
        let records = Records {
            bits: vec![POSITION_BITS, 64],
            columns: vec![vec![0, 5000, 9000], vec![1, u64::MAX, 77]],
            page_records: vec![3],
        };
        let buffer = records.encode();
        assert!(CheckpointTable::parse(&buffer, 3).is_ok());
        assert!(CheckpointTable::parse(&buffer, 4).is_err());
        assert!(CheckpointTable::parse(&buffer[..buffer.len() - 1], 3).is_err());
        assert!(CheckpointTable::parse(&[], 3).is_err());
    }

    #[test]
    fn coarsen_keeps_every_nth_record_per_page() {
        let records = Records {
            bits: vec![POSITION_BITS],
            columns: vec![vec![10, 20, 30, 40, 50, 1, 2, 3]],
            page_records: vec![5, 3],
        };
        let coarse = records.coarsen(2);
        assert_eq!(coarse.columns, vec![vec![20, 40, 2]]);
        assert_eq!(coarse.page_records, vec![2, 1]);
    }

    #[test]
    fn steady_columns_pack_small() {
        let positions = (0..255u64)
            .map(|k| 4096 + 3000 * k + (k * 31) % 17)
            .collect::<Vec<_>>();
        assert!(fit_column(&positions, POSITION_BITS).width <= 5);
        assert_eq!(fit_column(&[9; 100], 64).width, 0);
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
