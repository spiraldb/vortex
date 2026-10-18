// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::hash::Hash;
use std::hash::Hasher;
use std::ops::Range;
use std::sync::Arc;

use pco::ChunkConfig;
use pco::FULL_BATCH_N;
use pco::PagingSpec;
use pco::data_types::Number;
use pco::data_types::NumberType;
use pco::match_number_enum;
use pco::wrapped::ChunkDecompressor;
use pco::wrapped::FileCompressor;
use pco::wrapped::FileDecompressor;
use prost::Message;
use vortex_array::Array;
use vortex_array::ArrayEq;
use vortex_array::ArrayHash;
use vortex_array::ArrayId;
use vortex_array::ArrayParts;
use vortex_array::ArrayRef;
use vortex_array::ArrayView;
use vortex_array::EqMode;
use vortex_array::ExecutionCtx;
use vortex_array::ExecutionResult;
use vortex_array::IntoArray;
use vortex_array::ProbeState;
use vortex_array::TypedArrayRef;
use vortex_array::array_slots;
use vortex_array::arrays::Primitive;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::buffer::BufferHandle;
use vortex_array::dtype::DType;
use vortex_array::dtype::NativePType;
use vortex_array::dtype::PType;
use vortex_array::dtype::half;
use vortex_array::scalar::Scalar;
use vortex_array::serde::ArrayChildren;
use vortex_array::validity::Validity;
use vortex_array::vtable::OperationsVTable;
use vortex_array::vtable::VTable;
use vortex_array::vtable::ValidityVTable;
use vortex_array::vtable::child_to_validity;
use vortex_array::vtable::validity_to_child;
use vortex_buffer::BufferMut;
use vortex_buffer::ByteBuffer;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_ensure;
use vortex_error::vortex_ensure_eq;
use vortex_error::vortex_err;
use vortex_mask::Mask;
use vortex_session::VortexSession;
use vortex_session::registry::CachedId;

use super::Pco2ChunkInfo;
use super::Pco2Metadata;
use super::Pco2PageInfo;
use super::checkpoint::PageCheckpoints;
use super::checkpoint::PageWalker;
use super::checkpoint::decode_blocks;
use crate::array::collect_valid;
use crate::array::number_type_from_dtype;
use crate::array::number_type_from_ptype;
use crate::array::vortex_err_from_pco;

/// The most numbers pco accepts in one chunk.
const MAX_CHUNK_N: usize = 1 << 24;

/// A [`Pco2`]-encoded Vortex array.
pub type Pco2Array = Array<Pco2>;

/// Compression settings for [`Pco2`].
#[derive(Clone, Copy, Debug)]
pub struct Pco2Options {
    /// Pco compression level.
    pub level: usize,
    /// Values per pco chunk, each stored as a single page when its delta encoding allows.
    pub values_per_chunk: usize,
    /// Values between checkpoints, a positive multiple of 256. This is the random-access
    /// granularity.
    pub checkpoint_interval: usize,
    /// Whether to store each checkpoint block's minimum and maximum.
    pub block_stats: bool,
}

impl Default for Pco2Options {
    fn default() -> Self {
        Self {
            level: pco::DEFAULT_COMPRESSION_LEVEL,
            values_per_chunk: pco::DEFAULT_MAX_PAGE_N,
            checkpoint_interval: 1024,
            block_stats: true,
        }
    }
}

#[derive(Clone, Debug)]
/// Pco2 array encoding marker.
pub struct Pco2;

impl Pco2 {
    /// Constructs a Pco2 array after validating its data and validity invariants.
    pub fn try_new(dtype: DType, data: Pco2Data, validity: Validity) -> VortexResult<Pco2Array> {
        let len = data.len();
        data.validate(&dtype, len, &validity)?;
        // SAFETY: `validate` checked the dtype, length, validity, metadata, and buffer invariants.
        Ok(unsafe { Self::new_unchecked(dtype, data, validity) })
    }

    /// Constructs a Pco2 array without validating its data and validity invariants.
    ///
    /// # Safety
    ///
    /// The caller must ensure that [`Pco2Data::validate`] would succeed for `dtype`, `data.len()`,
    /// and `validity`.
    pub unsafe fn new_unchecked(dtype: DType, data: Pco2Data, validity: Validity) -> Pco2Array {
        let len = data.len();
        let slots = Pco2Slots {
            validity: validity_to_child(&validity, data.unsliced_n_rows()),
        }
        .into_slots();
        unsafe { Array::from_parts_unchecked(ArrayParts::new(Pco2, dtype, len, data, slots)) }
    }

    /// Compress a primitive array with large pco pages and mid-page checkpoints.
    pub fn from_primitive(
        parray: ArrayView<'_, Primitive>,
        options: &Pco2Options,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Pco2Array> {
        let dtype = parray.dtype().clone();
        let validity = parray.validity()?;
        let data = Pco2Data::from_primitive(parray, options, ctx)?;
        Self::try_new(dtype, data, validity)
    }
}

#[array_slots(Pco2)]
pub struct Pco2Slots {
    /// The validity bitmap indicating which elements are non-null.
    #[slot(0)]
    pub validity: Option<ArrayRef>,
}

/// Additional typed accessors for Pco2 arrays.
pub trait Pco2ArrayExt: Pco2ArraySlotsExt {
    /// Reconstruct the unsliced [`Validity`] from the validity slot.
    fn unsliced_validity(&self) -> Validity {
        child_to_validity(
            self.slots()[Pco2Slots::VALIDITY].as_ref(),
            self.dtype().nullability(),
        )
    }
}
impl<T: TypedArrayRef<Pco2>> Pco2ArrayExt for T {}

/// Where one pco page lives, derived from [`Pco2Metadata`].
#[derive(Clone, Debug)]
pub(crate) struct PageEntry {
    pub(crate) chunk: usize,
    /// Positions among the array's valid values.
    pub(crate) values: Range<usize>,
    /// Byte range in the chunk's page buffer.
    pub(crate) bytes: Range<usize>,
    /// Byte range in the chunk's checkpoint buffer.
    pub(crate) checkpoints: Range<usize>,
    /// Index of the page's first checkpoint block among all blocks of the array.
    pub(crate) first_block: usize,
}

fn build_page_index(metadata: &Pco2Metadata) -> VortexResult<Vec<PageEntry>> {
    let interval = metadata.checkpoint_interval as usize;
    vortex_ensure!(
        interval > 0 && interval.is_multiple_of(FULL_BATCH_N),
        "Pco2 checkpoint interval {interval} must be a positive multiple of {FULL_BATCH_N}"
    );

    let mut entries = Vec::new();
    let mut value_start = 0usize;
    let mut first_block = 0usize;
    for (chunk_idx, chunk) in metadata.chunks.iter().enumerate() {
        let record_bytes = chunk.checkpoint_record_bytes as usize;
        let mut byte_start = 0usize;
        let mut checkpoint_start = 0usize;
        let mut chunk_n = 0usize;
        for page in &chunk.pages {
            let n_values = page.n_values as usize;
            vortex_ensure!(
                n_values != 0,
                "Pco2 chunk {chunk_idx} contains an empty page"
            );
            let n_checkpoints = (n_values - 1) / interval;
            vortex_ensure!(
                n_checkpoints == 0 || record_bytes >= size_of::<u32>(),
                "Pco2 chunk {chunk_idx} has a page longer than the checkpoint interval but no \
                 checkpoint records"
            );
            let byte_end = byte_start + page.n_bytes as usize;
            let checkpoint_end = n_checkpoints
                .checked_mul(record_bytes)
                .and_then(|bytes| bytes.checked_add(checkpoint_start))
                .ok_or_else(|| vortex_err!("Pco2 checkpoint size overflows usize"))?;
            let value_end = value_start
                .checked_add(n_values)
                .ok_or_else(|| vortex_err!("Pco2 value count overflows usize"))?;
            entries.push(PageEntry {
                chunk: chunk_idx,
                values: value_start..value_end,
                bytes: byte_start..byte_end,
                checkpoints: checkpoint_start..checkpoint_end,
                first_block,
            });
            value_start = value_end;
            byte_start = byte_end;
            checkpoint_start = checkpoint_end;
            first_block += n_values.div_ceil(interval);
            chunk_n += n_values;
        }
        vortex_ensure!(
            chunk_n <= MAX_CHUNK_N,
            "Pco2 chunk {chunk_idx} contains {chunk_n} values, exceeding the maximum of \
             {MAX_CHUNK_N}"
        );
    }
    Ok(entries)
}

#[derive(Clone, Debug)]
/// Encoding-specific data for a [`Pco2Array`].
pub struct Pco2Data {
    pub(crate) chunk_metas: Vec<ByteBuffer>,
    /// One buffer per chunk holding its pages back to back.
    pub(crate) pages: Vec<ByteBuffer>,
    /// One buffer per chunk holding its pages' checkpoint records back to back.
    pub(crate) checkpoints: Vec<ByteBuffer>,
    /// The minimum and maximum of every checkpoint block: a little-endian `u32` length, the
    /// pco-compressed minimums of that length, then the pco-compressed maximums.
    pub(crate) block_stats: Option<ByteBuffer>,
    pub(crate) metadata: Pco2Metadata,
    pub(crate) index: Arc<[PageEntry]>,
    ptype: PType,
    unsliced_n_rows: usize,
    slice_start: usize,
    slice_stop: usize,
}

impl Display for Pco2Data {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ptype: {}, nrows: {}, slice: {}..{}, checkpoint interval: {}",
            self.ptype,
            self.unsliced_n_rows,
            self.slice_start,
            self.slice_stop,
            self.metadata.checkpoint_interval
        )
    }
}

impl ArrayHash for Pco2Data {
    fn array_hash<H: Hasher>(&self, state: &mut H, accuracy: EqMode) {
        self.unsliced_n_rows.hash(state);
        self.slice_start.hash(state);
        self.slice_stop.hash(state);
        for buffer in self.buffers() {
            buffer.array_hash(state, accuracy);
        }
    }
}

impl ArrayEq for Pco2Data {
    fn array_eq(&self, other: &Self, accuracy: EqMode) -> bool {
        self.unsliced_n_rows == other.unsliced_n_rows
            && self.slice_start == other.slice_start
            && self.slice_stop == other.slice_stop
            && self.metadata == other.metadata
            && self.buffers().count() == other.buffers().count()
            && self
                .buffers()
                .zip(other.buffers())
                .all(|(a, b)| a.array_eq(b, accuracy))
    }
}

impl Pco2Data {
    /// Constructs unsliced Pco2 data, deriving the page index from `metadata`.
    ///
    /// The result must still pass [`Self::validate`] before it is decompressed or published as
    /// part of an array.
    pub fn try_new(
        chunk_metas: Vec<ByteBuffer>,
        pages: Vec<ByteBuffer>,
        checkpoints: Vec<ByteBuffer>,
        block_stats: Option<ByteBuffer>,
        ptype: PType,
        metadata: Pco2Metadata,
        len: usize,
    ) -> VortexResult<Self> {
        vortex_ensure!(
            matches!(
                ptype,
                PType::F16
                    | PType::F32
                    | PType::F64
                    | PType::I16
                    | PType::I32
                    | PType::I64
                    | PType::U16
                    | PType::U32
                    | PType::U64
            ),
            "Pco2 does not support {ptype}"
        );
        let index = build_page_index(&metadata)?;
        Ok(Self {
            chunk_metas,
            pages,
            checkpoints,
            block_stats,
            metadata,
            index: index.into(),
            ptype,
            unsliced_n_rows: len,
            slice_start: 0,
            slice_stop: len,
        })
    }

    /// Validate dtype, validity, slice, and Pco2 component invariants.
    pub fn validate(&self, dtype: &DType, len: usize, validity: &Validity) -> VortexResult<()> {
        vortex_ensure_eq!(dtype.as_ptype(), self.ptype);
        vortex_ensure_eq!(dtype.nullability(), validity.nullability());
        vortex_ensure!(
            self.slice_start <= self.slice_stop && self.slice_stop <= self.unsliced_n_rows,
            "invalid slice range {}..{} for {} rows",
            self.slice_start,
            self.slice_stop,
            self.unsliced_n_rows
        );
        vortex_ensure_eq!(self.slice_stop - self.slice_start, len);
        if let Some(validity_len) = validity.maybe_len() {
            vortex_ensure_eq!(validity_len, self.unsliced_n_rows);
        }

        let n_chunks = self.metadata.chunks.len();
        vortex_ensure_eq!(self.chunk_metas.len(), n_chunks);
        vortex_ensure_eq!(self.pages.len(), n_chunks);
        vortex_ensure_eq!(self.checkpoints.len(), n_chunks);
        for chunk in 0..n_chunks {
            let last = self.index.iter().rev().find(|entry| entry.chunk == chunk);
            let (page_bytes, checkpoint_bytes) =
                last.map_or((0, 0), |entry| (entry.bytes.end, entry.checkpoints.end));
            vortex_ensure_eq!(
                self.pages[chunk].len(),
                page_bytes,
                "Pco2 chunk {chunk} page bytes do not match metadata"
            );
            vortex_ensure_eq!(
                self.checkpoints[chunk].len(),
                checkpoint_bytes,
                "Pco2 chunk {chunk} checkpoint bytes do not match metadata"
            );
        }

        match &self.block_stats {
            Some(stats) => {
                vortex_ensure!(self.metadata.block_stats, "unexpected Pco2 block stats");
                split_block_stats(stats)?;
            }
            None => vortex_ensure!(!self.metadata.block_stats, "missing Pco2 block stats"),
        }

        let n_values = self.n_values();
        vortex_ensure!(
            n_values <= self.unsliced_n_rows,
            "Pco2 contains {n_values} values for only {} rows",
            self.unsliced_n_rows
        );
        if validity.definitely_no_nulls() {
            vortex_ensure_eq!(
                n_values,
                self.unsliced_n_rows,
                "Pco2 value count must match the row count of a non-null array"
            );
        } else if validity.definitely_all_null() {
            vortex_ensure_eq!(n_values, 0, "Pco2 contains values for an all-null array");
        }
        Ok(())
    }

    /// Compress a primitive array into Pco2 data.
    pub fn from_primitive(
        parray: ArrayView<'_, Primitive>,
        options: &Pco2Options,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Self> {
        let interval = options.checkpoint_interval;
        vortex_ensure!(
            interval > 0 && interval.is_multiple_of(FULL_BATCH_N),
            "Pco2 checkpoint interval {interval} must be a positive multiple of {FULL_BATCH_N}"
        );
        vortex_ensure!(
            (1..=MAX_CHUNK_N).contains(&options.values_per_chunk),
            "Pco2 values per chunk must be in 1..={MAX_CHUNK_N}"
        );

        let number_type = number_type_from_dtype(parray.dtype());
        let values = collect_valid(parray, ctx)?;
        let compressed = match_number_enum!(
            number_type,
            NumberType<T> => {
                compress_values::<T>(values.as_slice::<T>(), options)?
            }
        );
        Self::try_new(
            compressed.chunk_metas,
            compressed.pages,
            compressed.checkpoints,
            compressed.block_stats,
            parray.dtype().as_ptype(),
            compressed.metadata,
            parray.len(),
        )
    }

    fn buffers(&self) -> impl Iterator<Item = &ByteBuffer> {
        self.chunk_metas
            .iter()
            .chain(&self.pages)
            .chain(&self.checkpoints)
            .chain(&self.block_stats)
    }

    /// Bytes spent on the random-access index: checkpoint records and block stats.
    pub fn index_nbytes(&self) -> (usize, usize) {
        (
            self.checkpoints.iter().map(|buffer| buffer.len()).sum(),
            self.block_stats.as_ref().map_or(0, |stats| stats.len()),
        )
    }

    pub(crate) fn interval(&self) -> usize {
        self.metadata.checkpoint_interval as usize
    }

    pub(crate) fn blocks_in_page(&self, entry: &PageEntry) -> usize {
        entry.values.len().div_ceil(self.interval())
    }

    /// The number of valid values stored in the unsliced array.
    pub(crate) fn n_values(&self) -> usize {
        self.index.last().map_or(0, |entry| entry.values.end)
    }

    /// The index of the page holding valid value `value`.
    pub(crate) fn page_of(&self, value: usize) -> usize {
        self.index
            .partition_point(|entry| entry.values.end <= value)
    }

    /// The array-wide index of the checkpoint block holding valid value `value` of `page`.
    pub(crate) fn block_of(&self, page: usize, value: usize) -> usize {
        let entry = &self.index[page];
        entry.first_block + (value - entry.values.start) / self.interval()
    }

    /// The number of checkpoint blocks in the unsliced array.
    pub(crate) fn n_blocks(&self) -> usize {
        self.index
            .last()
            .map_or(0, |entry| entry.first_block + self.blocks_in_page(entry))
    }

    /// The minimum and maximum of every checkpoint block, if stored.
    pub(crate) fn block_stats<T: Number>(&self) -> VortexResult<Option<(Vec<T>, Vec<T>)>> {
        let Some(stats) = &self.block_stats else {
            return Ok(None);
        };
        let (mins, maxes) = split_block_stats(stats)?;
        let mins = pco::standalone::simple_decompress::<T>(mins).map_err(vortex_err_from_pco)?;
        let maxes = pco::standalone::simple_decompress::<T>(maxes).map_err(vortex_err_from_pco)?;
        let n_blocks = self.n_blocks();
        vortex_ensure!(
            mins.len() == n_blocks && maxes.len() == n_blocks,
            "Pco2 block stats hold {} minimums and {} maximums for {n_blocks} blocks",
            mins.len(),
            maxes.len()
        );
        Ok(Some((mins, maxes)))
    }

    /// The valid-value range covered by this (possibly sliced) array.
    pub(crate) fn value_range(&self, mask: &Mask) -> Range<usize> {
        let bounds = mask.valid_counts_for_indices(&[self.slice_start, self.slice_stop]);
        bounds[0]..bounds[1]
    }

    /// Decompress this Pco2 data into a primitive array.
    pub(crate) fn decompress(
        &self,
        unsliced_validity: &Validity,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<PrimitiveArray> {
        let mask = unsliced_validity.execute_mask(self.unsliced_n_rows, ctx)?;
        let values = self.value_range(&mask);
        let number_type = number_type_from_ptype(self.ptype);
        let values_byte_buffer = match_number_enum!(
            number_type,
            NumberType<T> => {
                let mut out = BufferMut::<T>::with_capacity_in(values.len(), ctx.allocator().clone());
                BlockDecoder::<T>::new(self)?.decode_values(values, &mut out)?;
                out.freeze().into_byte_buffer()
            }
        );
        Ok(PrimitiveArray::from_values_byte_buffer(
            values_byte_buffer,
            self.ptype,
            unsliced_validity.slice(self.slice_start..self.slice_stop)?,
            self.len(),
            ctx,
        ))
    }

    pub(crate) fn _slice(&self, start: usize, stop: usize) -> Self {
        Self {
            slice_start: self.slice_start + start,
            slice_stop: self.slice_start + stop,
            ..self.clone()
        }
    }

    /// Returns the number of elements in the array.
    pub fn len(&self) -> usize {
        self.slice_stop - self.slice_start
    }

    /// Returns `true` if the array contains no elements.
    pub fn is_empty(&self) -> bool {
        self.slice_stop == self.slice_start
    }

    pub(crate) fn ptype(&self) -> PType {
        self.ptype
    }

    pub(crate) fn slice_start(&self) -> usize {
        self.slice_start
    }

    pub(crate) fn slice_stop(&self) -> usize {
        self.slice_stop
    }

    pub(crate) fn unsliced_n_rows(&self) -> usize {
        self.unsliced_n_rows
    }
}

struct Compressed {
    chunk_metas: Vec<ByteBuffer>,
    pages: Vec<ByteBuffer>,
    checkpoints: Vec<ByteBuffer>,
    block_stats: Option<ByteBuffer>,
    metadata: Pco2Metadata,
}

fn compress_values<T: Number + NativePType>(
    values: &[T],
    options: &Pco2Options,
) -> VortexResult<Compressed> {
    let interval = options.checkpoint_interval;
    let config = |max_page_n: usize| {
        ChunkConfig::default()
            .with_compression_level(options.level)
            .with_paging_spec(PagingSpec::EqualPagesUpTo(max_page_n))
    };

    let fc = FileCompressor::default();
    let mut header = vec![];
    fc.write_header(&mut header).map_err(vortex_err_from_pco)?;

    let mut chunk_metas = vec![];
    let mut pages = vec![];
    let mut checkpoints = vec![];
    let mut chunk_infos = vec![];
    let mut block_stats = options.block_stats.then(|| {
        let n_blocks = values.len().div_ceil(interval);
        (Vec::with_capacity(n_blocks), Vec::with_capacity(n_blocks))
    });
    for chunk in values.chunks(options.values_per_chunk) {
        let mut cc = fc
            .chunk_compressor(chunk, &config(options.values_per_chunk))
            .map_err(vortex_err_from_pco)?;
        let mut walker = PageWalker::try_new(cc.meta())?;
        if walker.is_none() && chunk.len() > interval {
            // The chosen delta encoding cannot be checkpointed, so fall back to pages no longer
            // than the checkpoint interval, which never need checkpoints.
            cc = fc
                .chunk_compressor(chunk, &config(interval))
                .map_err(vortex_err_from_pco)?;
            walker = None;
        }

        let mut chunk_meta = Vec::with_capacity(cc.meta_size_hint());
        cc.write_meta(&mut chunk_meta)
            .map_err(vortex_err_from_pco)?;
        chunk_metas.push(ByteBuffer::from(chunk_meta));

        let mut page_buffer = vec![];
        let mut checkpoint_buffer = vec![];
        let mut page_infos = vec![];
        for (page_idx, page_n) in cc.n_per_page().into_iter().enumerate() {
            let page_start = page_buffer.len();
            page_buffer.reserve(cc.page_size_hint(page_idx));
            cc.write_page(page_idx, &mut page_buffer)
                .map_err(vortex_err_from_pco)?;
            if page_n > interval {
                walker
                    .as_mut()
                    .ok_or_else(|| vortex_err!("Pco2 page of {page_n} values has no walker"))?
                    .checkpoint_page(
                        &page_buffer[page_start..],
                        page_n,
                        interval,
                        &mut checkpoint_buffer,
                    )?;
            }
            page_infos.push(Pco2PageInfo {
                n_values: u32::try_from(page_n)?,
                n_bytes: u32::try_from(page_buffer.len() - page_start)?,
            });
        }
        let checkpoint_record_bytes = match &walker {
            Some(walker) if !checkpoint_buffer.is_empty() => u32::try_from(walker.record_bytes())?,
            _ => 0,
        };
        pages.push(ByteBuffer::from(page_buffer));
        checkpoints.push(ByteBuffer::from(checkpoint_buffer));
        chunk_infos.push(Pco2ChunkInfo {
            pages: page_infos,
            checkpoint_record_bytes,
        });

        if let Some((mins, maxes)) = &mut block_stats {
            // Pages split chunks into contiguous runs, and blocks restart at each page.
            let mut page_start = 0;
            for page_n in cc.n_per_page() {
                for block in chunk[page_start..page_start + page_n].chunks(interval) {
                    let (min, max) = min_max(block);
                    mins.push(min);
                    maxes.push(max);
                }
                page_start += page_n;
            }
        }
    }

    let block_stats = block_stats
        .map(|(mins, maxes)| compress_block_stats(&mins, &maxes, options.level))
        .transpose()?;
    Ok(Compressed {
        chunk_metas,
        pages,
        checkpoints,
        block_stats,
        metadata: Pco2Metadata {
            header,
            checkpoint_interval: u32::try_from(interval)?,
            chunks: chunk_infos,
            block_stats: options.block_stats,
        },
    })
}

/// Block minimums and maximums compress well on their own (they trend with sorted data and
/// cluster near the extremes of random data), so each gets its own pco blob.
fn compress_block_stats<T: Number>(
    mins: &[T],
    maxes: &[T],
    level: usize,
) -> VortexResult<ByteBuffer> {
    let config = ChunkConfig::default().with_compression_level(level);
    let mins = pco::standalone::simple_compress(mins, &config).map_err(vortex_err_from_pco)?;
    let maxes = pco::standalone::simple_compress(maxes, &config).map_err(vortex_err_from_pco)?;
    let mut stats = Vec::with_capacity(size_of::<u32>() + mins.len() + maxes.len());
    stats.extend_from_slice(&u32::try_from(mins.len())?.to_le_bytes());
    stats.extend_from_slice(&mins);
    stats.extend_from_slice(&maxes);
    Ok(ByteBuffer::from(stats))
}

/// Splits a block stats buffer into its compressed minimums and maximums.
fn split_block_stats(stats: &[u8]) -> VortexResult<(&[u8], &[u8])> {
    let (len, rest) = stats
        .split_first_chunk::<{ size_of::<u32>() }>()
        .ok_or_else(|| vortex_err!("Pco2 block stats are truncated"))?;
    let len = u32::from_le_bytes(*len) as usize;
    vortex_ensure!(len <= rest.len(), "Pco2 block stats are truncated");
    Ok(rest.split_at(len))
}

/// The minimum and maximum of a non-empty slice under Vortex's total order.
fn min_max<T: NativePType>(values: &[T]) -> (T, T) {
    let mut min = values[0];
    let mut max = values[0];
    for &value in &values[1..] {
        if value.is_lt(min) {
            min = value;
        }
        if value.is_gt(max) {
            max = value;
        }
    }
    (min, max)
}

/// Decodes checkpoint blocks of a [`Pco2Data`], reusing one chunk decompressor at a time.
pub(crate) struct BlockDecoder<'a, T: Number> {
    data: &'a Pco2Data,
    file: FileDecompressor,
    chunk: Option<(usize, ChunkDecompressor<T>)>,
    page_scratch: Vec<u8>,
    values_scratch: Vec<T>,
}

impl<'a, T: Number> BlockDecoder<'a, T> {
    pub(crate) fn new(data: &'a Pco2Data) -> VortexResult<Self> {
        let (file, _) =
            FileDecompressor::new(data.metadata.header.as_slice()).map_err(vortex_err_from_pco)?;
        Ok(Self {
            data,
            file,
            chunk: None,
            page_scratch: Vec::new(),
            values_scratch: Vec::new(),
        })
    }

    /// Decodes checkpoint blocks `blocks`, numbered within page `page`, into `dst`.
    pub(crate) fn decode_page_blocks(
        &mut self,
        page: usize,
        blocks: Range<usize>,
        dst: &mut [T],
    ) -> VortexResult<()> {
        let data = self.data;
        let entry = &data.index[page];
        if !matches!(&self.chunk, Some((chunk, _)) if *chunk == entry.chunk) {
            let (cd, _) = self
                .file
                .chunk_decompressor(data.chunk_metas[entry.chunk].as_ref())
                .map_err(vortex_err_from_pco)?;
            self.chunk = Some((entry.chunk, cd));
        }
        let Some((_, cd)) = &mut self.chunk else {
            unreachable!("the chunk decompressor was just set");
        };
        let checkpoints = PageCheckpoints {
            records: &data.checkpoints[entry.chunk].as_slice()[entry.checkpoints.clone()],
            record_bytes: data.metadata.chunks[entry.chunk].checkpoint_record_bytes as usize,
            interval: data.interval(),
        };
        decode_blocks(
            cd,
            &data.pages[entry.chunk].as_slice()[entry.bytes.clone()],
            entry.values.len(),
            checkpoints,
            blocks,
            dst,
            &mut self.page_scratch,
        )
    }

    /// Decodes checkpoint blocks `blocks` of page `page` into a scratch buffer and returns it.
    pub(crate) fn decode_page_blocks_scratch(
        &mut self,
        page: usize,
        blocks: Range<usize>,
    ) -> VortexResult<&[T]> {
        let interval = self.data.interval();
        let page_n = self.data.index[page].values.len();
        let n = (blocks.end * interval).min(page_n) - blocks.start * interval;
        let mut values = std::mem::take(&mut self.values_scratch);
        values.resize(n, T::default());
        let result = self.decode_page_blocks(page, blocks, &mut values);
        self.values_scratch = values;
        result?;
        Ok(&self.values_scratch)
    }

    /// Appends valid values `values` to `out`, decoding only the blocks they touch.
    pub(crate) fn decode_values(
        &mut self,
        values: Range<usize>,
        out: &mut BufferMut<T>,
    ) -> VortexResult<()> {
        let data = self.data;
        let interval = data.interval();
        for (page, entry) in data
            .index
            .iter()
            .enumerate()
            .skip(data.page_of(values.start))
        {
            if entry.values.start >= values.end {
                break;
            }
            let local = values.start.max(entry.values.start) - entry.values.start
                ..values.end.min(entry.values.end) - entry.values.start;
            let blocks = local.start / interval..local.end.div_ceil(interval);
            let decoded = blocks.start * interval..(blocks.end * interval).min(entry.values.len());
            let old_len = out.len();
            out.reserve(decoded.len());
            // SAFETY: pco either writes every element of the new tail or returns an error, in
            // which case the length is restored before anything observes the tail.
            unsafe { out.set_len(old_len + decoded.len()) };
            if let Err(err) = self.decode_page_blocks(page, blocks, &mut out[old_len..]) {
                unsafe { out.set_len(old_len) };
                return Err(err);
            }
            // Blocks are decoded whole, so drop the values before and after the requested range.
            let skip = local.start - decoded.start;
            if skip > 0 {
                out.copy_within(old_len + skip..old_len + skip + local.len(), old_len);
            }
            out.truncate(old_len + local.len());
        }
        Ok(())
    }
}

impl VTable for Pco2 {
    type TypedArrayData = Pco2Data;

    type OperationsVTable = Self;
    type ValidityVTable = Self;

    fn id(&self) -> ArrayId {
        static ID: CachedId = CachedId::new("vortex.pco2");
        *ID
    }

    fn validate(
        &self,
        data: &Pco2Data,
        dtype: &DType,
        len: usize,
        slots: &[Option<ArrayRef>],
    ) -> VortexResult<()> {
        let validity = child_to_validity(
            Pco2SlotsView::from_slots(slots).validity,
            dtype.nullability(),
        );
        data.validate(dtype, len, &validity)
    }

    fn nbuffers(array: ArrayView<'_, Self>) -> usize {
        array.buffers().count()
    }

    fn buffer(array: ArrayView<'_, Self>, idx: usize) -> BufferHandle {
        let n_chunks = array.chunk_metas.len();
        let buffer = if idx < n_chunks {
            &array.chunk_metas[idx]
        } else if idx < 2 * n_chunks {
            &array.pages[idx - n_chunks]
        } else if idx < 3 * n_chunks {
            &array.checkpoints[idx - 2 * n_chunks]
        } else {
            array
                .block_stats
                .as_ref()
                .vortex_expect("Pco2 buffer index out of bounds")
        };
        BufferHandle::new_host(buffer.clone())
    }

    fn buffer_name(array: ArrayView<'_, Self>, idx: usize) -> Option<String> {
        let n_chunks = array.chunk_metas.len();
        Some(if idx < n_chunks {
            format!("chunk_meta_{idx}")
        } else if idx < 2 * n_chunks {
            format!("pages_{}", idx - n_chunks)
        } else if idx < 3 * n_chunks {
            format!("checkpoints_{}", idx - 2 * n_chunks)
        } else {
            "block_stats".to_string()
        })
    }

    fn with_buffers(
        &self,
        array: ArrayView<'_, Self>,
        buffers: &[BufferHandle],
    ) -> VortexResult<ArrayParts<Self>> {
        let mut data = array.data().clone();
        let (chunk_metas, pages, checkpoints, block_stats) =
            split_buffers(&data.metadata, buffers)?;
        data.chunk_metas = chunk_metas;
        data.pages = pages;
        data.checkpoints = checkpoints;
        data.block_stats = block_stats;
        Ok(ArrayParts::new(
            self.clone(),
            array.dtype().clone(),
            array.len(),
            data,
            array.slots().iter().cloned().collect(),
        ))
    }

    fn serialize(
        array: ArrayView<'_, Self>,
        _session: &VortexSession,
    ) -> VortexResult<Option<Vec<u8>>> {
        Ok(Some(array.metadata.clone().encode_to_vec()))
    }

    fn deserialize(
        &self,
        dtype: &DType,
        len: usize,
        metadata: &[u8],
        buffers: &[BufferHandle],
        children: &dyn ArrayChildren,
        _session: &VortexSession,
    ) -> VortexResult<ArrayParts<Self>> {
        let metadata = Pco2Metadata::decode(metadata)?;
        let validity = if children.is_empty() {
            Validity::from(dtype.nullability())
        } else if children.len() == 1 {
            let validity = children.get(0, &Validity::DTYPE, len)?;
            Validity::Array(validity)
        } else {
            vortex_bail!("Pco2Array expected 0 or 1 child, got {}", children.len());
        };

        let (chunk_metas, pages, checkpoints, block_stats) = split_buffers(&metadata, buffers)?;
        let slots = Pco2Slots {
            validity: validity_to_child(&validity, len),
        }
        .into_slots();
        // `Array::try_from_parts`, which consumes these parts, validates the data before
        // publishing the array.
        let data = Pco2Data::try_new(
            chunk_metas,
            pages,
            checkpoints,
            block_stats,
            dtype.as_ptype(),
            metadata,
            len,
        )?;
        Ok(ArrayParts::new(
            self.clone(),
            dtype.clone(),
            len,
            data,
            slots,
        ))
    }

    fn slot_name(_array: ArrayView<'_, Self>, idx: usize) -> String {
        Pco2Slots::NAMES[idx].to_string()
    }

    fn execute(array: Array<Self>, ctx: &mut ExecutionCtx) -> VortexResult<ExecutionResult> {
        let unsliced_validity = array.unsliced_validity();
        Ok(ExecutionResult::done(
            array
                .data()
                .decompress(&unsliced_validity, ctx)?
                .into_array(),
        ))
    }

    fn reduce_parent(
        array: ArrayView<'_, Self>,
        parent: &ArrayRef,
        child_idx: usize,
    ) -> VortexResult<Option<ArrayRef>> {
        super::compute::RULES.evaluate(array, parent, child_idx)
    }
}

type SplitBuffers = (
    Vec<ByteBuffer>,
    Vec<ByteBuffer>,
    Vec<ByteBuffer>,
    Option<ByteBuffer>,
);

/// Splits the flat buffer list into chunk metas, pages, checkpoints, and block stats.
fn split_buffers(metadata: &Pco2Metadata, buffers: &[BufferHandle]) -> VortexResult<SplitBuffers> {
    let n_chunks = metadata.chunks.len();
    let expected = 3 * n_chunks + usize::from(metadata.block_stats);
    vortex_ensure_eq!(
        buffers.len(),
        expected,
        "Pco2Array expected {expected} buffers"
    );
    let mut host = buffers
        .iter()
        .map(|buffer| buffer.clone().try_to_host_sync())
        .collect::<VortexResult<Vec<_>>>()?
        .into_iter();
    let chunk_metas = host.by_ref().take(n_chunks).collect();
    let pages = host.by_ref().take(n_chunks).collect();
    let checkpoints = host.by_ref().take(n_chunks).collect();
    let block_stats = host.next();
    Ok((chunk_metas, pages, checkpoints, block_stats))
}

impl ValidityVTable<Pco2> for Pco2 {
    fn validity(array: ArrayView<'_, Pco2>) -> VortexResult<Validity> {
        array
            .unsliced_validity()
            .slice(array.slice_start()..array.slice_stop())
    }
}

impl OperationsVTable<Pco2> for Pco2 {
    type ProbeState = super::probe::Pco2ProbeState;

    fn probe_scalar(
        state: &mut ProbeState<'_, Pco2>,
        index: usize,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Scalar> {
        let array = state.array();
        super::probe::scalar_at(array, index, state.retained(), ctx)
    }

    fn scalar_at(
        array: ArrayView<'_, Pco2>,
        index: usize,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Scalar> {
        super::probe::scalar_at(array, index, None, ctx)
    }
}
