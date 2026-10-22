// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::any::Any;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::hash::Hash;
use std::hash::Hasher;
use std::ops::Range;
use std::sync::Arc;

use parking_lot::Mutex;
use pco::ChunkConfig;
use pco::FULL_BATCH_N;
use pco::PagingSpec;
use pco::data_types::Number;
use pco::data_types::NumberType;
use pco::errors::PcoError;
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
use vortex_buffer::Alignment;
use vortex_buffer::BufferMut;
use vortex_buffer::ByteBuffer;
use vortex_error::VortexError;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_ensure;
use vortex_error::vortex_ensure_eq;
use vortex_error::vortex_err;
use vortex_mask::Mask;
use vortex_session::VortexSession;
use vortex_session::registry::CachedId;

use crate::PcoChunkInfo;
use crate::PcoMetadata;
use crate::PcoPageInfo;
use crate::checkpoint::CheckpointTable;
use crate::checkpoint::PageCheckpoints;
use crate::checkpoint::PageWalker;
use crate::checkpoint::decode_blocks;

// Overall approach here:
// Chunk the array into Pco chunks (currently using the default recommended size for good
// compression), and each chunk into Pco pages, each written as its own buffer after the chunk's
// metadata buffer. Only valid values are compressed.
//
// The v2 wire format (`vortex.pco.v2`) adds a random-access index on top of the same chunks and
// pages: every `checkpoint_interval` values inside a page, it records pco's decoder state, so a
// read can resume mid-page instead of at the page start (see `checkpoint`). That lets v2 write
// one large page per chunk, which compresses and decodes like the largest pages, while reading
// blocks as small as the checkpoint interval. It also stores every block's minimum and maximum.
// The interval is per chunk: a chunk whose delta encoding cannot be checkpointed has none, and
// writes v1-sized pages instead.
//
// Reads work in blocks: the values between consecutive checkpoints of a page, or a whole page
// when it has none. A read decodes from the start of the block holding its first value through
// the batch of 256 values holding its last.

/// The most values per chunk the v1 format accepts. v2 keeps the same limit so that dropping its
/// index always leaves valid v1 data.
pub(crate) const VALUES_PER_CHUNK: usize = pco::DEFAULT_MAX_PAGE_N;

/// A chunk's index may take up to `1 / INDEX_SHARE` of the chunk's compressed size before its
/// checkpoint interval doubles. At this share, v2 stays within 1% of v1's 8,192-value pages on
/// every column measured, while most columns keep 1,024-value blocks.
const INDEX_SHARE: usize = 64;

/// A [`Pco`]-encoded Vortex array.
pub type PcoArray = Array<Pco>;

/// Compression settings for [`Pco`].
///
/// Without a checkpoint interval the array serializes as the frozen `vortex.pco` format; with one
/// it carries a random-access index and serializes as `vortex.pco.v2`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PcoOptions {
    /// Pco compression level.
    pub level: usize,
    /// Values per pco page. Zero means one page per chunk.
    pub values_per_page: usize,
    /// Values between checkpoints, a positive multiple of 256, or `None` for no index.
    pub checkpoint_interval: Option<usize>,
    /// The longest block a chunk may read in. A chunk doubles its checkpoint interval up to this
    /// when checkpoints every `checkpoint_interval` values would take more than a small share of
    /// its compressed size, as in near-constant data, and a chunk whose delta encoding cannot be
    /// checkpointed writes pages of this many values. Equal to `checkpoint_interval` to read
    /// every chunk in blocks of that size.
    pub max_checkpoint_interval: usize,
    /// Whether to store each block's minimum and maximum, which lets comparisons skip blocks.
    /// Requires checkpoints. Stats are left out when blocks span most of the value range, as in
    /// shuffled data, since they would skip almost nothing.
    pub block_stats: bool,
}

impl PcoOptions {
    /// The v1 format: pages of `values_per_page` values (zero for one per chunk), no index.
    pub const fn v1(level: usize, values_per_page: usize) -> Self {
        Self {
            level,
            values_per_page,
            checkpoint_interval: None,
            max_checkpoint_interval: 0,
            block_stats: false,
        }
    }

    /// The v2 format: one page per chunk, checkpoints every 1,024 values, and block stats. No
    /// block is longer than v1's default page of 8,192 values.
    pub const fn v2() -> Self {
        Self {
            level: pco::DEFAULT_COMPRESSION_LEVEL,
            values_per_page: 0,
            checkpoint_interval: Some(1024),
            max_checkpoint_interval: 8192,
            block_stats: true,
        }
    }

    fn validate(&self) -> VortexResult<()> {
        if let Some(interval) = self.checkpoint_interval {
            vortex_ensure!(
                interval > 0 && interval.is_multiple_of(FULL_BATCH_N),
                "Pco checkpoint interval {interval} must be a positive multiple of {FULL_BATCH_N}"
            );
            vortex_ensure!(
                self.max_checkpoint_interval >= interval
                    && u32::try_from(self.max_checkpoint_interval).is_ok(),
                "Pco maximum checkpoint interval {} must be at least the interval {interval}",
                self.max_checkpoint_interval
            );
        } else {
            vortex_ensure!(!self.block_stats, "Pco block stats require checkpoints");
        }
        Ok(())
    }
}

impl Default for PcoOptions {
    fn default() -> Self {
        Self::v2()
    }
}

impl ArrayHash for PcoData {
    fn array_hash<H: Hasher>(&self, state: &mut H, accuracy: EqMode) {
        self.unsliced_n_rows.hash(state);
        self.slice_start.hash(state);
        self.slice_stop.hash(state);
        for buffer in self.buffers() {
            buffer.array_hash(state, accuracy);
        }
    }
}

impl ArrayEq for PcoData {
    fn array_eq(&self, other: &Self, accuracy: EqMode) -> bool {
        self.unsliced_n_rows == other.unsliced_n_rows
            && self.slice_start == other.slice_start
            && self.slice_stop == other.slice_stop
            && self.metadata == other.metadata
            && self.index.as_ref().map(PcoIndex::layout)
                == other.index.as_ref().map(PcoIndex::layout)
            && self.buffers().count() == other.buffers().count()
            && self
                .buffers()
                .zip(other.buffers())
                .all(|(a, b)| a.array_eq(b, accuracy))
    }
}

impl VTable for Pco {
    type TypedArrayData = PcoData;

    type OperationsVTable = Self;
    type ValidityVTable = Self;

    fn id(&self) -> ArrayId {
        static ID: CachedId = CachedId::new("vortex.pco");
        *ID
    }

    fn validate(
        &self,
        data: &PcoData,
        dtype: &DType,
        len: usize,
        slots: &[Option<ArrayRef>],
    ) -> VortexResult<()> {
        let validity = child_to_validity(
            PcoSlotsView::from_slots(slots).validity,
            dtype.nullability(),
        );
        data.validate(dtype, len, &validity)
    }

    fn nbuffers(array: ArrayView<'_, Self>) -> usize {
        array.buffers().count()
    }

    fn buffer(array: ArrayView<'_, Self>, idx: usize) -> BufferHandle {
        let n_chunks = array.chunk_metas.len();
        let n_pages = array.pages.len();
        let buffer = if idx < n_chunks {
            &array.chunk_metas[idx]
        } else if idx < n_chunks + n_pages {
            &array.pages[idx - n_chunks]
        } else {
            let index = array
                .index
                .as_ref()
                .unwrap_or_else(|| unreachable!("Pco buffer {idx} out of bounds"));
            let checkpoint_idx = idx - n_chunks - n_pages;
            index
                .checkpoints
                .get(checkpoint_idx)
                .or(index.block_stats.as_ref())
                .unwrap_or_else(|| unreachable!("Pco buffer {idx} out of bounds"))
        };
        BufferHandle::new_host(buffer.clone())
    }

    fn buffer_name(array: ArrayView<'_, Self>, idx: usize) -> Option<String> {
        let n_chunks = array.chunk_metas.len();
        let n_pages = array.pages.len();
        Some(if idx < n_chunks {
            format!("chunk_meta_{idx}")
        } else if idx < n_chunks + n_pages {
            format!("page_{}", idx - n_chunks)
        } else if idx < 2 * n_chunks + n_pages {
            format!("checkpoints_{}", idx - n_chunks - n_pages)
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
        let n_chunks = data.chunk_metas.len();
        let n_pages = data.pages.len();
        let mut buffers = buffers
            .iter()
            .map(|buffer| buffer.clone().try_to_host_sync())
            .collect::<VortexResult<Vec<_>>>()?
            .into_iter();
        data.chunk_metas = buffers.by_ref().take(n_chunks).collect();
        data.pages = buffers.by_ref().take(n_pages).collect();
        if let Some(index) = &mut data.index {
            index.checkpoints = buffers.by_ref().take(n_chunks).collect();
            if index.block_stats.is_some() {
                index.block_stats = buffers.next();
            }
        }
        data.rederive()?;
        Ok(ArrayParts::new(
            self.clone(),
            array.dtype().clone(),
            array.len(),
            data,
            array.slots().iter().cloned().collect(),
        ))
    }

    /// Serializes the v1 format. Arrays with a random-access index need [`crate::PcoPlugin`],
    /// which writes them as v2.
    fn serialize(
        array: ArrayView<'_, Self>,
        _session: &VortexSession,
    ) -> VortexResult<Option<Vec<u8>>> {
        vortex_ensure!(
            array.index.is_none(),
            "Pco arrays with a random-access index serialize through PcoPlugin"
        );
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
        let metadata = PcoMetadata::decode(metadata)?;
        let validity = deserialize_validity(dtype, len, children)?;

        vortex_ensure!(buffers.len() >= metadata.chunks.len());
        let chunk_metas = host_buffers(&buffers[..metadata.chunks.len()])?;
        let pages = host_buffers(&buffers[metadata.chunks.len()..])?;

        let expected_n_pages = metadata
            .chunks
            .iter()
            .map(|info| info.pages.len())
            .sum::<usize>();
        vortex_ensure_eq!(pages.len(), expected_n_pages);

        let slots = PcoSlots {
            validity: validity_to_child(&validity, len),
        }
        .into_slots();
        // `Array::try_from_parts`, which consumes these parts, validates the data before
        // publishing the array.
        let data = PcoData::try_new(chunk_metas, pages, dtype.as_ptype(), metadata, None, len)?;
        Ok(ArrayParts::new(
            self.clone(),
            dtype.clone(),
            len,
            data,
            slots,
        ))
    }

    fn slot_name(_array: ArrayView<'_, Self>, idx: usize) -> String {
        PcoSlots::NAMES[idx].to_string()
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
        crate::rules::RULES.evaluate(array, parent, child_idx)
    }
}

pub(crate) fn host_buffers(buffers: &[BufferHandle]) -> VortexResult<Vec<ByteBuffer>> {
    buffers
        .iter()
        .map(|buffer| buffer.clone().try_to_host_sync())
        .collect()
}

pub(crate) fn deserialize_validity(
    dtype: &DType,
    len: usize,
    children: &dyn ArrayChildren,
) -> VortexResult<Validity> {
    Ok(if children.is_empty() {
        Validity::from(dtype.nullability())
    } else if children.len() == 1 {
        Validity::Array(children.get(0, &Validity::DTYPE, len)?)
    } else {
        vortex_bail!("PcoArray expected 0 or 1 child, got {}", children.len());
    })
}

pub(crate) fn number_type_from_dtype(dtype: &DType) -> NumberType {
    number_type_from_ptype(dtype.as_ptype())
}

pub(crate) fn number_type_from_ptype(ptype: PType) -> NumberType {
    match ptype {
        PType::F16 => NumberType::F16,
        PType::F32 => NumberType::F32,
        PType::F64 => NumberType::F64,
        PType::I16 => NumberType::I16,
        PType::I32 => NumberType::I32,
        PType::I64 => NumberType::I64,
        PType::U16 => NumberType::U16,
        PType::U32 => NumberType::U32,
        PType::U64 => NumberType::U64,
        _ => unreachable!("PType not supported by Pco: {:?}", ptype),
    }
}

pub(crate) fn collect_valid(
    parray: ArrayView<'_, Primitive>,
    ctx: &mut ExecutionCtx,
) -> VortexResult<PrimitiveArray> {
    let mask = parray
        .array()
        .validity()?
        .execute_mask(parray.array().len(), ctx)?;
    let result = parray
        .array()
        .filter(mask)?
        .execute::<PrimitiveArray>(ctx)?;
    Ok(result)
}

pub(crate) fn vortex_err_from_pco(err: PcoError) -> VortexError {
    use pco::errors::ErrorKind::*;
    match err.kind {
        Io(io_kind) => VortexError::from(std::io::Error::new(io_kind, err.message)),
        InvalidArgument => vortex_err!(InvalidArgument: "{}", err.message),
        other => vortex_err!("Pco {:?} error: {}", other, err.message),
    }
}

#[derive(Clone, Debug)]
/// Pco array encoding marker.
pub struct Pco;

impl Pco {
    /// Constructs a Pco array after validating its data and validity invariants.
    pub fn try_new(dtype: DType, data: PcoData, validity: Validity) -> VortexResult<PcoArray> {
        let len = data.len();
        data.validate(&dtype, len, &validity)?;
        // SAFETY: `validate` checked the dtype, length, validity, metadata, and buffer invariants.
        Ok(unsafe { Self::new_unchecked(dtype, data, validity) })
    }

    /// Constructs a Pco array without validating its data and validity invariants.
    ///
    /// # Safety
    ///
    /// The caller must ensure that [`PcoData::validate`] would succeed for `dtype`, `data.len()`,
    /// and `validity`.
    pub unsafe fn new_unchecked(dtype: DType, data: PcoData, validity: Validity) -> PcoArray {
        let len = data.len();
        let slots = PcoSlots {
            validity: validity_to_child(&validity, data.unsliced_n_rows()),
        }
        .into_slots();
        unsafe { Array::from_parts_unchecked(ArrayParts::new(Pco, dtype, len, data, slots)) }
    }

    /// Compress a primitive array using pcodec in the v1 format.
    pub fn from_primitive(
        parray: ArrayView<'_, Primitive>,
        level: usize,
        values_per_page: usize,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<PcoArray> {
        Self::compress(parray, &PcoOptions::v1(level, values_per_page), ctx)
    }

    /// Compress a primitive array using pcodec with the given options.
    pub fn compress(
        parray: ArrayView<'_, Primitive>,
        options: &PcoOptions,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<PcoArray> {
        let dtype = parray.dtype().clone();
        let validity = parray.validity()?;
        let data = PcoData::compress(parray, options, ctx)?;
        Self::try_new(dtype, data, validity)
    }
}

#[array_slots(Pco)]
pub struct PcoSlots {
    /// The validity bitmap indicating which elements are non-null.
    #[slot(0)]
    pub validity: Option<ArrayRef>,
}

/// Additional typed accessors for Pco arrays.
pub trait PcoArrayExt: PcoArraySlotsExt {
    /// Reconstruct the unsliced [`Validity`] from the validity slot.
    fn unsliced_validity(&self) -> Validity {
        child_to_validity(
            self.slots()[PcoSlots::VALIDITY].as_ref(),
            self.dtype().nullability(),
        )
    }
}
impl<T: TypedArrayRef<Pco>> PcoArrayExt for T {}

/// The v2 random-access index: decoder checkpoints inside pages, and optional block stats.
#[derive(Clone, Debug)]
pub(crate) struct PcoIndex {
    /// For each chunk, the values between its checkpoints, a positive multiple of 256, or zero
    /// when the chunk has none and its pages are its blocks.
    pub(crate) checkpoint_intervals: Vec<u32>,
    /// For each chunk, its [`CheckpointTable`]: a record for every interval of a page after the
    /// first. Empty when no page of the chunk is longer than its interval.
    pub(crate) checkpoints: Vec<ByteBuffer>,
    /// The minimum and maximum of every block: a little-endian `u32` length, the pco-compressed
    /// minimums of that length, then the pco-compressed maximums.
    pub(crate) block_stats: Option<ByteBuffer>,
}

impl PcoIndex {
    /// The parts of the index that are not buffers.
    fn layout(&self) -> (&[u32], bool) {
        (&self.checkpoint_intervals, self.block_stats.is_some())
    }
}

/// Chunk decompressors kept between random reads, one slot per chunk. Building a chunk
/// decompressor rebuilds the chunk's tANS tables, which can cost more than decoding a block.
#[derive(Default)]
pub(crate) struct DecoderCache(Box<[Mutex<Option<CachedDecompressor>>]>);

/// A `ChunkDecompressor<T>` of the array's number type.
type CachedDecompressor = Box<dyn Any + Send>;

impl DecoderCache {
    fn new(n_chunks: usize) -> Self {
        Self((0..n_chunks).map(|_| Mutex::new(None)).collect())
    }

    fn take<T: Number>(&self, chunk: usize) -> Option<ChunkDecompressor<T>> {
        let cached = self.0.get(chunk)?.lock().take()?;
        cached.downcast().ok().map(|cd| *cd)
    }

    fn put<T: Number>(&self, chunk: usize, cd: ChunkDecompressor<T>) {
        if let Some(slot) = self.0.get(chunk) {
            slot.lock().get_or_insert_with(|| Box::new(cd));
        }
    }
}

impl Debug for DecoderCache {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DecoderCache").finish_non_exhaustive()
    }
}

/// Where one pco page lives and how it splits into blocks, derived from the metadata.
#[derive(Clone, Debug)]
pub(crate) struct PageEntry {
    pub(crate) chunk: usize,
    /// Positions among the array's valid values.
    pub(crate) values: Range<usize>,
    /// The page's records in its chunk's checkpoint table, one per block after the first.
    pub(crate) checkpoints: Range<usize>,
    /// Values per block: the checkpoint interval, or the whole page without an index.
    pub(crate) block_len: usize,
    /// Index of the page's first block among all blocks of the array.
    pub(crate) first_block: usize,
}

impl PageEntry {
    pub(crate) fn n_blocks(&self) -> usize {
        self.values.len().div_ceil(self.block_len)
    }

    /// The values of local blocks `blocks`, relative to the page start.
    pub(crate) fn block_values(&self, blocks: Range<usize>) -> Range<usize> {
        blocks.start * self.block_len..(blocks.end * self.block_len).min(self.values.len())
    }
}

/// Derives the page index from the metadata, and with an index, each chunk's checkpoint count.
fn build_page_index(
    metadata: &PcoMetadata,
    index: Option<&PcoIndex>,
) -> VortexResult<(Vec<PageEntry>, Vec<usize>)> {
    if let Some(index) = index {
        vortex_ensure_eq!(
            index.checkpoint_intervals.len(),
            metadata.chunks.len(),
            "Pco checkpoint intervals do not match the chunk count"
        );
    }

    let mut entries = Vec::new();
    let mut n_records = Vec::with_capacity(metadata.chunks.len());
    let mut value_start = 0usize;
    let mut first_block = 0usize;
    for (chunk_idx, chunk) in metadata.chunks.iter().enumerate() {
        let interval = index.map_or(0, |index| index.checkpoint_intervals[chunk_idx] as usize);
        vortex_ensure!(
            interval.is_multiple_of(FULL_BATCH_N),
            "Pco checkpoint interval {interval} must be a multiple of {FULL_BATCH_N}"
        );
        let mut chunk_records = 0usize;
        let mut chunk_n = 0usize;
        for page in &chunk.pages {
            let n_values = page.n_values as usize;
            vortex_ensure!(
                n_values != 0,
                "Pco chunk {chunk_idx} contains an empty page"
            );
            let block_len = if interval == 0 { n_values } else { interval };
            let n_checkpoints = (n_values - 1) / block_len;
            let value_end = value_start
                .checked_add(n_values)
                .ok_or_else(|| vortex_err!("Pco value count overflows usize"))?;
            let entry = PageEntry {
                chunk: chunk_idx,
                values: value_start..value_end,
                checkpoints: chunk_records..chunk_records + n_checkpoints,
                block_len,
                first_block,
            };
            first_block += entry.n_blocks();
            entries.push(entry);
            value_start = value_end;
            chunk_records += n_checkpoints;
            chunk_n += n_values;
        }
        vortex_ensure!(
            chunk_n <= VALUES_PER_CHUNK,
            "Pco chunk {chunk_idx} contains {chunk_n} values, exceeding the maximum of \
             {VALUES_PER_CHUNK}"
        );
        n_records.push(chunk_records);
    }
    Ok((entries, n_records))
}

/// Parses each chunk's checkpoint table, checking it holds the chunk's records.
fn parse_checkpoint_tables(
    index: Option<&PcoIndex>,
    n_records: &[usize],
) -> VortexResult<Vec<Option<CheckpointTable>>> {
    let Some(index) = index else {
        return Ok(Vec::new());
    };
    vortex_ensure_eq!(
        index.checkpoints.len(),
        n_records.len(),
        "Pco checkpoint buffers do not match the chunk count"
    );
    index
        .checkpoints
        .iter()
        .zip(n_records)
        .enumerate()
        .map(|(chunk, (buffer, &n_records))| {
            if n_records == 0 {
                vortex_ensure!(
                    buffer.is_empty(),
                    "Pco chunk {chunk} has no checkpoints but a checkpoint buffer"
                );
                Ok(None)
            } else {
                CheckpointTable::parse(buffer, n_records).map(Some)
            }
        })
        .collect()
}

/// At most one scan-sized decoded window is retained per v2 array, shared by its slices.
#[derive(Debug)]
struct DecodedWindow {
    values: Range<usize>,
    buffer: ByteBuffer,
}

const MAX_DECODED_WINDOW_BYTES: usize = 256 * 1024;

#[derive(Clone, Debug)]
/// Encoding-specific data for a [`PcoArray`].
pub struct PcoData {
    pub(crate) chunk_metas: Vec<ByteBuffer>,
    pub(crate) pages: Vec<ByteBuffer>,
    pub(crate) metadata: PcoMetadata,
    pub(crate) index: Option<PcoIndex>,
    pub(crate) entries: Arc<[PageEntry]>,
    /// Each chunk's parsed checkpoint table, empty without an index.
    tables: Arc<[Option<CheckpointTable>]>,
    decoders: Arc<DecoderCache>,
    decoded_window: Option<Arc<Mutex<Option<DecodedWindow>>>>,
    ptype: PType,
    unsliced_n_rows: usize,
    slice_start: usize,
    slice_stop: usize,
}

impl Display for PcoData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ptype: {}, nrows: {}, slice: {}..{}",
            self.ptype, self.unsliced_n_rows, self.slice_start, self.slice_stop
        )?;
        if let Some(index) = &self.index {
            write!(f, ", checkpoint intervals: [")?;
            for (chunk, interval) in index.checkpoint_intervals.iter().enumerate() {
                if chunk > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{interval}")?;
            }
            write!(f, "]")?;
        }
        Ok(())
    }
}

impl PcoData {
    /// Validate dtype, validity, slice, and Pco component invariants.
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
        let (entries, n_records) = build_page_index(&self.metadata, self.index.as_ref())?;
        vortex_ensure_eq!(
            entries.len(),
            self.entries.len(),
            "Pco page index does not match metadata"
        );
        vortex_ensure_eq!(
            self.pages.len(),
            entries.len(),
            "page count does not match metadata"
        );
        let tables = parse_checkpoint_tables(self.index.as_ref(), &n_records)?;
        vortex_ensure_eq!(
            tables.len(),
            self.tables.len(),
            "Pco checkpoint tables do not match the index"
        );
        if let Some(stats) = self
            .index
            .as_ref()
            .and_then(|index| index.block_stats.as_ref())
        {
            split_block_stats(stats)?;
        }

        let n_values = self.n_values();
        vortex_ensure!(
            n_values <= self.unsliced_n_rows,
            "Pco contains {n_values} values for only {} rows",
            self.unsliced_n_rows
        );
        if validity.definitely_no_nulls() {
            vortex_ensure_eq!(
                n_values,
                self.unsliced_n_rows,
                "Pco value count must match the row count of a non-null array"
            );
        } else if validity.definitely_all_null() {
            vortex_ensure_eq!(n_values, 0, "Pco contains values for an all-null array");
        }
        Ok(())
    }

    /// Constructs unsliced v1 Pco data without validating its metadata and buffers.
    ///
    /// # Safety
    ///
    /// The returned data must pass [`Self::validate`] before it is decompressed or published as
    /// part of an array, and `metadata` must describe pages no longer than a chunk.
    pub unsafe fn new_unchecked(
        chunk_metas: Vec<ByteBuffer>,
        pages: Vec<ByteBuffer>,
        ptype: PType,
        metadata: PcoMetadata,
        len: usize,
    ) -> Self {
        let (entries, _) = build_page_index(&metadata, None).unwrap_or_default();
        Self {
            decoders: Arc::new(DecoderCache::new(chunk_metas.len())),
            chunk_metas,
            pages,
            metadata,
            index: None,
            decoded_window: None,
            entries: entries.into(),
            tables: Arc::new([]),
            ptype,
            unsliced_n_rows: len,
            slice_start: 0,
            slice_stop: len,
        }
    }

    /// Constructs unsliced Pco data, deriving the page index from the metadata.
    ///
    /// The result must still pass [`Self::validate`] before it is decompressed or published as
    /// part of an array.
    pub(crate) fn try_new(
        chunk_metas: Vec<ByteBuffer>,
        pages: Vec<ByteBuffer>,
        ptype: PType,
        metadata: PcoMetadata,
        index: Option<PcoIndex>,
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
            "Pco does not support {ptype}"
        );
        let (entries, n_records) = build_page_index(&metadata, index.as_ref())?;
        let tables = parse_checkpoint_tables(index.as_ref(), &n_records)?;
        Ok(Self {
            decoders: Arc::new(DecoderCache::new(chunk_metas.len())),
            chunk_metas,
            pages,
            metadata,
            decoded_window: index.as_ref().map(|_| Arc::default()),
            index,
            entries: entries.into(),
            tables: tables.into(),
            ptype,
            unsliced_n_rows: len,
            slice_start: 0,
            slice_stop: len,
        })
    }

    /// Compress a primitive array into v1 Pco data.
    pub fn from_primitive(
        parray: ArrayView<'_, Primitive>,
        level: usize,
        values_per_page: usize,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Self> {
        Self::compress(parray, &PcoOptions::v1(level, values_per_page), ctx)
    }

    /// Compress a primitive array into Pco data with the given options.
    pub fn compress(
        parray: ArrayView<'_, Primitive>,
        options: &PcoOptions,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Self> {
        Self::compress_with_values_per_chunk(parray, options, VALUES_PER_CHUNK, ctx)
    }

    #[cfg(test)]
    pub(crate) fn from_primitive_with_values_per_chunk(
        parray: ArrayView<'_, Primitive>,
        level: usize,
        values_per_chunk: usize,
        values_per_page: usize,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Self> {
        Self::compress_with_values_per_chunk(
            parray,
            &PcoOptions::v1(level, values_per_page),
            values_per_chunk,
            ctx,
        )
    }

    pub(crate) fn compress_with_values_per_chunk(
        parray: ArrayView<'_, Primitive>,
        options: &PcoOptions,
        values_per_chunk: usize,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Self> {
        options.validate()?;
        vortex_ensure!(
            (1..=VALUES_PER_CHUNK).contains(&values_per_chunk),
            "Pco values per chunk must be in 1..={VALUES_PER_CHUNK}"
        );
        let number_type = number_type_from_dtype(parray.dtype());
        let values = collect_valid(parray, ctx)?;
        let compressed = match_number_enum!(
            number_type,
            NumberType<T> => {
                compress_values::<T>(values.as_slice::<T>(), options, values_per_chunk)?
            }
        );
        Self::try_new(
            compressed.chunk_metas,
            compressed.pages,
            parray.dtype().as_ptype(),
            compressed.metadata,
            compressed.index,
            parray.len(),
        )
    }

    /// Downcast and compress an array into v1 Pco data.
    ///
    /// # Errors
    ///
    /// Returns an error if the input is not a primitive array or compression fails.
    pub fn from_array(
        array: ArrayRef,
        level: usize,
        nums_per_page: usize,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Self> {
        let parray = array.try_downcast::<Primitive>().map_err(|a| {
            vortex_err!(
                "Pco can only encode primitive arrays, got {}",
                a.encoding_id()
            )
        })?;
        Self::from_primitive(parray.as_view(), level, nums_per_page, ctx)
    }

    /// The same data without its random-access index, which serializes as the v1 format.
    pub fn without_index(&self) -> Self {
        let mut data = self.clone();
        data.index = None;
        data.decoded_window = None;
        data.entries = build_page_index(&data.metadata, None)
            .unwrap_or_default()
            .0
            .into();
        data.tables = Arc::new([]);
        data
    }

    /// Re-derives the page index and checkpoint tables after the buffers were replaced.
    fn rederive(&mut self) -> VortexResult<()> {
        let (entries, n_records) = build_page_index(&self.metadata, self.index.as_ref())?;
        self.tables = parse_checkpoint_tables(self.index.as_ref(), &n_records)?.into();
        self.entries = entries.into();
        self.decoders = Arc::new(DecoderCache::new(self.chunk_metas.len()));
        self.decoded_window = self.index.as_ref().map(|_| Arc::default());
        Ok(())
    }

    /// Whether this data carries the v2 random-access index.
    pub fn has_index(&self) -> bool {
        self.index.is_some()
    }

    /// Bytes spent on the v2 random-access index: checkpoint records and block stats.
    pub fn index_nbytes(&self) -> (usize, usize) {
        self.index.as_ref().map_or((0, 0), |index| {
            (
                index.checkpoints.iter().map(|buffer| buffer.len()).sum(),
                index.block_stats.as_ref().map_or(0, |stats| stats.len()),
            )
        })
    }

    pub(crate) fn buffers(&self) -> impl Iterator<Item = &ByteBuffer> {
        self.chunk_metas.iter().chain(&self.pages).chain(
            self.index
                .iter()
                .flat_map(|index| index.checkpoints.iter().chain(&index.block_stats)),
        )
    }

    /// The number of valid values stored in the unsliced array.
    pub(crate) fn n_values(&self) -> usize {
        self.entries.last().map_or(0, |entry| entry.values.end)
    }

    /// The index of the page holding valid value `value`.
    pub(crate) fn page_of(&self, value: usize) -> usize {
        self.entries
            .partition_point(|entry| entry.values.end <= value)
    }

    /// The array-wide index of the block holding valid value `value` of `page`.
    pub(crate) fn block_of(&self, page: usize, value: usize) -> usize {
        let entry = &self.entries[page];
        entry.first_block + (value - entry.values.start) / entry.block_len
    }

    /// The page holding array-wide block `block`.
    pub(crate) fn page_of_block(&self, block: usize) -> usize {
        self.entries
            .partition_point(|entry| entry.first_block + entry.n_blocks() <= block)
    }

    /// The number of blocks in the unsliced array.
    pub(crate) fn n_blocks(&self) -> usize {
        self.entries
            .last()
            .map_or(0, |entry| entry.first_block + entry.n_blocks())
    }

    /// The minimum and maximum of every block, if stored.
    pub(crate) fn block_stats<T: Number>(&self) -> VortexResult<Option<(Vec<T>, Vec<T>)>> {
        let Some(stats) = self
            .index
            .as_ref()
            .and_then(|index| index.block_stats.as_ref())
        else {
            return Ok(None);
        };
        let (mins, maxes) = split_block_stats(stats)?;
        let mins = pco::standalone::simple_decompress::<T>(mins).map_err(vortex_err_from_pco)?;
        let maxes = pco::standalone::simple_decompress::<T>(maxes).map_err(vortex_err_from_pco)?;
        let n_blocks = self.n_blocks();
        vortex_ensure!(
            mins.len() == n_blocks && maxes.len() == n_blocks,
            "Pco block stats hold {} minimums and {} maximums for {n_blocks} blocks",
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

    /// Decompress this Pco data into a primitive array.
    pub(crate) fn decompress(
        &self,
        unsliced_validity: &Validity,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<PrimitiveArray> {
        let mask = unsliced_validity.execute_mask(self.unsliced_n_rows, ctx)?;
        let values = self.value_range(&mask);
        // Validation bounds the stored values by the row count, but a corrupt row count must not
        // become an allocation size.
        vortex_ensure!(
            values.end <= self.n_values(),
            "Pco contains {} values, but the requested range ends at {}",
            self.n_values(),
            values.end
        );
        // Conjuncts commonly read the same scan window independently. Retain only one small
        // slice, so a full-array scan never leaves an unbounded decompressed copy behind.
        let cache = self.decoded_window.as_ref().filter(|_| {
            values.len() < self.n_values()
                && values.len() <= MAX_DECODED_WINDOW_BYTES / self.ptype.byte_width()
        });
        let cached = cache.and_then(|cache| {
            cache
                .lock()
                .as_ref()
                .filter(|window| window.values == values)
                .map(|window| window.buffer.clone())
        });
        let values_byte_buffer = if let Some(buffer) = cached {
            buffer
        } else {
            let number_type = number_type_from_ptype(self.ptype);
            let (buffer, fits_cache) = match_number_enum!(
                number_type,
                NumberType<T> => {
                    let mut out = BufferMut::<T>::with_capacity_in(values.len(), ctx.allocator().clone());
                    // Keep chunk decompressors for later reads of slices, not of whole arrays.
                    let retain = values.len() < self.n_values();
                    BlockDecoder::<T>::new(self, retain)?.decode_values(values.clone(), &mut out)?;
                    // Initial allocations include fixed alignment padding; reject buffers
                    // that grew to hold extra decoded values, not that padding.
                    let capacity_limit = MAX_DECODED_WINDOW_BYTES
                        + Alignment::DEFAULT_ALIGNMENT.as_usize();
                    let fits_cache = out.capacity() <= capacity_limit / size_of::<T>();
                    (out.freeze().into_byte_buffer(), fits_cache)
                }
            );
            if let Some(cache) = cache.filter(|_| fits_cache) {
                *cache.lock() = Some(DecodedWindow {
                    values,
                    buffer: buffer.clone(),
                });
            }
            buffer
        };
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
    metadata: PcoMetadata,
    index: Option<PcoIndex>,
}

fn compress_values<T: Number + NativePType>(
    values: &[T],
    options: &PcoOptions,
    values_per_chunk: usize,
) -> VortexResult<Compressed> {
    let max_page_n = if options.values_per_page == 0 {
        values_per_chunk
    } else {
        options.values_per_page
    };
    let config = |max_page_n: usize| {
        ChunkConfig::default()
            .with_compression_level(options.level)
            .with_paging_spec(PagingSpec::EqualPagesUpTo(max_page_n))
    };

    let fc = FileCompressor::default();
    let mut header = vec![];
    fc.write_header(&mut header).map_err(vortex_err_from_pco)?;

    let interval = options.checkpoint_interval;
    let mut chunk_metas = vec![]; // the Pco component
    let mut chunk_infos = vec![]; // the Vortex metadata
    let mut pages = vec![];
    let mut checkpoints = vec![];
    let mut checkpoint_intervals = vec![];
    let mut block_stats = (interval.is_some() && options.block_stats).then(|| {
        let n_blocks = values.len().div_ceil(interval.unwrap_or(1));
        (Vec::with_capacity(n_blocks), Vec::with_capacity(n_blocks))
    });
    for chunk in values.chunks(values_per_chunk) {
        let mut cc = fc
            .chunk_compressor(chunk, &config(max_page_n))
            .map_err(vortex_err_from_pco)?;
        let mut walker = None;
        let mut chunk_interval = 0;
        if let Some(interval) = interval {
            walker = PageWalker::try_new(cc.meta())?;
            if walker.is_some() {
                chunk_interval = interval;
            } else if cc.n_per_page().iter().any(|&page_n| page_n > interval) {
                // The chosen delta encoding (lookback) cannot be checkpointed and beats the ones
                // that can, so keep it with pages of the longest block as the chunk's blocks.
                cc = fc
                    .chunk_compressor(chunk, &config(options.max_checkpoint_interval))
                    .map_err(vortex_err_from_pco)?;
            }
        }

        let mut chunk_meta = Vec::with_capacity(cc.meta_size_hint());
        cc.write_meta(&mut chunk_meta)
            .map_err(vortex_err_from_pco)?;
        chunk_metas.push(ByteBuffer::from(chunk_meta));

        let mut records = walker.as_ref().map(PageWalker::new_records);
        let mut page_infos = vec![];
        let mut chunk_bytes = chunk_metas.last().map_or(0, ByteBuffer::len);
        for (page_idx, page_n) in cc.n_per_page().into_iter().enumerate() {
            let mut page = Vec::with_capacity(cc.page_size_hint(page_idx));
            cc.write_page(page_idx, &mut page)
                .map_err(vortex_err_from_pco)?;
            if let (Some(walker), Some(records)) = (&mut walker, &mut records)
                && page_n > chunk_interval
            {
                walker.checkpoint_page(&page, page_n, chunk_interval, records)?;
            }
            chunk_bytes += page.len();
            pages.push(ByteBuffer::from(page));
            page_infos.push(PcoPageInfo {
                n_values: u32::try_from(page_n)?,
            });
        }
        chunk_infos.push(PcoChunkInfo { pages: page_infos });

        let mut table = None;
        if let Some(records) = records.filter(|records| !records.is_empty()) {
            // Highly compressible chunks spend more on checkpoints than on data, so space them out
            // until the index takes a small share of the chunk.
            let page_ns = cc.n_per_page();
            let mut factor = 1;
            loop {
                let coarse = records.coarsen(factor);
                let encoded = coarse.encode();
                let stats_bytes = if options.block_stats {
                    estimate_block_stats_bytes(
                        chunk,
                        &page_ns,
                        factor * chunk_interval,
                        options.level,
                    )?
                } else {
                    0
                };
                let index_bytes = encoded.len() + stats_bytes;
                if coarse.is_empty()
                    || 2 * factor * chunk_interval > options.max_checkpoint_interval
                    || index_bytes * INDEX_SHARE <= chunk_bytes
                {
                    if !coarse.is_empty() {
                        table = Some(encoded);
                    }
                    chunk_interval *= factor;
                    break;
                }
                factor *= 2;
            }
        }

        if interval.is_some() {
            checkpoint_intervals.push(u32::try_from(chunk_interval)?);
            checkpoints.push(table.map_or_else(ByteBuffer::empty, ByteBuffer::from));
        }

        if let Some((mins, maxes)) = &mut block_stats {
            // Pages split chunks into contiguous runs, and blocks restart at each page.
            let mut page_start = 0;
            for page_n in cc.n_per_page() {
                let block_len = if chunk_interval == 0 {
                    page_n
                } else {
                    chunk_interval
                };
                for block in chunk[page_start..page_start + page_n].chunks(block_len) {
                    let (min, max) = min_max(block);
                    mins.push(min);
                    maxes.push(max);
                }
                page_start += page_n;
            }
        }
    }

    let index = match interval {
        Some(_) => Some(PcoIndex {
            checkpoint_intervals,
            checkpoints,
            block_stats: block_stats
                .filter(|(mins, maxes)| blocks_narrow_range(mins, maxes))
                .map(|(mins, maxes)| compress_block_stats(&mins, &maxes, options.level))
                .transpose()?,
        }),
        None => None,
    };
    Ok(Compressed {
        chunk_metas,
        pages,
        metadata: PcoMetadata {
            header,
            chunks: chunk_infos,
        },
        index,
    })
}

/// The largest average fraction of the value range a block may span for block stats to be kept.
const MAX_BLOCK_RANGE_FRACTION: f64 = 0.5;

/// Whether blocks cover narrow enough parts of the value range for their stats to decide many
/// blocks of a comparison: on average at most [`MAX_BLOCK_RANGE_FRACTION`] of it. Ranges the
/// test cannot measure, such as a constant array's or one ending in an infinity, keep the stats.
fn blocks_narrow_range<T: NativePType>(mins: &[T], maxes: &[T]) -> bool {
    let (Some(&first_min), Some(&first_max)) = (mins.first(), maxes.first()) else {
        return true;
    };
    let min = mins
        .iter()
        .fold(first_min, |acc, &v| if v.is_lt(acc) { v } else { acc });
    let max = maxes
        .iter()
        .fold(first_max, |acc, &v| if v.is_gt(acc) { v } else { acc });
    let to_f64 = |value: T| value.to_f64().unwrap_or(f64::NAN);
    let range = to_f64(max) - to_f64(min);
    if !(range.is_finite() && range > 0.0) {
        return true;
    }
    let spanned = mins
        .iter()
        .zip(maxes)
        .map(|(&min, &max)| (to_f64(max) - to_f64(min)) / range)
        .sum::<f64>();
    // A NaN sum keeps the stats.
    spanned.partial_cmp(&(MAX_BLOCK_RANGE_FRACTION * mins.len() as f64)) != Some(Ordering::Greater)
}

/// The compressed size of the block stats of one chunk split into pages of `page_ns` values and
/// blocks of `block_len`, or zero if they would not be kept.
fn estimate_block_stats_bytes<T: Number + NativePType>(
    chunk: &[T],
    page_ns: &[usize],
    block_len: usize,
    level: usize,
) -> VortexResult<usize> {
    let mut mins = Vec::new();
    let mut maxes = Vec::new();
    let mut page_start = 0;
    for &page_n in page_ns {
        for block in chunk[page_start..page_start + page_n].chunks(block_len) {
            let (min, max) = min_max(block);
            mins.push(min);
            maxes.push(max);
        }
        page_start += page_n;
    }
    if !blocks_narrow_range(&mins, &maxes) {
        return Ok(0);
    }
    Ok(compress_block_stats(&mins, &maxes, level)?.len())
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
        .ok_or_else(|| vortex_err!("Pco block stats are truncated"))?;
    let len = u32::from_le_bytes(*len) as usize;
    vortex_ensure!(len <= rest.len(), "Pco block stats are truncated");
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

/// Decodes blocks of a [`PcoData`], reusing one chunk decompressor at a time.
pub(crate) struct BlockDecoder<'a, T: Number> {
    data: &'a PcoData,
    file: FileDecompressor,
    chunk: Option<(usize, ChunkDecompressor<T>)>,
    page_scratch: Vec<u8>,
    /// Whether to return chunk decompressors to the data's cache for later reads.
    retain: bool,
}

impl<'a, T: Number> BlockDecoder<'a, T> {
    /// A decoder that, with `retain`, takes chunk decompressors from the data's cache and returns
    /// them there when done. Random reads retain; whole-array reads do not, so that a scan does
    /// not keep every chunk's tables alive.
    pub(crate) fn new(data: &'a PcoData, retain: bool) -> VortexResult<Self> {
        let (file, _) =
            FileDecompressor::new(data.metadata.header.as_slice()).map_err(vortex_err_from_pco)?;
        Ok(Self {
            data,
            file,
            chunk: None,
            page_scratch: Vec::new(),
            retain,
        })
    }

    pub(crate) fn data(&self) -> &'a PcoData {
        self.data
    }

    fn release(&mut self) {
        if let Some((chunk, cd)) = self.chunk.take()
            && self.retain
        {
            self.data.decoders.put(chunk, cd);
        }
    }

    /// Decodes `dst.len()` values of page `page` into `dst`, starting at the first value of block
    /// `start_block` of the page. `dst` must end on a batch boundary or at the end of the page.
    fn decode_page(&mut self, page: usize, start_block: usize, dst: &mut [T]) -> VortexResult<()> {
        let data = self.data;
        let entry = &data.entries[page];
        if !matches!(&self.chunk, Some((chunk, _)) if *chunk == entry.chunk) {
            self.release();
            let cached = if self.retain {
                data.decoders.take(entry.chunk)
            } else {
                None
            };
            let cd = match cached {
                Some(cd) => cd,
                None => {
                    self.file
                        .chunk_decompressor(data.chunk_metas[entry.chunk].as_ref())
                        .map_err(vortex_err_from_pco)?
                        .0
                }
            };
            self.chunk = Some((entry.chunk, cd));
        }
        let Some((_, cd)) = &mut self.chunk else {
            unreachable!("the chunk decompressor was just set");
        };
        let checkpoints = PageCheckpoints {
            table: data
                .tables
                .get(entry.chunk)
                .and_then(Option::as_ref)
                .zip(data.index.as_ref())
                .map(|(table, index)| (table, index.checkpoints[entry.chunk].as_slice())),
            first_record: entry.checkpoints.start,
            block_len: entry.block_len,
        };
        decode_blocks(
            cd,
            data.pages[page].as_slice(),
            entry.values.len(),
            checkpoints,
            start_block,
            dst,
            &mut self.page_scratch,
        )
    }

    /// Appends values `local` of page `page`, numbered within the page, to `out`. Decodes from
    /// the start of the block holding the first value through the batch holding the last.
    pub(crate) fn append_page_values(
        &mut self,
        page: usize,
        local: Range<usize>,
        out: &mut BufferMut<T>,
    ) -> VortexResult<()> {
        let entry = &self.data.entries[page];
        let page_n = entry.values.len();
        vortex_ensure!(
            local.start < local.end && local.end <= page_n,
            "invalid range {local:?} of a page of {page_n} values"
        );
        let start_block = local.start / entry.block_len;
        let decode_start = start_block * entry.block_len;
        let decode_end =
            (decode_start + (local.end - decode_start).next_multiple_of(FULL_BATCH_N)).min(page_n);
        let n = decode_end - decode_start;
        let old_len = out.len();
        out.reserve(n);
        // SAFETY: pco either writes every element of the new tail or returns an error, in which
        // case the length is restored before anything observes the tail.
        unsafe { out.set_len(old_len + n) };
        if let Err(err) = self.decode_page(page, start_block, &mut out[old_len..]) {
            unsafe { out.set_len(old_len) };
            return Err(err);
        }
        let skip = local.start - decode_start;
        if skip > 0 {
            out.copy_within(old_len + skip..old_len + skip + local.len(), old_len);
        }
        out.truncate(old_len + local.len());
        Ok(())
    }

    /// Appends the values of blocks `blocks` of page `page` to `out`.
    pub(crate) fn append_page_blocks(
        &mut self,
        page: usize,
        blocks: Range<usize>,
        out: &mut BufferMut<T>,
    ) -> VortexResult<()> {
        let local = self.data.entries[page].block_values(blocks);
        self.append_page_values(page, local, out)
    }

    /// Appends valid values `values` to `out`, decoding only the blocks they touch.
    pub(crate) fn decode_values(
        &mut self,
        values: Range<usize>,
        out: &mut BufferMut<T>,
    ) -> VortexResult<()> {
        let data = self.data;
        for (page, entry) in data
            .entries
            .iter()
            .enumerate()
            .skip(data.page_of(values.start))
        {
            if entry.values.start >= values.end {
                break;
            }
            let local = values.start.max(entry.values.start) - entry.values.start
                ..values.end.min(entry.values.end) - entry.values.start;
            if !local.is_empty() {
                self.append_page_values(page, local, out)?;
            }
        }
        Ok(())
    }
}

impl<T: Number> Drop for BlockDecoder<'_, T> {
    fn drop(&mut self) {
        self.release();
    }
}

impl ValidityVTable<Pco> for Pco {
    fn validity(array: ArrayView<'_, Pco>) -> VortexResult<Validity> {
        array
            .unsliced_validity()
            .slice(array.slice_start()..array.slice_stop())
    }
}

impl OperationsVTable<Pco> for Pco {
    type ProbeState = crate::probe::PcoProbeState;

    fn probe_scalar(
        state: &mut ProbeState<'_, Pco>,
        index: usize,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Scalar> {
        let array = state.array();
        crate::probe::scalar_at(array, index, state.retained(), ctx)
    }

    fn scalar_at(
        array: ArrayView<'_, Pco>,
        index: usize,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Scalar> {
        crate::probe::scalar_at(array, index, None, ctx)
    }
}

#[cfg(test)]
mod tests {
    use vortex_array::IntoArray;
    use vortex_array::VortexSessionExecute;
    use vortex_array::array_session;
    use vortex_array::arrays::PrimitiveArray;
    use vortex_array::assert_arrays_eq;
    use vortex_array::validity::Validity;
    use vortex_buffer::buffer;
    use vortex_error::VortexResult;

    use super::MAX_DECODED_WINDOW_BYTES;
    use super::VALUES_PER_CHUNK;
    use crate::Pco;
    use crate::PcoData;
    use crate::PcoOptions;

    #[test]
    fn decoded_window_is_bounded_and_reset_on_rederive() -> VortexResult<()> {
        let mut ctx = array_session().create_execution_ctx();
        let values = PrimitiveArray::from_iter(0..65_536u64);
        let pco = Pco::compress(values.as_view(), &PcoOptions::v2(), &mut ctx)?;
        let data = pco.data();
        let cache = data.decoded_window.as_ref().unwrap();
        let limit = MAX_DECODED_WINDOW_BYTES / size_of::<u64>();

        let whole = data.decompress(&Validity::NonNullable, &mut ctx)?;
        assert_arrays_eq!(whole, values, &mut ctx);
        assert!(cache.lock().is_none());
        let oversized = data._slice(0, limit + 1);
        let decoded = oversized.decompress(&Validity::NonNullable, &mut ctx)?;
        assert_arrays_eq!(
            decoded,
            values.clone().into_array().slice(0..limit + 1)?,
            &mut ctx
        );
        assert!(cache.lock().is_none());
        let unaligned = data._slice(1, limit + 1);
        let decoded = unaligned.decompress(&Validity::NonNullable, &mut ctx)?;
        assert_arrays_eq!(
            decoded,
            values.clone().into_array().slice(1..limit + 1)?,
            &mut ctx
        );
        assert!(cache.lock().is_none());

        let window = data._slice(0, limit);
        let first = window.decompress(&Validity::NonNullable, &mut ctx)?;
        let second = window.decompress(&Validity::NonNullable, &mut ctx)?;
        assert_arrays_eq!(first, second, &mut ctx);
        assert_eq!(
            cache.lock().as_ref().unwrap().buffer.len(),
            MAX_DECODED_WINDOW_BYTES
        );
        let mut rebound = data.clone();
        rebound.rederive()?;
        assert!(rebound.decoded_window.as_ref().unwrap().lock().is_none());
        Ok(())
    }

    #[test]
    fn test_slice_nullable() {
        let mut ctx = array_session().create_execution_ctx();
        // Create a nullable array with some nulls
        let values = PrimitiveArray::new(
            buffer![10u32, 20, 30, 40, 50, 60],
            Validity::from_iter([false, true, true, true, true, false]),
        );
        let pco = Pco::from_primitive(values.as_view(), 0, 128, &mut ctx).unwrap();
        assert_arrays_eq!(
            pco,
            PrimitiveArray::from_option_iter([
                None,
                Some(20u32),
                Some(30),
                Some(40),
                Some(50),
                None
            ]),
            &mut ctx
        );

        // Slice to get only the non-null values in the middle
        let sliced = pco.slice(1..5).unwrap();
        let expected =
            PrimitiveArray::from_option_iter([Some(20u32), Some(30), Some(40), Some(50)])
                .into_array();
        assert_arrays_eq!(sliced, expected, &mut ctx);
    }

    #[test]
    fn test_decompress_bounds_initial_allocation() -> VortexResult<()> {
        let mut ctx = array_session().create_execution_ctx();
        let values = PrimitiveArray::from_iter([42u32]);
        let pco = Pco::from_primitive(values.as_view(), 0, 128, &mut ctx)?;
        let mut data = pco.data().clone();

        // Simulate a corrupt logical length whose validity claims far more values than the
        // encoded pages contain. This must not be used directly as an allocation size.
        data.unsliced_n_rows = usize::MAX;
        data.slice_stop = usize::MAX;

        let result = data.decompress(&Validity::NonNullable, &mut ctx);
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn test_validate_rejects_oversized_chunk() -> VortexResult<()> {
        let mut ctx = array_session().create_execution_ctx();
        let values = PrimitiveArray::from_iter([42u32]);
        let pco = Pco::from_primitive(values.as_view(), 0, 128, &mut ctx)?;
        let mut metadata = pco.metadata.clone();
        metadata.chunks[0].pages[0].n_values = u32::try_from(VALUES_PER_CHUNK + 1)?;
        // SAFETY: the data is only validated, never decompressed or published.
        let data = unsafe {
            PcoData::new_unchecked(
                pco.chunk_metas.clone(),
                pco.pages.clone(),
                pco.dtype().as_ptype(),
                metadata,
                pco.len(),
            )
        };

        assert!(
            data.validate(pco.dtype(), pco.len(), &Validity::NonNullable)
                .is_err()
        );
        Ok(())
    }
}
