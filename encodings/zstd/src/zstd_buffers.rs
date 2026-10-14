// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;

use prost::Message as _;
use vortex_array::Array;
use vortex_array::ArrayDeserialization;
use vortex_array::ArrayEq;
use vortex_array::ArrayHash;
use vortex_array::ArrayId;
use vortex_array::ArrayParts;
use vortex_array::ArrayRef;
use vortex_array::ArraySlots;
use vortex_array::ArrayView;
use vortex_array::EqMode;
use vortex_array::ExecutionCtx;
use vortex_array::ExecutionResult;
use vortex_array::buffer::BufferHandle;
use vortex_array::dtype::DType;
use vortex_array::scalar::Scalar;
use vortex_array::serde::ArrayChildren;
use vortex_array::session::ArraySessionExt;
use vortex_array::validity::Validity;
use vortex_array::vtable::OperationsVTable;
use vortex_array::vtable::VTable;
use vortex_array::vtable::ValidityVTable;
use vortex_buffer::Alignment;
use vortex_buffer::ByteBuffer;
use vortex_buffer::ByteBufferMut;
use vortex_error::VortexResult;
use vortex_error::vortex_ensure;
use vortex_error::vortex_ensure_eq;
use vortex_error::vortex_err;
use vortex_session::VortexSession;
use vortex_session::registry::CachedId;

use crate::ZstdBuffersMetadata;
use crate::ZstdBuffersValidity;
use crate::decompressor::with_decompressor;
use crate::validate_frame_content_size;

/// A [`ZstdBuffers`]-encoded Vortex array.
pub type ZstdBuffersArray = Array<ZstdBuffers>;

#[derive(Clone, Debug)]
/// Encoding marker for buffer-level zstd compression.
pub struct ZstdBuffers;

impl ZstdBuffers {
    /// Construct a [`ZstdBuffersArray`] from compressed buffer data whose inner array has no
    /// children and whose validity is not stored as a child.
    pub fn try_new(
        dtype: DType,
        len: usize,
        data: ZstdBuffersData,
    ) -> VortexResult<ZstdBuffersArray> {
        Array::try_from_parts(ArrayParts::new(
            ZstdBuffers,
            dtype,
            len,
            data,
            ArraySlots::new(),
        ))
    }

    /// Compress every top-level buffer of `array` independently with zstd.
    ///
    /// The wrapped array's serialized representation is captured so it can be rebuilt after
    /// decompression, including any buffers or children selected by its serializer.
    pub fn compress(
        array: &ArrayRef,
        level: i32,
        session: &VortexSession,
    ) -> VortexResult<ZstdBuffersArray> {
        let serialization = session
            .array_serialize(array)?
            .ok_or_else(|| vortex_err!("[ZstdBuffers]: Array does not support serialization"))?;

        let mut compressed_buffers = Vec::with_capacity(serialization.buffers.len());
        let mut uncompressed_sizes = Vec::with_capacity(serialization.buffers.len());
        let mut buffer_alignments = Vec::with_capacity(serialization.buffers.len());

        let (validity, validity_child) = match array.validity()? {
            Validity::NonNullable | Validity::AllValid => (ZstdBuffersValidity::AllValid, None),
            Validity::AllInvalid => (ZstdBuffersValidity::AllInvalid, None),
            Validity::Array(validity) => (ZstdBuffersValidity::Child, Some(validity)),
        };

        let mut compressor = zstd::bulk::Compressor::new(level)?;
        for buffer in &serialization.buffers {
            buffer_alignments.push(u32::from(buffer.alignment()));
            uncompressed_sizes.push(buffer.len() as u64);
            let mut compressed = compressor.compress(buffer)?;
            compressed.shrink_to_fit();
            compressed_buffers.push(BufferHandle::new_host(ByteBuffer::from(compressed)));
        }

        let data = ZstdBuffersData {
            inner_encoding_id: serialization.serialized_id,
            inner_metadata: serialization.metadata,
            compressed_buffers,
            uncompressed_sizes,
            buffer_alignments,
            validity,
        };
        let slots: ArraySlots = serialization
            .children
            .into_iter()
            .chain(validity_child)
            .map(Some)
            .collect();
        let compressed = Array::try_from_parts(ArrayParts::new(
            ZstdBuffers,
            array.dtype().clone(),
            array.len(),
            data,
            slots,
        ))?;
        compressed.statistics().inherit_from(array.statistics());
        Ok(compressed)
    }

    /// Rebuild the wrapped array from decompressed buffer handles.
    pub fn build_inner(
        array: &ZstdBuffersArray,
        buffer_handles: &[BufferHandle],
        session: &VortexSession,
    ) -> VortexResult<ArrayRef> {
        let inner_vtable = session
            .arrays()
            .registry()
            .get(&array.data().inner_encoding_id)
            .ok_or_else(|| {
                vortex_err!("Unknown inner encoding: {}", array.data().inner_encoding_id)
            })?;

        let children: Vec<ArrayRef> = array
            .slots()
            .iter()
            .take(array.data().inner_child_count(array.slots().len()))
            .flatten()
            .cloned()
            .collect();
        inner_vtable.deserialize(
            ArrayDeserialization::new(
                array.data().inner_encoding_id,
                array.dtype(),
                array.len(),
                &array.data().inner_metadata,
                buffer_handles,
                &children.as_slice(),
            ),
            session,
        )
    }

    fn decompress_and_build_inner(
        array: &ZstdBuffersArray,
        session: &VortexSession,
    ) -> VortexResult<ArrayRef> {
        let decompressed_buffers = array.data().decompress_buffers()?;
        Self::build_inner(array, &decompressed_buffers, session)
    }
}

/// An encoding that ZSTD-compresses the buffers of any wrapped array.
///
/// Unlike [`ZstdArray`](crate::ZstdArray), which interleaves string lengths with content bytes,
/// `ZstdBuffersArray` compresses each buffer independently. This enables zero-conversion
/// GPU decompression since the original buffer layout is preserved after decompression.
#[derive(Clone, Debug)]
pub struct ZstdBuffersData {
    inner_encoding_id: ArrayId,
    inner_metadata: Vec<u8>,
    compressed_buffers: Vec<BufferHandle>,
    uncompressed_sizes: Vec<u64>,
    buffer_alignments: Vec<u32>,
    validity: ZstdBuffersValidity,
}

impl Display for ZstdBuffersData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "inner_encoding: {}", self.inner_encoding_id)
    }
}

#[derive(Clone, Debug)]
/// Decode plan for buffer-level zstd decompression.
pub struct ZstdBuffersDecodePlan {
    compressed_buffers: Vec<BufferHandle>,
    frame_sizes: Arc<[usize]>,
    output_sizes: Arc<[usize]>,
    output_offsets: Vec<usize>,
    output_alignments: Vec<Alignment>,
    output_size_total: usize,
    output_size_max: usize,
}

impl ZstdBuffersDecodePlan {
    /// Compressed buffers to decode.
    pub fn compressed_buffers(&self) -> &[BufferHandle] {
        &self.compressed_buffers
    }

    /// Compressed frame sizes in bytes.
    pub fn frame_sizes(&self) -> Arc<[usize]> {
        Arc::clone(&self.frame_sizes)
    }

    /// Decompressed output size for each buffer.
    pub fn output_sizes(&self) -> Arc<[usize]> {
        Arc::clone(&self.output_sizes)
    }

    /// Byte offsets of each decompressed buffer in one contiguous output allocation.
    pub fn output_offsets(&self) -> &[usize] {
        &self.output_offsets
    }

    /// Total byte size of the planned contiguous output allocation.
    pub fn output_size_total(&self) -> usize {
        self.output_size_total
    }

    /// Largest single decompressed buffer size.
    pub fn output_size_max(&self) -> usize {
        self.output_size_max
    }

    /// Number of compressed frames/buffers in the plan.
    pub fn num_frames(&self) -> usize {
        self.compressed_buffers.len()
    }

    /// Split a contiguous decompressed output buffer into per-buffer handles using planned
    /// offsets/sizes and enforce each buffer's required alignment.
    pub fn split_output_handle(
        &self,
        output_handle: &BufferHandle,
    ) -> VortexResult<Vec<BufferHandle>> {
        self.output_offsets
            .iter()
            .zip(self.output_sizes.iter())
            .zip(self.output_alignments.iter())
            .map(|((&offset, &size), &alignment)| {
                output_handle
                    .slice(offset..offset + size)
                    .ensure_aligned(alignment)
            })
            .collect::<VortexResult<Vec<_>>>()
    }
}

impl ZstdBuffersData {
    /// Number of leading slots that belong to the inner array, out of `n_slots`.
    fn inner_child_count(&self, n_slots: usize) -> usize {
        match self.validity {
            ZstdBuffersValidity::Child => n_slots.saturating_sub(1),
            ZstdBuffersValidity::AllValid | ZstdBuffersValidity::AllInvalid => n_slots,
        }
    }

    fn validate(&self) -> VortexResult<()> {
        vortex_ensure_eq!(
            self.compressed_buffers.len(),
            self.uncompressed_sizes.len(),
            "zstd_buffers metadata mismatch"
        );
        vortex_ensure_eq!(
            self.compressed_buffers.len(),
            self.buffer_alignments.len(),
            "zstd_buffers metadata mismatch"
        );
        Ok(())
    }

    fn decompress_buffers(&self) -> VortexResult<Vec<BufferHandle>> {
        // CPU decode path: zstd::bulk works on host bytes, so compressed buffers are
        // materialized on the host via `try_to_host_sync`.
        with_decompressor(|decompressor| {
            let mut result = Vec::with_capacity(self.compressed_buffers.len());
            for (i, (buf, &uncompressed_size)) in self
                .compressed_buffers
                .iter()
                .zip(&self.uncompressed_sizes)
                .enumerate()
            {
                let size = usize::try_from(uncompressed_size)?;
                let alignment = self.buffer_alignments.get(i).copied().unwrap_or(1);

                let aligned = Alignment::try_from(alignment)?;
                let compressed = buf.clone().try_to_host_sync()?;
                validate_frame_content_size(compressed.as_slice(), uncompressed_size, i)?;
                let mut output = ByteBufferMut::with_capacity_aligned(size, aligned);
                let spare = output.spare_capacity_mut();

                // This is currently guaranteed, but still good to check because
                // of the unsafe calls below.
                if spare.len() < size {
                    return Err(vortex_err!(
                        "Insufficient output capacity: expected at least {}, got {}",
                        size,
                        spare.len()
                    ));
                }
                // SAFETY: we only expose the first `size` bytes and mark them initialized via
                // `set_len(size)` after zstd reports how many bytes were written.
                let dst = unsafe {
                    std::slice::from_raw_parts_mut(spare.as_mut_ptr().cast::<u8>(), size)
                };
                let written = decompressor.decompress_to_buffer(compressed.as_slice(), dst)?;
                if written != size {
                    return Err(vortex_err!(
                        "Decompressed size mismatch: expected {}, got {}",
                        size,
                        written
                    ));
                }
                // SAFETY: zstd wrote exactly `size` initialized bytes into `dst`.
                unsafe { output.set_len(size) };
                result.push(BufferHandle::new_host(output.freeze()));
            }
            Ok(result)
        })
    }

    /// Build a decode plan for external or device decompression.
    pub fn decode_plan(&self) -> VortexResult<ZstdBuffersDecodePlan> {
        // If invariants are somehow broken, device decompression could have UB, so ensure
        // they still hold.
        self.validate()?;
        // Only host-resident frames are checked: reading a device frame's header would cost a D2H
        // copy per frame, and the device output allocation is fallible so it cannot abort.
        for (index, (buffer, &metadata_size)) in self
            .compressed_buffers
            .iter()
            .zip(&self.uncompressed_sizes)
            .enumerate()
        {
            if let Some(frame) = buffer.as_host_opt() {
                validate_frame_content_size(frame.as_slice(), metadata_size, index)?;
            }
        }

        let output_sizes = self
            .uncompressed_sizes
            .iter()
            .map(|&size| usize::try_from(size))
            .collect::<Result<Vec<_>, _>>()?;
        let output_size_max = output_sizes.iter().copied().max().unwrap_or(0);

        let output_alignments = self
            .buffer_alignments
            .iter()
            .map(|&alignment| Alignment::try_from(alignment))
            .collect::<VortexResult<Vec<_>>>()?;

        let (output_offsets, output_size_total) =
            compute_output_layout(&output_sizes, &output_alignments);

        let compressed_buffers = self.compressed_buffers.clone();
        let frame_sizes: Arc<[usize]> = compressed_buffers
            .iter()
            .map(BufferHandle::len)
            .collect::<Vec<_>>()
            .into();
        let output_sizes: Arc<[usize]> = output_sizes.into();

        Ok(ZstdBuffersDecodePlan {
            compressed_buffers,
            frame_sizes,
            output_sizes,
            output_offsets,
            output_alignments,
            output_size_total,
            output_size_max,
        })
    }
}

fn compute_output_layout(
    output_sizes: &[usize],
    output_alignments: &[Alignment],
) -> (Vec<usize>, usize) {
    // Compute aligned offsets for each decompressed buffer in one contiguous output allocation.
    // Each buffer starts at the next multiple of its required alignment.
    let mut offsets = Vec::with_capacity(output_sizes.len());
    let mut total_size = 0usize;

    for (&size, &alignment) in output_sizes.iter().zip(output_alignments.iter()) {
        total_size = total_size.next_multiple_of(alignment.as_usize());
        offsets.push(total_size);
        total_size += size;
    }

    (offsets, total_size)
}

#[expect(clippy::disallowed_methods, reason = "interning a dynamic id")]
fn array_id_from_string(s: &str) -> ArrayId {
    ArrayId::new(s)
}

impl ArrayHash for ZstdBuffersData {
    fn array_hash<H: Hasher>(&self, state: &mut H, accuracy: EqMode) {
        self.inner_encoding_id.hash(state);
        self.inner_metadata.hash(state);
        for buf in &self.compressed_buffers {
            buf.array_hash(state, accuracy);
        }
        self.uncompressed_sizes.hash(state);
        self.buffer_alignments.hash(state);
        self.validity.hash(state);
    }
}

impl ArrayEq for ZstdBuffersData {
    fn array_eq(&self, other: &Self, accuracy: EqMode) -> bool {
        self.inner_encoding_id == other.inner_encoding_id
            && self.inner_metadata == other.inner_metadata
            && self.compressed_buffers.len() == other.compressed_buffers.len()
            && self
                .compressed_buffers
                .iter()
                .zip(&other.compressed_buffers)
                .all(|(a, b)| a.array_eq(b, accuracy))
            && self.uncompressed_sizes == other.uncompressed_sizes
            && self.buffer_alignments == other.buffer_alignments
            && self.validity == other.validity
    }
}

impl VTable for ZstdBuffers {
    type TypedArrayData = ZstdBuffersData;
    type OperationsVTable = Self;
    type ValidityVTable = Self;

    fn id(&self) -> ArrayId {
        static ID: CachedId = CachedId::new("vortex.zstd_buffers");
        *ID
    }

    fn validate(
        &self,
        data: &Self::TypedArrayData,
        dtype: &DType,
        len: usize,
        slots: &[Option<ArrayRef>],
    ) -> VortexResult<()> {
        data.validate()?;
        if data.validity == ZstdBuffersValidity::Child {
            vortex_ensure!(
                dtype.is_nullable(),
                "non-nullable ZstdBuffers has a validity child"
            );
            let validity = slots
                .last()
                .and_then(Option::as_ref)
                .ok_or_else(|| vortex_err!("ZstdBuffers validity child is missing"))?;
            vortex_ensure_eq!(validity.dtype(), &Validity::DTYPE);
            vortex_ensure_eq!(validity.len(), len);
        }
        Ok(())
    }

    fn nbuffers(array: ArrayView<'_, Self>) -> usize {
        array.compressed_buffers.len()
    }

    fn buffer(array: ArrayView<'_, Self>, idx: usize) -> BufferHandle {
        array.compressed_buffers[idx].clone()
    }

    fn buffer_name(_array: ArrayView<'_, Self>, idx: usize) -> Option<String> {
        Some(format!("compressed_{idx}"))
    }

    fn with_buffers(
        &self,
        array: ArrayView<'_, Self>,
        buffers: &[BufferHandle],
    ) -> VortexResult<ArrayParts<Self>> {
        let mut data = array.data().clone();
        data.compressed_buffers = buffers.to_vec();
        Ok(ArrayParts::new(
            self.clone(),
            array.dtype().clone(),
            array.len(),
            data,
            array.slots().iter().cloned().collect(),
        ))
    }

    fn slot_name(array: ArrayView<'_, Self>, idx: usize) -> String {
        if idx >= array.data().inner_child_count(array.slots().len()) {
            "validity".to_string()
        } else {
            format!("child_{idx}")
        }
    }

    fn serialize(
        array: ArrayView<'_, Self>,
        _session: &VortexSession,
    ) -> VortexResult<Option<Vec<u8>>> {
        let children: Vec<&ArrayRef> = array
            .slots()
            .iter()
            .take(array.data().inner_child_count(array.slots().len()))
            .flatten()
            .collect();
        let child_dtypes = children
            .iter()
            .map(|child| child.dtype().try_into())
            .collect::<VortexResult<Vec<_>>>()?;
        let child_lens = children.iter().map(|child| child.len() as u64).collect();

        Ok(Some(
            ZstdBuffersMetadata {
                inner_encoding_id: array.inner_encoding_id.to_string(),
                inner_metadata: array.inner_metadata.clone(),
                uncompressed_sizes: array.uncompressed_sizes.clone(),
                buffer_alignments: array.buffer_alignments.clone(),
                child_dtypes,
                child_lens,
                validity: Some(array.validity as i32),
            }
            .encode_to_vec(),
        ))
    }

    fn deserialize(
        &self,
        dtype: &DType,
        len: usize,
        metadata: &[u8],
        buffers: &[BufferHandle],
        children: &dyn ArrayChildren,
        session: &VortexSession,
    ) -> VortexResult<ArrayParts<Self>> {
        let metadata = ZstdBuffersMetadata::decode(metadata)?;
        let compressed_buffers: Vec<BufferHandle> = buffers.to_vec();

        let stored_validity = metadata
            .validity
            .map(ZstdBuffersValidity::try_from)
            .transpose()
            .map_err(|_| vortex_err!("unknown ZstdBuffers validity {:?}", metadata.validity))?;
        let n_inner_children = metadata.child_dtypes.len();
        let has_validity_child = stored_validity == Some(ZstdBuffersValidity::Child);

        // Children belong to inner encodings, and serialization doesn't
        // preserve their dtypes and values. Check dtypes are recovered from
        // metadata.
        vortex_ensure_eq!(
            n_inner_children + usize::from(has_validity_child),
            children.len()
        );
        vortex_ensure_eq!(metadata.child_lens.len(), n_inner_children);

        let mut slots: ArraySlots = (0..n_inner_children)
            .map(|i| {
                let child_dtype = DType::from_proto(&metadata.child_dtypes[i], session)?;
                let child_len = usize::try_from(metadata.child_lens[i])?;
                children.get(i, &child_dtype, child_len).map(Some)
            })
            .collect::<VortexResult<Vec<_>>>()?
            .into();
        if has_validity_child {
            slots.push(Some(children.get(
                n_inner_children,
                &Validity::DTYPE,
                len,
            )?));
        }

        let mut data = ZstdBuffersData {
            inner_encoding_id: array_id_from_string(&metadata.inner_encoding_id),
            inner_metadata: metadata.inner_metadata.clone(),
            compressed_buffers,
            uncompressed_sizes: metadata.uncompressed_sizes.clone(),
            buffer_alignments: metadata.buffer_alignments.clone(),
            validity: stored_validity.unwrap_or(ZstdBuffersValidity::AllValid),
        };
        data.validate()?;

        // Arrays written before validity was stored: read it from the inner array once, here,
        // where a session is available to rebuild it.
        if stored_validity.is_none() && dtype.is_nullable() {
            let parts = ArrayParts::new(self.clone(), dtype.clone(), len, data.clone(), slots);
            let array = Array::try_from_parts(parts)?;
            let inner_validity =
                ZstdBuffers::decompress_and_build_inner(&array, session)?.validity()?;
            let mut slots: ArraySlots = array.slots().iter().cloned().collect();
            data.validity = match inner_validity {
                Validity::NonNullable | Validity::AllValid => ZstdBuffersValidity::AllValid,
                Validity::AllInvalid => ZstdBuffersValidity::AllInvalid,
                Validity::Array(validity) => {
                    slots.push(Some(validity));
                    ZstdBuffersValidity::Child
                }
            };
            return Ok(ArrayParts::new(
                self.clone(),
                dtype.clone(),
                len,
                data,
                slots,
            ));
        }

        Ok(ArrayParts::new(
            self.clone(),
            dtype.clone(),
            len,
            data,
            slots,
        ))
    }

    // with_slots handles child replacement via the slots mechanism

    fn execute(array: Array<Self>, ctx: &mut ExecutionCtx) -> VortexResult<ExecutionResult> {
        let session = ctx.session();
        let inner_array = ZstdBuffers::decompress_and_build_inner(&array, session)?;
        inner_array
            .execute::<ArrayRef>(ctx)
            .map(ExecutionResult::done)
    }
}

impl OperationsVTable<ZstdBuffers> for ZstdBuffers {
    type ProbeState = ();

    fn scalar_at(
        array: ArrayView<'_, ZstdBuffers>,
        index: usize,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Scalar> {
        // TODO(os): maybe we should not support scalar_at, it is really slow, and adding a cache
        // layer here is weird. Valid use of zstd buffers array would be by executing it first into
        // canonical
        let inner_array =
            ZstdBuffers::decompress_and_build_inner(&array.into_owned(), ctx.session())?;
        inner_array.execute_scalar(index, ctx)
    }
}

impl ValidityVTable<ZstdBuffers> for ZstdBuffers {
    fn validity(array: ArrayView<'_, ZstdBuffers>) -> VortexResult<Validity> {
        Ok(match array.data().validity {
            ZstdBuffersValidity::AllValid => Validity::AllValid,
            ZstdBuffersValidity::AllInvalid => Validity::AllInvalid,
            ZstdBuffersValidity::Child => Validity::Array(
                array
                    .slots()
                    .last()
                    .and_then(Option::clone)
                    .ok_or_else(|| vortex_err!("ZstdBuffers validity child is missing"))?,
            ),
        })
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use vortex_array::ArrayContext;
    use vortex_array::ArrayRef;
    use vortex_array::IntoArray;
    use vortex_array::VortexSessionExecute;
    use vortex_array::arrays::PrimitiveArray;
    use vortex_array::arrays::VarBinViewArray;
    use vortex_array::assert_arrays_eq;
    use vortex_array::expr::stats::Precision;
    use vortex_array::expr::stats::Stat;
    use vortex_array::expr::stats::StatsProvider;
    use vortex_array::serde::SerializeOptions;
    use vortex_array::serde::SerializedArray;
    use vortex_array::session::ArraySessionExt;
    use vortex_buffer::ByteBufferMut;
    use vortex_error::VortexExpect;
    use vortex_error::VortexResult;
    use vortex_session::registry::ReadContext;

    use super::*;

    fn make_primitive_array() -> ArrayRef {
        PrimitiveArray::from_iter(0i32..100).into_array()
    }

    fn make_varbinview_array() -> ArrayRef {
        VarBinViewArray::from_iter_str(["hello", "world", "foo", "bar", "a longer string here"])
            .into_array()
    }

    fn make_nullable_primitive_array() -> ArrayRef {
        PrimitiveArray::from_option_iter([Some(1i32), None, Some(3), None, Some(5)]).into_array()
    }

    fn make_nullable_varbinview_array() -> ArrayRef {
        VarBinViewArray::from_iter_nullable_str([
            Some("hello"),
            None,
            Some("world"),
            None,
            Some("a moderately long string for testing"),
        ])
        .into_array()
    }

    fn make_empty_primitive_array() -> ArrayRef {
        PrimitiveArray::from_iter(Vec::<i32>::new()).into_array()
    }

    fn make_inlined_varbinview_array() -> ArrayRef {
        VarBinViewArray::from_iter_str(["hi", "ok", "yes", "no"]).into_array()
    }

    #[rstest]
    #[case::primitive(make_primitive_array())]
    #[case::varbinview(make_varbinview_array())]
    #[case::nullable_primitive(make_nullable_primitive_array())]
    #[case::nullable_varbinview(make_nullable_varbinview_array())]
    #[case::empty_primitive(make_empty_primitive_array())]
    #[case::inlined_varbinview(make_inlined_varbinview_array())]
    fn test_roundtrip(#[case] input: ArrayRef) -> VortexResult<()> {
        let compressed = ZstdBuffers::compress(&input, 3, &vortex_aggregates::session())?;

        assert_eq!(compressed.len(), input.len());
        assert_eq!(compressed.dtype(), input.dtype());

        let mut ctx = vortex_aggregates::session().create_execution_ctx();
        let decompressed = compressed.into_array().execute::<ArrayRef>(&mut ctx)?;

        assert_arrays_eq!(input, decompressed, &mut ctx);
        Ok(())
    }

    #[rstest]
    #[case::primitive(make_primitive_array())]
    #[case::varbinview(make_varbinview_array())]
    #[case::nullable_primitive(make_nullable_primitive_array())]
    #[case::nullable_varbinview(make_nullable_varbinview_array())]
    #[case::empty_primitive(make_empty_primitive_array())]
    #[case::inlined_varbinview(make_inlined_varbinview_array())]
    fn test_serde_roundtrip(#[case] input: ArrayRef) -> VortexResult<()> {
        let session = vortex_aggregates::session();
        session.arrays().register(ZstdBuffers);

        let compressed = ZstdBuffers::compress(&input, 3, &session)?.into_array();
        let dtype = compressed.dtype().clone();
        let len = compressed.len();

        let array_ctx = ArrayContext::empty();
        let serialized =
            compressed.serialize(&array_ctx, &session, &SerializeOptions::default())?;

        let mut concat = ByteBufferMut::empty();
        for buf in serialized {
            concat.extend_from_slice(buf.as_ref());
        }
        let parts = SerializedArray::try_from(concat.freeze())?;
        let decoded = parts.decode(&dtype, len, &ReadContext::new(array_ctx.to_ids()), &session)?;

        let mut ctx = session.create_execution_ctx();
        let decoded = decoded.execute::<ArrayRef>(&mut ctx)?;
        assert_arrays_eq!(input, decoded, &mut ctx);
        Ok(())
    }

    #[test]
    fn test_compress_inherits_stats() -> VortexResult<()> {
        let input = make_primitive_array();
        input.statistics().set(Stat::Min, Precision::exact(0i32));

        let compressed = ZstdBuffers::compress(&input, 3, &vortex_aggregates::session())?;

        assert!(!compressed.statistics().get(Stat::Min).is_absent());
        Ok(())
    }

    #[test]
    fn test_rejects_mismatched_frame_content_size_before_output_allocation() -> VortexResult<()> {
        let compressed =
            ZstdBuffers::compress(&make_primitive_array(), 3, &vortex_aggregates::session())?;
        let mut data = compressed.data().clone();
        data.uncompressed_sizes[0] = 16 * 1024 * 1024 * 1024;

        for error in [
            data.decompress_buffers().unwrap_err(),
            data.decode_plan().unwrap_err(),
        ] {
            assert!(
                error.to_string().contains("metadata declares"),
                "unexpected error: {error}"
            );
        }
        Ok(())
    }

    #[test]
    fn test_validity_delegates_for_nullable_input() -> VortexResult<()> {
        let input = make_nullable_primitive_array();
        let compressed =
            ZstdBuffers::compress(&input, 3, &vortex_aggregates::session())?.into_array();

        let mut ctx = vortex_aggregates::session().create_execution_ctx();
        assert_eq!(compressed.all_valid(&mut ctx)?, input.all_valid(&mut ctx)?);
        assert_eq!(
            compressed.all_invalid(&mut ctx)?,
            input.all_invalid(&mut ctx)?
        );

        for i in 0..input.len() {
            assert_eq!(
                compressed.is_valid(i, &mut ctx)?,
                input.is_valid(i, &mut ctx)?
            );
        }

        Ok(())
    }

    /// Arrays written before validity was stored have no validity in their metadata and no
    /// validity child, so deserialization reads it from the inner array.
    #[test]
    fn test_deserialize_without_stored_validity() -> VortexResult<()> {
        let session = vortex_aggregates::session();
        let input = make_nullable_primitive_array();
        let compressed = ZstdBuffers::compress(&input, 3, &session)?;
        assert_eq!(compressed.data().validity, ZstdBuffersValidity::Child);

        let metadata = <ZstdBuffers as VTable>::serialize(compressed.as_view(), &session)?
            .vortex_expect("ZstdBuffers always serializes");
        let mut metadata = ZstdBuffersMetadata::decode(metadata.as_slice())?;
        metadata.validity = None;
        let n_inner = compressed
            .data()
            .inner_child_count(compressed.slots().len());
        let inner_children: Vec<ArrayRef> = compressed.slots()[..n_inner]
            .iter()
            .flatten()
            .cloned()
            .collect();

        let parts = ZstdBuffers.deserialize(
            compressed.dtype(),
            compressed.len(),
            &metadata.encode_to_vec(),
            &compressed.data().compressed_buffers,
            &inner_children.as_slice(),
            &session,
        )?;
        let legacy = Array::try_from_parts(parts)?;
        assert_eq!(legacy.data().validity, ZstdBuffersValidity::Child);

        let legacy = legacy.into_array();
        let mut ctx = session.create_execution_ctx();
        for i in 0..input.len() {
            assert_eq!(legacy.is_valid(i, &mut ctx)?, input.is_valid(i, &mut ctx)?);
        }
        Ok(())
    }
}
