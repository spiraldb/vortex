use ::serde::{Deserialize, Serialize};
pub use compress::*;
use fastlanes::BitPacking;
use vortex::array::PrimitiveArray;
use vortex::encoding::ids;
use vortex::stats::{ArrayStatisticsCompute, StatsSet};
use vortex::validity::{ArrayValidity, LogicalValidity, Validity, ValidityMetadata};
use vortex::variants::{ArrayVariants, PrimitiveArrayTrait};
use vortex::visitor::{AcceptArrayVisitor, ArrayVisitor};
use vortex::{
    impl_encoding, Array, ArrayDType, ArrayDef, ArrayTrait, Canonical, IntoCanonical, TypedArray,
};
use vortex_buffer::Buffer;
use vortex_dtype::{DType, NativePType, Nullability, PType};
use vortex_error::{
    vortex_bail, vortex_err, vortex_panic, VortexError, VortexExpect as _, VortexResult,
};

mod compress;
mod compute;

impl_encoding!("fastlanes.bitpacked", ids::FL_BITPACKED, BitPacked);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BitPackedMetadata {
    // TODO(ngates): serialize into compact form
    validity: ValidityMetadata,
    bit_width: usize,
    offset: usize, // Know to be <1024
    length: usize, // Store end padding instead <1024
    patches_len: usize,
    patches_indices_offset: usize,
}

/// NB: All non-null values in the patches array are considered patches
impl BitPackedArray {
    /// Create a new bitpacked array using a buffer of packed data.
    ///
    /// The packed data should be interpreted as a sequence of values with size `bit_width`.
    pub fn try_new(
        packed: Buffer,
        ptype: PType,
        validity: Validity,
        patches: Option<(Array, Array, usize)>,
        bit_width: usize,
        len: usize,
    ) -> VortexResult<Self> {
        Self::try_new_from_offset(packed, ptype, validity, patches, bit_width, len, 0)
    }

    pub(crate) fn try_new_from_offset(
        packed: Buffer,
        ptype: PType,
        validity: Validity,
        patches: Option<(Array, Array, usize)>,
        bit_width: usize,
        length: usize,
        offset: usize,
    ) -> VortexResult<Self> {
        let dtype = DType::Primitive(ptype, validity.nullability());

        if !dtype.is_unsigned_int() {
            vortex_bail!(MismatchedTypes: "uint", &dtype);
        }

        if bit_width > u64::BITS as usize {
            vortex_bail!("Unsupported bit width {}", bit_width);
        }
        if offset > 1023 {
            vortex_bail!(
                "Offset must be less than full block, i.e. 1024, got {}",
                offset
            );
        }

        // expected packed size is in bytes
        let expected_packed_size = ((length + offset + 1023) / 1024) * (128 * bit_width);
        if packed.len() != expected_packed_size {
            return Err(vortex_err!(
                "Expected {} packed bytes, got {}",
                expected_packed_size,
                packed.len()
            ));
        }

        if let Some((indices, values, _)) = patches.as_ref() {
            if !matches!(indices.dtype(), &DType::IDX) {
                vortex_bail!("Cannot use {} as indices", indices.dtype());
            }
            if values.dtype() != &dtype.with_nullability(Nullability::NonNullable) {
                vortex_bail!(
                    "patches values dtype, {}, must be non-nullable version of values dtype, {}",
                    values.dtype(),
                    dtype
                );
            }
        }

        // if let Some((indices, values)) = patches.as_ref() {
        //     // if parray.len() != length {
        //     //     vortex_bail!(
        //     //         "Mismatched length in BitPackedArray between encoded {} and it's patches({}) {}",
        //     //         length,
        //     //         parray.encoding().id(),
        //     //         parray.len()
        //     //     )
        //     // }

        //     // if SparseArray::try_from(parray)?.indices().is_empty() {
        //     //     vortex_bail!("cannot construct BitPackedArray using patches without indices");
        //     // }
        // }

        let metadata = BitPackedMetadata {
            validity: validity.to_metadata(length)?,
            offset,
            length,
            bit_width,
            patches_len: patches
                .clone()
                .map(|(x, y, _)| -> VortexResult<_> {
                    if x.len() != y.len() {
                        vortex_bail!(
                            "expected patches arrays to have same length {} {}",
                            x.len(),
                            y.len()
                        );
                    }
                    Ok(x.len())
                })
                .transpose()?
                .unwrap_or(0),
            patches_indices_offset: patches
                .clone()
                .map(|(_, _, indices_offset)| indices_offset)
                .unwrap_or(0),
        };

        let mut children = Vec::with_capacity(2);
        if let Some((indices, values, _)) = patches {
            children.push(indices);
            children.push(values);
        }
        if let Some(a) = validity.into_array() {
            children.push(a)
        }

        Ok(Self {
            typed: TypedArray::try_from_parts(
                dtype,
                length,
                metadata,
                Some(packed),
                children.into(),
                StatsSet::new(),
            )?,
        })
    }

    #[inline]
    pub fn packed(&self) -> &Buffer {
        self.as_ref()
            .buffer()
            .vortex_expect("BitPackedArray must contain packed buffer")
    }

    /// The number of bit-packed values.
    ///
    /// The [`Self::packed_len()`] plus the number of patches is equal to [`Self::len()`].
    #[inline]
    pub fn packed_len(&self) -> usize {
        self.len() - self.patches_len()
    }

    /// The number of non-bit-packed (patch) values.
    ///
    /// These values are stored as two arrays: a dense array of values and their logical indices in
    /// this array.
    #[inline]
    pub fn patches_len(&self) -> usize {
        self.metadata().patches_len
    }

    /// Access the slice of packed values as an array of `T`
    #[inline]
    pub fn packed_slice<T: NativePType + BitPacking>(&self) -> &[T] {
        let packed_bytes = self.packed();
        let packed_ptr: *const T = packed_bytes.as_ptr().cast();
        // Return number of elements of type `T` packed in the buffer
        let packed_len = packed_bytes.len() / size_of::<T>();

        // SAFETY: maybe_null_slice points to buffer memory that outlives the lifetime of `self`.
        //  Unfortunately Rust cannot understand this, so we reconstruct the slice from raw parts
        //  to get it to reinterpret the lifetime.
        unsafe { std::slice::from_raw_parts(packed_ptr, packed_len) }
    }

    #[inline]
    pub fn bit_width(&self) -> usize {
        self.metadata().bit_width
    }

    #[inline]
    pub fn has_patches(&self) -> bool {
        self.metadata().patches_len > 0
    }

    /// Access the patches array.
    // ///
    // /// If present, patches MUST be a `SparseArray` with equal-length to this array, and whose
    // /// indices indicate the locations of patches. The indices must have non-zero length.
    #[inline]
    pub fn _patches(&self) -> Option<(Array, Array, usize)> {
        self.has_patches().then(|| {
            (
                self.as_ref()
                    .child(
                        0,
                        &DType::IDX, // .with_nullability(Nullability::Nullable) // FIXME(DK): why was this here?
                        self.patches_len(),
                    )
                    .vortex_expect("BitPackedArray: patches child 0"),
                self.as_ref()
                    .child(
                        1,
                        &self.dtype().with_nullability(Nullability::NonNullable),
                        self.patches_len(),
                    )
                    .vortex_expect("BitPackedArray: patches child 1"),
                self.metadata().patches_indices_offset,
            )
        })
    }

    #[inline]
    pub fn offset(&self) -> usize {
        self.metadata().offset
    }

    pub fn validity(&self) -> Validity {
        let validity_child_idx = if self.has_patches() { 2 } else { 0 };

        self.metadata().validity.to_validity(|| {
            self.as_ref()
                .child(validity_child_idx, &Validity::DTYPE, self.len())
                .vortex_expect("BitPackedArray: validity child")
        })
    }

    pub fn encode(array: &Array, bit_width: usize) -> VortexResult<Self> {
        if let Ok(parray) = PrimitiveArray::try_from(array) {
            bitpack_encode(parray, bit_width)
        } else {
            vortex_bail!("Bitpacking can only encode primitive arrays");
        }
    }

    #[inline]
    pub fn ptype(&self) -> PType {
        self.dtype().try_into().unwrap_or_else(|err: VortexError| {
            vortex_panic!(
                err,
                "Failed to convert BitpackedArray DType {} to PType",
                self.dtype()
            )
        })
    }

    #[inline]
    pub fn max_packed_value(&self) -> usize {
        (1 << self.bit_width()) - 1
    }
}

impl IntoCanonical for BitPackedArray {
    fn into_canonical(self) -> VortexResult<Canonical> {
        unpack(self).map(Canonical::Primitive)
    }
}

impl ArrayValidity for BitPackedArray {
    fn is_valid(&self, index: usize) -> bool {
        self.validity().is_valid(index)
    }

    fn logical_validity(&self) -> LogicalValidity {
        self.validity().to_logical(self.len())
    }
}

impl AcceptArrayVisitor for BitPackedArray {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        visitor.visit_buffer(self.packed())?;
        if let Some((indices, values, _)) = self._patches().as_ref() {
            // visitor.visit_child("patches", patches)?;
            visitor.visit_child("indices", indices)?;
            visitor.visit_child("values", values)?;
        }
        visitor.visit_validity(&self.validity())
    }
}

impl ArrayStatisticsCompute for BitPackedArray {}

impl ArrayTrait for BitPackedArray {
    fn nbytes(&self) -> usize {
        // Ignore any overheads like padding or the bit-width flag.
        let packed_size = ((self.bit_width() * self.len()) + 7) / 8;
        packed_size
            + self
                ._patches()
                .map(|(indices, values, _)| indices.nbytes() + values.nbytes())
                .unwrap_or(0)
    }
}

impl ArrayVariants for BitPackedArray {
    fn as_primitive_array(&self) -> Option<&dyn PrimitiveArrayTrait> {
        Some(self)
    }
}

impl PrimitiveArrayTrait for BitPackedArray {}

#[cfg(test)]
mod test {
    use vortex::array::PrimitiveArray;
    use vortex::{IntoArray, IntoArrayVariant};

    use crate::BitPackedArray;

    #[test]
    fn test_encode() {
        let values = vec![Some(1), None, Some(1), None, Some(1), None, Some(u64::MAX)];
        let uncompressed = PrimitiveArray::from_nullable_vec(values);
        let packed = BitPackedArray::encode(uncompressed.as_ref(), 1).unwrap();
        let expected = &[1, 0, 1, 0, 1, 0, u64::MAX];
        let results = packed
            .into_array()
            .into_primitive()
            .unwrap()
            .maybe_null_slice::<u64>()
            .to_vec();
        assert_eq!(results, expected);
    }

    #[test]
    fn test_encode_too_wide() {
        let values = vec![Some(1u8), None, Some(1), None, Some(1), None];
        let uncompressed = PrimitiveArray::from_nullable_vec(values);
        let _packed = BitPackedArray::encode(uncompressed.as_ref(), 8)
            .expect_err("Cannot pack value into the same width");
        let _packed = BitPackedArray::encode(uncompressed.as_ref(), 9)
            .expect_err("Cannot pack value into larger width");
    }
}
