use ::serde::{Deserialize, Serialize};
pub use compress::*;
use vortex::array::{Primitive, PrimitiveArray};
use vortex::stats::{ArrayStatisticsCompute, StatsSet};
use vortex::validity::{ArrayValidity, LogicalValidity, Validity, ValidityMetadata};
use vortex::variants::{ArrayVariants, PrimitiveArrayTrait};
use vortex::visitor::{AcceptArrayVisitor, ArrayVisitor};
use vortex::{impl_encoding, Array, ArrayDType, ArrayDef, ArrayTrait, Canonical, IntoCanonical};
use vortex_dtype::{Nullability, PType};
use vortex_error::{vortex_bail, vortex_err, VortexResult};

mod compress;
mod compute;

impl_encoding!("fastlanes.bitpacked", 14u16, BitPacked);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BitPackedMetadata {
    // TODO(ngates): serialize into compact form
    validity: ValidityMetadata,
    bit_width: usize,
    offset: usize, // Know to be <1024
    length: usize, // Store end padding instead <1024
    has_patches: bool,
}

/// NB: All non-null values in the patches array are considered patches
impl BitPackedArray {
    pub fn try_new(
        packed: Array,
        validity: Validity,
        patches: Option<Array>,
        bit_width: usize,
        len: usize,
    ) -> VortexResult<Self> {
        Self::try_new_from_offset(packed, validity, patches, bit_width, len, 0)
    }

    pub(crate) fn try_new_from_offset(
        packed: Array,
        validity: Validity,
        patches: Option<Array>,
        bit_width: usize,
        length: usize,
        offset: usize,
    ) -> VortexResult<Self> {
        let dtype = packed.dtype().with_nullability(validity.nullability());
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

        let ptype = PType::try_from(&dtype)?;
        let expected_packed_size =
            ((length + offset + 1023) / 1024) * (128 * bit_width / ptype.byte_width());
        if packed.len() != expected_packed_size {
            return Err(vortex_err!(
                "Expected {} packed bytes, got {}",
                expected_packed_size,
                packed.len()
            ));
        }

        if let Some(parray) = patches.as_ref() {
            if parray.len() != length {
                vortex_bail!(
                    "Mismatched length in BitPackedArray between encoded {} and it's patches({}) {}",
                    length,
                    parray.encoding().id(),
                    parray.len()
                )
            }
        }

        let metadata = BitPackedMetadata {
            validity: validity.to_metadata(length)?,
            offset,
            length,
            bit_width,
            has_patches: patches.is_some(),
        };

        let mut children = Vec::with_capacity(3);
        children.push(packed);
        if let Some(p) = patches {
            children.push(p);
        }
        if let Some(a) = validity.into_array() {
            children.push(a)
        }

        Self::try_from_parts(dtype, length, metadata, children.into(), StatsSet::new())
    }

    fn packed_len(&self) -> usize {
        ((self.len() + self.offset() + 1023) / 1024)
            * (128 * self.bit_width() / self.ptype().byte_width())
    }

    #[inline]
    pub fn packed(&self) -> Array {
        self.array()
            .child(
                0,
                &self.dtype().with_nullability(Nullability::NonNullable),
                self.packed_len(),
            )
            .expect("Missing packed array")
    }

    #[inline]
    pub fn bit_width(&self) -> usize {
        self.metadata().bit_width
    }

    #[inline]
    pub fn patches(&self) -> Option<Array> {
        (self.metadata().has_patches)
            .then(|| {
                self.array().child(
                    1,
                    &self.dtype().with_nullability(Nullability::Nullable),
                    self.len(),
                )
            })
            .flatten()
    }

    #[inline]
    pub fn offset(&self) -> usize {
        self.metadata().offset
    }

    pub fn validity(&self) -> Validity {
        let validity_child_idx = if self.metadata().has_patches { 2 } else { 1 };

        self.metadata().validity.to_validity(self.array().child(
            validity_child_idx,
            &Validity::DTYPE,
            self.len(),
        ))
    }

    pub fn encode(array: &Array, bit_width: usize) -> VortexResult<Self> {
        if array.encoding().id() == Primitive::ID {
            bitpack_encode(PrimitiveArray::try_from(array)?, bit_width)
        } else {
            vortex_bail!("Bitpacking can only encode primitive arrays");
        }
    }

    #[inline]
    pub fn ptype(&self) -> PType {
        self.dtype().try_into().unwrap()
    }

    #[inline]
    pub fn max_packed_value(&self) -> usize {
        1 << self.bit_width() - 1
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
        visitor.visit_child("packed", &self.packed())?;
        if let Some(patches) = self.patches().as_ref() {
            visitor.visit_child("patches", patches)?;
        }
        visitor.visit_validity(&self.validity())
    }
}

impl ArrayStatisticsCompute for BitPackedArray {}

impl ArrayTrait for BitPackedArray {
    fn nbytes(&self) -> usize {
        // Ignore any overheads like padding or the bit-width flag.
        let packed_size = ((self.bit_width() * self.len()) + 7) / 8;
        packed_size + self.patches().map(|p| p.nbytes()).unwrap_or(0)
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
        let packed = BitPackedArray::encode(uncompressed.array(), 1).unwrap();
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
        let _packed = BitPackedArray::encode(uncompressed.array(), 8)
            .expect_err("Cannot pack value into the same width");
        let _packed = BitPackedArray::encode(uncompressed.array(), 9)
            .expect_err("Cannot pack value into larger width");
    }
}
