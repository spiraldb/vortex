use ::serde::{Deserialize, Serialize};
pub use compress::*;
use vortex::array::primitive::{Primitive, PrimitiveArray};
use vortex::stats::ArrayStatisticsCompute;
use vortex::validity::{ArrayValidity, LogicalValidity, Validity, ValidityMetadata};
use vortex::visitor::{AcceptArrayVisitor, ArrayVisitor};
use vortex::{impl_encoding, ArrayDType, Canonical, IntoCanonical};
use vortex_dtype::{Nullability, PType};
use vortex_error::{vortex_bail, vortex_err};

mod compress;
mod compute;

impl_encoding!("fastlanes.bitpacked", BitPacked);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BitPackedMetadata {
    // TODO(ngates): serialize into compact form
    validity: ValidityMetadata,
    patches: bool,
    bit_width: usize,
    offset: usize, // Know to be <1024
    length: usize, // Store end padding instead <1024
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
        if bit_width > 64 {
            vortex_bail!("Unsupported bit width {}", bit_width);
        }

        let ptype: PType = (&dtype).try_into()?;
        let expected_packed_size =
            ((length + 1023) / 1024) * (128 * bit_width / ptype.byte_width());
        if packed.len() != expected_packed_size {
            return Err(vortex_err!(
                "Expected {} packed bytes, got {}",
                expected_packed_size,
                packed.len()
            ));
        }

        let metadata = BitPackedMetadata {
            validity: validity.to_metadata(length)?,
            patches: patches.is_some(),
            offset,
            length,
            bit_width,
        };

        let mut children = Vec::with_capacity(3);
        children.push(packed);
        if let Some(p) = patches {
            children.push(p);
        }
        if let Some(a) = validity.into_array() {
            children.push(a)
        }

        Self::try_from_parts(dtype, metadata, children.into(), StatsSet::new())
    }

    #[inline]
    pub fn packed(&self) -> Array {
        self.array()
            .child(0, &self.dtype().with_nullability(Nullability::NonNullable))
            .expect("Missing packed array")
    }

    #[inline]
    pub fn bit_width(&self) -> usize {
        self.metadata().bit_width
    }

    #[inline]
    pub fn patches(&self) -> Option<Array> {
        self.metadata().patches.then(|| {
            self.array()
                .child(1, &self.dtype().with_nullability(Nullability::Nullable))
                .expect("Missing patches array")
        })
    }

    #[inline]
    pub fn offset(&self) -> usize {
        self.metadata().offset
    }

    pub fn validity(&self) -> Validity {
        self.metadata().validity.to_validity(self.array().child(
            if self.metadata().patches { 2 } else { 1 },
            &Validity::DTYPE,
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
        if self.metadata().patches {
            visitor.visit_child(
                "patches",
                &self.patches().expect("Expected patches to be present "),
            )?;
        }
        visitor.visit_validity(&self.validity())
    }
}

impl ArrayStatisticsCompute for BitPackedArray {}

impl ArrayTrait for BitPackedArray {
    fn len(&self) -> usize {
        self.metadata().length
    }

    fn nbytes(&self) -> usize {
        // Ignore any overheads like padding or the bit-width flag.
        let packed_size = ((self.bit_width() * self.len()) + 7) / 8;
        packed_size + self.patches().map(|p| p.nbytes()).unwrap_or(0)
    }
}

#[cfg(test)]
mod test {
    use vortex::array::primitive::PrimitiveArray;
    use vortex::compute::slice::slice;
    use vortex::compute::unary::scalar_at::scalar_at;
    use vortex::{IntoArray, IntoCanonical};

    use crate::BitPackedArray;

    #[test]
    fn slice_within_block() {
        let packed = BitPackedArray::encode(
            PrimitiveArray::from((0..10_000).map(|i| (i % 63) as u8).collect::<Vec<_>>()).array(),
            7,
        )
        .unwrap();

        let compressed = slice(packed.array(), 768, 9999).unwrap();
        assert_eq!(
            scalar_at(&compressed, 0).unwrap(),
            ((768 % 63) as u8).into()
        );
        assert_eq!(
            scalar_at(&compressed, compressed.len() - 1).unwrap(),
            ((9998 % 63) as u8).into()
        );
    }

    #[test]
    fn slice_block_boundary() {
        let packed = BitPackedArray::encode(
            PrimitiveArray::from((0..10_000).map(|i| (i % 63) as u8).collect::<Vec<_>>()).array(),
            7,
        )
        .unwrap();

        let compressed = slice(packed.array(), 7168, 9216).unwrap();
        assert_eq!(
            scalar_at(&compressed, 0).unwrap(),
            ((7168 % 63) as u8).into()
        );
        assert_eq!(
            scalar_at(&compressed, compressed.len() - 1).unwrap(),
            ((9215 % 63) as u8).into()
        );
    }

    #[test]
    fn test_encode() {
        let values = vec![Some(1), None, Some(1), None, Some(1), None, Some(u64::MAX)];
        let uncompressed = PrimitiveArray::from_nullable_vec(values);
        let packed = BitPackedArray::encode(uncompressed.array(), 1).unwrap();
        let expected = &[1, 0, 1, 0, 1, 0, u64::MAX];
        let results = packed
            .into_array()
            .into_canonical()
            .unwrap()
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
