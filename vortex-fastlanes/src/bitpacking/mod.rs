use std::cmp::max;
use std::sync::{Arc, RwLock};

pub use compress::*;
use vortex::array::{check_slice_bounds, Array, ArrayRef};
use vortex::compress::EncodingCompression;
use vortex::compute::ArrayCompute;
use vortex::encoding::{Encoding, EncodingId, EncodingRef};
use vortex::formatter::{ArrayDisplay, ArrayFormatter};
use vortex::serde::{ArraySerde, EncodingSerde};
use vortex::stats::{Stat, Stats, StatsCompute, StatsSet};
use vortex::validity::Validity;
use vortex::validity::{OwnedValidity, ValidityView};
use vortex::view::AsView;
use vortex::{impl_array, ArrayWalker};
use vortex_error::{vortex_bail, vortex_err, VortexResult};
use vortex_schema::{DType, IntWidth, Nullability, Signedness};

mod compress;
mod compute;
mod serde;

/// NB: All non-null values in the patches array are considered patches
#[derive(Debug, Clone)]
pub struct BitPackedArray {
    encoded: ArrayRef,
    validity: Option<Validity>,
    patches: Option<ArrayRef>,
    offset: usize,
    len: usize,
    bit_width: usize,
    dtype: DType,
    stats: Arc<RwLock<StatsSet>>,
}

impl BitPackedArray {
    const ENCODED_DTYPE: DType =
        DType::Int(IntWidth::_8, Signedness::Unsigned, Nullability::NonNullable);

    pub fn try_new(
        encoded: ArrayRef,
        validity: Option<Validity>,
        patches: Option<ArrayRef>,
        bit_width: usize,
        dtype: DType,
        len: usize,
    ) -> VortexResult<Self> {
        Self::try_new_from_offset(encoded, validity, patches, bit_width, dtype, len, 0)
    }

    pub(crate) fn try_new_from_offset(
        encoded: ArrayRef,
        validity: Option<Validity>,
        patches: Option<ArrayRef>,
        bit_width: usize,
        dtype: DType,
        len: usize,
        offset: usize,
    ) -> VortexResult<Self> {
        if encoded.dtype() != &Self::ENCODED_DTYPE {
            vortex_bail!(MismatchedTypes: Self::ENCODED_DTYPE, encoded.dtype());
        }
        if let Some(v) = &validity {
            assert_eq!(v.len(), len);
        }
        if bit_width > 64 {
            return Err(vortex_err!("Unsupported bit width {}", bit_width));
        }
        if !matches!(dtype, DType::Int(_, _, _)) {
            return Err(vortex_err!(MismatchedTypes: "int", dtype));
        }

        let expected_packed_size = ((len + 1023) / 1024) * 128 * bit_width;
        if encoded.len() != expected_packed_size {
            return Err(vortex_err!(
                "Expected {} packed bytes, got {}",
                expected_packed_size,
                encoded.len()
            ));
        }

        Ok(Self {
            encoded,
            validity,
            patches,
            offset,
            len,
            bit_width,
            dtype,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        })
    }

    #[inline]
    pub fn encoded(&self) -> &ArrayRef {
        &self.encoded
    }

    #[inline]
    pub fn bit_width(&self) -> usize {
        self.bit_width
    }

    #[inline]
    pub fn patches(&self) -> Option<&ArrayRef> {
        self.patches.as_ref()
    }

    #[inline]
    pub fn offset(&self) -> usize {
        self.offset
    }
}

impl Array for BitPackedArray {
    impl_array!();
    #[inline]
    fn with_compute_mut(
        &self,
        f: &mut dyn FnMut(&dyn ArrayCompute) -> VortexResult<()>,
    ) -> VortexResult<()> {
        f(self)
    }

    #[inline]
    fn len(&self) -> usize {
        self.len
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    fn dtype(&self) -> &DType {
        &self.dtype
    }

    #[inline]
    fn stats(&self) -> Stats {
        Stats::new(&self.stats, self)
    }

    fn slice(&self, start: usize, stop: usize) -> VortexResult<ArrayRef> {
        check_slice_bounds(self, start, stop)?;
        let offset = start % 1024;
        let block_start = max(0, start - offset);
        let block_stop = ((stop + 1023) / 1024) * 1024;

        let encoded_start = (block_start / 8) * self.bit_width;
        let encoded_stop = (block_stop / 8) * self.bit_width;
        Self::try_new_from_offset(
            self.encoded().slice(encoded_start, encoded_stop)?,
            self.validity().map(|v| v.slice(start, stop)).transpose()?,
            self.patches().map(|p| p.slice(start, stop)).transpose()?,
            self.bit_width(),
            self.dtype().clone(),
            stop - start,
            offset,
        )
        .map(|a| a.into_array())
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &BitPackedEncoding
    }

    #[inline]
    fn nbytes(&self) -> usize {
        // Ignore any overheads like padding or the bit-width flag.
        let packed_size = ((self.bit_width * self.len()) + 7) / 8;
        packed_size
            + self.patches().map(|p| p.nbytes()).unwrap_or(0)
            + self.validity().map(|v| v.nbytes()).unwrap_or(0)
    }

    fn serde(&self) -> Option<&dyn ArraySerde> {
        Some(self)
    }

    fn walk(&self, walker: &mut dyn ArrayWalker) -> VortexResult<()> {
        walker.visit_child(self.encoded())
    }
}

impl OwnedValidity for BitPackedArray {
    fn validity(&self) -> Option<ValidityView> {
        self.validity.as_view()
    }
}

impl ArrayDisplay for BitPackedArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.property("offset", self.offset)?;
        f.property("packed", format!("u{}", self.bit_width()))?;
        f.child("encoded", self.encoded())?;
        f.maybe_child("patches", self.patches())?;
        f.validity(self.validity())
    }
}

impl StatsCompute for BitPackedArray {
    fn compute(&self, _stat: &Stat) -> VortexResult<StatsSet> {
        Ok(StatsSet::default())
    }
}

#[derive(Debug)]
pub struct BitPackedEncoding;

impl BitPackedEncoding {
    pub const ID: EncodingId = EncodingId::new("fastlanes.bitpacked");
}

impl Encoding for BitPackedEncoding {
    fn id(&self) -> EncodingId {
        Self::ID
    }

    fn compression(&self) -> Option<&dyn EncodingCompression> {
        Some(self)
    }

    fn serde(&self) -> Option<&dyn EncodingSerde> {
        Some(self)
    }
}

#[macro_export]
macro_rules! match_integers_by_width {
    ($self:expr, | $_:tt $enc:ident | $($body:tt)*) => ({
        macro_rules! __with__ {( $_ $enc:ident ) => ( $($body)* )}
        use vortex::ptype::PType;
        use vortex_error::vortex_bail;
        match $self {
            PType::I8 | PType::U8 => __with__! { u8 },
            PType::I16 | PType::U16 => __with__! { u16 },
            PType::I32 | PType::U32 => __with__! { u32 },
            PType::I64 | PType::U64 => __with__! { u64 },
            _ => vortex_bail!("Unsupported ptype {}", $self),
        }
    })
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use vortex::array::primitive::PrimitiveArray;
    use vortex::array::Array;
    use vortex::compress::{CompressConfig, CompressCtx};
    use vortex::compute::scalar_at::scalar_at;
    use vortex::encoding::EncodingRef;

    use crate::BitPackedEncoding;

    #[test]
    fn slice_within_block() {
        let cfg = CompressConfig::new().with_enabled([&BitPackedEncoding as EncodingRef]);
        let ctx = CompressCtx::new(Arc::new(cfg));

        let compressed = ctx
            .compress(
                &PrimitiveArray::from((0..10_000).map(|i| (i % 63) as u8).collect::<Vec<_>>()),
                None,
            )
            .unwrap()
            .slice(768, 9999)
            .unwrap();
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
        let cfg = CompressConfig::new().with_enabled([&BitPackedEncoding as EncodingRef]);
        let ctx = CompressCtx::new(Arc::new(cfg));

        let compressed = ctx
            .compress(
                &PrimitiveArray::from((0..10_000).map(|i| (i % 63) as u8).collect::<Vec<_>>()),
                None,
            )
            .unwrap()
            .slice(7168, 9216)
            .unwrap();
        assert_eq!(
            scalar_at(&compressed, 0).unwrap(),
            ((7168 % 63) as u8).into()
        );
        assert_eq!(
            scalar_at(&compressed, compressed.len() - 1).unwrap(),
            ((9215 % 63) as u8).into()
        );
    }
}
