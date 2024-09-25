use std::fmt::Debug;

pub use compress::*;
use serde::{Deserialize, Serialize};
use vortex::array::PrimitiveArray;
use vortex::encoding::ids;
use vortex::stats::{ArrayStatisticsCompute, StatsSet};
use vortex::validity::{ArrayValidity, LogicalValidity, Validity, ValidityMetadata};
use vortex::variants::{ArrayVariants, PrimitiveArrayTrait};
use vortex::visitor::{AcceptArrayVisitor, ArrayVisitor};
use vortex::{
    impl_encoding, Array, ArrayDType, ArrayDef, ArrayTrait, Canonical, IntoArray, IntoCanonical,
};
use vortex_dtype::{match_each_unsigned_integer_ptype, NativePType, PType};
use vortex_error::{vortex_bail, vortex_panic, VortexExpect as _, VortexResult};

mod compress;
mod compute;

impl_encoding!("fastlanes.delta", ids::FL_DELTA, Delta);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaMetadata {
    validity: ValidityMetadata,
    deltas_len: usize,
    logical_len: usize,
    offset: usize,           // must be <1024
    trailing_garbage: usize, // must be <1024
}

/// A FastLanes-style delta-encoded array of primitive values.
///
/// A [`DeltaArray`] comprises a sequence of _chunks_ each representing 1,024 delta-encoded values,
/// except the last chunk which may represent from one to 1,024 values.
///
/// # Examples
///
/// ```
/// use vortex_fastlanes::DeltaArray;
/// let array = DeltaArray::try_from_vec(vec![1_u32, 2, 3, 5, 10, 11]).unwrap();
/// ```
///
/// # Details
///
/// To facilitate slicing, this array has an `offset` and `limit`. Both values must be strictly less
/// than 1,024. The `offset` is physical offset into the first chunk of deltas. The `limit` is a
/// physical limit of the last chunk. These values permit logical slicing while preserving all
/// values in any chunk containing a kept value. Logical slicing permits preservation of values
/// necessary to decompress the delta-encoding, which is described in detail below. While later
/// values in a chunk are not necsesary to decode earlier ones, a logical limit preserves full
/// chunks which permits the decompression function go assume all chunks are exactly 1,024 values.
///
/// A `limit` of `None` is a convenient alternative to computing the length of the last
/// block. Internally, this array does not store the `limit`; instead it stores the number of
/// trailing logically-excluded values: `trailing_garbage`.
///
/// Each chunk is stored as a vector of bases and a vector of deltas. There are as many bases as there
/// are _lanes_ of this type in a 1024-bit register. For example, for 64-bit values, there are 16
/// bases because there are 16 _lanes_. Each lane is a
/// [delta-encoding](https://en.wikipedia.org/wiki/Delta_encoding) `1024 / bit_width` long vector of
/// avlues. The deltas are stored in the
/// [FastLanes](https://www.vldb.org/pvldb/vol16/p2132-afroozeh.pdf) order which splits the 1,024
/// values into one contiguous sub-sequence per-lane, thus permitting delta encoding.
impl DeltaArray {
    pub fn try_from_vec<T: NativePType>(vec: Vec<T>) -> VortexResult<Self> {
        Self::try_from_primitive_array(&PrimitiveArray::from(vec))
    }

    pub fn try_from_primitive_array(array: &PrimitiveArray) -> VortexResult<Self> {
        let (bases, deltas) = delta_compress(array)?;

        Self::try_from_delta_compress_parts(
            bases.into_array(),
            deltas.into_array(),
            Validity::NonNullable,
        )
    }

    pub fn try_from_delta_compress_parts(
        bases: Array,
        deltas: Array,
        validity: Validity,
    ) -> VortexResult<Self> {
        Self::try_new(bases, deltas, validity, 0, None)
    }

    pub fn try_new(
        bases: Array,
        deltas: Array,
        validity: Validity,
        offset: usize,
        limit: Option<usize>,
    ) -> VortexResult<Self> {
        if offset >= 1024 {
            vortex_bail!("offset must be less than 1024: {}", offset);
        }

        if let Some(l) = limit {
            if l >= 1024 {
                vortex_bail!("limit must be less than 1024: {}", l);
            }
        }

        if bases.dtype() != deltas.dtype() {
            vortex_bail!(
                "DeltaArray: bases and deltas must have the same dtype, got {:?} and {:?}",
                bases.dtype(),
                deltas.dtype()
            );
        }

        let dtype = bases.dtype().clone();
        let trailing_garbage = match (limit, deltas.len() % 1024) {
            (None, _) => 0,
            (Some(l), 0) => 1024 - l,
            (Some(l), remainder) => remainder - l,
        };
        let logical_len = deltas.len() - offset - trailing_garbage;
        let metadata = DeltaMetadata {
            validity: validity.to_metadata(logical_len)?,
            deltas_len: deltas.len(),
            logical_len,
            offset,
            trailing_garbage,
        };

        let mut children = vec![bases, deltas];
        if let Some(varray) = validity.into_array() {
            children.push(varray)
        }

        let delta = Self::try_from_parts(
            dtype,
            logical_len,
            metadata,
            children.into(),
            StatsSet::new(),
        )?;

        if delta.bases().len() != delta.bases_len() {
            vortex_bail!(
                "DeltaArray: bases.len() ({}) != expected_bases_len ({}), based on len ({}) and lane count ({})",
                delta.bases().len(),
                delta.bases_len(),
                logical_len,
                delta.lanes()
            );
        }

        if (delta.deltas_len() % 1024 == 0) != (delta.bases_len() % delta.lanes() == 0) {
            vortex_bail!(
                "deltas length ({}) is a multiple of 1024 iff bases length ({}) is a multiple of LANES ({})",
                delta.deltas_len(),
                delta.bases_len(),
                delta.lanes(),
            );
        }

        Ok(delta)
    }

    #[inline]
    pub fn bases(&self) -> Array {
        self.as_ref()
            .child(0, self.dtype(), self.bases_len())
            .vortex_expect("DeltaArray is missing bases child array")
    }

    #[inline]
    pub fn deltas(&self) -> Array {
        self.as_ref()
            .child(1, self.dtype(), self.metadata().deltas_len)
            .vortex_expect("DeltaArray is missing deltas child array")
    }

    #[inline]
    fn lanes(&self) -> usize {
        let ptype: PType = self.dtype().try_into().unwrap_or_else(|err| {
            vortex_panic!(
                err,
                "Failed to convert DeltaArray DType {} to PType",
                self.dtype()
            )
        });
        match_each_unsigned_integer_ptype!(ptype.to_unsigned(), |$T| {
            <$T as fastlanes::FastLanes>::LANES
        })
    }

    #[inline]
    /// The logical offset into the first chunk of [`Self::deltas`].
    pub fn offset(&self) -> usize {
        self.metadata().offset
    }

    #[inline]
    /// The logical "right-offset" of the last chunk of [`Self::deltas`].
    pub fn trailing_garbage(&self) -> usize {
        self.metadata().trailing_garbage
    }

    pub fn validity(&self) -> Validity {
        self.metadata().validity.to_validity(|| {
            self.as_ref()
                .child(2, &Validity::DTYPE, self.len())
                .vortex_expect("DeltaArray: validity child")
        })
    }

    fn bases_len(&self) -> usize {
        let num_chunks = self.deltas().len() / 1024;
        let remainder_base_size = if self.deltas().len() % 1024 > 0 { 1 } else { 0 };
        num_chunks * self.lanes() + remainder_base_size
    }

    fn deltas_len(&self) -> usize {
        self.metadata().deltas_len
    }
}

impl ArrayTrait for DeltaArray {}

impl ArrayVariants for DeltaArray {
    fn as_primitive_array(&self) -> Option<&dyn PrimitiveArrayTrait> {
        Some(self)
    }
}

impl PrimitiveArrayTrait for DeltaArray {}

impl IntoCanonical for DeltaArray {
    fn into_canonical(self) -> VortexResult<Canonical> {
        delta_decompress(self).map(Canonical::Primitive)
    }
}

impl ArrayValidity for DeltaArray {
    fn is_valid(&self, index: usize) -> bool {
        self.validity().is_valid(index)
    }

    fn logical_validity(&self) -> LogicalValidity {
        self.validity().to_logical(self.len())
    }
}

impl AcceptArrayVisitor for DeltaArray {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        visitor.visit_child("bases", &self.bases())?;
        visitor.visit_child("deltas", &self.deltas())
    }
}

impl ArrayStatisticsCompute for DeltaArray {}
