use std::collections::HashMap;
use std::fmt::{Debug, Display};

use arrow_buffer::{BooleanBuffer, MutableBuffer};
pub use compress::*;
use croaring::Native;
pub use croaring::{Bitmap, Portable};
use serde::{Deserialize, Serialize};
use vortex::array::BoolArray;
use vortex::encoding::ids;
use vortex::stats::{Stat, StatsSet};
use vortex::validity::{ArrayValidity, LogicalValidity, Validity};
use vortex::variants::{ArrayVariants, BoolArrayTrait};
use vortex::visitor::{AcceptArrayVisitor, ArrayVisitor};
use vortex::{impl_encoding, Array, ArrayTrait, Canonical, IntoArray, IntoCanonical, TypedArray};
use vortex_buffer::Buffer;
use vortex_dtype::DType;
use vortex_dtype::Nullability::NonNullable;
use vortex_error::{vortex_bail, vortex_err, VortexExpect as _, VortexResult, VortexUnwrap};

mod compress;
mod compute;
mod stats;

impl_encoding!("vortex.roaring_bool", ids::ROARING_BOOL, RoaringBool);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoaringBoolMetadata;

impl Display for RoaringBoolMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(self, f)
    }
}

impl RoaringBoolArray {
    pub fn try_new(bitmap: Bitmap, length: usize) -> VortexResult<Self> {
        if length < bitmap.cardinality() as usize {
            vortex_bail!("RoaringBoolArray length is less than bitmap cardinality")
        } else {
            let roaring_stats = bitmap.statistics();
            let stats = StatsSet::from(HashMap::from([
                (
                    Stat::Min,
                    (roaring_stats.cardinality == length as u64).into(),
                ),
                (Stat::Max, (roaring_stats.cardinality > 0).into()),
                (
                    Stat::IsConstant,
                    (roaring_stats.cardinality == length as u64 || roaring_stats.cardinality == 0)
                        .into(),
                ),
                (Stat::TrueCount, roaring_stats.cardinality.into()),
            ]));

            Ok(Self {
                typed: TypedArray::try_from_parts(
                    DType::Bool(NonNullable),
                    length,
                    RoaringBoolMetadata,
                    Some(Buffer::from(bitmap.serialize::<Native>())),
                    vec![].into(),
                    stats,
                )?,
            })
        }
    }

    pub fn bitmap(&self) -> Bitmap {
        //TODO(@jdcasale): figure out a way to avoid this deserialization per-call
        Bitmap::deserialize::<Native>(self.buffer().as_ref())
    }

    pub fn encode(array: Array) -> VortexResult<Array> {
        if let Ok(bools) = BoolArray::try_from(array) {
            roaring_bool_encode(bools).map(|a| a.into_array())
        } else {
            vortex_bail!("RoaringBool can only encode boolean arrays")
        }
    }

    pub fn buffer(&self) -> &Buffer {
        self.as_ref()
            .buffer()
            .vortex_expect("Missing buffer in PrimitiveArray")
    }
}

impl ArrayTrait for RoaringBoolArray {}

impl ArrayVariants for RoaringBoolArray {
    fn as_bool_array(&self) -> Option<&dyn BoolArrayTrait> {
        Some(self)
    }
}

impl BoolArrayTrait for RoaringBoolArray {
    fn maybe_null_indices_iter<'a>(&'a self) -> Box<dyn Iterator<Item = usize> + 'a> {
        todo!()
    }

    fn maybe_null_slices_iter<'a>(&'a self) -> Box<dyn Iterator<Item = (usize, usize)> + 'a> {
        todo!()
    }

    fn not(&self) -> Array {
        RoaringBoolArray::try_new(self.bitmap().flip(0..(self.len() as u32)), self.len())
            .vortex_unwrap()
            .into_array()
    }
}

impl AcceptArrayVisitor for RoaringBoolArray {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        // TODO(ngates): should we store a buffer in memory? Or delay serialization?
        //  Or serialize into metadata? The only reason we support buffers is so we can write to
        //  the wire without copying into FlatBuffers. But if we need to allocate to serialize
        //  the bitmap anyway, then may as well shove it into metadata.
        visitor.visit_buffer(self.buffer())
    }
}

impl ArrayValidity for RoaringBoolArray {
    fn is_valid(&self, _index: usize) -> bool {
        true
    }

    fn logical_validity(&self) -> LogicalValidity {
        LogicalValidity::AllValid(self.len())
    }
}

impl IntoCanonical for RoaringBoolArray {
    fn into_canonical(self) -> VortexResult<Canonical> {
        // TODO(ngates): benchmark the fastest conversion from BitMap.
        //  Via bitset requires two copies.
        let bitset = self
            .bitmap()
            .to_bitset()
            .ok_or_else(|| vortex_err!("Failed to convert RoaringBitmap to Bitset"))?;

        let byte_length = (self.len() + 7) / 8;
        let mut buffer = MutableBuffer::with_capacity(byte_length);
        buffer.extend_from_slice(bitset.as_slice());
        if byte_length > bitset.size_in_bytes() {
            buffer.extend_zeros(byte_length - bitset.size_in_bytes());
        }
        BoolArray::try_new(
            BooleanBuffer::new(buffer.into(), 0, self.len()),
            Validity::NonNullable,
        )
        .map(Canonical::Bool)
    }
}

#[cfg(test)]
mod test {
    use std::iter;

    use vortex::array::BoolArray;
    use vortex::{IntoArray, IntoArrayVariant};

    use crate::RoaringBoolArray;

    #[test]
    #[cfg_attr(miri, ignore)]
    pub fn iter() {
        let bool: BoolArray = BoolArray::from(vec![true, false, true, true]);
        let array = RoaringBoolArray::encode(bool.into_array()).unwrap();
        let round_trip = RoaringBoolArray::try_from(array).unwrap();
        let values = round_trip.bitmap().to_vec();
        assert_eq!(values, vec![0, 2, 3]);
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    pub fn trailing_false() {
        let bool: BoolArray = BoolArray::from(
            vec![true, true]
                .into_iter()
                .chain(iter::repeat(false).take(100))
                .collect::<Vec<_>>(),
        );
        let array = RoaringBoolArray::encode(bool.into_array()).unwrap();
        let round_trip = RoaringBoolArray::try_from(array).unwrap();
        let bool_arr = round_trip.into_bool().unwrap();
        assert_eq!(bool_arr.len(), 102);
    }
}
