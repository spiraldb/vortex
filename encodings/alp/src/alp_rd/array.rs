use serde::{Deserialize, Serialize};
use vortex::array::{PrimitiveArray, SparseArray};
use vortex::encoding::ids;
use vortex::stats::{ArrayStatisticsCompute, StatsSet};
use vortex::validity::{ArrayValidity, LogicalValidity};
use vortex::visitor::{AcceptArrayVisitor, ArrayVisitor};
use vortex::{impl_encoding, Array, ArrayDType, ArrayDef, ArrayTrait, Canonical, IntoCanonical};
use vortex_dtype::{DType, PType};
use vortex_error::{vortex_bail, VortexExpect, VortexResult};

use crate::alp_rd::alp_rd_decode;

impl_encoding!("vortex.alprd", ids::ALP_RD, ALPRD);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ALPRDMetadata {
    right_bit_width: u8,
    // left_bit_width is implicit from the dict_len.
    dict_len: u8,
    dict: [u16; 8],
    left_parts_dtype: DType,
    has_exceptions: bool,
}

impl ALPRDArray {
    pub fn try_new(
        dtype: DType,
        left_parts: Array,
        left_parts_dict: impl AsRef<[u16]>,
        right_parts: Array,
        right_bit_width: u8,
        left_parts_exceptions: Option<Array>,
    ) -> VortexResult<Self> {
        if !dtype.is_float() {
            vortex_bail!("ALPRDArray given invalid DType ({dtype})");
        }

        if left_parts.len() != right_parts.len() {
            vortex_bail!("left_parts and right_parts must be of same length");
        }

        let len = left_parts.len();

        if !left_parts.dtype().is_unsigned_int() {
            vortex_bail!("left_parts dtype must be uint");
        }

        let left_parts_dtype = left_parts.dtype().clone();

        if !right_parts.dtype().is_unsigned_int() {
            vortex_bail!("right_parts dtype must be uint");
        }

        let mut children = vec![left_parts, right_parts];
        let has_exceptions = left_parts_exceptions.is_some();

        if let Some(exceptions) = left_parts_exceptions {
            // Enforce that the exceptions are SparseArray so that we have access to indices and values.
            if exceptions.encoding().id().code() != ids::SPARSE {
                vortex_bail!("left_parts_exceptions must be SparseArray encoded");
            }
            children.push(exceptions);
        }

        let mut dict = [0u16; 8];
        for (idx, v) in left_parts_dict.as_ref().iter().enumerate() {
            dict[idx] = *v;
        }

        Self::try_from_parts(
            dtype,
            len,
            ALPRDMetadata {
                right_bit_width,
                dict_len: left_parts_dict.as_ref().len() as u8,
                dict,
                left_parts_dtype,
                has_exceptions,
            },
            children.into(),
            StatsSet::new(),
        )
    }

    /// The leftmost (most significant) bits of the floating point values stored in the array.
    ///
    /// These are bit-packed and dictionary encoded, and cannot directly be interpreted without
    /// the metadata of this array.
    pub fn left_parts(&self) -> Array {
        self.as_ref()
            .child(0, &self.metadata().left_parts_dtype, self.len())
            .vortex_expect("ALPRDArray: left_parts child")
    }

    /// The rightmost (least significant) bits of the floating point values stored in the array.
    pub fn right_parts(&self) -> Array {
        self.as_ref()
            .child(
                1,
                &DType::Primitive(PType::U64, self.metadata().left_parts_dtype.nullability()),
                self.len(),
            )
            .vortex_expect("ALPRDArray: right_parts child")
    }

    /// Patches of left-most bits.
    pub fn left_parts_exceptions(&self) -> Option<Array> {
        self.metadata().has_exceptions.then(|| {
            self.as_ref()
                .child(
                    2,
                    &self.metadata().left_parts_dtype.as_nullable(),
                    self.len(),
                )
                .vortex_expect("ALPRDArray: left_parts_exceptions child")
        })
    }

    /// The dictionary that maps the codes in `left_parts` into bit patterns.
    #[inline]
    pub fn left_parts_dict(&self) -> &[u16] {
        &self.metadata().dict[0..self.metadata().dict_len as usize]
    }

    #[inline]
    pub(crate) fn right_bit_width(&self) -> u8 {
        self.metadata().right_bit_width
    }
}

impl IntoCanonical for ALPRDArray {
    fn into_canonical(self) -> VortexResult<Canonical> {
        let left_parts = self.left_parts().into_canonical()?.into_primitive()?;
        let right_parts = self.right_parts().into_canonical()?.into_primitive()?;

        // Decode the left_parts using our builtin dictionary.
        let left_parts_dict = &self.metadata().dict[0..self.metadata().dict_len as usize];

        let exc_pos: Vec<u64>;
        let exc_u16: PrimitiveArray;

        if let Some(left_parts_exceptions) = self.left_parts_exceptions() {
            let left_parts_exceptions = SparseArray::try_from(left_parts_exceptions)
                .vortex_expect("ALPRDArray: exceptions must be SparseArray encoded");
            exc_pos = left_parts_exceptions
                .resolved_indices()
                .into_iter()
                .map(|v| v as _)
                .collect();
            exc_u16 = left_parts_exceptions
                .values()
                .into_canonical()?
                .into_primitive()?;
        } else {
            exc_pos = Vec::new();
            exc_u16 = PrimitiveArray::from(Vec::<u16>::new());
        }

        let decoded = alp_rd_decode(
            left_parts.maybe_null_slice::<u16>(),
            left_parts_dict,
            self.metadata().right_bit_width,
            right_parts.maybe_null_slice::<u64>(),
            &exc_pos,
            exc_u16.maybe_null_slice::<u16>(),
        );

        let decoded_array =
            PrimitiveArray::from_vec(decoded, self.logical_validity().into_validity());

        Ok(Canonical::Primitive(decoded_array))
    }
}

impl ArrayValidity for ALPRDArray {
    fn is_valid(&self, index: usize) -> bool {
        // Use validity from left_parts
        self.left_parts().with_dyn(|a| a.is_valid(index))
    }

    fn logical_validity(&self) -> LogicalValidity {
        self.left_parts().with_dyn(|a| a.logical_validity())
    }
}

impl AcceptArrayVisitor for ALPRDArray {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        visitor.visit_child("left_parts", &self.left_parts())?;
        visitor.visit_child("right_parts", &self.right_parts())?;
        if let Some(left_parts_exceptions) = self.left_parts_exceptions() {
            visitor.visit_child("left_parts_exceptions", &left_parts_exceptions)
        } else {
            Ok(())
        }
    }
}

impl ArrayStatisticsCompute for ALPRDArray {}

impl ArrayTrait for ALPRDArray {}

#[cfg(test)]
mod test {
    use vortex::array::PrimitiveArray;
    use vortex::{IntoArray, IntoCanonical};

    use crate::alp_rd;

    fn real_doubles(seed: f64, n: usize) -> Vec<f64> {
        (0..n)
            .scan(seed, |state, _| {
                let prev = *state;
                *state = state.next_up();
                Some(prev)
            })
            .collect()
    }

    #[test]
    fn test_array_encode_with_nulls_and_exceptions() {
        const SEED: f64 = 1.123_848_591_110_992_f64;
        // Create a vector of 1024 "real" doubles
        let reals = real_doubles(SEED, 1024);
        // Null out some of the values.
        let mut reals: Vec<Option<f64>> = reals.into_iter().map(Some).collect();
        reals[1] = None;
        reals[5] = None;
        reals[90] = None;

        // Create a new array from this.
        let real_doubles = PrimitiveArray::from_nullable_vec(reals.clone());

        // Pick a seed that we know will trigger lots of exceptions.
        let encoder = alp_rd::Encoder::new(&[100.0f64]);

        let rd_array = encoder.encode(&real_doubles);

        let decoded = rd_array
            .into_array()
            .into_canonical()
            .unwrap()
            .into_primitive()
            .unwrap();

        let maybe_null_reals: Vec<f64> = reals.into_iter().map(|v| v.unwrap_or_default()).collect();
        assert_eq!(decoded.maybe_null_slice::<f64>(), &maybe_null_reals);
    }
}
