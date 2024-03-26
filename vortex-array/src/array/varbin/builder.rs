use crate::array::primitive::PrimitiveArray;
use crate::array::varbin::VarBinArray;
use crate::array::Array;
use crate::ptype::NativePType;
use crate::validity::Validity;
use arrow_buffer::NullBufferBuilder;
use num_traits::PrimInt;
use vortex_schema::DType;

pub struct VarBinBuilder<O: NativePType + PrimInt> {
    offsets: Vec<O>,
    data: Vec<u8>,
    validity: NullBufferBuilder,
}

impl<O: NativePType + PrimInt> VarBinBuilder<O> {
    pub fn with_capacity(len: usize) -> Self {
        let mut offsets = Vec::with_capacity(len + 1);
        offsets.push(O::zero());
        Self {
            offsets,
            data: Vec::new(),
            validity: NullBufferBuilder::new(len),
        }
    }

    pub fn push(&mut self, value: Option<&[u8]>) {
        match value {
            Some(v) => {
                self.offsets
                    .push(O::from(self.data.len() + v.len()).unwrap());
                self.data.extend_from_slice(v);
                self.validity.append_non_null();
            }
            None => {
                self.offsets.push(self.offsets[self.offsets.len() - 1]);
                self.validity.append_null();
            }
        }
    }

    pub fn finish(self, dtype: DType) -> VarBinArray {
        let offsets = PrimitiveArray::from(self.offsets);
        let data = PrimitiveArray::from(self.data);
        // TODO(ngates): create our own ValidityBuilder that doesn't need mut or clone on finish.
        let validity = self.validity.finish_cloned().map(Validity::from);
        VarBinArray::new(offsets.into_array(), data.into_array(), dtype, validity)
    }
}
