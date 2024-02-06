use crate::array::primitive::PrimitiveArray;
use crate::array::ArrayRef;
use crate::compress::{sampled_compression, ArrayCompression, CompressCtx};

impl ArrayCompression for PrimitiveArray {
    fn compress(&self, ctx: CompressCtx) -> ArrayRef {
        sampled_compression(self, ctx)
    }
}

#[cfg(test)]
mod test {
    use crate::array::constant::ConstantEncoding;
    use crate::array::primitive::PrimitiveArray;
    use crate::array::Encoding;
    use crate::compress::{ArrayCompression, CompressCtx};

    #[test]
    pub fn compress_constant() {
        let arr = PrimitiveArray::from_vec(vec![1, 1, 1, 1]);
        let res = arr.compress(CompressCtx::default());
        assert_eq!(res.encoding().id(), ConstantEncoding.id());
        assert_eq!(res.scalar_at(3).unwrap().try_into(), Ok(1));
    }
}
