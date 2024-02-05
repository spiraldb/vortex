use arrow::buffer::Buffer;
use half::f16;
use polars_arrow::io::iterator::StreamingIterator;

use crate::array::constant::ConstantEncoding;
use crate::array::primitive::PrimitiveArray;
use crate::array::{encodings, Array, ArrayRef};
use crate::compress::{ArrayCompression, CompressCtx, Compressor, EncodingCompression};
use crate::ptype::match_each_native_ptype;
use crate::ptype::PType;
use crate::sampling::default_sample;

impl ArrayCompression for PrimitiveArray {
    fn compress(&self, ctx: CompressCtx) -> ArrayRef {
        // First, we try constant compression
        if let Some(compressor) = ConstantEncoding.compressor(self, ctx.options()) {
            return compressor(self, ctx);
        }

        let candidate_compressors: Vec<&Compressor> = encodings()
            .filter_map(|encoding| encoding.compression())
            .filter_map(|compression| compression.compressor(self, ctx.options()))
            .collect();

        if candidate_compressors.is_empty() {
            return dyn_clone::clone_box(self);
        }

        if ctx.is_sample() {
            let (_, compressed_sample) = candidate_compressors.iter().fold(
                (self.nbytes(), None),
                |(compressed_bytes, curr_best), compressor| {
                    let compressed = compressor(self, ctx.clone());

                    if compressed.nbytes() < compressed_bytes {
                        (compressed.nbytes(), Some(compressed))
                    } else {
                        (compressed_bytes, curr_best)
                    }
                },
            );
            return compressed_sample.unwrap_or_else(|| dyn_clone::clone_box(self));
        }

        let sample = match_each_native_ptype!(self.ptype(), |$P| {
            PrimitiveArray::new(
                self.ptype().clone(),
                Buffer::from_vec(default_sample(
                    self.buffer().typed_data::<$P>(),
                    ctx.options().sample_size,
                    ctx.options().sample_count,
                )),
                None,
            )
        });

        let sample_opts = ctx.for_sample();
        let compression_ratios: Vec<(&Compressor, f32)> = candidate_compressors
            .iter()
            .map(|compressor| {
                (
                    *compressor,
                    compressor(self, sample_opts.clone()).nbytes() as f32 / sample.nbytes() as f32,
                )
            })
            .collect();

        compression_ratios
            .into_iter()
            .filter(|(_, ratio)| *ratio < 1.0)
            .min_by(|(_, first_ratio), (_, second_ratio)| first_ratio.total_cmp(second_ratio))
            .map(|(compressor, _)| compressor(self, ctx))
            .unwrap_or_else(|| dyn_clone::clone_box(self))
    }
}

#[cfg(test)]
mod test {
    use crate::array::constant::ConstantEncoding;
    use crate::array::primitive::PrimitiveArray;
    use crate::array::ree::REEEncoding;
    use crate::array::Encoding;
    use crate::compress::{ArrayCompression, CompressCtx};

    #[test]
    pub fn compress_ree() {
        let arr = PrimitiveArray::from_vec(vec![1, 1, 1, 2, 3, 4, 4, 4, 4, 2, 2, 3, 3]);
        let res = arr.compress(CompressCtx::default());
        assert_eq!(res.encoding().id(), REEEncoding.id());
        assert_eq!(res.len(), 13);
        assert_eq!(res.scalar_at(5).unwrap().try_into(), Ok(4));
    }

    #[test]
    pub fn compress_constant() {
        let arr = PrimitiveArray::from_vec(vec![1, 1, 1, 1]);
        let res = arr.compress(CompressCtx::default());
        assert_eq!(res.encoding().id(), ConstantEncoding.id());
        assert_eq!(res.scalar_at(3).unwrap().try_into(), Ok(1));
    }
}
