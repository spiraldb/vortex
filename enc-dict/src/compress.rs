use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use half::f16;
use log::info;

use enc::array::primitive::PrimitiveArray;
use enc::array::{Array, ArrayKind, ArrayRef, Encoding};
use enc::compress::{
    ArrayCompression, CompressConfig, CompressCtx, Compressor, EncodingCompression,
};
use enc::match_each_native_ptype;
use enc::ptype::NativePType;
use enc::ptype::PType;
use enc::scalar::AsBytes;

use crate::dict::{DictArray, DictEncoding};

impl ArrayCompression for DictArray {
    fn compress(&self, ctx: CompressCtx) -> ArrayRef {
        DictArray::new(ctx.compress(self.codes()), ctx.compress(self.dict())).boxed()
    }
}

impl EncodingCompression for DictEncoding {
    fn compressor(
        &self,
        array: &dyn Array,
        config: &CompressConfig,
    ) -> Option<&'static Compressor> {
        if !config.is_enabled(self.id()) {
            info!("Skipping Dict: disabled");
            return None;
        }

        // TODO(robert): support non primitive
        let Some(_) = array.as_any().downcast_ref::<PrimitiveArray>() else {
            info!("Skipping Dict: not primitive");
            return None;
        };

        info!("Compressing with Dict");
        Some(&(dict_compressor as Compressor))
    }
}

#[derive(Debug)]
struct Value<T>(T);

impl<T: AsBytes> Hash for Value<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.as_bytes().hash(state)
    }
}

impl<T: AsBytes> PartialEq<Self> for Value<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_bytes().eq(other.0.as_bytes())
    }
}

impl<T: AsBytes> Eq for Value<T> {}

fn dict_compressor(array: &dyn Array, _ctx: CompressCtx) -> ArrayRef {
    let (codes, values) = match ArrayKind::from(array) {
        ArrayKind::Primitive(p) => dict_encode_primitive(p),
        _ => panic!("This encoding should have been excluded already"),
    };
    DictArray::new(codes.boxed(), values.boxed()).boxed()
}

pub fn dict_encode_primitive(array: &PrimitiveArray) -> (PrimitiveArray, PrimitiveArray) {
    match_each_native_ptype!(array.ptype(), |$P| {
        dict_encode_typed_primitive::<$P>(array)
    })
}

/// Dictionary encode primitive array with given PType.
/// Null values in the original array are encoded in the dictionary.
// TODO(robert): Consider parameterizing over dictionary size. But maybe bitpacking makes it irrelevant?
fn dict_encode_typed_primitive<T: NativePType>(
    array: &PrimitiveArray,
) -> (PrimitiveArray, PrimitiveArray) {
    let mut lookup_dict: HashMap<Value<T>, u64> = HashMap::new();
    let mut codes: Vec<u64> = Vec::new();
    let mut values: Vec<T> = Vec::new();
    for v in array.buffer().typed_data::<T>() {
        let code: u64 = match lookup_dict.entry(Value(*v)) {
            Entry::Vacant(vac) => {
                let next_code = values.len() as u64;
                vac.insert(next_code);
                values.push(*v);
                next_code
            }
            Entry::Occupied(o) => *o.get(),
        };
        codes.push(code)
    }

    (
        PrimitiveArray::from_nullable(codes, array.validity().map(|v| v.clone())),
        PrimitiveArray::from_vec(values),
    )
}

#[cfg(test)]
mod test {
    use enc::array::primitive::PrimitiveArray;
    use enc::array::Array;

    use crate::compress::dict_encode_typed_primitive;

    #[test]
    fn encode_primitive() {
        let arr = PrimitiveArray::from_vec(vec![1, 1, 3, 3, 3]);
        let (codes, values) = dict_encode_typed_primitive::<i32>(&arr);
        assert_eq!(codes.buffer().typed_data::<u64>(), &[0, 0, 1, 1, 1]);
        assert_eq!(values.buffer().typed_data::<i32>(), &[1, 3]);
    }

    #[test]
    fn encode_primitive_nulls() {
        let arr = PrimitiveArray::from_iter(
            vec![
                Some(1),
                Some(1),
                None,
                Some(3),
                Some(3),
                None,
                Some(3),
                None,
            ]
            .into_iter(),
        );
        let (codes, values) = dict_encode_typed_primitive::<i32>(&arr);
        assert_eq!(
            codes.buffer().typed_data::<u64>(),
            &[0, 0, 1, 2, 2, 1, 2, 1]
        );
        assert!(!codes.is_valid(2));
        assert!(!codes.is_valid(5));
        assert!(!codes.is_valid(7));
        assert_eq!(values.scalar_at(0), Ok(1.into()));
        assert_eq!(values.scalar_at(2), Ok(3.into()));
    }
}
