use std::hash::{Hash, Hasher};

use ahash::RandomState;
use hashbrown::hash_map::{Entry, RawEntryMut};
use hashbrown::HashMap;
use log::info;
use num_traits::AsPrimitive;

use enc::array::primitive::PrimitiveArray;
use enc::array::varbin::VarBinArray;
use enc::array::{Array, ArrayKind, ArrayRef, Encoding};
use enc::compress::{
    ArrayCompression, CompressConfig, CompressCtx, Compressor, EncodingCompression,
};
use enc::dtype::DType;
use enc::match_each_native_ptype;
use enc::ptype::NativePType;
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

        // TODO(robert): Add support for VarBinView
        if !matches!(
            ArrayKind::from(array),
            ArrayKind::Primitive(_) | ArrayKind::VarBin(_)
        ) {
            info!("Skipping Dict: not primitive or varbin");
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
    match ArrayKind::from(array) {
        ArrayKind::Primitive(p) => {
            let (codes, values) = dict_encode_primitive(p);
            DictArray::new(codes.boxed(), values.boxed()).boxed()
        }
        ArrayKind::VarBin(vb) => {
            let (codes, values) = dict_encode_varbin(vb);
            DictArray::new(codes.boxed(), values.boxed()).boxed()
        }
        _ => panic!("This encoding should have been excluded already"),
    }
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
            Entry::Occupied(o) => *o.get(),
            Entry::Vacant(vac) => {
                let next_code = values.len() as u64;
                vac.insert(next_code);
                values.push(*v);
                next_code
            }
        };
        codes.push(code)
    }

    (
        PrimitiveArray::from_nullable(codes, array.validity().cloned()),
        PrimitiveArray::from_vec(values),
    )
}

/// Dictionary encode varbin array. Specializes for primitive byte arrays to avoid double copying
pub fn dict_encode_varbin(array: &VarBinArray) -> (PrimitiveArray, VarBinArray) {
    if let Some(bytes) = array.bytes().as_any().downcast_ref::<PrimitiveArray>() {
        if let Some(offsets) = array.offsets().as_any().downcast_ref::<PrimitiveArray>() {
            return match_each_native_ptype!(offsets.ptype(), |$P| {
                let offsets = offsets.buffer().typed_data::<$P>();
                let bytes = bytes.buffer().typed_data::<u8>();

                dict_encode_typed_varbin(
                    array.dtype().clone(),
                    |idx| bytes_at(offsets, bytes, idx),
                    array.len(),
                    array.validity(),
                )
            });
        }
    }

    dict_encode_typed_varbin(
        array.dtype().clone(),
        |idx| array.bytes_at(idx).unwrap(),
        array.len(),
        array.validity(),
    )
}

fn dict_encode_typed_varbin<V, U>(
    dtype: DType,
    value_lookup: V,
    len: usize,
    validity: Option<&ArrayRef>,
) -> (PrimitiveArray, VarBinArray)
where
    V: Fn(usize) -> U,
    U: AsRef<[u8]>,
{
    let hasher = RandomState::new();
    let mut lookup_dict: HashMap<u64, (), ()> = HashMap::with_hasher(());
    let mut codes: Vec<u64> = Vec::with_capacity(len);
    let mut bytes: Vec<u8> = Vec::new();
    let mut offsets: Vec<u64> = Vec::new();
    offsets.push(0);

    for i in 0..len {
        let byte_val = value_lookup(i);
        let byte_ref = byte_val.as_ref();
        let value_hash = hasher.hash_one(byte_ref);
        let raw_entry = lookup_dict.raw_entry_mut().from_hash(value_hash, |idx| {
            byte_ref == value_lookup(*idx as usize).as_ref()
        });

        let code: u64 = match raw_entry {
            RawEntryMut::Occupied(o) => *o.into_key(),
            RawEntryMut::Vacant(vac) => {
                let next_code = offsets.len() as u64 - 1;
                bytes.extend_from_slice(byte_ref);
                offsets.push(bytes.len() as u64);
                vac.insert_with_hasher(value_hash, next_code, (), |idx| {
                    hasher.hash_one(value_lookup(*idx as usize).as_ref())
                });
                next_code
            }
        };
        codes.push(code)
    }
    (
        PrimitiveArray::from_nullable(codes, validity.cloned()),
        VarBinArray::new(
            PrimitiveArray::from_vec(offsets).boxed(),
            PrimitiveArray::from_vec(bytes).boxed(),
            dtype,
            None,
        ),
    )
}

fn bytes_at<'a, T: NativePType + AsPrimitive<usize>>(
    offsets: &'a [T],
    bytes: &'a [u8],
    idx: usize,
) -> &'a [u8] {
    let begin: usize = offsets[idx].as_();
    let end: usize = offsets[idx + 1].as_();
    &bytes[begin..end]
}

#[cfg(test)]
mod test {
    use enc::array::primitive::PrimitiveArray;
    use enc::array::varbin::VarBinArray;
    use enc::array::Array;

    use crate::compress::{dict_encode_typed_primitive, dict_encode_varbin};

    #[test]
    fn encode_primitive() {
        let arr = PrimitiveArray::from_vec(vec![1, 1, 3, 3, 3]);
        let (codes, values) = dict_encode_typed_primitive::<i32>(&arr);
        assert_eq!(codes.buffer().typed_data::<u64>(), &[0, 0, 1, 1, 1]);
        assert_eq!(values.buffer().typed_data::<i32>(), &[1, 3]);
    }

    #[test]
    fn encode_primitive_nulls() {
        let arr = PrimitiveArray::from_iter(vec![
            Some(1),
            Some(1),
            None,
            Some(3),
            Some(3),
            None,
            Some(3),
            None,
        ]);
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

    #[test]
    fn encode_varbin() {
        let arr = VarBinArray::from(vec!["hello", "world", "hello", "again", "world"]);
        let (codes, values) = dict_encode_varbin(&arr);
        assert_eq!(codes.buffer().typed_data::<u64>(), &[0, 1, 0, 2, 1]);
        assert_eq!(
            String::from_utf8(values.bytes_at(0).unwrap()).unwrap(),
            "hello"
        );
        assert_eq!(
            String::from_utf8(values.bytes_at(1).unwrap()).unwrap(),
            "world"
        );
        assert_eq!(
            String::from_utf8(values.bytes_at(2).unwrap()).unwrap(),
            "again"
        );
    }

    #[test]
    fn encode_varbin_nulls() {
        let arr: VarBinArray = vec![
            Some("hello"),
            None,
            Some("world"),
            Some("hello"),
            None,
            Some("again"),
            Some("world"),
            None,
        ]
        .into_iter()
        .collect();
        let (codes, values) = dict_encode_varbin(&arr);
        assert_eq!(
            codes.buffer().typed_data::<u64>(),
            &[0, 1, 2, 0, 1, 3, 2, 1]
        );
        assert!(!codes.is_valid(1));
        assert!(!codes.is_valid(4));
        assert!(!codes.is_valid(7));
        assert_eq!(
            String::from_utf8(values.bytes_at(0).unwrap()).unwrap(),
            "hello"
        );
        assert_eq!(String::from_utf8(values.bytes_at(1).unwrap()).unwrap(), "");
        assert_eq!(
            String::from_utf8(values.bytes_at(2).unwrap()).unwrap(),
            "world"
        );
        assert_eq!(
            String::from_utf8(values.bytes_at(3).unwrap()).unwrap(),
            "again"
        );
    }
}
