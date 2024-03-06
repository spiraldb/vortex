use vortex::array::primitive::PrimitiveArray;
use vortex::array::varbin::VarBinArray;
use vortex::array::{Array, ArrayRef};
use vortex::dtype::DType;
use vortex::dtype::Nullability::NonNullable;
use vortex_dict::dict_encode_varbin;

fn main() {
    divan::main();
}

#[divan::bench(args = [100_000, 10_000_000])]
fn dict_compress_varbin(n: usize) -> ArrayRef {
    // Compress an array of 1-byte strings.
    let offsets = PrimitiveArray::from((0..=n).map(|i| i as i64).collect::<Vec<_>>()).boxed();
    let bytes = PrimitiveArray::from(vec![1u8; n]).boxed();
    let vb = VarBinArray::new(offsets, bytes, DType::Utf8(NonNullable), None);

    let (_codes, values) = dict_encode_varbin(&vb);
    values.boxed()
}
