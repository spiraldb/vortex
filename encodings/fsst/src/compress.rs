// Compress a set of values into an Array.

use fsst::{Compressor, Symbol};
use vortex::accessor::ArrayAccessor;
use vortex::array::builder::VarBinBuilder;
use vortex::array::{PrimitiveArray, VarBinArray, VarBinViewArray};
use vortex::validity::Validity;
use vortex::{Array, ArrayDType, IntoArray};
use vortex_dtype::DType;

use crate::FSSTArray;

/// Compress an array using FSST.
///
/// # Panics
///
/// If the `strings` array is not encoded as either [`VarBinArray`] or [`VarBinViewArray`].
pub fn fsst_compress(strings: &Array, compressor: &Compressor) -> FSSTArray {
    let len = strings.len();
    let dtype = strings.dtype().clone();

    // Compress VarBinArray
    if let Ok(varbin) = VarBinArray::try_from(strings) {
        let compressed = varbin
            .with_iterator(|iter| fsst_compress_iter(iter, len, dtype, compressor))
            .unwrap();

        return compressed;
    }

    // Compress VarBinViewArray
    if let Ok(varbin_view) = VarBinViewArray::try_from(strings) {
        let compressed = varbin_view
            .with_iterator(|iter| fsst_compress_iter(iter, len, dtype, compressor))
            .unwrap();

        return compressed;
    }

    panic!(
        "cannot fsst_compress array with unsupported encoding {:?}",
        strings.encoding().id()
    )
}

/// Train a compressor from an array.
///
/// # Panics
///
/// If the provided array is not FSST compressible.
pub fn fsst_train_compressor(array: &Array) -> Compressor {
    if let Ok(varbin) = VarBinArray::try_from(array) {
        varbin
            .with_iterator(|iter| fsst_train_compressor_iter(iter))
            .unwrap()
    } else if let Ok(varbin_view) = VarBinViewArray::try_from(array) {
        varbin_view
            .with_iterator(|iter| fsst_train_compressor_iter(iter))
            .unwrap()
    } else {
        panic!(
            "cannot fsst_compress array with unsupported encoding {:?}",
            array.encoding().id()
        )
    }
}

/// Train a [compressor][Compressor] from an iterator of bytestrings.
fn fsst_train_compressor_iter<'a, I>(iter: I) -> Compressor
where
    I: Iterator<Item = Option<&'a [u8]>>,
{
    let mut lines = Vec::with_capacity(8_192);

    for string in iter {
        match string {
            None => {}
            Some(b) => lines.push(b),
        }
    }

    Compressor::train(&lines)
}

/// Compress from an iterator of bytestrings using FSST.
pub fn fsst_compress_iter<'a, I>(
    iter: I,
    len: usize,
    dtype: DType,
    compressor: &Compressor,
) -> FSSTArray
where
    I: Iterator<Item = Option<&'a [u8]>>,
{
    // TODO(aduffy): this might be too small.
    let mut buffer = Vec::with_capacity(16 * 1024 * 1024);
    let mut builder = VarBinBuilder::<i32>::with_capacity(len);
    let mut uncompressed_lengths: Vec<i32> = Vec::with_capacity(len);
    for string in iter {
        match string {
            None => builder.push_null(),
            Some(s) => {
                uncompressed_lengths.push(s.len() as i32);

                // SAFETY: buffer is large enough
                unsafe { compressor.compress_into(s, &mut buffer) };

                builder.push_value(&buffer);
            }
        }
    }

    let codes = builder
        .finish(DType::Binary(dtype.nullability()))
        .into_array();
    let symbols_vec: Vec<Symbol> = compressor.symbol_table().to_vec();
    // SAFETY: Symbol and u64 are same size
    let symbols_u64: Vec<u64> = unsafe { std::mem::transmute(symbols_vec) };
    let symbols = PrimitiveArray::from_vec(symbols_u64, Validity::NonNullable).into_array();

    let symbol_lengths_vec: Vec<u8> = compressor.symbol_lengths().to_vec();
    let symbol_lengths =
        PrimitiveArray::from_vec(symbol_lengths_vec, Validity::NonNullable).into_array();
    let uncompressed_lengths =
        PrimitiveArray::from_vec(uncompressed_lengths, Validity::NonNullable).into_array();

    FSSTArray::try_new(dtype, symbols, symbol_lengths, codes, uncompressed_lengths)
        .expect("building FSSTArray from parts")
}
