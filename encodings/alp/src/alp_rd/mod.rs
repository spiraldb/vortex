//! Encoding for "real doubles", i.e. doubles that don't compress easily via the typical ALP
//! algorithm.
//!
//! ALP-RD uses the algorithm outlined in Section 3.4 of the paper, as well as relevant MIT-licensed
//! C++ code from CWI.
//!
//! The crux of it is that the front (most significant) bits of many double vectors tend to be
//! the same, i.e. most doubles in a vector often use the same exponent and front bits. Compression
//! proceeds by finding the best prefix of up to 16 bits that can be collapsed into a dictionary of
//! up to 8 elements. Each double can then be broken into the front/left `L` bits, which neatly
//! bit-packs down to 3 bits per element. The remaining `R` bits are bit-packed as well.
//!
//! Even in the ideal case, this gets about ~24% compression.

pub use array::*;

mod array;
mod compute;

use std::collections::HashMap;

use itertools::Itertools;
use vortex::array::{PrimitiveArray, SparseArray};
use vortex::{ArrayDType, IntoArray};
use vortex_dtype::{DType, PType};
use vortex_error::{VortexExpect, VortexUnwrap};
use vortex_fastlanes::BitPackedArray;

/// Max number of bits to cut from the MSB section of each float.
const CUT_LIMIT: usize = 16;

const MAX_DICT_SIZE: u8 = 8;

/// Encoder for ALP-RD (real doubles) values.
///
/// The encoder builds a sample of values from there.
pub struct Encoder {
    right_bit_width: u8,
    dictionary: HashMap<u16, u16>,
    codes: Vec<u16>,
}

impl Encoder {
    /// Build a new encoder from a sample of doubles.
    pub fn new(sample: &[f64]) -> Self {
        let dictionary = find_best_dictionary(sample);
        Self {
            right_bit_width: dictionary.right_bit_width,
            dictionary: dictionary.dictionary,
            codes: dictionary.codes,
        }
    }

    /// Encode a set of floating point values with ALP-RD.
    ///
    /// Each value will be split into a left and right component, which are compressed individually.
    pub fn encode(&self, array: &PrimitiveArray) -> ALPRDArray {
        let doubles = array.maybe_null_slice::<f64>();

        let mut left_parts: Vec<u16> = Vec::with_capacity(doubles.len());
        let mut right_parts: Vec<u64> = Vec::with_capacity(doubles.len());
        let mut exceptions_pos: Vec<u64> = Vec::with_capacity(doubles.len() / 4);
        let mut exceptions: Vec<u16> = Vec::with_capacity(doubles.len() / 4);

        // mask for right-parts
        let right_mask = (1u64 << self.right_bit_width) - 1;
        let left_bit_width = self.dictionary.len().next_power_of_two().ilog2().max(1) as u8;

        for v in doubles.iter().copied() {
            right_parts.push(v.to_bits() & right_mask);
            left_parts.push((v.to_bits() >> self.right_bit_width) as u16);
        }

        // dict-encode the left-parts, keeping track of exceptions
        for (idx, left) in left_parts.iter_mut().enumerate() {
            // TODO: revisit if we need to change the branch order for perf.
            if let Some(code) = self.dictionary.get(left) {
                *left = *code;
            } else {
                exceptions.push(*left);
                exceptions_pos.push(idx as _);

                *left = self.dictionary.len() as u16;
            }
        }

        // Bit-pack down the encoded left-parts array that have been dictionary encoded.
        let primitive_left = PrimitiveArray::from_vec(left_parts, array.validity());
        let packed_left = BitPackedArray::encode(primitive_left.as_ref(), left_bit_width as _)
            .vortex_unwrap()
            .into_array();

        let primitive_right = PrimitiveArray::from_vec(right_parts, array.validity());
        let packed_right =
            BitPackedArray::encode(primitive_right.as_ref(), self.right_bit_width as _)
                .vortex_unwrap()
                .into_array();

        // Bit-pack the dict-encoded left-parts
        // Bit-pack the right-parts
        // SparseArray for exceptions.
        let exceptions = (!exceptions_pos.is_empty()).then(|| {
            let max_exc_pos = exceptions_pos.last().copied().unwrap_or_default();
            // Add one to get next power of two as well here.
            // If we're going to be doing more of this, it just works.
            let bw = (max_exc_pos + 1).next_power_of_two().ilog2() as usize;

            let exc_pos_array = PrimitiveArray::from(exceptions_pos);
            let packed_pos = BitPackedArray::encode(exc_pos_array.as_ref(), bw)
                .vortex_unwrap()
                .into_array();

            let exc_array = PrimitiveArray::from(exceptions).into_array();
            SparseArray::try_new(packed_pos, exc_array, doubles.len(), 0u16.into())
                .vortex_expect("ALP-RD: construction of exceptions SparseArray")
                .into_array()
        });

        ALPRDArray::try_new(
            DType::Primitive(PType::F64, packed_left.dtype().nullability()),
            packed_left,
            &self.codes,
            packed_right,
            self.right_bit_width,
            exceptions,
        )
        .vortex_expect("ALPRDArray construction in encode")
    }
}

// Only applies for F64.
pub fn alp_rd_decode(
    left_parts: &[u16],
    left_parts_dict: &[u16],
    right_bit_width: u8,
    right_parts: &[u64],
    exc_pos: &[u64],
    exceptions: &[u16],
) -> Vec<f64> {
    assert_eq!(
        left_parts.len(),
        right_parts.len(),
        "alp_rd_decode: left_parts.len != right_parts.len"
    );

    assert_eq!(
        exc_pos.len(),
        exceptions.len(),
        "alp_rd_decode: exc_pos.len != exceptions.len"
    );

    // Prepare the dictionary for decoding by adding the extra value for lookups to match.
    let mut dict = Vec::with_capacity(left_parts_dict.len() + 1);
    dict.extend_from_slice(left_parts_dict);
    // Add an extra code for out-of-dict values. These will be overwritten with exceptions later.
    const EXCEPTION_SENTINEL: u16 = 0xDEAD;
    dict.push(EXCEPTION_SENTINEL);

    let mut left_parts_decoded = Vec::with_capacity(left_parts.len());

    // Decode with bit-packing and dict unpacking.
    for code in left_parts {
        left_parts_decoded.push(dict[*code as usize] as u64);
    }

    // Apply the exception patches. Only applies for the left-parts
    for (pos, val) in exc_pos.iter().zip(exceptions.iter()) {
        left_parts_decoded[*pos as usize] = *val as u64;
    }

    // recombine the left-and-right parts, adjusting by the right_bit_width.
    left_parts_decoded
        .into_iter()
        .zip(right_parts.iter().copied())
        .map(|(left, right)| f64::from_bits((left << right_bit_width) | right))
        .collect()
}

/// Find the best "cut point" for a set of floating point values such that we can
/// cast them all to the relevant value instead.
fn find_best_dictionary(samples: &[f64]) -> ALPRDDictionary {
    let mut best_est_size = f64::MAX;
    let mut best_dict = ALPRDDictionary::default();

    for p in 1u8..=16 {
        let candidate_right_bw = 64 - p;
        let (dictionary, exception_count) =
            build_left_parts_dictionary(samples, candidate_right_bw, MAX_DICT_SIZE);
        let estimated_size = estimate_compression_size(
            dictionary.right_bit_width,
            dictionary.left_bit_width,
            exception_count,
            samples.len(),
        );
        if estimated_size < best_est_size {
            best_est_size = estimated_size;
            best_dict = dictionary;
        }
    }

    best_dict
}

/// Build dictionary of the leftmost bits.
fn build_left_parts_dictionary(
    samples: &[f64],
    right_bw: u8,
    max_dict_size: u8,
) -> (ALPRDDictionary, usize) {
    assert!(
        right_bw >= (64 - CUT_LIMIT) as _,
        "left-parts must be <= 16 bits"
    );

    // Count the number of occurrences of each left bit pattern
    let counts = samples
        .iter()
        .copied()
        .map(|v| (v.to_bits() >> right_bw) as u16)
        .counts();

    // Sorted counts: sort by negative count so that heavy hitters sort first.
    let mut sorted_bit_counts: Vec<(u16, usize)> = counts.into_iter().collect_vec();
    sorted_bit_counts.sort_by_key(|(_, count)| count.wrapping_neg());

    // Assign the most-frequently occurring left-bits as dictionary codes, up to `dict_size`...
    let mut dictionary = HashMap::with_capacity(max_dict_size as _);
    let mut code = 0u16;
    while code < (max_dict_size as _) && (code as usize) < sorted_bit_counts.len() {
        let (bits, _) = sorted_bit_counts[code as usize];
        dictionary.insert(bits, code);
        code += 1;
    }

    // ...and the rest are exceptions.
    let exception_count: usize = sorted_bit_counts
        .iter()
        .skip(code as _)
        .map(|(_, count)| *count)
        .sum();

    // Left bit-width is determined based on the actual dictionary size.
    let left_bw = dictionary.len().next_power_of_two().ilog2().max(1) as u8;

    let mut codes = vec![0; dictionary.len()];
    for (bits, code) in dictionary.iter() {
        codes[*code as usize] = *bits;
    }

    (
        ALPRDDictionary {
            dictionary,
            codes,
            right_bit_width: right_bw,
            left_bit_width: left_bw,
        },
        exception_count,
    )
}

/// Estimate the bits-per-value when using these compression settings.
fn estimate_compression_size(
    right_bw: u8,
    left_bw: u8,
    exception_count: usize,
    sample_n: usize,
) -> f64 {
    const EXC_POSITION_SIZE: usize = 16; // two bytes for exception position.
    const EXC_SIZE: usize = 16; // two bytes for each exception (up to 16 front bits).

    let exceptions_size = exception_count * (EXC_POSITION_SIZE + EXC_SIZE);
    (right_bw as f64) + (left_bw as f64) + ((exceptions_size as f64) / (sample_n as f64))
}

/// The ALP-RD dictionary, encoding the "left parts" and their dictionary encoding.
#[derive(Debug, Default)]
struct ALPRDDictionary {
    /// Items in the dictionary are bit patterns, along with their 16-bit encoding.
    dictionary: HashMap<u16, u16>,
    /// codes[i] = the left-bits pattern of the i-th code.
    codes: Vec<u16>,
    /// Recreate the dictionary by encoding the hash instead.
    /// The (compressed) left bit width. This is after bit-packing the dictionary codes.
    left_bit_width: u8,
    /// The right bit width. This is the bit-packed width of each of the "real double" values.
    right_bit_width: u8,
}
