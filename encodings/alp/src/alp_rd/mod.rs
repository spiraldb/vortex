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

// mod array;

use std::collections::HashMap;

use itertools::Itertools;
use vortex::array::PrimitiveArray;
use vortex::{Array, IntoArray};

/// Max number of bits to cut from the MSB section of each float.
const CUT_LIMIT: usize = 16;

const SAMPLE_SIZE: usize = 32;

/// Encode an array.
///
/// # Returns
///
/// Returns a tuple containing the left-parts array, the right-parts, and the exceptions.
pub fn alp_rd_encode_f64(values: &[f64]) -> (Array, Array, Array, Array) {
    let sample = (values.len() > SAMPLE_SIZE).then(|| {
        values
            .iter()
            .step_by(values.len() / SAMPLE_SIZE)
            .cloned()
            .collect_vec()
    });

    // We want a set of left and right parts here as well.
    let dictionary = find_best_dictionary(sample.as_deref().unwrap_or(values));
    println!("BEST_DICT: {dictionary:?}");
    let mut left_parts: Vec<u16> = Vec::with_capacity(values.len());
    let mut right_parts: Vec<u64> = Vec::with_capacity(values.len());
    let mut exceptions_pos: Vec<u32> = Vec::with_capacity(values.len() / 4);
    let mut exceptions: Vec<u16> = Vec::with_capacity(values.len() / 4);

    let right_mask = (1u64 << dictionary.right_bit_width) - 1;
    for v in values.iter().copied() {
        // Left contains the last valid left-value, else a code indicating the highest unused code
        // position.
        right_parts.push(v.to_bits() & right_mask);
        left_parts.push((v.to_bits() >> dictionary.right_bit_width) as u16);
    }

    // dict-encode the left-parts, keeping track of exceptions
    for (idx, left) in left_parts.iter_mut().enumerate() {
        // TODO: revisit if we need to change the branch order for perf.
        if let Some(code) = dictionary.dictionary.get(left) {
            *left = *code;
        } else {
            exceptions.push(*left);
            exceptions_pos.push(idx as u32);

            *left = dictionary.dictionary.len() as u16;
        }
    }

    // we need to return the left-parts (codes), the right-parts (infallibly bit-packed values),
    // the exception positions, and the exceptions. We can encode the exceptions as a Sparse
    // array of u16.

    (
        PrimitiveArray::from(left_parts).into_array(),
        PrimitiveArray::from(right_parts).into_array(),
        PrimitiveArray::from(exceptions_pos).into_array(),
        PrimitiveArray::from(exceptions).into_array(),
    )
}

/// Find the best "cut point" for a set of floating point values such that we can
/// cast them all to the relevant value instead.
fn find_best_dictionary(samples: &[f64]) -> ALPRDDictionary {
    let mut best_est_size = f64::MAX;
    let mut best_dict: Option<ALPRDDictionary> = None;

    for p in 1u8..=16 {
        let candidate_right_bw = 64 - p;
        println!("trying candidated_right_bw {candidate_right_bw}");
        let (dictionary, exception_count) =
            build_left_parts_dictionary(samples, candidate_right_bw, 8);
        let estimated_size = estimate_compression_size(
            dictionary.right_bit_width,
            dictionary.left_bit_width,
            exception_count,
            samples.len(),
        );
        if estimated_size < best_est_size {
            println!("new best: right_bw={candidate_right_bw}");
            best_est_size = estimated_size;
            best_dict = Some(dictionary);
        }
    }

    best_dict.expect("ALP-RD should find at least one dictionary")
}

/// Build dictionary of the left-side of the floats.
fn build_left_parts_dictionary(
    samples: &[f64],
    right_bw: u8,
    dict_size: u8,
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
    let mut dictionary = HashMap::with_capacity(dict_size as _);
    let mut code = 0u16;
    while code < (dict_size as _) && (code as usize) < sorted_bit_counts.len() {
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
    let left_bw = dictionary.len().next_power_of_two().ilog2() as u8;
    println!(
        "FOUND: dict.len() = {} => left_bw of {}",
        dictionary.len(),
        left_bw
    );

    (
        ALPRDDictionary {
            dictionary,
            right_bit_width: right_bw,
            left_bit_width: left_bw,
        },
        exception_count,
    )
}

// Estimate the bits-per-value that the given encoding collects
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
#[derive(Debug)]
pub(crate) struct ALPRDDictionary {
    /// Items in the dictionary are bit patterns, along with their 16-bit encoding.
    dictionary: HashMap<u16, u16>,
    /// Recreate the dictionary by encoding the hash instead.
    /// The (compressed) left bit width. This is after bit-packing the dictionary codes.
    left_bit_width: u8,
    /// The right bit width. This is the bit-packed width of each of the "real double" values.
    right_bit_width: u8,
}

#[cfg(test)]
mod test {
    use vortex::array::PrimitiveArray;

    use crate::alp_rd::alp_rd_encode_f64;

    #[test]
    fn test_encode() {
        // get the left and right parts from here
        let (left, right, exc_pos, exc) = alp_rd_encode_f64(&[
            1.12384859111099191f64,
            2.12384859111099191f64,
            3.12384859111099191f64,
            3.12384859111099191f64,
            4.12384859111099191f64,
            5.12384859111099191f64,
            6.12384859111099191f64,
            7.12384859111099191f64,
            8.12384859111099191f64,
            9.12384859111099191f64,
            10.12384859111099191f64,
        ]);

        println!(
            "left: {:?}",
            PrimitiveArray::try_from(left)
                .unwrap()
                .maybe_null_slice::<u16>()
        );
        println!(
            "right: {:?}",
            PrimitiveArray::try_from(right)
                .unwrap()
                .maybe_null_slice::<u64>()
        );
        println!(
            "exc_pos: {:?}",
            PrimitiveArray::try_from(exc_pos)
                .unwrap()
                .maybe_null_slice::<u32>()
        );
        println!(
            "excs: {:?}",
            PrimitiveArray::try_from(exc)
                .unwrap()
                .maybe_null_slice::<u16>()
        );
        panic!("print");
    }
}
