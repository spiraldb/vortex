#![no_main]

use libfuzzer_sys::arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    let x = u8::arbitrary(&mut u).unwrap();
    if x % 2 == 0 {
        panic!("Even!");
    }
    // fuzzed code goes here
});
