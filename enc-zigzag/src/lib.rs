use enc::array::EncodingProvider;
pub use zigzag::*;

mod compress;
mod stats;
mod zigzag;

inventory::submit! {
    EncodingProvider::new(&ZigZagEncoding)
}
