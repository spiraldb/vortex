//! Implementation of Dictionary encoding.
//!
//! Expose a [DictArray] which is zero-copy equivalent to Arrow's
//! [DictionaryArray](https://docs.rs/arrow/latest/arrow/array/struct.DictionaryArray.html).
pub use array::*;
pub use compress::*;

mod array;
mod compress;
mod compute;
mod stats;
mod variants;
