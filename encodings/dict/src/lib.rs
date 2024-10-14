//! Implementation of Dictionary encoding.
//!
//! Expose a [DictArray] which is zero-copy equivalent to Arrow's
//! [DictionaryArray](https://docs.rs/arrow/latest/arrow/array/struct.DictionaryArray.html).
pub use compress::*;
pub use array::*;

mod compress;
mod compute;
mod array;
mod stats;
mod variants;
