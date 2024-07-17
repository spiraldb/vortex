//! Implementation of Dictionary encoding.
//!
//! Expose a [DictArray] which is zero-copy equivalent to Arrow's
//! [DictionaryArray](https://docs.rs/arrow/latest/arrow/array/struct.DictionaryArray.html).
pub use compress::*;
pub use dict::*;

mod compress;
mod compute;
mod dict;
mod stats;
mod variants;
