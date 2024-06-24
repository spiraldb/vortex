//! Implementation of Dictionary encoding.
//!
//! Expose a [DictArray] which is zero-copy equivalent to Arrow's
//! [arrow_array::array::DictionaryArray] type.
pub use compress::*;
pub use dict::*;

mod compress;
mod compute;
mod dict;
mod stats;
