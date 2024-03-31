use std::fmt::Debug;

mod view;
pub use view::*;
use vortex_error::VortexResult;
use vortex_schema::DType;

mod context;
mod vtable;
pub use context::*;

pub use vtable::*;

pub trait ArrayMetadata: Debug + Send + Sync + Sized {
    fn to_bytes(&self) -> Option<Vec<u8>>;

    fn try_from_bytes<'a>(bytes: Option<&'a [u8]>, dtype: &DType) -> VortexResult<Self>;
}
