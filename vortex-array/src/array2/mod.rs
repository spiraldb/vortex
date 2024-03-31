use std::fmt::Debug;

mod data;
#[allow(unused_imports)]
pub use data::*;
mod view;
pub use view::*;
use vortex_error::VortexResult;
use vortex_schema::DType;

mod typed_view;
pub use typed_view::*;
mod context;
mod vtable;
pub use context::*;

pub use vtable::*;

pub trait ArrayMetadata: Debug + Send + Sync + Sized {
    fn to_bytes(&self) -> Option<Vec<u8>>;

    fn try_from_bytes<'a>(bytes: Option<&'a [u8]>, dtype: &DType) -> VortexResult<Self>;
}

pub trait ArrayEncoding {
    type Metadata: ArrayMetadata;
}
