pub use array::*;
use linkme::distributed_slice;
pub use typed::*;
use vortex_dtype::CompositeID;

mod array;
mod compute;
mod typed;

#[distributed_slice]
pub static VORTEX_COMPOSITE_EXTENSIONS: [&'static dyn CompositeExtension] = [..];

pub fn find_extension(id: &str) -> Option<&'static dyn CompositeExtension> {
    VORTEX_COMPOSITE_EXTENSIONS
        .iter()
        .find(|ext| ext.id().0 == id)
        .copied()
}

pub fn find_extension_id(id: &str) -> Option<CompositeID> {
    find_extension(id).map(|e| e.id())
}
