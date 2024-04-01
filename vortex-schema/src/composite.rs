use crate::CompositeID;
use linkme::distributed_slice;
use std::fmt::Debug;

pub trait CompositeExtension: Debug + Send + Sync + 'static {
    fn id(&self) -> CompositeID;

    // fn as_typed_compute(&self, array: &CompositeArray) -> Box<dyn ArrayCompute>;
}

#[distributed_slice]
pub static COMPOSITE_EXTENSIONS: [&'static dyn CompositeExtension] = [..];

pub fn find_extension(id: &str) -> Option<&'static dyn CompositeExtension> {
    COMPOSITE_EXTENSIONS
        .iter()
        .find(|ext| ext.id().0 == id)
        .copied()
}

pub fn find_extension_id(id: &str) -> Option<CompositeID> {
    find_extension(id).map(|e| e.id())
}
