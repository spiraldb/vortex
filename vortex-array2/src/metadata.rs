use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

/// Dynamic trait used to represent opaque owned Array metadata
/// Note that this allows us to restrict the ('static + Send + Sync) requirement to just the
/// metadata trait, and not the entire array trait.
#[allow(dead_code)]
pub trait ArrayMetadata: 'static + Send + Sync + Debug {
    fn as_any(&self) -> &dyn Any;
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync>;
    fn to_arc(&self) -> Arc<dyn ArrayMetadata>;
    fn into_arc(self) -> Arc<dyn ArrayMetadata>;
}
