use take::TakeFn;

pub mod add;
pub mod as_contiguous;
pub mod cast;
pub mod repeat;
pub mod search_sorted;
pub mod take;

pub trait ArrayCompute {
    fn take(&self) -> Option<&dyn TakeFn> {
        None
    }
}
