mod mutable;
#[derive(Clone)]
pub struct BoolArray {
    buffer: arrow2::array::BooleanArray,
}
