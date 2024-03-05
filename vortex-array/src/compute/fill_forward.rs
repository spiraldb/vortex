use crate::array::{Array, ArrayRef};
use crate::error::{VortexError, VortexResult};

pub trait FillForwardFn {
    fn fill_forward(&self) -> VortexResult<ArrayRef>;
}

/// Carries forward last non null value or 0 if there's no previous non null value
pub fn fill_forward(array: &dyn Array) -> VortexResult<ArrayRef> {
    array
        .fill_forward()
        .map(|t| t.fill_forward())
        .unwrap_or_else(|| {
            Err(VortexError::NotImplemented(
                "fill_forward",
                array.encoding().id(),
            ))
        })
}

#[cfg(test)]
mod test {
    use crate::array::downcast::DowncastArrayBuiltin;
    use crate::array::primitive::PrimitiveArray;
    use crate::compute;

    #[test]
    fn leading_null() {
        let arr = PrimitiveArray::from_iter(vec![None, Some(8u8), None, Some(10), None]);
        let filled = compute::fill_forward::fill_forward(arr.as_ref()).unwrap();
        assert_eq!(
            filled.as_primitive().typed_data::<u8>(),
            vec![0, 8, 8, 10, 10]
        );
    }
}
