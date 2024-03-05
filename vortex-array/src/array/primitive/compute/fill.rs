use crate::array::primitive::PrimitiveArray;
use crate::array::ArrayRef;
use crate::compute::fill::FillForwardFn;
use crate::error::VortexResult;

impl FillForwardFn for PrimitiveArray {
    fn fill_forward(&self) -> VortexResult<ArrayRef> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use crate::array::downcast::DowncastArrayBuiltin;
    use crate::array::primitive::PrimitiveArray;
    use crate::compute;

    #[test]
    fn leading_none() {
        let arr = PrimitiveArray::from_iter(vec![None, Some(8u8), None, Some(10), None]);
        let filled = compute::fill::fill_forward(arr.as_ref()).unwrap();
        let filled_primitive = filled.as_primitive();
        assert_eq!(filled_primitive.typed_data::<u8>(), vec![0, 8, 8, 10, 10]);
        assert!(filled_primitive.validity().is_none());
    }
}
