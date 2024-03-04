impl ScalarAtFn for PrimitiveArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Box<dyn Scalar>> {
        if self.is_valid(index) {
            Ok(
                match_each_native_ptype!(self.ptype, |$T| self.buffer.typed_data::<$T>()
                    .get(index)
                    .unwrap()
                    .clone()
                    .into()
                ),
            )
        } else {
            Ok(NullableScalar::none(self.dtype().clone()).boxed())
        }
    }
}
