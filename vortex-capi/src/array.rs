use vortex::array::PrimitiveArray;
use vortex::{Array, IntoArray};
use vortex_dtype::{match_each_native_ptype, DType, Nullability};

use crate::VortexDType;

/// Opaque wrapper around a Vortex array.
pub struct VortexArray(Array);

impl From<Array> for VortexArray {
    fn from(inner: Array) -> Self {
        Self(inner)
    }
}

impl VortexArray {
    /// Access the inner Array.
    pub fn inner(&self) -> &Array {
        &self.0
    }

    /// Move out of self and receive ownership of the inner Array.
    pub fn into_inner(self) -> Array {
        self.0
    }
}

/// Create a new vortex array of primitive values.
#[no_mangle]
pub unsafe extern "C" fn vortex_array_new_primitive(
    dtype: *mut VortexDType,
    ptr: *const (),
    length: usize,
) -> *mut VortexArray {
    let dtype = unsafe {
        dtype
            .as_ref()
            .expect("unwrapping VortexDType pointer in vortex_array_new_primitive")
    };
    let DType::Primitive(ptype, nullability) = dtype.inner() else {
        panic!("dtype must be Primitive");
    };

    // Null values if we have to build a primitive array.

    let ptype = *ptype;
    let nullability = *nullability;

    // You need a null vector
    let array = match nullability {
        Nullability::NonNullable => {
            match_each_native_ptype!(ptype, |$P| {
                let ptr_cast: *const $P = ptr.cast();
                let mut values = Vec::<$P>::with_capacity(length);
                (0..length).for_each(|offset| {
                    let value = unsafe { ptr_cast.add(offset).read_unaligned() };
                    values.push(value)
                });
                PrimitiveArray::from_vec(values, vortex::validity::Validity::NonNullable).into_array()
            })
        }
        Nullability::Nullable => {
            match_each_native_ptype!(ptype, |$P| {
                let ptr_cast: *const $P = ptr.cast();
                let mut values = Vec::<Option<$P>>::with_capacity(length);
                (0..length).for_each(|offset| {
                    let value = unsafe { ptr_cast.add(offset).read_unaligned() };
                    values.push(value)
                });
                PrimitiveArray::from_nullable_vec(values, vortex::validity::Validity::NonNullable).into_array()
            })
        }
    };

    let array = Box::new(VortexArray(array));
    Box::into_raw(array)
}

/// Free the memory associated with the pointee Vortex array.
pub unsafe extern "C" fn vortex_array_free(array: *mut VortexArray) {
    // SAFETY: checked by the caller
    let boxed = unsafe { Box::from_raw(array) };
    drop(boxed);
}

#[cfg(test)]
mod test {
    use vortex::compute::unary::scalar_at_unchecked;

    use crate::array::vortex_array_new_primitive;
    use crate::{vortex_dtype_i16, vortex_dtype_info, DTYPE_PRIMITIVE_I16};

    #[test]
    fn test_create() {
        // Create a new pointer to a bunch of i32 values.
        let dtype = vortex_dtype_i16(false);

        unsafe {
            assert_eq!(vortex_dtype_info(dtype), DTYPE_PRIMITIVE_I16);

            let values: Vec<i16> = vec![1, 2, 3, 4];
            let array = vortex_array_new_primitive(dtype, values.as_ptr().cast(), values.len());
            let array_ref = array.as_ref().unwrap().inner();

            assert_eq!(array_ref.len(), 4);
            assert_eq!(scalar_at_unchecked(array_ref, 0), 1i16.into());
            assert_eq!(scalar_at_unchecked(array_ref, 1), 2i16.into());
            assert_eq!(scalar_at_unchecked(array_ref, 2), 3i16.into());
            assert_eq!(scalar_at_unchecked(array_ref, 3), 4i16.into());
        }
    }
}
