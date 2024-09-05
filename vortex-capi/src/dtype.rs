use vortex_dtype::{DType, PType};

/// Opaque wrapper over a Vortex DType.
// #[repr(C)]
pub struct VortexDType(DType);

impl VortexDType {
    /// Move out of self to access the inner DType.
    pub fn into_inner(self) -> DType {
        self.0
    }

    /// Access the inner DType by reference.
    pub fn inner(&self) -> &DType {
        &self.0
    }
}

/// Free the VortexDType memory.
#[no_mangle]
pub unsafe extern "C" fn vortex_dtype_free(dtype_ptr: *mut VortexDType) {
    // SAFETY: checked by the caller.
    let boxed = unsafe { Box::from_raw(dtype_ptr) };
    drop(boxed);
}

#[no_mangle]
pub unsafe extern "C" fn vortex_dtype_is_nullable(dtype_ptr: *mut VortexDType) -> bool {
    // SAFETY: checked by the caller.
    unsafe {
        dtype_ptr
            .as_ref()
            .expect("unwrapping VortexDType pointer in vortex_dtype_nullable")
            .inner()
            .is_nullable()
    }
}

#[no_mangle]
pub unsafe extern "C" fn vortex_dtype_info(dtype: *mut VortexDType) -> u8 {
    // SAFETY: enforced by the caller.
    let dtype = unsafe {
        dtype
            .as_ref()
            .expect("unwrapping VortexDType pointer in vortex_dtype_info")
    };

    match dtype.inner() {
        DType::Null => DTYPE_NULL,
        DType::Bool(_) => DTYPE_BOOL,
        DType::Primitive(ptype, _) => match ptype {
            PType::U8 => DTYPE_PRIMITIVE_U8,
            PType::U16 => DTYPE_PRIMITIVE_U16,
            PType::U32 => DTYPE_PRIMITIVE_U32,
            PType::U64 => DTYPE_PRIMITIVE_U64,
            PType::I8 => DTYPE_PRIMITIVE_I8,
            PType::I16 => DTYPE_PRIMITIVE_I16,
            PType::I32 => DTYPE_PRIMITIVE_I32,
            PType::I64 => DTYPE_PRIMITIVE_I64,
            PType::F16 => DTYPE_PRIMITIVE_F16,
            PType::F32 => DTYPE_PRIMITIVE_F32,
            PType::F64 => DTYPE_PRIMITIVE_F64,
        },
        DType::Utf8(_) => DTYPE_UTF8,
        DType::Binary(_) => DTYPE_BINARY,
        DType::Struct(..) => DTYPE_STRUCT,
        DType::List(..) => DTYPE_LIST,
        DType::Extension(..) => DTYPE_EXTENSION,
    }
}

macro_rules! make_dtype_fn {
    ($name:ident, $dtype:path) => {
        paste::paste! {
            #[doc = concat!("Create a new ", stringify!($dtype), " with optional nullability")]
            #[no_mangle]
            pub extern "C" fn [<vortex_dtype_ $name>](nullable: bool) -> *mut VortexDType {
                let dtype = Box::new(VortexDType($dtype(nullable.into())));

                Box::into_raw(dtype)
            }
        }
    };
}

make_dtype_fn!(bool, DType::Bool);
make_dtype_fn!(binary, DType::Binary);
make_dtype_fn!(utf8, DType::Utf8);

macro_rules! make_primitive_dtype_fn {
    ($ptype:expr, $name:ident) => {
        paste::paste! {
            #[doc = concat!("Create a new DType::Primitive(", stringify!($ptype), ") with optional nullability")]
            #[no_mangle]
            pub extern "C" fn [<vortex_dtype_ $name>](nullable: bool) -> *mut VortexDType {
                let dtype = Box::new(VortexDType(DType::Primitive($ptype, nullable.into())));
                Box::into_raw(dtype)
            }
        }
    };
}

make_primitive_dtype_fn!(PType::U8, u8);
make_primitive_dtype_fn!(PType::U16, u16);
make_primitive_dtype_fn!(PType::U32, u32);
make_primitive_dtype_fn!(PType::U64, u64);
make_primitive_dtype_fn!(PType::I8, i8);
make_primitive_dtype_fn!(PType::I16, i16);
make_primitive_dtype_fn!(PType::I32, i32);
make_primitive_dtype_fn!(PType::I64, i64);
make_primitive_dtype_fn!(PType::F32, f32);
make_primitive_dtype_fn!(PType::F64, f64);

pub const DTYPE_PRIMITIVE_U8: u8 = 0;
pub const DTYPE_PRIMITIVE_U16: u8 = 1;
pub const DTYPE_PRIMITIVE_U32: u8 = 2;
pub const DTYPE_PRIMITIVE_U64: u8 = 3;
pub const DTYPE_PRIMITIVE_I8: u8 = 4;
pub const DTYPE_PRIMITIVE_I16: u8 = 5;
pub const DTYPE_PRIMITIVE_I32: u8 = 6;
pub const DTYPE_PRIMITIVE_I64: u8 = 7;
pub const DTYPE_PRIMITIVE_F16: u8 = 8;
pub const DTYPE_PRIMITIVE_F32: u8 = 9;
pub const DTYPE_PRIMITIVE_F64: u8 = 10;
pub const DTYPE_BOOL: u8 = 11;
pub const DTYPE_BINARY: u8 = 12;
pub const DTYPE_UTF8: u8 = 13;
pub const DTYPE_STRUCT: u8 = 14;
pub const DTYPE_LIST: u8 = 15;
pub const DTYPE_EXTENSION: u8 = 16;
pub const DTYPE_NULL: u8 = 17;

#[cfg(test)]
mod test {
    use crate::{
        DTYPE_PRIMITIVE_F32, vortex_dtype_free, vortex_dtype_f32, vortex_dtype_info,
        vortex_dtype_is_nullable,
    };

    #[test]
    fn test_dtype_create_destroy() {
        // Make sure create and destroy work as expected.
        let dtype = vortex_dtype_f32(false);
        unsafe { vortex_dtype_free(dtype) };
    }

    #[test]
    fn test_dtype_info() {
        let dtype = vortex_dtype_f32(false);
        unsafe {
            assert_eq!(vortex_dtype_info(dtype), DTYPE_PRIMITIVE_F32);
            assert!(!vortex_dtype_is_nullable(dtype));
            vortex_dtype_free(dtype);
        }
    }
}
