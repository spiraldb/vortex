pub mod convert;
pub mod wrappers;

#[macro_export]
macro_rules! match_arrow_numeric_type {
    ($self:expr, | $_:tt $enc:ident | $($body:tt)*) => ({
        macro_rules! __with__ {( $_ $enc:ident ) => ( $($body)* )}
        use $crate::dtype::DType::*;
        use $crate::dtype::IntWidth::*;
        use $crate::dtype::Signedness::*;
        use $crate::dtype::FloatWidth;
        use arrow_array::types::*;
        match $self {
            Int(_8, Unsigned, _) => __with__! {UInt8Type},
            Int(_16, Unsigned, _) => __with__!{UInt16Type},
            Int(_32, Unsigned, _) => __with__!{UInt32Type},
            Int(_64, Unsigned, _) => __with__!{UInt64Type},
            Int(_8, Signed, _) => __with__! {Int8Type},
            Int(_16, Signed, _) => __with__!{Int16Type},
            Int(_32, Signed, _) => __with__!{Int32Type},
            Int(_64, Signed, _) => __with__!{Int64Type},
            Float(FloatWidth::_16, _) => __with__!{Float16Type},
            Float(FloatWidth::_32, _) => __with__!{Float32Type},
            Float(FloatWidth::_64, _) => __with__!{Float64Type},
            _ => unimplemented!("Convert this DType to ArrowPrimitiveType")
        }
    })
}

pub use match_arrow_numeric_type;
