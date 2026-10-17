// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Vortex mappings for the function package's semantic capabilities.

use rowfn::InputBinding;
use rowfn::Utf8;
use vortex_array::ArrayRef;
use vortex_array::ExecutionCtx;
use vortex_array::arrays::ExtensionArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::extension::ExtensionArrayExt;
use vortex_array::dtype::DType;
use vortex_array::dtype::Nullability;
use vortex_array::dtype::PType;
use vortex_array::extension::datetime::TimeUnit;
use vortex_array::extension::datetime::Timestamp;
use vortex_array::scalar_fn::unstable::rowfn::VortexHost;
use vortex_buffer::Buffer;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_ensure;

use super::TicksView;
use crate::ComparisonHost;
use crate::ComparisonKind;
use crate::FloatWidth;
use crate::Integer;
use crate::IntegerHost;
use crate::ListHost;
use crate::TextComparisonHost;
use crate::TextLengthHost;
use crate::TimestampHost;
use crate::TimestampTicks;

impl IntegerHost for VortexHost {
    fn integer(dtype: &DType) -> VortexResult<Integer> {
        Ok(match dtype {
            DType::Primitive(PType::I8, _) => Integer::I8,
            DType::Primitive(PType::I16, _) => Integer::I16,
            DType::Primitive(PType::I32, _) => Integer::I32,
            DType::Primitive(PType::I64, _) => Integer::I64,
            DType::Primitive(PType::U8, _) => Integer::U8,
            DType::Primitive(PType::U16, _) => Integer::U16,
            DType::Primitive(PType::U32, _) => Integer::U32,
            DType::Primitive(PType::U64, _) => Integer::U64,
            _ => vortex_bail!("expected semantic integer input, got {dtype}"),
        })
    }
}

impl ComparisonHost for VortexHost {
    fn comparison_kind(dtype: &DType) -> VortexResult<ComparisonKind> {
        match dtype {
            DType::Primitive(PType::F32, _) => Ok(ComparisonKind::Float32),
            DType::Primitive(PType::F64, _) => Ok(ComparisonKind::Float64),
            _ => Self::integer(dtype).map(ComparisonKind::Integer),
        }
    }
}

impl TextComparisonHost for VortexHost {
    fn validate_text_pair(lhs: &DType, rhs: &DType) -> VortexResult<()> {
        if !matches!(lhs, DType::Utf8(_)) {
            vortex_bail!("expected UTF-8 comparison input, got {lhs}");
        }

        if !matches!(rhs, DType::Utf8(_)) {
            vortex_bail!("expected UTF-8 comparison input, got {rhs}");
        }

        Ok(())
    }
}

impl ListHost for VortexHost {
    fn list_shape(dtype: &DType) -> VortexResult<(FloatWidth, usize)> {
        match dtype {
            DType::FixedSizeList(child, width, _) => match child.as_ref() {
                DType::Primitive(PType::F32, _) => {
                    <Self as InputBinding<rowfn::FixedSizeList<f32>>>::validate(dtype)?;
                    Ok((FloatWidth::F32, *width as usize))
                }
                DType::Primitive(PType::F64, _) => {
                    <Self as InputBinding<rowfn::FixedSizeList<f64>>>::validate(dtype)?;
                    Ok((FloatWidth::F64, *width as usize))
                }
                _ => vortex_bail!("expected float list children, got {child}"),
            },
            _ => vortex_bail!("expected fixed-size list, got {dtype}"),
        }
    }
}

impl TimestampHost for VortexHost {
    fn timestamp_label(dtype: &DType) -> VortexResult<DType> {
        let DType::Extension(extension) = dtype else {
            vortex_bail!("expected timestamp extension, got {dtype}");
        };
        let Some(metadata) = extension.metadata_opt::<Timestamp>() else {
            vortex_bail!("unknown extension is not timestamp ticks");
        };
        vortex_ensure!(
            metadata.unit != TimeUnit::Days,
            "timestamp tick adapter does not support day units"
        );

        Ok(dtype.with_nullability(Nullability::NonNullable))
    }
}

impl InputBinding<TimestampTicks> for VortexHost {
    type Decoded = Buffer<i64>;
    type View<'a> = TicksView<'a>;
    const DENSE_SAFE: bool = true;
    const DECODE_INFALLIBLE: bool = true;

    fn validate(dtype: &DType) -> VortexResult<()> {
        Self::timestamp_label(dtype).map(|_| ())
    }

    fn decode(column: &ArrayRef, scalar: bool, ctx: &mut ExecutionCtx) -> VortexResult<Buffer<i64>> {
        let column = vortex_array::scalar_fn::unstable::rowfn::decoded_input(column, scalar)?;
        let extension = column.execute::<ExtensionArray>(ctx)?;
        Ok(extension
            .storage_array()
            .clone()
            .execute::<PrimitiveArray>(ctx)?
            .into_buffer::<i64>())
    }

    fn can_decode_null_tolerant(_: &ArrayRef) -> VortexResult<bool> {
        Ok(true)
    }

    fn view(decoded: &Buffer<i64>) -> TicksView<'_> {
        TicksView(decoded.as_slice())
    }
}

impl TextLengthHost for VortexHost {
    fn large_utf8(dtype: &DType) -> VortexResult<bool> {
        <Self as InputBinding<Utf8>>::validate(dtype)?;
        Ok(false)
    }
}
