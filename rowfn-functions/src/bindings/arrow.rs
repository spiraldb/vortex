// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Arrow mappings for the function package's semantic capabilities.

use arrow_array::ArrayRef as ArrowArrayRef;
use arrow_array::PrimitiveArray as ArrowPrimitiveArray;
use arrow_array::types::TimestampMicrosecondType;
use arrow_array::types::TimestampMillisecondType;
use arrow_array::types::TimestampNanosecondType;
use arrow_array::types::TimestampSecondType;
use arrow_buffer::ScalarBuffer;
use arrow_schema::ArrowError;
use arrow_schema::DataType;
use arrow_schema::Field;
use arrow_schema::TimeUnit as ArrowTimeUnit;
use rowfn::Host;
use rowfn::InputBinding;
use rowfn::Utf8;
use rowfn_arrow::ArrowHost;

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

impl IntegerHost for ArrowHost {
    fn integer(dtype: &Field) -> Result<Integer, ArrowError> {
        if dtype.metadata().contains_key("ARROW:extension:name") {
            return Err(Self::error("unknown extension is not an integer domain"));
        }

        Ok(match dtype.data_type() {
            DataType::Int8 => Integer::I8,
            DataType::Int16 => Integer::I16,
            DataType::Int32 => Integer::I32,
            DataType::Int64 => Integer::I64,
            DataType::UInt8 => Integer::U8,
            DataType::UInt16 => Integer::U16,
            DataType::UInt32 => Integer::U32,
            DataType::UInt64 => Integer::U64,
            _ => return Err(Self::error("expected semantic integer input")),
        })
    }
}

impl ComparisonHost for ArrowHost {
    fn comparison_kind(dtype: &Field) -> Result<ComparisonKind, ArrowError> {
        if dtype.metadata().contains_key("ARROW:extension:name") {
            return Err(Self::error("unknown extension is not a numeric comparison domain"));
        }

        match dtype.data_type() {
            DataType::Float32 => Ok(ComparisonKind::Float32),
            DataType::Float64 => Ok(ComparisonKind::Float64),
            _ => Self::integer(dtype).map(ComparisonKind::Integer),
        }
    }
}

impl TextComparisonHost for ArrowHost {
    fn validate_text_pair(lhs: &Field, rhs: &Field) -> Result<(), ArrowError> {
        if lhs.metadata().contains_key("ARROW:extension:name")
            || rhs.metadata().contains_key("ARROW:extension:name")
            || !matches!(lhs.data_type(), DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)
            || lhs.data_type() != rhs.data_type()
        {
            return Err(Self::error("Arrow string comparisons require matching non-extension types"));
        }

        Ok(())
    }
}

impl ListHost for ArrowHost {
    fn list_shape(dtype: &Field) -> Result<(FloatWidth, usize), ArrowError> {
        let DataType::FixedSizeList(child, width) = dtype.data_type() else {
            return Err(Self::error("expected fixed-size list"));
        };
        let width = usize::try_from(*width).map_err(|_| Self::error("list width must be nonnegative"))?;

        match child.data_type() {
            DataType::Float32 => {
                <Self as InputBinding<rowfn::FixedSizeList<f32>>>::validate(dtype)?;
                Ok((FloatWidth::F32, width))
            }
            DataType::Float64 => {
                <Self as InputBinding<rowfn::FixedSizeList<f64>>>::validate(dtype)?;
                Ok((FloatWidth::F64, width))
            }
            _ => Err(Self::error("expected float list children")),
        }
    }
}

impl TimestampHost for ArrowHost {
    fn timestamp_label(dtype: &Field) -> Result<Field, ArrowError> {
        if !matches!(dtype.data_type(), DataType::Timestamp(_, _))
            || dtype.metadata().contains_key("ARROW:extension:name")
        {
            return Err(Self::error("expected native Arrow timestamp ticks"));
        }

        Ok(dtype.clone().with_name("result").with_nullable(false))
    }
}

impl InputBinding<TimestampTicks> for ArrowHost {
    type Decoded = ScalarBuffer<i64>;
    type View<'a> = TicksView<'a>;
    const DENSE_SAFE: bool = true;
    const DECODE_INFALLIBLE: bool = true;

    fn validate(dtype: &Field) -> Result<(), ArrowError> {
        Self::timestamp_label(dtype).map(|_| ())
    }

    fn decode(column: &ArrowArrayRef, _: bool, _: &mut ()) -> Result<ScalarBuffer<i64>, ArrowError> {
        macro_rules! ticks {
            ($t:ty) => {
                column
                    .as_any()
                    .downcast_ref::<ArrowPrimitiveArray<$t>>()
                    .map(|array| array.values().clone())
                    .ok_or_else(|| Self::error("timestamp storage downcast failed"))
            };
        }

        match column.data_type() {
            DataType::Timestamp(ArrowTimeUnit::Second, _) => ticks!(TimestampSecondType),
            DataType::Timestamp(ArrowTimeUnit::Millisecond, _) => ticks!(TimestampMillisecondType),
            DataType::Timestamp(ArrowTimeUnit::Microsecond, _) => ticks!(TimestampMicrosecondType),
            DataType::Timestamp(ArrowTimeUnit::Nanosecond, _) => ticks!(TimestampNanosecondType),
            _ => Err(Self::error("expected timestamp storage")),
        }
    }

    fn can_decode_null_tolerant(_: &ArrowArrayRef) -> Result<bool, ArrowError> {
        Ok(true)
    }

    fn view(decoded: &ScalarBuffer<i64>) -> TicksView<'_> {
        TicksView(decoded.as_ref())
    }
}

impl TextLengthHost for ArrowHost {
    fn large_utf8(dtype: &Field) -> Result<bool, ArrowError> {
        <Self as InputBinding<Utf8>>::validate(dtype)?;
        Ok(dtype.data_type() == &DataType::LargeUtf8)
    }
}
