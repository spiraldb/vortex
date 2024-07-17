use datafusion_common::ScalarValue;
use datafusion_expr::{Expr, Operator};
use vortex::{
    array::{bool::BoolArray, constant::ConstantArray, null::NullArray, struct_::StructArray},
    compute::compare,
    validity::Validity,
    Array, IntoArray, IntoArrayVariant,
};
use vortex_error::{vortex_bail, VortexResult};

pub struct ExpressionEvaluator;

impl ExpressionEvaluator {
    pub fn eval(input: StructArray, expr: &Expr) -> VortexResult<Array> {
        match expr {
            Expr::BinaryExpr(expr) => {
                let lhs = expr.left.as_ref();
                let rhs = expr.right.as_ref();

                match expr.op {
                    Operator::And => {
                        let lhs = ExpressionEvaluator::eval(input.clone(), lhs)?.into_bool()?;
                        let rhs = ExpressionEvaluator::eval(input, rhs)?.into_bool()?;
                        let buffer = &lhs.boolean_buffer() & &rhs.boolean_buffer();
                        Ok(BoolArray::from(buffer).into_array())
                    }
                    Operator::Or => {
                        let lhs = ExpressionEvaluator::eval(input.clone(), lhs)?.into_bool()?;
                        let rhs = ExpressionEvaluator::eval(input, rhs)?.into_bool()?;
                        let buffer = &lhs.boolean_buffer() | &rhs.boolean_buffer();
                        Ok(BoolArray::from(buffer).into_array())
                    }
                    Operator::Eq => eval_eq_impl(&input, lhs, rhs),
                    _ => vortex_bail!("{} is an unsupported operator", expr.op),
                }
            }
            _ => unimplemented!("IMO it shouldn't get to this point"),
        }
    }
}

fn eval_eq_impl(input: &StructArray, lhs: &Expr, rhs: &Expr) -> VortexResult<Array> {
    match (lhs, rhs) {
        (Expr::Column(left), Expr::Column(right)) => {
            let lhs = input.field_by_name(left.name());
            let rhs = input.field_by_name(right.name());

            if let Some((lhs, rhs)) = lhs.zip(rhs) {
                compare(&lhs, &rhs, vortex_expr::Operator::Eq)
            } else {
                Ok(BoolArray::from_vec(vec![false; input.len()], Validity::AllValid).into_array())
            }
        }
        (Expr::Literal(l), Expr::Column(c)) | (Expr::Column(c), Expr::Literal(l)) => {
            if let Some(col) = input.field_by_name(c.name()) {
                let const_array = df_scalar_to_const_array(l, col.len())?;
                compare(&col, &const_array, vortex_expr::Operator::Eq)
            } else {
                Ok(BoolArray::from_vec(vec![false; input.len()], Validity::AllValid).into_array())
            }
        }
        _ => vortex_bail!("Unsupported expression combination for eq"),
    }
}

fn df_scalar_to_const_array(scalar: &ScalarValue, len: usize) -> VortexResult<Array> {
    let array = match scalar {
        ScalarValue::Null => None,
        ScalarValue::Boolean(b) => b.map(|b| ConstantArray::new(b, len).into_array()),
        ScalarValue::Float16(f) => f.map(|f| ConstantArray::new(f, len).into_array()),
        ScalarValue::Float32(f) => f.map(|f| ConstantArray::new(f, len).into_array()),
        ScalarValue::Float64(f) => f.map(|f| ConstantArray::new(f, len).into_array()),
        ScalarValue::Int8(i) => i.map(|i| ConstantArray::new(i, len).into_array()),
        ScalarValue::Int16(i) => i.map(|i| ConstantArray::new(i, len).into_array()),
        ScalarValue::Int32(i) => i.map(|i| ConstantArray::new(i, len).into_array()),
        ScalarValue::Int64(i) => i.map(|i| ConstantArray::new(i, len).into_array()),
        ScalarValue::UInt8(i) => i.map(|i| ConstantArray::new(i, len).into_array()),
        ScalarValue::UInt16(i) => i.map(|i| ConstantArray::new(i, len).into_array()),
        ScalarValue::UInt32(i) => i.map(|i| ConstantArray::new(i, len).into_array()),
        ScalarValue::UInt64(i) => i.map(|i| ConstantArray::new(i, len).into_array()),
        ScalarValue::Utf8(s) => s
            .as_ref()
            .map(|s| ConstantArray::new(s.as_str(), len).into_array()),
        ScalarValue::Utf8View(s) => s
            .as_ref()
            .map(|s| ConstantArray::new(s.as_str(), len).into_array()),
        ScalarValue::LargeUtf8(s) => s
            .as_ref()
            .map(|s| ConstantArray::new(s.as_str(), len).into_array()),
        ScalarValue::Binary(_b) => todo!(),
        ScalarValue::BinaryView(_b) => todo!(),
        ScalarValue::Decimal128(..) => todo!(),
        ScalarValue::Decimal256(..) => todo!(),
        ScalarValue::FixedSizeBinary(..) => todo!(),
        ScalarValue::LargeBinary(_b) => todo!(),
        ScalarValue::FixedSizeList(_) => todo!(),
        ScalarValue::List(_) => todo!(),
        ScalarValue::LargeList(_) => todo!(),
        ScalarValue::Struct(_) => todo!(),
        ScalarValue::Map(_) => todo!(),
        ScalarValue::Date32(_) => todo!(),
        ScalarValue::Date64(_) => todo!(),
        ScalarValue::Time32Second(_) => todo!(),
        ScalarValue::Time32Millisecond(_) => todo!(),
        ScalarValue::Time64Microsecond(_) => todo!(),
        ScalarValue::Time64Nanosecond(_) => todo!(),
        ScalarValue::TimestampSecond(..) => todo!(),
        ScalarValue::TimestampMillisecond(..) => todo!(),
        ScalarValue::TimestampMicrosecond(..) => todo!(),
        ScalarValue::TimestampNanosecond(..) => todo!(),
        ScalarValue::IntervalYearMonth(_) => todo!(),
        ScalarValue::IntervalDayTime(_) => todo!(),
        ScalarValue::IntervalMonthDayNano(_) => todo!(),
        ScalarValue::DurationSecond(_) => todo!(),
        ScalarValue::DurationMillisecond(_) => todo!(),
        ScalarValue::DurationMicrosecond(_) => todo!(),
        ScalarValue::DurationNanosecond(_) => todo!(),
        ScalarValue::Union(..) => todo!(),
        ScalarValue::Dictionary(..) => todo!(),
    };

    Ok(array.unwrap_or_else(|| NullArray::new(len).into_array()))
}
