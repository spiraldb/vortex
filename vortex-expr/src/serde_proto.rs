#![cfg(feature = "proto")]

use vortex_error::{vortex_bail, vortex_err, VortexError};

use crate::proto::expr as pb;
use crate::proto::expr::predicate::Rhs;
use crate::{Operator, Predicate, Value};

impl TryFrom<&pb::Predicate> for Predicate {
    type Error = VortexError;

    fn try_from(value: &pb::Predicate) -> Result<Self, Self::Error> {
        Ok(Predicate {
            lhs: value
                .lhs
                .as_ref()
                .ok_or_else(|| vortex_err!(InvalidSerde: "Lhs is missing"))?
                .try_into()?,
            op: value.op().try_into()?,
            rhs: match value
                .rhs
                .as_ref()
                .ok_or_else(|| vortex_err!(InvalidSerde: "Rhs is missing"))?
            {
                Rhs::Field(f) => Value::Field(f.try_into()?),
                Rhs::Scalar(scalar) => Value::Literal(scalar.try_into()?),
            },
        })
    }
}

impl TryFrom<pb::Operator> for Operator {
    type Error = VortexError;

    fn try_from(value: pb::Operator) -> Result<Self, Self::Error> {
        match value {
            pb::Operator::Unknown => {
                vortex_bail!(InvalidSerde: "Unknown operator {}", value.as_str_name())
            }
            pb::Operator::Eq => Ok(Self::Eq),
            pb::Operator::Neq => Ok(Self::NotEq),
            pb::Operator::Lt => Ok(Self::Lt),
            pb::Operator::Lte => Ok(Self::Lte),
            pb::Operator::Gt => Ok(Self::Gt),
            pb::Operator::Gte => Ok(Self::Gte),
        }
    }
}
