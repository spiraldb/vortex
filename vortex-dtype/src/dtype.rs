use std::fmt::{Debug, Display, Formatter};
use std::hash::Hash;
use std::sync::Arc;

use itertools::Itertools;
use serde::{Deserialize, Serialize};
use DType::*;

use crate::{CompositeID, Nullability, PType};

pub type FieldNames = Vec<Arc<String>>;

pub type Metadata = Vec<u8>;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DType {
    Null,
    Bool(Nullability),
    #[serde(with = "crate::serde::dtype_primitive")]
    Primitive(PType, Nullability),
    Utf8(Nullability),
    Binary(Nullability),
    Struct {
        names: FieldNames,
        dtypes: Vec<DType>,
    },
    List(Box<DType>, Nullability),
    Composite(CompositeID, Nullability),
}

impl DType {
    pub const BYTES: DType = Primitive(PType::U8, Nullability::NonNullable);

    /// The default DType for indices
    pub const IDX: DType = Primitive(PType::U64, Nullability::NonNullable);

    pub fn nullability(&self) -> Nullability {
        self.is_nullable().into()
    }

    pub fn is_nullable(&self) -> bool {
        use Nullability::*;

        match self {
            Null => true,
            Bool(n) => matches!(n, Nullable),
            Primitive(_, n) => matches!(n, Nullable),
            Utf8(n) => matches!(n, Nullable),
            Binary(n) => matches!(n, Nullable),
            Struct { dtypes, .. } => dtypes.iter().all(|dt| dt.is_nullable()),
            List(_, n) => matches!(n, Nullable),
            Composite(_, n) => matches!(n, Nullable),
        }
    }

    pub fn as_nonnullable(&self) -> Self {
        self.with_nullability(Nullability::NonNullable)
    }

    pub fn as_nullable(&self) -> Self {
        self.with_nullability(Nullability::Nullable)
    }

    pub fn with_nullability(&self, nullability: Nullability) -> Self {
        match self {
            Null => Null,
            Bool(_) => Bool(nullability),
            Primitive(p, _) => Primitive(*p, nullability),
            Utf8(_) => Utf8(nullability),
            Binary(_) => Binary(nullability),
            Struct { names, dtypes } => Struct {
                names: names.clone(),
                dtypes: dtypes
                    .iter()
                    .map(|dt| dt.with_nullability(nullability))
                    .collect(),
            },
            List(c, _) => List(c.clone(), nullability),
            Composite(id, _) => Composite(id.clone(), nullability),
        }
    }

    pub fn eq_ignore_nullability(&self, other: &Self) -> bool {
        self.as_nullable().eq(&other.as_nullable())
    }
}

impl Display for DType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Null => write!(f, "null"),
            Bool(n) => write!(f, "bool{}", n),
            Primitive(p, n) => write!(f, "{}{}", p, n),
            Utf8(n) => write!(f, "utf8{}", n),
            Binary(n) => write!(f, "binary{}", n),
            Struct { names, dtypes } => write!(
                f,
                "{{{}}}",
                names
                    .iter()
                    .zip(dtypes.iter())
                    .map(|(n, dt)| format!("{}={}", n, dt))
                    .join(", ")
            ),
            List(c, n) => write!(f, "list({}){}", c, n),
            Composite(id, n) => write!(f, "<{}>{}", id, n),
        }
    }
}

#[cfg(test)]
mod test {
    use std::mem;

    use crate::dtype::DType;

    #[test]
    fn size_of() {
        assert_eq!(mem::size_of::<DType>(), 48);
    }
}
