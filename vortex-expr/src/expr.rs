use std::any::Any;
use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::Arc;

use vortex::array::{ConstantArray, StructArray};
use vortex::compute::{compare, Operator as ArrayOperator};
use vortex::variants::StructArrayTrait;
use vortex::{Array, ArrayDType, IntoArray};
use vortex_dtype::field::Field;
use vortex_error::{vortex_err, VortexExpect as _, VortexResult};
use vortex_scalar::Scalar;

use crate::Operator;

pub trait VortexExpr: Debug + Send + Sync + PartialEq<dyn Any> {
    fn as_any(&self) -> &dyn Any;

    fn evaluate(&self, batch: &Array) -> VortexResult<Array>;

    fn references(&self) -> HashSet<Field>;

    fn project(&self, projection: &[Field]) -> Option<Arc<dyn VortexExpr>>;

    fn is_constant(&self) -> bool;
}

// Taken from apache-datafusion, necessary since you can't require VortexExpr implement PartialEq<dyn VortexExpr>
fn unbox_any(any: &dyn Any) -> &dyn Any {
    if any.is::<Arc<dyn VortexExpr>>() {
        any.downcast_ref::<Arc<dyn VortexExpr>>()
            .vortex_expect("any.is::<Arc<dyn VortexExpr>> returned true but downcast_ref failed")
            .as_any()
    } else if any.is::<Box<dyn VortexExpr>>() {
        any.downcast_ref::<Box<dyn VortexExpr>>()
            .vortex_expect("any.is::<Box<dyn VortexExpr>> returned true but downcast_ref failed")
            .as_any()
    } else {
        any
    }
}

#[derive(Debug, Clone)]
pub struct BinaryExpr {
    lhs: Arc<dyn VortexExpr>,
    operator: Operator,
    rhs: Arc<dyn VortexExpr>,
}

impl BinaryExpr {
    pub fn new(lhs: Arc<dyn VortexExpr>, operator: Operator, rhs: Arc<dyn VortexExpr>) -> Self {
        Self { lhs, operator, rhs }
    }

    pub fn lhs(&self) -> &Arc<dyn VortexExpr> {
        &self.lhs
    }

    pub fn rhs(&self) -> &Arc<dyn VortexExpr> {
        &self.rhs
    }

    pub fn op(&self) -> Operator {
        self.operator
    }
}

impl VortexExpr for BinaryExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn evaluate(&self, batch: &Array) -> VortexResult<Array> {
        let lhs = self.lhs.evaluate(batch)?;
        let rhs = self.rhs.evaluate(batch)?;

        let array = match self.operator {
            Operator::Eq => compare(lhs, rhs, ArrayOperator::Eq)?,
            Operator::NotEq => compare(lhs, rhs, ArrayOperator::NotEq)?,
            Operator::Lt => compare(lhs, rhs, ArrayOperator::Lt)?,
            Operator::Lte => compare(lhs, rhs, ArrayOperator::Lte)?,
            Operator::Gt => compare(lhs, rhs, ArrayOperator::Gt)?,
            Operator::Gte => compare(lhs, rhs, ArrayOperator::Gte)?,
            Operator::And => vortex::compute::and(lhs, rhs)?,
            Operator::Or => vortex::compute::or(lhs, rhs)?,
        };

        Ok(array)
    }

    fn references(&self) -> HashSet<Field> {
        let mut res = self.lhs.references();
        res.extend(self.rhs.references());
        res
    }

    fn project(&self, projection: &[Field]) -> Option<Arc<dyn VortexExpr>> {
        let lhs_proj = self.lhs.project(projection);
        let rhs_proj = self.rhs.project(projection);
        if self.operator == Operator::And {
            if let Some(lhsp) = lhs_proj {
                if let Some(rhsp) = rhs_proj {
                    Some(Arc::new(BinaryExpr::new(lhsp, self.operator, rhsp)))
                } else {
                    // TODO(robert): This might be too broad of a check since it should be limited only to fields in the projection
                    self.rhs
                        .references()
                        .intersection(&lhsp.references())
                        .next()
                        .is_none()
                        .then_some(lhsp)
                }
            } else if self
                .lhs
                .references()
                .intersection(&self.rhs.references())
                .next()
                .is_none()
            {
                rhs_proj
            } else {
                None
            }
        } else {
            Some(Arc::new(BinaryExpr::new(
                lhs_proj?,
                self.operator,
                rhs_proj?,
            )))
        }
    }

    fn is_constant(&self) -> bool {
        self.lhs.is_constant() && self.rhs.is_constant()
    }
}

impl PartialEq<dyn Any> for BinaryExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        unbox_any(other)
            .downcast_ref::<Self>()
            .map(|x| x.operator == self.operator && x.lhs.eq(&self.lhs) && x.rhs.eq(&self.rhs))
            .unwrap_or(false)
    }
}

#[derive(Debug, PartialEq, Hash, Clone, Eq)]
pub struct Column {
    field: Field,
}

impl Column {
    pub fn new(field: Field) -> Self {
        Self { field }
    }

    pub fn field(&self) -> &Field {
        &self.field
    }
}

impl From<String> for Column {
    fn from(value: String) -> Self {
        Column::new(value.into())
    }
}

impl From<usize> for Column {
    fn from(value: usize) -> Self {
        Column::new(value.into())
    }
}

impl VortexExpr for Column {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn evaluate(&self, batch: &Array) -> VortexResult<Array> {
        let s = StructArray::try_from(batch)?;

        let column = match &self.field {
            Field::Name(n) => s.field_by_name(n),
            Field::Index(i) => s.field(*i),
        }
        .ok_or_else(|| {
            vortex_err!(
                "Array {} doesn't contain child {}",
                batch.dtype(),
                self.field
            )
        })?;
        Ok(column)
    }

    fn references(&self) -> HashSet<Field> {
        HashSet::from([self.field.clone()])
    }

    fn project(&self, projection: &[Field]) -> Option<Arc<dyn VortexExpr>> {
        projection
            .contains(&self.field)
            .then(|| Arc::new(Column::new(self.field.clone())) as Arc<dyn VortexExpr>)
    }

    fn is_constant(&self) -> bool {
        false
    }
}

impl PartialEq<dyn Any> for Column {
    fn eq(&self, other: &dyn Any) -> bool {
        unbox_any(other)
            .downcast_ref::<Self>()
            .map(|x| x == self)
            .unwrap_or(false)
    }
}

#[derive(Debug, PartialEq)]
pub struct Literal {
    value: Scalar,
}

impl Literal {
    pub fn new(value: Scalar) -> Self {
        Self { value }
    }
}

impl VortexExpr for Literal {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn evaluate(&self, batch: &Array) -> VortexResult<Array> {
        Ok(ConstantArray::new(self.value.clone(), batch.len()).into_array())
    }

    fn references(&self) -> HashSet<Field> {
        HashSet::new()
    }

    fn project(&self, _projection: &[Field]) -> Option<Arc<dyn VortexExpr>> {
        Some(Arc::new(Literal::new(self.value.clone())))
    }

    fn is_constant(&self) -> bool {
        true
    }
}

impl PartialEq<dyn Any> for Literal {
    fn eq(&self, other: &dyn Any) -> bool {
        unbox_any(other)
            .downcast_ref::<Self>()
            .map(|x| x == self)
            .unwrap_or(false)
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct Identity;

impl VortexExpr for Identity {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn evaluate(&self, batch: &Array) -> VortexResult<Array> {
        Ok(batch.clone())
    }

    fn references(&self) -> HashSet<Field> {
        HashSet::new()
    }

    fn project(&self, _projection: &[Field]) -> Option<Arc<dyn VortexExpr>> {
        Some(Arc::new(Identity))
    }

    fn is_constant(&self) -> bool {
        false
    }
}

impl PartialEq<dyn Any> for Identity {
    fn eq(&self, other: &dyn Any) -> bool {
        unbox_any(other)
            .downcast_ref::<Self>()
            .map(|x| x == self)
            .unwrap_or(false)
    }
}

#[derive(Debug)]
pub struct Not {
    child: Arc<dyn VortexExpr>,
}

impl Not {
    pub fn new(child: Arc<dyn VortexExpr>) -> Self {
        Self { child }
    }

    pub fn child(&self) -> &Arc<dyn VortexExpr> {
        &self.child
    }
}

impl VortexExpr for Not {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn evaluate(&self, batch: &Array) -> VortexResult<Array> {
        let child_result = self.child.evaluate(batch)?;
        child_result.with_dyn(|a| {
            a.as_bool_array()
                .ok_or_else(|| vortex_err!("Child was not a bool array"))
                .map(|b| b.not())
        })
    }

    fn references(&self) -> HashSet<Field> {
        self.child.references()
    }

    fn project(&self, projection: &[Field]) -> Option<Arc<dyn VortexExpr>> {
        self.child
            .project(projection)
            .map(|c| Arc::new(Not::new(c)) as _)
    }

    fn is_constant(&self) -> bool {
        false
    }
}

impl PartialEq<dyn Any> for Not {
    fn eq(&self, other: &dyn Any) -> bool {
        unbox_any(other)
            .downcast_ref::<Self>()
            .map(|x| x.child.eq(&self.child))
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use vortex_dtype::field::Field;

    use crate::{BinaryExpr, Column, Literal, Operator, VortexExpr};

    #[test]
    fn project_and() {
        let band = BinaryExpr::new(
            Arc::new(Column::new(Field::from("a"))),
            Operator::And,
            Arc::new(Column::new(Field::from("b"))),
        );
        let projection = vec![Field::from("b")];
        assert_eq!(
            *band.project(&projection).unwrap(),
            *Column::new(Field::from("b")).as_any()
        );
    }

    #[test]
    fn project_or() {
        let bor = BinaryExpr::new(
            Arc::new(Column::new(Field::from("a"))),
            Operator::Or,
            Arc::new(Column::new(Field::from("b"))),
        );
        let projection = vec![Field::from("b")];
        assert!(bor.project(&projection).is_none());
    }

    #[test]
    fn project_nested() {
        let band = BinaryExpr::new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new(Field::from("a"))),
                Operator::Lt,
                Arc::new(Column::new(Field::from("b"))),
            )),
            Operator::And,
            Arc::new(BinaryExpr::new(
                Arc::new(Literal::new(5.into())),
                Operator::Lt,
                Arc::new(Column::new(Field::from("b"))),
            )),
        );
        let projection = vec![Field::from("b")];
        assert!(band.project(&projection).is_none());
    }

    #[test]
    fn project_multicolumn() {
        let blt = BinaryExpr::new(
            Arc::new(Column::new(Field::from("a"))),
            Operator::Lt,
            Arc::new(Column::new(Field::from("b"))),
        );
        let projection = vec![Field::from("a"), Field::from("b")];
        assert_eq!(
            *blt.project(&projection).unwrap(),
            *BinaryExpr::new(
                Arc::new(Column::new(Field::from("a"))),
                Operator::Lt,
                Arc::new(Column::new(Field::from("b"))),
            )
            .as_any()
        );
    }
}
