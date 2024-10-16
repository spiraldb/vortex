use std::any::Any;
use std::collections::HashSet;

use vortex::array::StructArray;
use vortex::variants::StructArrayTrait;
use vortex::{Array, IntoArray};
use vortex_dtype::field::Field;
use vortex_error::{vortex_bail, VortexResult};

use crate::{unbox_any, Column, VortexExpr};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Select {
    columns: Vec<Column>,
}

impl Select {
    pub fn new(columns: Vec<Column>) -> Self {
        Self { columns }
    }
}

impl PartialEq<dyn Any> for Select {
    fn eq(&self, other: &dyn Any) -> bool {
        unbox_any(other)
            .downcast_ref::<Self>()
            .map(|x| self == x)
            .unwrap_or(false)
    }
}

impl VortexExpr for Select {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn evaluate(&self, batch: &Array) -> VortexResult<Array> {
        let Ok(st) = StructArray::try_from(batch) else {
            vortex_bail!("Argument must be a struct")
        };
        let fields = self
            .columns
            .iter()
            .map(|c| c.evaluate(batch))
            .collect::<VortexResult<Vec<_>>>()?;
        let names = self
            .columns
            .iter()
            .map(|c| match c.field() {
                Field::Name(n) => n.as_str().into(),
                Field::Index(i) => st.names()[*i].clone(),
            })
            .collect();

        StructArray::try_new(names, fields, st.len(), st.validity()).map(IntoArray::into_array)
    }

    fn references(&self) -> HashSet<Field> {
        self.columns
            .iter()
            .flat_map(|c| c.references().into_iter())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use vortex::array::{PrimitiveArray, StructArray};
    use vortex::IntoArray;

    use crate::{Column, Select, VortexExpr};

    #[test]
    pub fn select_columns() {
        let st = StructArray::from_fields(&[
            ("a", PrimitiveArray::from(vec![0, 1, 2]).into_array()),
            ("b", PrimitiveArray::from(vec![4, 5, 6]).into_array()),
        ]);
        let select = Select::new(vec![Column::from("a".to_string())]);
        let selected = select.evaluate(st.as_ref()).unwrap();
        let selected_names = selected.with_dyn(|a| a.as_struct_array_unchecked().names().clone());
        assert_eq!(selected_names.as_ref(), &["a".into()]);
    }
}
