use vortex_error::VortexResult;

use crate::visitor::ArrayVisitor;
use crate::{Array, ArrayData, ArrayTrait, ToArrayData, WithArray};

/// TODO(ngates): do we want this to be references?
#[derive(Debug)]
pub struct ColumnBatch {
    columns: Vec<ArrayData>,
    length: usize,
}

impl ColumnBatch {
    pub fn from_array(array: &dyn ArrayTrait) -> Self {
        // We want to walk the struct array extracting all nested columns
        let mut batch = ColumnBatch {
            columns: vec![],
            length: array.len(),
        };
        array.accept(&mut batch).unwrap();
        batch
    }

    pub fn columns(&self) -> &[ArrayData] {
        self.columns.as_slice()
    }

    pub fn ncolumns(&self) -> usize {
        self.columns.len()
    }
}

impl From<&Array<'_>> for ColumnBatch {
    fn from(value: &Array) -> Self {
        value.with_array(|a| ColumnBatch::from_array(a))
    }
}

/// Collect all the nested column leaves from an array.
impl ArrayVisitor for ColumnBatch {
    fn visit_column(&mut self, _name: &str, array: &Array) -> VortexResult<()> {
        let ncols = self.columns.len();
        array.with_array(|a| a.accept(self))?;
        if ncols == self.columns.len() {
            assert_eq!(self.length, array.len());
            self.columns.push(array.to_array_data())
        }
        Ok(())
    }

    fn visit_child(&mut self, _name: &str, array: &Array) -> VortexResult<()> {
        // Stop traversing when we hit the first non-column array.
        assert_eq!(self.length, array.len());
        self.columns.push(array.to_array_data());
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use crate::array::primitive::PrimitiveData;
    use crate::array::r#struct::StructData;
    use crate::batch::ColumnBatch;
    use crate::{IntoArray, IntoArrayData};

    #[test]
    fn batch_visitor() {
        let col = PrimitiveData::from(vec![0, 1, 2]).into_array_data();
        let nested_struct = StructData::try_new(
            vec![Arc::new("x".into()), Arc::new("y".into())],
            vec![col.clone(), col.clone()],
            3,
        )
        .unwrap();

        let arr = StructData::try_new(
            vec![Arc::new("a".into()), Arc::new("b".into())],
            vec![col.clone(), nested_struct.into_array_data()],
            3,
        )
        .unwrap()
        .into_array();

        let batch = ColumnBatch::from(&arr);
        assert_eq!(batch.columns().len(), 3);
    }
}
