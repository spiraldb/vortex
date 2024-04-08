mod compute;

use serde::{Deserialize, Serialize};
use vortex_error::{vortex_bail, VortexResult};
use vortex_schema::{DType, FieldNames};

use crate::impl_encoding;
use crate::validity::ArrayValidity;
use crate::visitor::{AcceptArrayVisitor, ArrayVisitor};
use crate::{Array, ArrayMetadata};
use crate::{ArrayData, TypedArrayData};
use crate::{ArrayView, ToArrayData};

impl_encoding!("vortex.struct", Struct);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StructMetadata {
    length: usize,
}

#[derive(Clone)]
pub struct StructArray<'a> {
    dtype: &'a DType,
    // Note(ngates): for arrays with variable-length children, we don't want to
    // allocate a Vec<Array>, so instead we defer child access by storing a reference to the parts.
    parts: &'a dyn ArrayParts,
    length: usize,
}

impl<'a> StructArray<'a> {
    pub fn child(&'a self, idx: usize) -> Option<Array<'a>> {
        let DType::Struct(_, fields) = self.dtype() else {
            unreachable!()
        };
        let dtype = fields.get(idx)?;
        self.parts.child(idx, dtype)
    }

    pub fn names(&self) -> &FieldNames {
        let DType::Struct(names, _fields) = self.dtype() else {
            unreachable!()
        };
        names
    }

    pub fn fields(&self) -> &[DType] {
        let DType::Struct(_names, fields) = self.dtype() else {
            unreachable!()
        };
        fields.as_slice()
    }

    pub fn ncolumns(&self) -> usize {
        self.fields().len()
    }

    pub fn len(&self) -> usize {
        self.length
    }
}

impl<'v> TryFromArrayParts<'v, StructMetadata> for StructArray<'v> {
    fn try_from_parts(
        parts: &'v dyn ArrayParts,
        metadata: &'v StructMetadata,
    ) -> VortexResult<Self> {
        let DType::Struct(_names, dtypes) = parts.dtype() else {
            unreachable!()
        };
        if parts.nchildren() != dtypes.len() {
            vortex_bail!(
                "Expected {} children, found {}",
                dtypes.len(),
                parts.nchildren()
            );
        }
        Ok(StructArray {
            dtype: parts.dtype(),
            parts,
            length: metadata.length,
        })
    }
}

impl ArrayTrait for StructArray<'_> {
    fn dtype(&self) -> &DType {
        self.dtype
    }

    fn len(&self) -> usize {
        self.length
    }
}

impl ArrayValidity for StructArray<'_> {
    fn is_valid(&self, _index: usize) -> bool {
        todo!()
    }
}

impl ToArrayData for StructArray<'_> {
    fn to_array_data(&self) -> ArrayData {
        todo!()
    }
}

impl AcceptArrayVisitor for StructArray<'_> {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        for (idx, name) in self.names().iter().enumerate() {
            let child = self.child(idx).unwrap();
            visitor.visit_column(name, &child)?;
        }
        Ok(())
    }
}
