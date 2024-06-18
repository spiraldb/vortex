use serde::{Deserialize, Serialize};
use vortex_dtype::{FieldNames, Nullability, StructDType};
use vortex_error::vortex_bail;

use crate::stats::ArrayStatisticsCompute;
use crate::validity::{ArrayValidity, LogicalValidity, Validity, ValidityMetadata};
use crate::visitor::{AcceptArrayVisitor, ArrayVisitor};
use crate::ArrayFlatten;
use crate::{impl_encoding, ArrayDType};

mod compute;

impl_encoding!("vortex.struct", Struct);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StructMetadata {
    length: usize,
    validity: ValidityMetadata,
}

impl StructArray {
    pub fn field(&self, idx: usize) -> Option<Array> {
        let DType::Struct(st, _) = self.dtype() else {
            unreachable!()
        };
        let dtype = st.dtypes().get(idx)?;
        self.array().child(idx, dtype)
    }

    pub fn names(&self) -> &FieldNames {
        let DType::Struct(st, _) = self.dtype() else {
            unreachable!()
        };
        st.names()
    }

    pub fn dtypes(&self) -> &[DType] {
        let DType::Struct(st, _) = self.dtype() else {
            unreachable!()
        };
        st.dtypes()
    }

    pub fn nfields(&self) -> usize {
        self.dtypes().len()
    }

    pub fn validity(&self) -> Validity {
        self.metadata()
            .validity
            .to_validity(self.array().child(self.nfields(), &Validity::DTYPE))
    }
}

impl<'a> StructArray {
    pub fn children(&'a self) -> impl Iterator<Item = Array> + '_ {
        (0..self.nfields()).map(move |idx| self.field(idx).unwrap())
    }
}

impl StructArray {
    pub fn try_new(
        names: FieldNames,
        fields: Vec<Array>,
        length: usize,
        validity: Validity,
    ) -> VortexResult<Self> {
        if names.len() != fields.len() {
            vortex_bail!("Got {} names and {} fields", names.len(), fields.len());
        }

        if fields.iter().any(|a| a.with_dyn(|a| a.len()) != length) {
            vortex_bail!("Expected all struct fields to have length {}", length);
        }

        let field_dtypes: Vec<_> = fields.iter().map(|d| d.dtype()).cloned().collect();

        let validity_metadata = validity.to_metadata(length)?;

        let mut children = Vec::with_capacity(fields.len() + 1);
        children.extend(fields);
        if let Some(v) = validity.into_array() {
            children.push(v);
        }

        Self::try_from_parts(
            DType::Struct(
                StructDType::new(names, field_dtypes),
                Nullability::NonNullable,
            ),
            StructMetadata {
                length,
                validity: validity_metadata,
            },
            children.into(),
            StatsSet::new(),
        )
    }
}

impl ArrayFlatten for StructArray {
    /// StructEncoding is the canonical form for a [DType::Struct] array, so return self.
    fn flatten(self) -> VortexResult<Flattened> {
        Ok(Flattened::Struct(self))
    }
}

impl ArrayTrait for StructArray {
    fn len(&self) -> usize {
        self.metadata().length
    }
}

impl ArrayValidity for StructArray {
    fn is_valid(&self, index: usize) -> bool {
        self.validity().is_valid(index)
    }

    fn logical_validity(&self) -> LogicalValidity {
        self.validity().to_logical(self.len())
    }
}

impl AcceptArrayVisitor for StructArray {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        for (idx, name) in self.names().iter().enumerate() {
            let child = self.field(idx).unwrap();
            visitor.visit_child(&format!("\"{}\"", name), &child)?;
        }
        Ok(())
    }
}

impl ArrayStatisticsCompute for StructArray {}

impl EncodingCompression for StructEncoding {}
