use serde::{Deserialize, Serialize};
use vortex_dtype::{DType, FieldName, FieldNames, Nullability, StructDType};
use vortex_error::{vortex_bail, vortex_err, VortexResult};

use crate::stats::{ArrayStatisticsCompute, StatsSet};
use crate::validity::{ArrayValidity, LogicalValidity, Validity, ValidityMetadata};
use crate::variants::{ArrayVariants, StructArrayTrait};
use crate::visitor::{AcceptArrayVisitor, ArrayVisitor};
use crate::{impl_encoding, Array, ArrayDType, ArrayDef, ArrayTrait, Canonical, IntoCanonical};

mod compute;

impl_encoding!("vortex.struct", 8u16, Struct);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StructMetadata {
    length: usize,
    validity: ValidityMetadata,
}

impl StructArray {
    pub fn validity(&self) -> Validity {
        self.metadata().validity.to_validity(self.array().child(
            self.nfields(),
            &Validity::DTYPE,
            self.len(),
        ))
    }

    pub fn children(&self) -> impl Iterator<Item = Array> + '_ {
        (0..self.nfields()).map(move |idx| self.field(idx).unwrap())
    }

    pub fn try_new(
        names: FieldNames,
        fields: Vec<Array>,
        length: usize,
        validity: Validity,
    ) -> VortexResult<Self> {
        if names.len() != fields.len() {
            vortex_bail!("Got {} names and {} fields", names.len(), fields.len());
        }

        if fields.iter().any(|a| a.len() != length) {
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
            length,
            StructMetadata {
                length,
                validity: validity_metadata,
            },
            children.into(),
            StatsSet::new(),
        )
    }

    pub fn from_fields<N: AsRef<str>>(items: &[(N, Array)]) -> Self {
        let names: Vec<FieldName> = items
            .iter()
            .map(|(name, _)| FieldName::from(name.as_ref()))
            .collect();
        let fields: Vec<Array> = items.iter().map(|(_, array)| array.clone()).collect();
        let len = fields.first().unwrap().len();

        Self::try_new(FieldNames::from(names), fields, len, Validity::NonNullable)
            .expect("building StructArray with helper")
    }

    // TODO(aduffy): Add equivalent function to support field masks for nested column access.
    /// Return a new StructArray with the given projection applied.
    ///
    /// Projection does not copy data arrays. Projection is defined by an ordinal array slice
    /// which specifies the new ordering of columns in the struct. The projection can be used to
    /// perform column re-ordering, deletion, or duplication at a logical level, without any data
    /// copying.
    ///
    /// # Panics
    /// This function will panic an error if the projection references columns not within the
    /// schema boundaries.
    pub fn project(&self, projection: &[usize]) -> VortexResult<Self> {
        let mut children = Vec::with_capacity(projection.len());
        let mut names = Vec::with_capacity(projection.len());

        for &column_idx in projection {
            children.push(
                self.field(column_idx)
                    .ok_or(vortex_err!(OutOfBounds: column_idx, 0, self.dtypes().len()))?,
            );
            names.push(self.names()[column_idx].clone());
        }

        StructArray::try_new(
            FieldNames::from(names.as_slice()),
            children,
            self.len(),
            self.validity(),
        )
    }
}

impl ArrayTrait for StructArray {}

impl ArrayVariants for StructArray {
    fn as_struct_array(&self) -> Option<&dyn StructArrayTrait> {
        Some(self)
    }
}

impl StructArrayTrait for StructArray {
    fn field(&self, idx: usize) -> Option<Array> {
        self.dtypes()
            .get(idx)
            .and_then(|dtype| self.array().child(idx, dtype, self.len()))
    }
}

impl IntoCanonical for StructArray {
    /// StructEncoding is the canonical form for a [DType::Struct] array, so return self.
    fn into_canonical(self) -> VortexResult<Canonical> {
        Ok(Canonical::Struct(self))
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

#[cfg(test)]
mod test {
    use vortex_dtype::{DType, FieldName, FieldNames, Nullability};

    use crate::array::primitive::PrimitiveArray;
    use crate::array::struct_::StructArray;
    use crate::array::varbin::VarBinArray;
    use crate::array::BoolArray;
    use crate::validity::Validity;
    use crate::variants::StructArrayTrait;
    use crate::IntoArray;

    #[test]
    fn test_project() {
        let xs = PrimitiveArray::from_vec(vec![0i64, 1, 2, 3, 4], Validity::NonNullable);
        let ys = VarBinArray::from_vec(
            vec!["a", "b", "c", "d", "e"],
            DType::Utf8(Nullability::NonNullable),
        );
        let zs = BoolArray::from_vec(vec![true, true, true, false, false], Validity::NonNullable);

        let struct_a = StructArray::try_new(
            FieldNames::from(["xs".into(), "ys".into(), "zs".into()]),
            vec![xs.into_array(), ys.into_array(), zs.into_array()],
            5,
            Validity::NonNullable,
        )
        .unwrap();

        let struct_b = struct_a.project(&[2usize, 0]).unwrap();
        assert_eq!(
            struct_b.names().as_ref(),
            [FieldName::from("zs"), FieldName::from("xs")],
        );

        assert_eq!(struct_b.len(), 5);

        let bools = BoolArray::try_from(struct_b.field(0).unwrap()).unwrap();
        assert_eq!(
            bools.boolean_buffer().iter().collect::<Vec<_>>(),
            vec![true, true, true, false, false]
        );

        let prims = PrimitiveArray::try_from(struct_b.field(1).unwrap()).unwrap();
        assert_eq!(prims.maybe_null_slice::<i64>(), [0i64, 1, 2, 3, 4]);
    }
}
