mod compute;
mod data;
mod stats;

use arrow_buffer::BooleanBuffer;
use serde::{Deserialize, Serialize};
use vortex_error::VortexResult;
use vortex_schema::DType;

use crate::buffer::Buffer;
use crate::validity::{ArrayValidity, ValidityMetadata};
use crate::validity::{LogicalValidity, Validity};
use crate::visitor::{AcceptArrayVisitor, ArrayVisitor};
use crate::ArrayMetadata;
use crate::TypedArrayData;
use crate::{impl_encoding, ArrayFlatten};

impl_encoding!("vortex.bool", Bool);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BoolMetadata {
    validity: ValidityMetadata,
    length: usize,
}

//
// pub struct BoolArray<'a> {
//     dtype: &'a DType,
//     buffer: &'a Buffer<'a>,
//     validity: Validity<'a>,
//     length: usize,
//     stats: &'a (dyn Statistics + 'a),
// }

impl BoolArray<'_> {
    pub fn buffer(&self) -> &Buffer {
        self.array().buffer(0).expect("missing buffer")
    }

    pub fn boolean_buffer(&self) -> BooleanBuffer {
        BooleanBuffer::new(BoolArray::buffer(self).clone().into(), 0, self.len())
    }

    pub fn validity(&self) -> Validity {
        self.metadata()
            .validity
            .to_validity(self.array().child(0, &Validity::DTYPE))
    }
}

impl ArrayTrait for BoolArray<'_> {
    fn dtype(&self) -> &DType {
        // FIXME(ngates): move this
        self.array().dtype()
    }

    fn len(&self) -> usize {
        self.metadata().length
    }

    fn metadata(&self) -> Arc<dyn ArrayMetadata> {
        // FIXME(ngates): move this
        Arc::new(self.metadata().clone())
    }
}

impl ArrayFlatten for BoolArray<'_> {
    fn flatten<'a>(self) -> VortexResult<Flattened<'a>>
    where
        Self: 'a,
    {
        Ok(Flattened::Bool(self))
    }
}

impl ArrayValidity for BoolArray<'_> {
    fn is_valid(&self, index: usize) -> bool {
        self.validity().is_valid(index)
    }

    fn logical_validity(&self) -> LogicalValidity {
        self.validity().to_logical(self.len())
    }
}

impl AcceptArrayVisitor for BoolArray<'_> {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        visitor.visit_buffer(self.buffer())?;
        visitor.visit_validity(&self.validity())
    }
}

#[cfg(test)]
mod tests {
    use crate::array::bool::BoolData;
    use crate::compute::scalar_at::scalar_at;
    use crate::IntoArray;

    #[test]
    fn bool_array() {
        let arr = BoolData::from(vec![true, false, true]).into_array();
        let scalar: bool = scalar_at(&arr, 0).unwrap().try_into().unwrap();
        assert!(scalar);
    }
}
