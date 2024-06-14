use std::mem::ManuallyDrop;

use itertools::Itertools;
use serde::{Deserialize, Serialize};
use vortex_buffer::Buffer;
use vortex_dtype::Nullability;

use crate::{
    impl_encoding,
    validity::{ArrayValidity, LogicalValidity, Validity, ValidityMetadata},
    visitor::{AcceptArrayVisitor, ArrayVisitor},
    ArrayFlatten,
};

mod compute;
mod stats;

impl_encoding!("vortex.byte_bool", ByteBool);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ByteBoolMetadata {
    validity: ValidityMetadata,
    length: usize,
}

impl ByteBoolArray {
    pub fn validity(&self) -> Validity {
        self.metadata()
            .validity
            .to_validity(self.array().child(0, &Validity::DTYPE))
    }

    pub fn buffer(&self) -> &Buffer {
        self.array().buffer().expect("missing mandatory buffer")
    }
}

impl From<Vec<bool>> for ByteBoolArray {
    fn from(value: Vec<bool>) -> Self {
        let mut value = ManuallyDrop::new(value);
        let ptr = value.as_mut_ptr() as *mut u8;
        let length = value.len();
        let capacity = value.capacity();

        let bytes_vec = unsafe { Vec::from_raw_parts(ptr, length, capacity) };

        let buffer = Buffer::from(bytes_vec);
        let typed = TypedArray::try_from_parts(
            DType::Bool(Nullability::NonNullable),
            ByteBoolMetadata {
                validity: ValidityMetadata::NonNullable,
                length,
            },
            Some(buffer),
            Validity::NonNullable
                .into_array_data()
                .into_iter()
                .collect_vec()
                .into(),
            StatsSet::new(),
        )
        .unwrap();

        typed.into()
    }
}

impl From<Vec<Option<bool>>> for ByteBoolArray {
    fn from(value: Vec<Option<bool>>) -> Self {
        let mut value = ManuallyDrop::new(value);
        let ptr = value.as_mut_ptr() as *mut u8;
        let length = value.len();
        let capacity = value.capacity();

        let validity = Validity::from_iter(value.iter());

        // SAFETY: `Option<bool>` is the same as `bool`, so as long as we keep the validity data the data is still valid.
        // If we ever want to turn this Array back to a Vec, we might have to do some work
        let bytes_vec = unsafe { Vec::from_raw_parts(ptr, length, capacity) };

        let buffer = Buffer::from(bytes_vec);
        let typed = TypedArray::try_from_parts(
            DType::Bool(Nullability::Nullable),
            ByteBoolMetadata {
                validity: validity.to_metadata(length).unwrap(),
                length,
            },
            Some(buffer),
            validity.into_array_data().into_iter().collect_vec().into(),
            StatsSet::new(),
        )
        .unwrap();

        typed.into()
    }
}

impl ArrayTrait for ByteBoolArray {
    fn len(&self) -> usize {
        self.metadata().length
    }
}

impl ArrayFlatten for ByteBoolArray {
    fn flatten(self) -> VortexResult<Flattened> {
        todo!()
        // Err(VortexError::NotImplemented((), (), ()))
    }
}

impl ArrayValidity for ByteBoolArray {
    fn is_valid(&self, index: usize) -> bool {
        self.validity().is_valid(index)
    }

    fn logical_validity(&self) -> LogicalValidity {
        self.validity().to_logical(self.len())
    }
}

impl AcceptArrayVisitor for ByteBoolArray {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        visitor.visit_buffer(self.buffer())?;
        visitor.visit_validity(&self.validity())
    }
}

impl EncodingCompression for ByteBoolEncoding {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validity_construction() {
        let v = vec![true, false];

        let arr = ByteBoolArray::from(v);
        for idx in 0..arr.len() {
            assert!(arr.is_valid(idx));
        }

        let v = vec![Some(true), None, Some(false)];
        let arr = ByteBoolArray::from(v);
        assert!(arr.is_valid(0));
        assert!(!arr.is_valid(1));
        assert!(arr.is_valid(2));
        assert_eq!(arr.len(), 3);

        let v: Vec<Option<bool>> = vec![None, None];

        let arr = ByteBoolArray::from(v);
        for idx in 0..arr.len() {
            assert!(!arr.is_valid(idx));
        }
        assert_eq!(arr.len(), 2);
    }
}
