use std::sync::Arc;

use vortex::array::primitive::PrimitiveEncoding;
use vortex_error::{vortex_err, VortexError, VortexResult};

use crate::array::primitive::PrimitiveDef;
use crate::buffer::{Buffer, OwnedBuffer};
use crate::encoding::{ArrayEncodingRef, EncodingRef};
use crate::stats::ArrayStatistics;
use crate::visitor::ArrayVisitor;
use crate::{Array, ArrayData, ArrayDef, ToArrayData, ToStatic, TryDeserializeArrayMetadata};

#[derive(Debug)]
pub struct TypedArray<'a, D: ArrayDef> {
    array: Array<'a>,
    metadata: D::Metadata,
}

impl<D: ArrayDef> TypedArray<'_, D> {
    pub fn array(&self) -> &Array {
        &self.array
    }

    pub fn metadata(&self) -> &D::Metadata {
        &self.metadata
    }
}

impl<D: ArrayDef> Clone for TypedArray<'_, D> {
    fn clone(&self) -> Self {
        Self {
            array: self.array.clone(),
            metadata: self.metadata.clone(),
        }
    }
}

impl<'a, D: ArrayDef> TryFrom<Array<'a>> for TypedArray<'a, D> {
    type Error = VortexError;

    fn try_from(array: Array<'a>) -> Result<Self, Self::Error> {
        if array.encoding().id() != D::ENCODING.id() {
            return Err(vortex_err!("incorrect encoding"));
        }
        let metadata = match &array {
            Array::Data(d) => d
                .metadata()
                .as_any()
                .downcast_ref::<D::Metadata>()
                .unwrap()
                .clone(),
            Array::DataRef(d) => d
                .metadata()
                .as_any()
                .downcast_ref::<D::Metadata>()
                .unwrap()
                .clone(),
            Array::View(v) => D::Metadata::try_deserialize_metadata(v.metadata())?,
        };
        Ok(TypedArray { array, metadata })
    }
}

impl<'a, D: ArrayDef> TryFrom<&'a Array<'a>> for TypedArray<'a, D> {
    type Error = VortexError;

    fn try_from(value: &'a Array<'a>) -> Result<Self, Self::Error> {
        value.clone().try_into()
    }
}

pub trait WithTypedArray {
    type D: ArrayDef;

    fn with_typed_array<'a, R, F>(array: &'a Array<'a>, mut f: F) -> R
    where
        F: FnMut(&TypedArray<'a, Self::D>) -> R,
    {
        let typed = TryFrom::<&Array>::try_from(array).unwrap();
        f(&typed)
    }
}

impl WithTypedArray for PrimitiveEncoding {
    type D = PrimitiveDef;
}

impl<D: ArrayDef> ArrayEncodingRef for TypedArray<'_, D> {
    fn encoding(&self) -> EncodingRef {
        self.array().encoding()
    }
}

impl<D: ArrayDef> ToArrayData for TypedArray<'_, D> {
    fn to_array_data(&self) -> ArrayData {
        match self.array() {
            Array::Data(d) => d.clone(),
            Array::DataRef(d) => (*d).clone(),
            Array::View(_) => {
                struct Visitor {
                    buffers: Vec<OwnedBuffer>,
                    children: Vec<ArrayData>,
                }
                impl ArrayVisitor for Visitor {
                    fn visit_child(&mut self, _name: &str, array: &Array) -> VortexResult<()> {
                        self.children.push(array.to_array_data());
                        Ok(())
                    }

                    fn visit_buffer(&mut self, buffer: &Buffer) -> VortexResult<()> {
                        self.buffers.push(buffer.to_static());
                        Ok(())
                    }
                }
                let mut visitor = Visitor {
                    buffers: vec![],
                    children: vec![],
                };
                self.array().with_dyn(|a| a.accept(&mut visitor).unwrap());
                ArrayData::try_new(
                    self.encoding(),
                    self.array().dtype().clone(),
                    Arc::new(self.metadata().clone()),
                    visitor.buffers.into(),
                    visitor.children.into(),
                    self.statistics().to_map(),
                )
                .unwrap()
            }
        }
    }
}
