use std::collections::HashMap;
use std::sync::Arc;

use vortex::scalar::Scalar;
use vortex_error::{vortex_err, VortexError, VortexResult};
use vortex_schema::DType;

use crate::buffer::{Buffer, OwnedBuffer};
use crate::encoding::{ArrayEncodingRef, EncodingRef};
use crate::stats::{ArrayStatistics, Stat, Statistics};
use crate::visitor::ArrayVisitor;
use crate::{
    Array, ArrayDType, ArrayData, ArrayDef, ArrayParts, IntoArray, IntoArrayData, ToArray,
    ToArrayData, ToStatic, TryDeserializeArrayMetadata,
};

#[derive(Debug)]
pub struct TypedArray<'a, D: ArrayDef> {
    array: Array<'a>,
    metadata: D::Metadata,
}

impl<D: ArrayDef> TypedArray<'_, D> {
    pub fn try_from_parts(
        dtype: DType,
        metadata: D::Metadata,
        buffers: Arc<[OwnedBuffer]>,
        children: Arc<[ArrayData]>,
        stats: HashMap<Stat, Scalar>,
    ) -> VortexResult<Self> {
        let array = Array::Data(ArrayData::try_new(
            D::ENCODING,
            dtype,
            Arc::new(metadata.clone()),
            buffers,
            children,
            stats,
        )?);
        Ok(Self { array, metadata })
    }

    pub fn len(&self) -> usize {
        self.array.with_dyn(|a| a.len())
    }

    pub fn is_empty(&self) -> bool {
        self.array.with_dyn(|a| a.is_empty())
    }

    pub fn dtype(&self) -> &DType {
        self.array.dtype()
    }

    pub fn metadata(&self) -> &D::Metadata {
        &self.metadata
    }
}

impl<'a, 'b, D: ArrayDef> TypedArray<'b, D> {
    pub fn array(&'a self) -> &'a Array<'b> {
        &self.array
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

impl<D: ArrayDef> ArrayDType for TypedArray<'_, D> {
    fn dtype(&self) -> &DType {
        self.array().dtype()
    }
}

impl<D: ArrayDef> ArrayEncodingRef for TypedArray<'_, D> {
    fn encoding(&self) -> EncodingRef {
        self.array().encoding()
    }
}

impl<D: ArrayDef> ArrayStatistics for TypedArray<'_, D> {
    fn statistics(&self) -> &(dyn Statistics + '_) {
        match self.array() {
            Array::Data(d) => d.statistics(),
            Array::DataRef(d) => d.statistics(),
            Array::View(v) => v.statistics(),
        }
    }
}

impl<D: ArrayDef> ToStatic for TypedArray<'_, D> {
    type Static = TypedArray<'static, D>;

    fn to_static(&self) -> Self::Static {
        TypedArray {
            array: Array::Data(self.to_array_data()),
            metadata: self.metadata.clone(),
        }
    }
}

impl<'a, D: ArrayDef> AsRef<Array<'a>> for TypedArray<'a, D> {
    fn as_ref(&self) -> &Array<'a> {
        &self.array
    }
}

impl<D: ArrayDef> ToArray for TypedArray<'_, D> {
    fn to_array(&self) -> Array {
        self.array.clone()
    }
}

impl<'a, D: ArrayDef> IntoArray<'a> for TypedArray<'a, D> {
    fn into_array(self) -> Array<'a> {
        self.array
    }
}

impl<D: ArrayDef> IntoArrayData for TypedArray<'_, D> {
    fn into_array_data(self) -> ArrayData {
        match self.array {
            Array::Data(d) => d,
            Array::DataRef(d) => d.clone(),
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

impl<D: ArrayDef> ToArrayData for TypedArray<'_, D> {
    fn to_array_data(&self) -> ArrayData {
        self.clone().into_array_data()
    }
}
