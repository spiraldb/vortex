use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::{Arc, RwLock};

use arrow_buffer::Buffer;
use vortex::scalar::Scalar;
use vortex_error::{vortex_bail, VortexError, VortexResult};
use vortex_schema::DType;

use crate::encoding::EncodingRef;
use crate::stats::Stat;
use crate::stats::Statistics;
use crate::{
    Array, ArrayDef, ArrayMetadata, ArrayParts, IntoArray, IntoArrayData, OwnedArray, ToArray,
    TryFromArrayParts,
};

#[derive(Clone, Debug)]
pub struct ArrayData {
    encoding: EncodingRef,
    dtype: DType,
    metadata: Arc<dyn ArrayMetadata>,
    buffers: Arc<[Buffer]>, // Should this just be an Option, not an Arc?
    children: Arc<[Option<ArrayData>]>,
    stats_set: Arc<RwLock<HashMap<Stat, Scalar>>>,
}

impl ArrayData {
    pub fn try_new(
        encoding: EncodingRef,
        dtype: DType,
        metadata: Arc<dyn ArrayMetadata>,
        buffers: Arc<[Buffer]>,
        children: Arc<[Option<ArrayData>]>,
    ) -> VortexResult<Self> {
        let data = Self {
            encoding,
            dtype,
            metadata,
            buffers,
            children,
            stats_set: Arc::new(RwLock::new(HashMap::new())),
        };

        // Validate here that the metadata correctly parses, so that an encoding can infallibly
        // implement Encoding::with_data().
        // encoding.with_data_mut(&data, &mut |_| Ok(()))?;

        Ok(data)
    }

    pub fn encoding(&self) -> EncodingRef {
        self.encoding
    }

    pub fn dtype(&self) -> &DType {
        &self.dtype
    }

    pub fn metadata(&self) -> &Arc<dyn ArrayMetadata> {
        &self.metadata
    }

    pub fn buffers(&self) -> &[Buffer] {
        &self.buffers
    }

    pub fn child(&self, index: usize) -> Option<&ArrayData> {
        self.children.get(index).and_then(|c| c.as_ref())
    }

    pub fn children(&self) -> &[Option<ArrayData>] {
        &self.children
    }

    pub fn into_typed_data<D: ArrayDef>(self) -> Option<TypedArrayData<D>> {
        TypedArrayData::<D>::try_from(self).ok()
    }

    pub fn depth_first_traversal(&self) -> ArrayDataIterator {
        ArrayDataIterator { stack: vec![self] }
    }

    /// Return the buffer offsets and the total length of all buffers, assuming the given alignment.
    /// This includes all child buffers.
    pub fn all_buffer_offsets(&self, alignment: usize) -> Vec<u64> {
        let mut offsets = Vec::with_capacity(self.buffers.len() + 1);
        let mut offset = 0;

        for col_data in self.depth_first_traversal() {
            for buffer in col_data.buffers() {
                offsets.push(offset as u64);

                let buffer_size = buffer.len();
                let aligned_size = (buffer_size + (alignment - 1)) & !(alignment - 1);
                offset += aligned_size;
            }
        }
        offsets.push(offset as u64);

        offsets
    }
}

/// A depth-first iterator over a ArrayData.
pub struct ArrayDataIterator<'a> {
    stack: Vec<&'a ArrayData>,
}

impl<'a> Iterator for ArrayDataIterator<'a> {
    type Item = &'a ArrayData;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.stack.pop()?;
        for child in next.children.as_ref().iter().flatten() {
            self.stack.push(child);
        }
        Some(next)
    }
}

impl ToArray for ArrayData {
    fn to_array(&self) -> Array {
        Array::DataRef(self)
    }
}

impl IntoArray<'static> for ArrayData {
    fn into_array(self) -> Array<'static> {
        Array::Data(self)
    }
}

#[derive(Debug)]
pub struct TypedArrayData<D: ArrayDef> {
    data: ArrayData,
    phantom: PhantomData<D>,
}

impl<D: ArrayDef> TypedArrayData<D> {
    pub fn new_unchecked(
        dtype: DType,
        metadata: Arc<D::Metadata>,
        buffers: Arc<[Buffer]>,
        children: Arc<[Option<ArrayData>]>,
    ) -> Self {
        Self::from_data_unchecked(
            ArrayData::try_new(D::ENCODING, dtype, metadata, buffers, children).unwrap(),
        )
    }

    pub fn from_data_unchecked(data: ArrayData) -> Self {
        Self {
            data,
            phantom: PhantomData,
        }
    }

    pub fn data(&self) -> &ArrayData {
        &self.data
    }

    pub fn metadata(&self) -> &D::Metadata {
        self.data
            .metadata()
            .as_any()
            .downcast_ref::<D::Metadata>()
            .unwrap()
    }

    pub fn into_metadata(self) -> Arc<D::Metadata> {
        self.data
            .metadata
            .as_any_arc()
            .downcast::<D::Metadata>()
            .unwrap()
    }

    pub fn as_typed_array(&self) -> D::Array<'_> {
        D::Array::try_from_parts(&self.data, self.metadata()).unwrap()
    }
}

impl<D: ArrayDef> ToArray for TypedArrayData<D> {
    fn to_array(&self) -> Array {
        Array::DataRef(&self.data)
    }
}

impl<D: ArrayDef> IntoArray<'static> for TypedArrayData<D> {
    fn into_array(self) -> OwnedArray {
        Array::Data(self.data)
    }
}

impl<D: ArrayDef> IntoArrayData for TypedArrayData<D> {
    fn into_array_data(self) -> ArrayData {
        self.data
    }
}

impl<D: ArrayDef> TryFrom<ArrayData> for TypedArrayData<D> {
    type Error = VortexError;

    fn try_from(data: ArrayData) -> Result<Self, Self::Error> {
        if data.encoding().id() != D::ID {
            vortex_bail!("Invalid encoding for array")
        }
        Ok(Self {
            data,
            phantom: PhantomData,
        })
    }
}

impl ArrayParts for ArrayData {
    fn dtype(&self) -> &DType {
        &self.dtype
    }

    fn buffer(&self, idx: usize) -> Option<&Buffer> {
        self.buffers().get(idx)
    }

    fn child(&self, idx: usize, _dtype: &DType) -> Option<Array> {
        // TODO(ngates): validate the DType
        self.child(idx).map(move |a| a.to_array())
    }

    fn nchildren(&self) -> usize {
        self.children.len()
    }

    fn statistics<'a>(&'a self) -> &'a (dyn Statistics + 'a) {
        self
    }
}

impl Statistics for ArrayData {
    fn compute(&self, stat: Stat) -> Option<Scalar> {
        let mut locked = self.stats_set.write().unwrap();
        let stats = self
            .encoding()
            .with_data(self, |a| a.compute_statistics(stat))
            .ok()?;
        for (k, v) in &stats {
            locked.insert(*k, v.clone());
        }
        stats.get(&stat).cloned()
    }

    fn get(&self, stat: Stat) -> Option<Scalar> {
        let locked = self.stats_set.read().unwrap();
        locked.get(&stat).cloned()
    }

    fn set(&self, stat: Stat, value: Scalar) {
        let mut locked = self.stats_set.write().unwrap();
        locked.insert(stat, value);
    }

    fn to_map(&self) -> HashMap<Stat, Scalar> {
        self.stats_set.read().unwrap().clone()
    }
}
