use std::collections::HashMap;

use vortex::scalar::AsBytes;
use vortex_error::VortexResult;
use vortex_schema::{CompositeID, DType};

use crate::array::composite::{find_extension, CompositeExtensionRef, TypedCompositeArray};
use crate::stats::ArrayStatisticsCompute;
use crate::validity::{ArrayValidity, LogicalValidity};
use crate::visitor::{AcceptArrayVisitor, ArrayVisitor};
use crate::{impl_encoding, ArrayFlatten, IntoArrayData, TryDeserializeArrayMetadata};

pub trait UnderlyingMetadata: ArrayMetadata {
    fn id(&self) -> CompositeID;
}

impl_encoding!("vortex.composite", Composite);

#[derive(Debug, Clone)]
pub struct CompositeMetadata {
    ext: CompositeExtensionRef,
    underlying_dtype: DType,
    underlying_metadata: Arc<[u8]>,
}

impl<'a> CompositeArray<'a> {
    pub fn new(id: CompositeID, metadata: Arc<[u8]>, underlying: Array<'a>) -> Self {
        let dtype = DType::Composite(id, underlying.dtype().is_nullable().into());
        let ext = find_extension(id.0).expect("Unrecognized composite extension");
        Self::try_from_parts(
            dtype,
            CompositeMetadata {
                ext,
                underlying_dtype: underlying.dtype().clone(),
                underlying_metadata: metadata,
            },
            vec![].into(),
            vec![underlying.into_array_data()].into(),
            HashMap::default(),
        )
        .unwrap()
    }
}

impl CompositeArray<'_> {
    #[inline]
    pub fn id(&self) -> CompositeID {
        self.metadata().ext.id()
    }

    #[inline]
    pub fn extension(&self) -> CompositeExtensionRef {
        find_extension(self.id().0).expect("Unrecognized composite extension")
    }

    pub fn underlying_metadata(&self) -> &Arc<[u8]> {
        &self.metadata().underlying_metadata
    }

    pub fn underlying_dtype(&self) -> &DType {
        &self.metadata().underlying_dtype
    }

    #[inline]
    pub fn underlying(&self) -> Array {
        self.array()
            .child(0, self.underlying_dtype())
            .expect("CompositeArray must have an underlying array")
    }

    pub fn with_dyn<R, F>(&self, mut f: F) -> R
    where
        F: FnMut(&dyn ArrayTrait) -> R,
    {
        let mut result = None;

        self.extension().as_typed_compute()

        self.encoding()
            .with_dyn(self, &mut |array| {
                result = Some(f(array));
                Ok(())
            })
            .unwrap();

        // Now we unwrap the optional, which we know to be populated by the closure.
        result.unwrap()
    }

    pub fn as_typed<M: UnderlyingMetadata + for<'a> TryDeserializeArrayMetadata<'a>>(
        &self,
    ) -> VortexResult<TypedCompositeArray<M>> {
        Ok(TypedCompositeArray::new(
            M::try_deserialize_metadata(Some(self.underlying_metadata().as_bytes()))?,
            self.underlying().clone(),
        ))
    }

    //
    // pub fn as_typed_compute(&self) -> Box<dyn ArrayCompute> {
    //     self.extension.as_typed_compute(self)
    // }
}

impl ArrayTrait for CompositeArray<'_> {
    fn len(&self) -> usize {
        self.underlying().len()
    }
}

impl ArrayFlatten for CompositeArray<'_> {
    fn flatten<'a>(self) -> VortexResult<Flattened<'a>>
    where
        Self: 'a,
    {
        todo!()
    }
}

impl ArrayValidity for CompositeArray<'_> {
    fn is_valid(&self, index: usize) -> bool {
        self.underlying().with_dyn(|a| a.is_valid(index))
    }

    fn logical_validity(&self) -> LogicalValidity {
        self.underlying().with_dyn(|a| a.logical_validity())
    }
}

impl AcceptArrayVisitor for CompositeArray<'_> {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        visitor.visit_child("underlying", &self.underlying())
    }
}

impl ArrayStatisticsCompute for CompositeArray<'_> {}
