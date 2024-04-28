use arrow_buffer::{ArrowNativeType, ScalarBuffer};
use itertools::Itertools;
use num_traits::AsPrimitive;
use serde::{Deserialize, Serialize};
use vortex_error::{vortex_bail, VortexResult};

use crate::buffer::Buffer;
use crate::compute::scalar_subtract::ScalarSubtractFn;
use crate::ptype::{NativePType, PType};
use crate::stats::ArrayStatistics;
use crate::validity::{ArrayValidity, LogicalValidity, Validity, ValidityMetadata};
use crate::visitor::{AcceptArrayVisitor, ArrayVisitor};
use crate::{impl_encoding, ArrayDType, OwnedArray};
use crate::{match_each_integer_ptype, scalar};
use crate::{match_each_native_ptype, ArrayFlatten};

mod accessor;
mod compute;
mod stats;

impl_encoding!("vortex.primitive", Primitive);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PrimitiveMetadata {
    validity: ValidityMetadata,
}

impl PrimitiveArray<'_> {
    pub fn try_new<T: NativePType + ArrowNativeType>(
        buffer: ScalarBuffer<T>,
        validity: Validity,
    ) -> VortexResult<Self> {
        Ok(Self {
            typed: TypedArray::try_from_parts(
                DType::from(T::PTYPE).with_nullability(validity.nullability()),
                PrimitiveMetadata {
                    validity: validity.to_metadata(buffer.len())?,
                },
                Some(Buffer::Owned(buffer.into_inner())),
                validity.into_array_data().into_iter().collect_vec().into(),
                HashMap::default(),
            )?,
        })
    }

    pub fn from_vec<T: NativePType + ArrowNativeType>(values: Vec<T>, validity: Validity) -> Self {
        Self::try_new(ScalarBuffer::from(values), validity).unwrap()
    }

    pub fn from_nullable_vec<T: NativePType + ArrowNativeType>(values: Vec<Option<T>>) -> Self {
        let elems: Vec<T> = values.iter().map(|v| v.unwrap_or_default()).collect();
        let validity = Validity::from(values.iter().map(|v| v.is_some()).collect::<Vec<_>>());
        Self::from_vec(elems, validity)
    }

    pub fn validity(&self) -> Validity {
        self.metadata()
            .validity
            .to_validity(self.array().child(0, &Validity::DTYPE))
    }

    pub fn ptype(&self) -> PType {
        // TODO(ngates): we can't really cache this anywhere?
        self.dtype().try_into().unwrap()
    }

    pub fn buffer(&self) -> &Buffer {
        self.array().buffer().expect("missing buffer")
    }

    pub fn scalar_buffer<T: NativePType>(&self) -> ScalarBuffer<T> {
        assert_eq!(
            T::PTYPE,
            self.ptype(),
            "Attempted to get scalar buffer of type {} from array of type {}",
            T::PTYPE,
            self.ptype(),
        );
        ScalarBuffer::new(self.buffer().clone().into(), 0, self.len())
    }

    pub fn typed_data<T: NativePType>(&self) -> &[T] {
        assert_eq!(
            T::PTYPE,
            self.ptype(),
            "Attempted to get typed_data of type {} from array of type {}",
            T::PTYPE,
            self.ptype(),
        );
        self.buffer().typed_data::<T>()
    }

    pub fn reinterpret_cast(&self, ptype: PType) -> Self {
        if self.ptype() == ptype {
            return self.clone();
        }

        assert_eq!(
            self.ptype().byte_width(),
            ptype.byte_width(),
            "can't reinterpret cast between integers of two different widths"
        );

        match_each_native_ptype!(ptype, |$P| {
            PrimitiveArray::try_new(
                ScalarBuffer::<$P>::new(self.buffer().clone().into(), 0, self.len()),
                self.validity(),
            )
            .unwrap()
        })
    }

    pub fn patch<P: AsPrimitive<usize>, T: NativePType>(
        self,
        positions: &[P],
        values: &[T],
    ) -> VortexResult<Self> {
        if self.ptype() != T::PTYPE {
            vortex_bail!(MismatchedTypes: self.dtype(), T::PTYPE)
        }

        let validity = self.validity().to_static();

        let mut own_values = self
            .into_buffer()
            .into_vec::<T>()
            .unwrap_or_else(|b| Vec::from(b.typed_data::<T>()));
        // TODO(robert): Also patch validity
        for (idx, value) in positions.iter().zip_eq(values.iter()) {
            own_values[(*idx).as_()] = *value;
        }
        Self::try_new(ScalarBuffer::from(own_values), validity)
    }
}

impl<'a> PrimitiveArray<'a> {
    pub fn into_buffer(self) -> Buffer<'a> {
        self.into_array().into_buffer().unwrap()
    }
}

impl<T: NativePType> From<Vec<T>> for PrimitiveArray<'_> {
    fn from(values: Vec<T>) -> Self {
        PrimitiveArray::from_vec(values, Validity::NonNullable)
    }
}

impl<T: NativePType> IntoArray<'static> for Vec<T> {
    fn into_array(self) -> OwnedArray {
        PrimitiveArray::from(self).into_array()
    }
}

impl ArrayFlatten for PrimitiveArray<'_> {
    fn flatten<'a>(self) -> VortexResult<Flattened<'a>>
    where
        Self: 'a,
    {
        Ok(Flattened::Primitive(self))
    }
}

impl ArrayTrait for PrimitiveArray<'_> {
    fn len(&self) -> usize {
        self.buffer().len() / self.ptype().byte_width()
    }
}

impl ArrayValidity for PrimitiveArray<'_> {
    fn is_valid(&self, index: usize) -> bool {
        self.validity().is_valid(index)
    }

    fn logical_validity(&self) -> LogicalValidity {
        self.validity().to_logical(self.len())
    }
}

impl AcceptArrayVisitor for PrimitiveArray<'_> {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        visitor.visit_buffer(self.buffer())?;
        visitor.visit_validity(&self.validity())
    }
}

impl<'a> Array<'a> {
    pub fn into_primitive(self) -> PrimitiveArray<'a> {
        PrimitiveArray::try_from(self).expect("expected primitive array")
    }

    pub fn as_primitive(&self) -> PrimitiveArray {
        PrimitiveArray::try_from(self).expect("expected primitive array")
    }
}

impl EncodingCompression for PrimitiveEncoding {}

impl ScalarSubtractFn for PrimitiveArray<'_> {
    fn scalar_subtract(&self, to_subtract: Scalar) -> VortexResult<OwnedArray> {
        if self.dtype() != to_subtract.dtype() {
            vortex_bail!(MismatchedTypes: self.dtype(), to_subtract.dtype())
        }

        let result = match to_subtract.dtype() {
            DType::Int(..) => {
                match_each_integer_ptype!(self.ptype(), |$T| {
                    let to_subtract = <scalar::Scalar as TryInto<$T>>::try_into(to_subtract)?;
                    let min = self.statistics().compute_as_cast(Stat::Min).unwrap_or($T::MAX);
                    if let (_, true) = min.overflowing_sub(to_subtract) {
                        vortex_bail!("Integer subtraction underflow")
                    }
                    let sub_vec : Vec<$T> = self.typed_data::<$T>().iter().map(|&v| v - to_subtract).collect_vec();
                    PrimitiveArray::from(sub_vec)
                })
            }
            DType::Decimal(..) | DType::Float(..) => {
                match_each_native_ptype!(self.ptype(), |$T| {
                    let to_subtract = <scalar::Scalar as TryInto<$T>>::try_into(to_subtract)?;
                    let sub_vec : Vec<$T> = self.typed_data::<$T>().iter().map(|&v| v - to_subtract).collect_vec();
                    PrimitiveArray::from(sub_vec)
                })
            }
            _ => vortex_bail!(InvalidArgument: "Can only subtract numeric types"),
        };

        Ok(result.into_array())
    }
}
