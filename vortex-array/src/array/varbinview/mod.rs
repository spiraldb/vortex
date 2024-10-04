use std::fmt::{Debug, Display, Formatter};
use std::ops::Deref;
use std::sync::Arc;
use std::{mem, slice};

use ::serde::{Deserialize, Serialize};
use arrow_array::builder::{BinaryViewBuilder, StringViewBuilder};
use arrow_array::{ArrayRef, BinaryViewArray, StringViewArray};
use arrow_buffer::ScalarBuffer;
use arrow_schema::DataType;
use itertools::Itertools;
use vortex_dtype::{DType, PType};
use vortex_error::{vortex_bail, vortex_panic, VortexError, VortexExpect as _, VortexResult};

use crate::array::varbin::VarBinArray;
use crate::arrow::FromArrowArray;
use crate::compute::slice;
use crate::encoding::ids;
use crate::stats::StatsSet;
use crate::validity::{ArrayValidity, LogicalValidity, Validity, ValidityMetadata};
use crate::visitor::{AcceptArrayVisitor, ArrayVisitor};
use crate::{
    impl_encoding, Array, ArrayDType, ArrayTrait, Canonical, IntoArrayVariant, IntoCanonical,
};

mod accessor;
mod compute;
mod stats;
mod variants;

#[derive(Clone, Copy, Debug)]
#[repr(C, align(8))]
pub struct Inlined {
    size: u32,
    data: [u8; BinaryView::MAX_INLINED_SIZE],
}

impl Inlined {
    pub fn new(value: &[u8]) -> Self {
        assert!(
            value.len() <= BinaryView::MAX_INLINED_SIZE,
            "Inlined strings must be shorter than 13 characters, {} given",
            value.len()
        );
        let mut inlined = Self {
            size: value.len() as u32,
            data: [0u8; BinaryView::MAX_INLINED_SIZE],
        };
        inlined.data[..value.len()].copy_from_slice(value);
        inlined
    }
}

#[derive(Clone, Copy, Debug)]
#[repr(C, align(8))]
pub struct Ref {
    size: u32,
    prefix: [u8; 4],
    buffer_index: u32,
    offset: u32,
}

impl Ref {
    pub fn new(size: u32, prefix: [u8; 4], buffer_index: u32, offset: u32) -> Self {
        Self {
            size,
            prefix,
            buffer_index,
            offset,
        }
    }
}

#[derive(Clone, Copy)]
#[repr(C, align(8))]
pub union BinaryView {
    inlined: Inlined,
    _ref: Ref,
}

impl BinaryView {
    pub const MAX_INLINED_SIZE: usize = 12;

    #[inline]
    pub fn size(&self) -> usize {
        unsafe { self.inlined.size as usize }
    }

    pub fn is_inlined(&self) -> bool {
        unsafe { self.inlined.size <= Self::MAX_INLINED_SIZE as u32 }
    }
}

impl Debug for BinaryView {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("BinaryView");
        if self.is_inlined() {
            s.field("inline", unsafe { &self.inlined });
        } else {
            s.field("ref", unsafe { &self._ref });
        }
        s.finish()
    }
}

// reminder: views are 16 bytes with 8-byte alignment
pub(crate) const VIEW_SIZE: usize = mem::size_of::<BinaryView>();

impl_encoding!("vortex.varbinview", ids::VAR_BIN_VIEW, VarBinView);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VarBinViewMetadata {
    validity: ValidityMetadata,
    data_lens: Vec<usize>,
}

impl Display for VarBinViewMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(self, f)
    }
}

impl VarBinViewArray {
    pub fn try_new(
        views: Array,
        data: Vec<Array>,
        dtype: DType,
        validity: Validity,
    ) -> VortexResult<Self> {
        if !matches!(views.dtype(), &DType::BYTES) {
            vortex_bail!(MismatchedTypes: "u8", views.dtype());
        }

        for d in data.iter() {
            if !matches!(d.dtype(), &DType::BYTES) {
                vortex_bail!(MismatchedTypes: "u8", d.dtype());
            }
        }

        if !matches!(dtype, DType::Binary(_) | DType::Utf8(_)) {
            vortex_bail!(MismatchedTypes: "utf8 or binary", dtype);
        }

        if dtype.is_nullable() == (validity == Validity::NonNullable) {
            vortex_bail!("incorrect validity {:?}", validity);
        }

        let num_views = views.len() / VIEW_SIZE;
        let metadata = VarBinViewMetadata {
            validity: validity.to_metadata(num_views)?,
            data_lens: data.iter().map(|a| a.len()).collect_vec(),
        };

        let mut children = Vec::with_capacity(data.len() + 2);
        children.push(views);
        children.extend(data);
        if let Some(a) = validity.into_array() {
            children.push(a)
        }

        Self::try_from_parts(dtype, num_views, metadata, children.into(), StatsSet::new())
    }

    fn view_slice(&self) -> &[BinaryView] {
        unsafe {
            slice::from_raw_parts(
                self.views()
                    .into_primitive()
                    .vortex_expect("Views must be a primitive array")
                    .maybe_null_slice::<u8>()
                    .as_ptr() as _,
                self.views().len() / VIEW_SIZE,
            )
        }
    }

    fn view_at(&self, index: usize) -> BinaryView {
        self.view_slice()[index]
    }

    #[inline]
    pub fn views(&self) -> Array {
        self.as_ref()
            .child(0, &DType::BYTES, self.len() * VIEW_SIZE)
            .vortex_expect("VarBinViewArray is missing its views")
    }

    #[inline]
    pub fn bytes(&self, idx: usize) -> Array {
        self.as_ref()
            .child(idx + 1, &DType::BYTES, self.metadata().data_lens[idx])
            .vortex_expect("VarBinViewArray is missing its data buffer")
    }

    pub fn validity(&self) -> Validity {
        self.metadata().validity.to_validity(|| {
            self.as_ref()
                .child(
                    self.metadata().data_lens.len() + 1,
                    &Validity::DTYPE,
                    self.len(),
                )
                .vortex_expect("VarBinViewArray: validity child")
        })
    }

    pub fn from_iter_str<T: AsRef<str>, I: IntoIterator<Item = T>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let mut builder = StringViewBuilder::with_capacity(iter.size_hint().0);
        for s in iter {
            builder.append_value(s);
        }
        let array = Array::from_arrow(&builder.finish(), false);
        VarBinViewArray::try_from(array)
            .vortex_expect("Failed to convert iterator of nullable strings to VarBinViewArray")
    }

    pub fn from_iter_nullable_str<T: AsRef<str>, I: IntoIterator<Item = Option<T>>>(
        iter: I,
    ) -> Self {
        let iter = iter.into_iter();
        let mut builder = StringViewBuilder::with_capacity(iter.size_hint().0);
        builder.extend(iter);

        let array = Array::from_arrow(&builder.finish(), true);
        VarBinViewArray::try_from(array)
            .vortex_expect("Failed to convert iterator of nullable strings to VarBinViewArray")
    }

    pub fn from_iter_bin<T: AsRef<[u8]>, I: IntoIterator<Item = T>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let mut builder = BinaryViewBuilder::with_capacity(iter.size_hint().0);
        for b in iter {
            builder.append_value(b);
        }
        let array = Array::from_arrow(&builder.finish(), false);
        VarBinViewArray::try_from(array)
            .vortex_expect("Failed to convert iterator of bytes to VarBinViewArray")
    }

    pub fn from_iter_nullable_bin<T: AsRef<[u8]>, I: IntoIterator<Item = Option<T>>>(
        iter: I,
    ) -> Self {
        let iter = iter.into_iter();
        let mut builder = BinaryViewBuilder::with_capacity(iter.size_hint().0);
        builder.extend(iter);
        let array = Array::from_arrow(&builder.finish(), true);
        VarBinViewArray::try_from(array)
            .vortex_expect("Failed to convert iterator of nullable bytes to VarBinViewArray")
    }

    pub fn bytes_at(&self, index: usize) -> VortexResult<Vec<u8>> {
        let view = self.view_at(index);
        unsafe {
            if !view.is_inlined() {
                let data_buf = slice(
                    self.bytes(view._ref.buffer_index as usize),
                    view._ref.offset as usize,
                    (view._ref.size + view._ref.offset) as usize,
                )?
                .into_primitive()?;
                Ok(data_buf.maybe_null_slice::<u8>().to_vec())
            } else {
                Ok(view.inlined.data[..view.size()].to_vec())
            }
        }
    }
}

impl ArrayTrait for VarBinViewArray {}

impl IntoCanonical for VarBinViewArray {
    fn into_canonical(self) -> VortexResult<Canonical> {
        let arrow_dtype = if matches!(self.dtype(), &DType::Utf8(_)) {
            &DataType::Utf8
        } else {
            &DataType::Binary
        };
        let nullable = self.dtype().is_nullable();
        let arrow_self = as_arrow(self);
        let arrow_varbin =
            arrow_cast::cast(arrow_self.deref(), arrow_dtype).map_err(VortexError::ArrowError)?;
        let vortex_array = Array::from_arrow(arrow_varbin, nullable);

        Ok(Canonical::VarBin(VarBinArray::try_from(&vortex_array)?))
    }
}

fn as_arrow(var_bin_view: VarBinViewArray) -> ArrayRef {
    // Views should be buffer of u8
    let views = var_bin_view
        .views()
        .into_primitive()
        .vortex_expect("Views must be a primitive array");
    assert_eq!(views.ptype(), PType::U8);
    let nulls = var_bin_view
        .logical_validity()
        .to_null_buffer()
        .vortex_expect("Failed to convert logical validity to null buffer");

    let data = (0..var_bin_view.metadata().data_lens.len())
        .map(|i| var_bin_view.bytes(i).into_primitive())
        .collect::<VortexResult<Vec<_>>>()
        .vortex_expect("VarBinView byte arrays must be primitive arrays");
    if !data.is_empty() {
        assert_eq!(data[0].ptype(), PType::U8);
        assert!(data.iter().map(|d| d.ptype()).all_equal());
    }

    let data = data
        .iter()
        .map(|p| p.buffer().clone().into_arrow())
        .collect::<Vec<_>>();

    // Switch on Arrow DType.
    match var_bin_view.dtype() {
        DType::Binary(_) => Arc::new(BinaryViewArray::new(
            ScalarBuffer::<u128>::from(views.buffer().clone().into_arrow()),
            data,
            nulls,
        )),
        DType::Utf8(_) => Arc::new(StringViewArray::new(
            ScalarBuffer::<u128>::from(views.buffer().clone().into_arrow()),
            data,
            nulls,
        )),
        _ => vortex_panic!("Expected utf8 or binary, got {}", var_bin_view.dtype()),
    }
}

impl ArrayValidity for VarBinViewArray {
    fn is_valid(&self, index: usize) -> bool {
        self.validity().is_valid(index)
    }

    fn logical_validity(&self) -> LogicalValidity {
        self.validity().to_logical(self.len())
    }
}

impl AcceptArrayVisitor for VarBinViewArray {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        visitor.visit_child("views", &self.views())?;
        for i in 0..self.metadata().data_lens.len() {
            visitor.visit_child(format!("bytes_{i}").as_str(), &self.bytes(i))?;
        }
        visitor.visit_validity(&self.validity())
    }
}

impl<'a> FromIterator<Option<&'a [u8]>> for VarBinViewArray {
    fn from_iter<T: IntoIterator<Item = Option<&'a [u8]>>>(iter: T) -> Self {
        Self::from_iter_nullable_bin(iter)
    }
}

impl FromIterator<Option<Vec<u8>>> for VarBinViewArray {
    fn from_iter<T: IntoIterator<Item = Option<Vec<u8>>>>(iter: T) -> Self {
        Self::from_iter_nullable_bin(iter)
    }
}

impl FromIterator<Option<String>> for VarBinViewArray {
    fn from_iter<T: IntoIterator<Item = Option<String>>>(iter: T) -> Self {
        Self::from_iter_nullable_str(iter)
    }
}

impl<'a> FromIterator<Option<&'a str>> for VarBinViewArray {
    fn from_iter<T: IntoIterator<Item = Option<&'a str>>>(iter: T) -> Self {
        Self::from_iter_nullable_str(iter)
    }
}

#[cfg(test)]
mod test {
    use vortex_scalar::Scalar;

    use crate::array::varbinview::{BinaryView, Inlined, Ref, VarBinViewArray, VIEW_SIZE};
    use crate::compute::slice;
    use crate::compute::unary::scalar_at;
    use crate::{Array, Canonical, IntoCanonical};

    #[test]
    pub fn varbin_view() {
        let binary_arr =
            VarBinViewArray::from_iter_str(["hello world", "hello world this is a long string"]);
        assert_eq!(binary_arr.len(), 2);
        assert_eq!(
            scalar_at(binary_arr.as_ref(), 0).unwrap(),
            Scalar::from("hello world")
        );
        assert_eq!(
            scalar_at(binary_arr.as_ref(), 1).unwrap(),
            Scalar::from("hello world this is a long string")
        );
    }

    #[test]
    pub fn slice_array() {
        let binary_arr = slice(
            VarBinViewArray::from_iter_str(["hello world", "hello world this is a long string"]),
            1,
            2,
        )
        .unwrap();
        assert_eq!(
            scalar_at(&binary_arr, 0).unwrap(),
            Scalar::from("hello world this is a long string")
        );
    }

    #[test]
    pub fn flatten_array() {
        let binary_arr = VarBinViewArray::from_iter_str(["string1", "string2"]);

        let flattened = binary_arr.into_canonical().unwrap();
        assert!(matches!(flattened, Canonical::VarBin(_)));

        let var_bin: Array = flattened.into();
        assert_eq!(scalar_at(&var_bin, 0).unwrap(), Scalar::from("string1"));
        assert_eq!(scalar_at(&var_bin, 1).unwrap(), Scalar::from("string2"));
    }

    #[test]
    pub fn binary_view_size_and_alignment() {
        assert_eq!(std::mem::size_of::<Inlined>(), 16);
        assert_eq!(std::mem::size_of::<Ref>(), 16);
        assert_eq!(std::mem::size_of::<BinaryView>(), VIEW_SIZE);
        assert_eq!(std::mem::size_of::<BinaryView>(), 16);
        assert_eq!(std::mem::align_of::<BinaryView>(), 8);
    }
}
