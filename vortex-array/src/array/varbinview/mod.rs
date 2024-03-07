use std::fmt::{Debug, Formatter};
use std::mem::ManuallyDrop;
use std::sync::{Arc, RwLock};
use std::{mem, slice};

use linkme::distributed_slice;
use vortex_error::{vortex_bail, vortex_err, VortexResult};
use vortex_schema::{DType, IntWidth, Nullability, Signedness};

use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::primitive::PrimitiveEncoding;
use crate::array::IntoArray;
use crate::array::{check_slice_bounds, Array, ArrayRef};
use crate::compute::flatten::flatten_primitive;
use crate::compute::ArrayCompute;
use crate::encoding::{Encoding, EncodingId, EncodingRef, ENCODINGS};
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::iterator::ArrayIter;
use crate::serde::{ArraySerde, EncodingSerde};
use crate::stats::{Stats, StatsSet};
use crate::validity::OwnedValidity;
use crate::validity::{Validity, ValidityView};
use crate::view::AsView;
use crate::{impl_array, ArrayWalker};

mod accessor;
mod compute;
mod serde;
mod stats;

#[derive(Clone, Copy, Debug)]
#[repr(C, align(8))]
struct Inlined {
    size: u32,
    data: [u8; BinaryView::MAX_INLINED_SIZE],
}

impl Inlined {
    #[allow(dead_code)]
    pub fn new(value: &[u8]) -> Self {
        assert!(
            value.len() <= BinaryView::MAX_INLINED_SIZE,
            "Inlined strings must be shorter than 13 characters, {} given",
            value.len()
        );
        let mut inlined = Inlined {
            size: value.len() as u32,
            data: [0u8; BinaryView::MAX_INLINED_SIZE],
        };
        inlined.data[..value.len()].copy_from_slice(value);
        inlined
    }
}

#[derive(Clone, Copy, Debug)]
#[repr(C, align(8))]
struct Ref {
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

pub const VIEW_SIZE: usize = mem::size_of::<BinaryView>();

#[derive(Debug, Clone)]
pub struct VarBinViewArray {
    views: ArrayRef,
    data: Vec<ArrayRef>,
    dtype: DType,
    validity: Option<Validity>,
    stats: Arc<RwLock<StatsSet>>,
}

impl VarBinViewArray {
    pub fn try_new(
        views: ArrayRef,
        data: Vec<ArrayRef>,
        dtype: DType,
        validity: Option<Validity>,
    ) -> VortexResult<Self> {
        if !matches!(
            views.dtype(),
            DType::Int(IntWidth::_8, Signedness::Unsigned, Nullability::NonNullable)
        ) {
            vortex_bail!(MismatchedTypes: "u8", views.dtype());
        }

        for d in data.iter() {
            if !matches!(
                d.dtype(),
                DType::Int(IntWidth::_8, Signedness::Unsigned, Nullability::NonNullable)
            ) {
                vortex_bail!(MismatchedTypes: "u8", d.dtype());
            }
        }

        if !matches!(dtype, DType::Binary(_) | DType::Utf8(_)) {
            vortex_bail!(MismatchedTypes: "utf8 or binary", dtype);
        }

        let dtype = if validity.is_some() && !dtype.is_nullable() {
            dtype.as_nullable()
        } else {
            dtype
        };

        Ok(Self {
            views,
            data,
            dtype,
            validity,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        })
    }

    pub(self) fn view_slice(&self) -> &[BinaryView] {
        unsafe {
            slice::from_raw_parts(
                self.views.as_primitive().typed_data::<u8>().as_ptr() as _,
                self.views.len() / VIEW_SIZE,
            )
        }
    }

    pub(self) fn view_at(&self, index: usize) -> BinaryView {
        self.view_slice()[index]
    }

    #[inline]
    pub fn views(&self) -> &ArrayRef {
        &self.views
    }

    #[inline]
    pub fn data(&self) -> &[ArrayRef] {
        &self.data
    }

    pub fn from_vec<T: AsRef<[u8]>>(vec: Vec<T>, dtype: DType) -> Self {
        let mut views: Vec<BinaryView> = Vec::with_capacity(vec.len());
        let mut values: Vec<Vec<u8>> = Vec::new();
        values.push(Vec::new());
        let mut current_bytes = values.last_mut().unwrap();
        let mut last_buf_idx = 0;
        for v in vec {
            let vbytes = v.as_ref();
            if current_bytes.len() + vbytes.len() > u32::MAX as usize {
                values.push(Vec::new());
                last_buf_idx += 1;
                current_bytes = values.last_mut().unwrap();
            }

            if vbytes.len() > BinaryView::MAX_INLINED_SIZE {
                views.push(BinaryView {
                    _ref: Ref::new(
                        vbytes.len() as u32,
                        vbytes[0..4].try_into().unwrap(),
                        last_buf_idx,
                        current_bytes.len() as u32,
                    ),
                });
                current_bytes.extend_from_slice(vbytes);
            } else {
                views.push(BinaryView {
                    inlined: Inlined::new(vbytes),
                });
            }
        }

        let views_u8: Vec<u8> = unsafe {
            let mut views_clone = ManuallyDrop::new(views);
            Vec::from_raw_parts(
                views_clone.as_mut_ptr() as _,
                views_clone.len() * VIEW_SIZE,
                views_clone.capacity() * VIEW_SIZE,
            )
        };

        VarBinViewArray::try_new(
            views_u8.into_array(),
            values
                .into_iter()
                .map(|v| v.into_array())
                .collect::<Vec<_>>(),
            dtype,
            None,
        )
        .unwrap()
    }

    pub fn from_iter<T: AsRef<[u8]>, I: IntoIterator<Item = Option<T>>>(
        iter: I,
        dtype: DType,
    ) -> Self {
        let iter = iter.into_iter();
        let (lower, _) = iter.size_hint();
        let mut validity: Vec<bool> = Vec::with_capacity(lower);
        let mut views: Vec<BinaryView> = Vec::with_capacity(lower);
        let mut values: Vec<Vec<u8>> = Vec::new();
        values.push(Vec::new());
        let mut current_bytes = values.last_mut().unwrap();
        let mut last_buf_idx = 0;
        for v in iter {
            if let Some(b) = v {
                let vbytes = b.as_ref();
                if current_bytes.len() + vbytes.len() > u32::MAX as usize {
                    values.push(Vec::new());
                    last_buf_idx += 1;
                    current_bytes = values.last_mut().unwrap();
                }

                if vbytes.len() > BinaryView::MAX_INLINED_SIZE {
                    views.push(BinaryView {
                        _ref: Ref::new(
                            vbytes.len() as u32,
                            vbytes[0..4].try_into().unwrap(),
                            last_buf_idx,
                            current_bytes.len() as u32,
                        ),
                    });
                    current_bytes.extend_from_slice(vbytes);
                } else {
                    views.push(BinaryView {
                        inlined: Inlined::new(vbytes),
                    });
                }
                validity.push(true)
            } else {
                views.push(BinaryView {
                    inlined: Inlined::new("".as_bytes()),
                });
                validity.push(false);
            }
        }

        let views_u8: Vec<u8> = unsafe {
            let mut views_clone = ManuallyDrop::new(views);
            Vec::from_raw_parts(
                views_clone.as_mut_ptr() as _,
                views_clone.len() * VIEW_SIZE,
                views_clone.capacity() * VIEW_SIZE,
            )
        };

        VarBinViewArray::try_new(
            views_u8.into_array(),
            values
                .into_iter()
                .map(|v| v.into_array())
                .collect::<Vec<_>>(),
            if validity.is_empty() {
                dtype
            } else {
                dtype.as_nullable()
            },
            (!validity.is_empty()).then(|| validity.into()),
        )
        .unwrap()
    }

    pub fn iter_primitive(&self) -> VortexResult<ArrayIter<'_, VarBinViewArray, &[u8]>> {
        if self
            .data()
            .iter()
            .all(|b| b.encoding().id() == PrimitiveEncoding::ID)
        {
            Ok(ArrayIter::new(self))
        } else {
            Err(vortex_err!("Bytes array was not a primitive array"))
        }
    }

    pub fn iter(&self) -> ArrayIter<'_, VarBinViewArray, Vec<u8>> {
        ArrayIter::new(self)
    }

    pub fn bytes_at(&self, index: usize) -> VortexResult<Vec<u8>> {
        let view = self.view_at(index);
        unsafe {
            if view.inlined.size > 12 {
                let arrow_data_buffer = flatten_primitive(
                    self.data
                        .get(view._ref.buffer_index as usize)
                        .unwrap()
                        .slice(
                            view._ref.offset as usize,
                            (view._ref.size + view._ref.offset) as usize,
                        )?
                        .as_ref(),
                )?;
                // TODO(ngates): can we avoid returning a copy?
                Ok(arrow_data_buffer.typed_data::<u8>().to_vec())
            } else {
                Ok(view.inlined.data[..view.inlined.size as usize].to_vec())
            }
        }
    }
}

impl Array for VarBinViewArray {
    impl_array!();

    #[inline]
    fn len(&self) -> usize {
        self.views.len() / std::mem::size_of::<BinaryView>()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.views.is_empty()
    }

    #[inline]
    fn dtype(&self) -> &DType {
        &self.dtype
    }

    #[inline]
    fn stats(&self) -> Stats {
        Stats::new(&self.stats, self)
    }

    fn slice(&self, start: usize, stop: usize) -> VortexResult<ArrayRef> {
        check_slice_bounds(self, start, stop)?;

        Ok(Self {
            views: self.views.slice(start * VIEW_SIZE, stop * VIEW_SIZE)?,
            data: self.data.clone(),
            dtype: self.dtype.clone(),
            validity: self.validity().map(|v| v.slice(start, stop)).transpose()?,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        }
        .into_array())
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &VarBinViewEncoding
    }

    fn nbytes(&self) -> usize {
        self.views.nbytes() + self.data.iter().map(|arr| arr.nbytes()).sum::<usize>()
    }

    #[inline]
    fn with_compute_mut(
        &self,
        f: &mut dyn FnMut(&dyn ArrayCompute) -> VortexResult<()>,
    ) -> VortexResult<()> {
        f(self)
    }

    fn serde(&self) -> Option<&dyn ArraySerde> {
        Some(self)
    }

    fn walk(&self, walker: &mut dyn ArrayWalker) -> VortexResult<()> {
        walker.visit_child(self.views())?;
        for data in self.data() {
            walker.visit_child(data)?;
        }
        Ok(())
    }
}

impl OwnedValidity for VarBinViewArray {
    fn validity(&self) -> Option<ValidityView> {
        self.validity.as_view()
    }
}

#[derive(Debug)]
pub struct VarBinViewEncoding;

impl VarBinViewEncoding {
    pub const ID: EncodingId = EncodingId::new("vortex.varbinview");
}

#[distributed_slice(ENCODINGS)]
static ENCODINGS_VARBINVIEW: EncodingRef = &VarBinViewEncoding;

impl Encoding for VarBinViewEncoding {
    fn id(&self) -> EncodingId {
        Self::ID
    }

    fn serde(&self) -> Option<&dyn EncodingSerde> {
        Some(self)
    }
}

impl ArrayDisplay for VarBinViewArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.child("views", self.views())?;
        for (i, d) in self.data().iter().enumerate() {
            f.child(&format!("data_{}", i), d.as_ref())?;
        }
        f.validity(self.validity())
    }
}

impl From<Vec<&[u8]>> for VarBinViewArray {
    fn from(value: Vec<&[u8]>) -> Self {
        VarBinViewArray::from_vec(value, DType::Binary(Nullability::NonNullable))
    }
}

impl From<Vec<Vec<u8>>> for VarBinViewArray {
    fn from(value: Vec<Vec<u8>>) -> Self {
        VarBinViewArray::from_vec(value, DType::Binary(Nullability::NonNullable))
    }
}

impl From<Vec<String>> for VarBinViewArray {
    fn from(value: Vec<String>) -> Self {
        VarBinViewArray::from_vec(value, DType::Utf8(Nullability::NonNullable))
    }
}

impl From<Vec<&str>> for VarBinViewArray {
    fn from(value: Vec<&str>) -> Self {
        VarBinViewArray::from_vec(value, DType::Utf8(Nullability::NonNullable))
    }
}

impl<'a> FromIterator<Option<&'a [u8]>> for VarBinViewArray {
    fn from_iter<T: IntoIterator<Item = Option<&'a [u8]>>>(iter: T) -> Self {
        VarBinViewArray::from_iter(iter, DType::Binary(Nullability::NonNullable))
    }
}

impl FromIterator<Option<Vec<u8>>> for VarBinViewArray {
    fn from_iter<T: IntoIterator<Item = Option<Vec<u8>>>>(iter: T) -> Self {
        VarBinViewArray::from_iter(iter, DType::Binary(Nullability::NonNullable))
    }
}

impl FromIterator<Option<String>> for VarBinViewArray {
    fn from_iter<T: IntoIterator<Item = Option<String>>>(iter: T) -> Self {
        VarBinViewArray::from_iter(iter, DType::Utf8(Nullability::NonNullable))
    }
}

impl<'a> FromIterator<Option<&'a str>> for VarBinViewArray {
    fn from_iter<T: IntoIterator<Item = Option<&'a str>>>(iter: T) -> Self {
        VarBinViewArray::from_iter(iter, DType::Utf8(Nullability::NonNullable))
    }
}

#[cfg(test)]
mod test {
    use arrow_array::array::StringViewArray as ArrowStringViewArray;

    use crate::array::varbinview::VarBinViewArray;
    use crate::array::Array;
    use crate::compute::as_arrow::as_arrow;
    use crate::compute::scalar_at::scalar_at;
    use crate::scalar::Scalar;

    #[test]
    pub fn varbin_view() {
        let binary_arr =
            VarBinViewArray::from(vec!["hello world", "hello world this is a long string"]);
        assert_eq!(binary_arr.len(), 2);
        assert_eq!(
            scalar_at(&binary_arr, 0).unwrap(),
            Scalar::from("hello world")
        );
        assert_eq!(
            scalar_at(&binary_arr, 1).unwrap(),
            Scalar::from("hello world this is a long string")
        );
    }

    #[test]
    pub fn slice() {
        let binary_arr =
            VarBinViewArray::from(vec!["hello world", "hello world this is a long string"])
                .slice(1, 2)
                .unwrap();
        assert_eq!(
            scalar_at(&binary_arr, 0).unwrap(),
            Scalar::from("hello world this is a long string")
        );
    }

    #[test]
    pub fn iter() {
        let binary_array =
            VarBinViewArray::from(vec!["hello world", "hello world this is a long string"]);
        assert_eq!(
            as_arrow(&binary_array)
                .unwrap()
                .as_any()
                .downcast_ref::<ArrowStringViewArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            ArrowStringViewArray::from(vec!["hello world", "hello world this is a long string",])
                .iter()
                .collect::<Vec<_>>()
        );
    }
}
