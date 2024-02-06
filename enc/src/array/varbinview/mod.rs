mod compress;

use std::any::Any;
use std::str::from_utf8_unchecked;
use std::sync::{Arc, RwLock};
use std::{iter, mem};

use arrow::array::cast::AsArray;
use arrow::array::types::UInt8Type;
use arrow::array::{ArrayRef as ArrowArrayRef, BinaryBuilder, StringBuilder};

use crate::array::{
    check_slice_bounds, check_validity_buffer, Array, ArrayRef, ArrowIterator, Encoding,
    EncodingId, EncodingRef,
};
use crate::arrow::CombineChunks;
use crate::compress::ArrayCompression;
use crate::dtype::{DType, IntWidth, Nullability, Signedness};
use crate::error::EncResult;
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::scalar::{NullableScalar, Scalar};
use crate::stats::{Stats, StatsSet};

#[derive(Clone, Copy)]
#[repr(C, align(8))]
struct Inlined {
    size: u32,
    data: [u8; 12],
}

impl Inlined {
    #[allow(dead_code)]
    pub fn new(value: &str) -> Self {
        assert!(
            value.len() < 13,
            "Inlined strings must be shorter than 13 characters, {} given",
            value.len()
        );
        let mut inlined = Inlined {
            size: value.len() as u32,
            data: [0u8; 12],
        };
        inlined.data[..value.len()].copy_from_slice(value.as_bytes());
        inlined
    }
}

#[derive(Clone, Copy)]
#[repr(C, align(8))]
struct Ref {
    size: u32,
    prefix: [u8; 4],
    buffer_index: u32,
    offset: u32,
}

#[derive(Clone, Copy)]
#[repr(C, align(8))]
union BinaryView {
    inlined: Inlined,
    _ref: Ref,
}

impl BinaryView {
    #[inline]
    pub fn from_le_bytes(bytes: [u8; 16]) -> BinaryView {
        unsafe { mem::transmute(bytes) }
    }

    #[inline]
    #[allow(dead_code)]
    pub fn to_le_bytes(self) -> [u8; 16] {
        unsafe { mem::transmute(self) }
    }
}

pub const VIEW_SIZE: usize = std::mem::size_of::<BinaryView>();

#[derive(Debug, Clone)]
pub struct VarBinViewArray {
    views: ArrayRef,
    data: Vec<ArrayRef>,
    dtype: DType,
    validity: Option<ArrayRef>,
    stats: Arc<RwLock<StatsSet>>,
}

impl VarBinViewArray {
    pub fn new(
        views: ArrayRef,
        data: Vec<ArrayRef>,
        dtype: DType,
        validity: Option<ArrayRef>,
    ) -> Self {
        if !matches!(
            views.dtype(),
            DType::Int(IntWidth::_8, Signedness::Unsigned, Nullability::NonNullable)
        ) {
            panic!("Unsupported type for views array {:?}", views.dtype());
        }
        data.iter().for_each(|d| {
            if !matches!(d.dtype(), DType::Int(IntWidth::_8, Signedness::Unsigned, _)) {
                panic!("Unsupported type for data array {:?}", d.dtype());
            }
        });
        if !matches!(dtype, DType::Binary(_) | DType::Utf8(_)) {
            panic!("Unsupported dtype for VarBinView array");
        }
        check_validity_buffer(validity.as_ref());

        Self {
            views,
            data,
            dtype,
            validity,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        }
    }

    fn is_valid(&self, index: usize) -> bool {
        self.validity
            .as_ref()
            .map(|v| v.scalar_at(index).unwrap().try_into().unwrap())
            .unwrap_or(true)
    }

    pub fn plain_size(&self) -> usize {
        (0..self.views.len() / VIEW_SIZE).fold(0usize, |acc, i| {
            let view = self.view_at(i);
            unsafe { acc + view.inlined.size as usize }
        })
    }

    pub(self) fn view_at(&self, index: usize) -> BinaryView {
        let view_slice = self
            .views
            .slice(index * VIEW_SIZE, (index + 1) * VIEW_SIZE)
            .unwrap()
            .iter_arrow()
            .next()
            .unwrap();
        let view_vec: &[u8] = view_slice.as_primitive::<UInt8Type>().values();
        BinaryView::from_le_bytes(view_vec.try_into().unwrap())
    }

    #[inline]
    pub fn views(&self) -> &dyn Array {
        self.views.as_ref()
    }

    #[inline]
    pub fn data(&self) -> &[ArrayRef] {
        &self.data
    }

    #[inline]
    pub fn validity(&self) -> Option<&ArrayRef> {
        self.validity.as_ref()
    }

    pub fn bytes_at(&self, index: usize) -> EncResult<Vec<u8>> {
        let view = self.view_at(index);
        unsafe {
            if view.inlined.size > 12 {
                let arrow_data_buffer = self
                    .data
                    .get(view._ref.buffer_index as usize)
                    .unwrap()
                    .slice(
                        view._ref.offset as usize,
                        (view._ref.size + view._ref.offset) as usize,
                    )?
                    .iter_arrow()
                    .combine_chunks();

                Ok(arrow_data_buffer
                    .as_primitive::<UInt8Type>()
                    .values()
                    .to_vec())
            } else {
                Ok(view.inlined.data[..view.inlined.size as usize].to_vec())
            }
        }
    }
}

impl Array for VarBinViewArray {
    #[inline]
    fn as_any(&self) -> &dyn Any {
        self
    }

    #[inline]
    fn boxed(self) -> ArrayRef {
        Box::new(self)
    }

    #[inline]
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

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

    fn scalar_at(&self, index: usize) -> EncResult<Box<dyn Scalar>> {
        if self.is_valid(index) {
            self.bytes_at(index).map(|bytes| {
                if matches!(self.dtype, DType::Utf8(_)) {
                    unsafe { String::from_utf8_unchecked(bytes) }.into()
                } else {
                    bytes.into()
                }
            })
        } else {
            Ok(NullableScalar::none(self.dtype.clone()).boxed())
        }
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        let data_arr: ArrowArrayRef = if matches!(self.dtype, DType::Utf8(_)) {
            let mut data_buf = StringBuilder::with_capacity(self.len(), self.plain_size());
            for i in 0..self.views.len() / VIEW_SIZE {
                if !self.is_valid(i) {
                    data_buf.append_null()
                } else {
                    unsafe {
                        data_buf.append_value(from_utf8_unchecked(
                            self.bytes_at(i).unwrap().as_slice(),
                        ));
                    }
                }
            }
            Arc::new(data_buf.finish())
        } else {
            let mut data_buf = BinaryBuilder::with_capacity(self.len(), self.plain_size());
            for i in 0..self.views.len() / VIEW_SIZE {
                if !self.is_valid(i) {
                    data_buf.append_null()
                } else {
                    data_buf.append_value(self.bytes_at(i).unwrap())
                }
            }
            Arc::new(data_buf.finish())
        };
        Box::new(iter::once(data_arr))
    }

    fn slice(&self, start: usize, stop: usize) -> EncResult<ArrayRef> {
        check_slice_bounds(self, start, stop)?;

        Ok(Self {
            views: self.views.slice(start * VIEW_SIZE, stop * VIEW_SIZE)?,
            data: self.data.clone(),
            dtype: self.dtype.clone(),
            validity: self
                .validity
                .as_ref()
                .map(|v| v.slice(start, stop))
                .transpose()?,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        }
        .boxed())
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &VarBinViewEncoding
    }

    fn nbytes(&self) -> usize {
        self.views.nbytes() + self.data.iter().map(|arr| arr.nbytes()).sum::<usize>()
    }

    fn compression(&self) -> Option<&dyn ArrayCompression> {
        Some(self)
    }
}

impl<'arr> AsRef<(dyn Array + 'arr)> for VarBinViewArray {
    fn as_ref(&self) -> &(dyn Array + 'arr) {
        self
    }
}

#[derive(Debug)]
struct VarBinViewEncoding;

pub const VARBINVIEW_ENCODING: EncodingId = EncodingId("enc.varbinview");

impl Encoding for VarBinViewEncoding {
    fn id(&self) -> &EncodingId {
        &VARBINVIEW_ENCODING
    }
}

impl ArrayDisplay for VarBinViewArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.writeln("views:")?;
        f.indent(|ind| ind.array(self.views()))?;
        f.writeln("data:")?;
        f.indent(|ind| {
            for d in self.data() {
                ind.array(d.as_ref())?;
            }
            Ok(())
        })
    }
}

#[cfg(test)]
mod test {
    use arrow::array::GenericStringArray as ArrowStringArray;

    use crate::array::primitive::PrimitiveArray;

    use super::*;

    fn binary_array() -> VarBinViewArray {
        let values =
            PrimitiveArray::from_vec("hello world this is a long string".as_bytes().to_vec());
        let view1 = BinaryView {
            inlined: Inlined::new("hello world"),
        };
        let view2 = BinaryView {
            _ref: Ref {
                size: 33,
                prefix: "hell".as_bytes().try_into().unwrap(),
                buffer_index: 0,
                offset: 0,
            },
        };
        let view_arr = PrimitiveArray::from_vec(
            vec![view1.to_le_bytes(), view2.to_le_bytes()]
                .into_iter()
                .flatten()
                .collect::<Vec<u8>>(),
        );

        VarBinViewArray::new(
            view_arr.boxed(),
            vec![values.boxed()],
            DType::Utf8(Nullability::NonNullable),
            None,
        )
    }

    #[test]
    pub fn varbin_view() {
        let binary_arr = binary_array();
        assert_eq!(binary_arr.len(), 2);
        assert_eq!(binary_arr.scalar_at(0), Ok("hello world".into()));
        assert_eq!(
            binary_arr.scalar_at(1),
            Ok("hello world this is a long string".into())
        )
    }

    #[test]
    pub fn slice() {
        let binary_arr = binary_array().slice(1, 2).unwrap();
        assert_eq!(
            binary_arr.scalar_at(0),
            Ok("hello world this is a long string".into())
        );
    }

    #[test]
    pub fn iter() {
        let binary_array = binary_array();
        assert_eq!(
            binary_array
                .iter_arrow()
                .combine_chunks()
                .as_string::<i32>(),
            &ArrowStringArray::<i32>::from(vec![
                "hello world",
                "hello world this is a long string",
            ])
        );
    }
}
