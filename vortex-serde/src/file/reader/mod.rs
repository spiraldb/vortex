use std::cmp::Ordering;
use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::{Bytes, BytesMut};
use filtering::RowFilter;
use futures::future::BoxFuture;
use futures::{ready, FutureExt, Stream};
use projections::Projection;
use schema::Schema;
use vortex::array::chunked::ChunkedArray;
use vortex::array::constant::ConstantArray;
use vortex::array::struct_::StructArray;
use vortex::compute::unary::subtract_scalar;
use vortex::compute::{and, filter, search_sorted, slice, take, SearchSortedSide};
use vortex::{Array, ArrayDType, IntoArray, IntoArrayVariant};
use vortex_dtype::{match_each_integer_ptype, DType, StructDType};
use vortex_error::{vortex_bail, VortexError, VortexResult};
use vortex_scalar::Scalar;

use super::layouts::{Layout, StructLayout};
use crate::file::file_writer::MAGIC_BYTES;
use crate::file::footer::Footer;
use crate::io::VortexReadAt;
use crate::{ArrayBufferReader, ReadResult};

pub mod filtering;
pub mod projections;
pub mod schema;

const DEFAULT_BATCH_SIZE: usize = 1024;

pub struct VortexBatchReaderBuilder<R> {
    reader: R,
    projection: Option<Projection>,
    len: Option<u64>,
    take_indices: Option<Array>,
    row_filter: Option<RowFilter>,
    batch_size: Option<usize>,
}

impl<R: VortexReadAt> VortexBatchReaderBuilder<R> {
    // Recommended read-size according to the AWS performance guide
    const FOOTER_READ_SIZE: usize = 8 * 1024 * 1024;
    const FOOTER_TRAILER_SIZE: usize = 20;

    pub fn new(reader: R) -> Self {
        Self {
            reader,
            projection: None,
            row_filter: None,
            len: None,
            take_indices: None,
            batch_size: None,
        }
    }

    pub fn with_length(mut self, len: u64) -> Self {
        self.len = Some(len);
        self
    }

    pub fn with_projection(mut self, projection: Projection) -> Self {
        self.projection = Some(projection);
        self
    }

    pub fn with_take_indices(mut self, array: Array) -> Self {
        // TODO(#441): Allow providing boolean masks
        assert!(
            array.dtype().is_int(),
            "Mask arrays have to be integer arrays"
        );
        self.take_indices = Some(array);
        self
    }

    pub fn with_row_filter(mut self, row_filter: RowFilter) -> Self {
        self.row_filter = Some(row_filter);
        self
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = Some(batch_size);
        self
    }

    pub async fn build(mut self) -> VortexResult<VortexBatchStream<R>> {
        let footer = self.read_footer().await?;

        // TODO(adamg): We probably want to unify everything that is going on here into a single type and implementation
        let layout = if let Layout::Struct(s) = footer.layout()? {
            s
        } else {
            vortex_bail!("Top level layout must be a 'StructLayout'");
        };
        let dtype = if let DType::Struct(s, _) = footer.dtype()? {
            s
        } else {
            vortex_bail!("Top level dtype must be a 'StructDType'");
        };

        let batch_size = self.batch_size.unwrap_or(DEFAULT_BATCH_SIZE);

        VortexBatchStream::try_new(
            self.reader,
            layout,
            dtype,
            self.row_filter.unwrap_or_default(),
            batch_size,
            self.projection,
            self.take_indices,
        )
    }

    async fn len(&self) -> usize {
        let len = match self.len {
            Some(l) => l,
            None => self.reader.size().await,
        };

        len as usize
    }

    pub async fn read_footer(&mut self) -> VortexResult<Footer> {
        let file_length = self.len().await;

        if file_length < Self::FOOTER_TRAILER_SIZE {
            vortex_bail!(
                "Malformed vortex file, length {} must be at least {}",
                file_length,
                Self::FOOTER_TRAILER_SIZE,
            )
        }

        let read_size = Self::FOOTER_READ_SIZE.min(file_length);
        let mut buf = BytesMut::with_capacity(read_size);
        unsafe { buf.set_len(read_size) }

        let read_offset = (file_length - read_size) as u64;
        buf = self.reader.read_at_into(read_offset, buf).await?;

        let magic_bytes_loc = read_size - MAGIC_BYTES.len();

        let magic_number = &buf[magic_bytes_loc..];
        if magic_number != MAGIC_BYTES {
            vortex_bail!("Malformed file, invalid magic bytes, got {magic_number:?}")
        }

        let footer_offset = u64::from_le_bytes(
            buf[magic_bytes_loc - 8..magic_bytes_loc]
                .try_into()
                .unwrap(),
        );
        let schema_offset = u64::from_le_bytes(
            buf[magic_bytes_loc - 16..magic_bytes_loc - 8]
                .try_into()
                .unwrap(),
        );

        Ok(Footer {
            schema_offset,
            footer_offset,
            leftovers: buf.freeze(),
            leftovers_offset: read_offset,
        })
    }
}

struct ColumnReader {
    #[allow(dead_code)]
    name: Arc<str>,
    dtype: DType,
    layouts: VecDeque<Layout>,
    arrays: VecDeque<Array>,
}

impl ColumnReader {
    fn new(name: Arc<str>, dtype: DType, layouts: VecDeque<Layout>) -> Self {
        Self {
            name,
            dtype,
            layouts,
            arrays: Default::default(),
        }
    }

    fn is_empty(&self) -> bool {
        self.layouts.is_empty() && self.arrays.is_empty()
    }

    fn buffered_row_count(&self) -> usize {
        self.arrays.iter().map(|arr| arr.len()).sum()
    }

    async fn load<R: VortexReadAt>(
        &mut self,
        reader: &mut R,
        batch_size: usize,
        context: Arc<vortex::Context>,
    ) -> VortexResult<()> {
        loop {
            if self.buffered_row_count() >= batch_size {
                return Ok(());
            }

            if let Some(layout) = self.layouts.pop_front() {
                let byte_range = layout.as_flat().unwrap().range;
                let mut buffer = BytesMut::with_capacity(byte_range.len());
                unsafe { buffer.set_len(byte_range.len()) };

                let mut buff = reader
                    .read_at_into(byte_range.begin, buffer)
                    .await
                    .map_err(VortexError::from)
                    .unwrap()
                    .freeze();

                let mut array_reader = ArrayBufferReader::new();
                let mut read_buf = Bytes::new();
                while let Some(ReadResult::ReadMore(u)) = array_reader.read(read_buf.clone())? {
                    read_buf = buff.split_to(u);
                }

                let array = array_reader
                    .into_array(context.clone(), self.dtype.clone())
                    .unwrap();

                self.arrays.push_back(array);
            } else {
                break Ok(());
            }
        }
    }

    fn read_rows(&mut self, mut rows_needed: usize) -> Option<VortexResult<Array>> {
        if self.buffered_row_count() == 0 && self.layouts.is_empty() {
            return None;
        }

        if self.layouts.is_empty() {
            rows_needed = usize::min(rows_needed, self.buffered_row_count());
        }

        let mut result = Vec::default();

        loop {
            if rows_needed == 0 {
                break;
            }

            match self.arrays.pop_front() {
                None => break,
                Some(array) => match array.len().cmp(&rows_needed) {
                    Ordering::Greater => {
                        let taken = slice(&array, 0, rows_needed).unwrap();
                        let leftover = slice(&array, rows_needed, array.len()).unwrap();
                        self.arrays.push_front(leftover);
                        rows_needed -= taken.len();
                        result.push(taken);
                    }
                    Ordering::Equal | Ordering::Less => {
                        rows_needed -= array.len();
                        result.push(array);
                    }
                },
            }
        }

        match result.len() {
            0 => None,
            1 => Some(Ok(result.remove(0))),
            _ => Some(Ok(ChunkedArray::try_new(result, self.dtype.clone())
                .unwrap()
                .into_array())),
        }
    }
}

pub struct VortexBatchStream<R> {
    dtype: StructDType,
    // TODO(robert): Have identity projection
    projection: Option<Projection>,
    take_indices: Option<Array>,
    row_filter: RowFilter,
    batch_reader: Option<BatchReader<R>>,
    state: StreamingState<R>,
    context: Arc<vortex::Context>,
    #[allow(dead_code)]
    metadata_layouts: Option<Vec<Layout>>,
    current_offset: usize,
    batch_size: usize,
}

impl<R: VortexReadAt> VortexBatchStream<R> {
    fn try_new(
        reader: R,
        mut layout: StructLayout,
        dtype: StructDType,
        row_filter: RowFilter,
        batch_size: usize,
        projection: Option<Projection>,
        take_indices: Option<Array>,
    ) -> VortexResult<Self> {
        let schema = Schema(dtype.clone());
        let mut batch_reader = BatchReader::new(reader, schema.clone());

        let metadata_layouts = layout
            .children
            .iter_mut()
            .map(|c| c.as_chunked_mut().unwrap().children.pop_front().unwrap())
            .collect::<Vec<_>>();

        for ((c_layout, col_name), dtype) in layout
            .children
            .iter_mut()
            .zip(schema.fields().iter().cloned())
            .zip(schema.types().iter().cloned())
        {
            let layout = c_layout.as_chunked_mut().unwrap();
            let chunked_children = std::mem::take(&mut layout.children);

            batch_reader.add_column(col_name, dtype, chunked_children);
        }

        Ok(VortexBatchStream {
            batch_reader: Some(batch_reader),
            dtype,
            projection,
            take_indices,
            row_filter,
            batch_size,
            metadata_layouts: Some(metadata_layouts),
            current_offset: 0,
            state: Default::default(),
            context: Default::default(),
        })
    }

    pub fn schema(&self) -> Schema {
        Schema(self.dtype.clone())
    }

    fn take_batch(&mut self, batch: &Array) -> VortexResult<Array> {
        let curr_offset = self.current_offset;
        let indices = self.take_indices.as_ref().expect("should be there");
        let left =
            search_sorted(indices, curr_offset, SearchSortedSide::Left)?.to_zero_offset_index();
        let right = search_sorted(indices, curr_offset + batch.len(), SearchSortedSide::Left)?
            .to_zero_offset_index();

        self.current_offset += batch.len();
        // TODO(ngates): this is probably too heavy to run on the event loop. We should spawn
        //  onto a worker pool.
        let indices_for_batch = slice(indices, left, right)?.into_primitive()?;
        let shifted_arr = match_each_integer_ptype!(indices_for_batch.ptype(), |$T| {
            subtract_scalar(&indices_for_batch.into_array(), &Scalar::from(curr_offset as $T))?
        });

        take(batch, &shifted_arr)
    }
}

type StreamStateFuture<R> = BoxFuture<'static, VortexResult<(BatchReader<R>, Option<Array>)>>;

#[derive(Default)]
enum StreamingState<R> {
    #[default]
    Init,
    Reading(StreamStateFuture<R>),
    Decoding(Option<Array>),
    Error,
}

impl<R: VortexReadAt + Unpin + Send + 'static> Stream for VortexBatchStream<R> {
    type Item = VortexResult<Array>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let batch_size = self.batch_size;
        loop {
            match &mut self.state {
                StreamingState::Init => {
                    let mut batch_reader = self.batch_reader.take().expect("reader should be here");

                    let context = self.context.clone();

                    let f = async move {
                        batch_reader.load(batch_size, context).await?;
                        let arr = batch_reader.next(batch_size).transpose()?;
                        VortexResult::Ok((batch_reader, arr))
                    }
                    .boxed();

                    self.state = StreamingState::Reading(f);
                }
                StreamingState::Decoding(arr) => match arr.take() {
                    Some(mut batch) => {
                        if self.take_indices.is_some() {
                            batch = self.take_batch(&batch)?;
                        }
                        let mut current_predicate =
                            ConstantArray::new(true, batch.len()).into_array();
                        for pred in self.row_filter._filters.iter_mut() {
                            let filter_bitmap = pred.evaluate(&batch)?;
                            current_predicate = and(&current_predicate, &filter_bitmap)?;
                        }

                        batch = filter(&batch, &current_predicate)?;
                        let projected = self
                            .projection
                            .as_ref()
                            .map(|p| {
                                StructArray::try_from(batch.clone())
                                    .unwrap()
                                    .project(p.indices())
                                    .unwrap()
                                    .into_array()
                            })
                            .unwrap_or(batch);

                        return Poll::Ready(Some(Ok(projected)));
                    }

                    None => {
                        if let Some(reader) = self.batch_reader.as_ref() {
                            if reader.is_empty() {
                                return Poll::Ready(None);
                            }
                        }

                        self.state = StreamingState::Init;
                    }
                },
                StreamingState::Reading(f) => match ready!(f.poll_unpin(cx)) {
                    Ok((batch_reader, arr)) => {
                        self.batch_reader = Some(batch_reader);
                        self.state = StreamingState::Decoding(arr)
                    }
                    Err(e) => {
                        self.state = StreamingState::Error;
                        return Poll::Ready(Some(Err(e)));
                    }
                },
                StreamingState::Error => return Poll::Ready(None),
            }
        }
    }
}

struct BatchReader<R> {
    readers: HashMap<Arc<str>, ColumnReader>,
    schema: Schema,
    reader: R,
}

impl<R: VortexReadAt> BatchReader<R> {
    fn new(reader: R, schema: Schema) -> Self {
        Self {
            reader,
            schema,
            readers: Default::default(),
        }
    }

    fn add_column(&mut self, name: Arc<str>, dtype: DType, layouts: VecDeque<Layout>) {
        self.readers
            .insert(name.clone(), ColumnReader::new(name, dtype, layouts));
    }

    fn is_empty(&self) -> bool {
        self.readers.values().all(|c| c.is_empty())
    }

    async fn load(&mut self, batch_size: usize, context: Arc<vortex::Context>) -> VortexResult<()> {
        for column_reader in self.readers.values_mut() {
            column_reader
                .load(&mut self.reader, batch_size, context.clone())
                .await?;
        }

        Ok(())
    }

    fn next(&mut self, batch_size: usize) -> Option<VortexResult<Array>> {
        let mut final_columns = vec![];

        for col_name in self.schema.fields().iter() {
            let column_reader = self.readers.get_mut(col_name).unwrap();

            match column_reader.read_rows(batch_size) {
                Some(Ok(array)) => final_columns.push((col_name.clone(), array)),
                Some(Err(e)) => return Some(Err(e)),
                None => return None,
            }
        }

        Some(VortexResult::Ok(
            StructArray::from_fields(final_columns.as_slice()).into_array(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use vortex::array::chunked::ChunkedArray;
    use vortex::array::primitive::PrimitiveArray;
    use vortex::array::struct_::StructArray;
    use vortex::array::varbin::VarBinArray;
    use vortex::validity::Validity;
    use vortex::variants::StructArrayTrait;
    use vortex::{ArrayDType, IntoArray, IntoArrayVariant};
    use vortex_dtype::PType;

    use super::*;
    use crate::file::file_writer::FileWriter;

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn test_read_simple() {
        let strings = ChunkedArray::from_iter([
            VarBinArray::from(vec!["ab", "foo", "bar", "baz"]).into_array(),
            VarBinArray::from(vec!["ab", "foo", "bar", "baz"]).into_array(),
        ])
        .into_array();

        let numbers = ChunkedArray::from_iter([
            PrimitiveArray::from(vec![1u32, 2, 3, 4]).into_array(),
            PrimitiveArray::from(vec![5u32, 6, 7, 8]).into_array(),
        ])
        .into_array();

        let st = StructArray::try_new(
            ["strings".into(), "numbers".into()].into(),
            vec![strings, numbers],
            8,
            Validity::NonNullable,
        )
        .unwrap();
        let buf = Vec::new();
        let mut writer = FileWriter::new(buf);
        writer = writer.write_array_columns(st.into_array()).await.unwrap();
        let written = writer.finalize().await.unwrap();

        let mut stream = VortexBatchReaderBuilder::new(written)
            .with_batch_size(5)
            .build()
            .await
            .unwrap();
        let mut batch_count = 0;
        let mut row_count = 0;

        while let Some(array) = stream.next().await {
            let array = array.unwrap();
            batch_count += 1;
            row_count += array.len();
        }

        assert_eq!(batch_count, 2);
        assert_eq!(row_count, 8);
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn test_read_projection() {
        let strings = ChunkedArray::from_iter([
            VarBinArray::from(vec!["ab", "foo", "bar", "baz"]).into_array(),
            VarBinArray::from(vec!["ab", "foo", "bar", "baz"]).into_array(),
        ])
        .into_array();

        let numbers = ChunkedArray::from_iter([
            PrimitiveArray::from(vec![1u32, 2, 3, 4]).into_array(),
            PrimitiveArray::from(vec![5u32, 6, 7, 8]).into_array(),
        ])
        .into_array();

        let st = StructArray::try_new(
            ["strings".into(), "numbers".into()].into(),
            vec![strings, numbers],
            8,
            Validity::NonNullable,
        )
        .unwrap();
        let buf = Vec::new();
        let mut writer = FileWriter::new(buf);
        writer = writer.write_array_columns(st.into_array()).await.unwrap();
        let written = writer.finalize().await.unwrap();

        let mut stream = VortexBatchReaderBuilder::new(written)
            .with_projection(Projection::new([0]))
            .with_batch_size(5)
            .build()
            .await
            .unwrap();
        let mut item_count = 0;
        let mut batch_count = 0;

        while let Some(array) = stream.next().await {
            let array = array.unwrap();
            item_count += array.len();
            batch_count += 1;

            let array = array.into_struct().unwrap();
            let struct_dtype = array.dtype().as_struct().unwrap();
            assert_eq!(struct_dtype.dtypes().len(), 1);
            assert_eq!(struct_dtype.names()[0].as_ref(), "strings");
        }

        assert_eq!(item_count, 8);
        assert_eq!(batch_count, 2);
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn unequal_batches() {
        let strings = ChunkedArray::from_iter([
            VarBinArray::from(vec!["ab", "foo", "bar", "bob"]).into_array(),
            VarBinArray::from(vec!["baz", "ab", "foo", "bar", "baz", "alice"]).into_array(),
        ])
        .into_array();

        let numbers = ChunkedArray::from_iter([
            PrimitiveArray::from(vec![1u32, 2, 3, 4, 5]).into_array(),
            PrimitiveArray::from(vec![6u32, 7, 8, 9, 10]).into_array(),
        ])
        .into_array();

        let st = StructArray::try_new(
            ["strings".into(), "numbers".into()].into(),
            vec![strings, numbers],
            10,
            Validity::NonNullable,
        )
        .unwrap();
        let buf = Vec::new();
        let mut writer = FileWriter::new(buf);
        writer = writer.write_array_columns(st.into_array()).await.unwrap();
        let written = writer.finalize().await.unwrap();

        let mut stream = VortexBatchReaderBuilder::new(written)
            .with_batch_size(5)
            .build()
            .await
            .unwrap();
        let mut batch_count = 0;
        let mut item_count = 0;

        while let Some(array) = stream.next().await {
            let array = array.unwrap();
            item_count += array.len();
            batch_count += 1;

            let numbers = StructArray::try_from(array)
                .unwrap()
                .field_by_name("numbers");

            if let Some(numbers) = numbers {
                let numbers = numbers.as_primitive();
                assert_eq!(numbers.ptype(), PType::U32);
            } else {
                panic!("Expected column doesn't exist")
            }
        }
        assert_eq!(item_count, 10);
        assert_eq!(batch_count, 2);
    }
}
