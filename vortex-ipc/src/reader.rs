use std::marker::PhantomData;
use std::{io, mem};

use arrow_buffer::Buffer as ArrowBuffer;
use flatbuffers::{root, root_unchecked};
use futures::executor::block_on;
use log::error;
use monoio::buf::SliceMut;
use monoio::io::{AsyncReadRent, AsyncReadRentExt, BufReader};
use nougat::gat;
use vortex::array::chunked::ChunkedArray;
use vortex::compute::scalar_subtract::subtract_scalar;
use vortex::compute::search_sorted::{search_sorted, SearchSortedSide};
use vortex::compute::slice::slice;
use vortex::compute::take::take;
use vortex::stats::{ArrayStatistics, Stat};
use vortex::{
    Array, ArrayDType, ArrayView, Context, IntoArray, OwnedArray, ToArray, ToStatic, ViewContext,
};
use vortex_buffer::Buffer;
use vortex_dtype::{match_each_integer_ptype, DType};
use vortex_error::{vortex_bail, vortex_err, VortexError, VortexResult};
use vortex_flatbuffers::ReadFlatBuffer;
use vortex_scalar::Scalar;

use crate::flatbuffers::ipc::Message;
use crate::iter::{FallibleIterator, FallibleLendingIterator, FallibleLendingIteratorඞItem};
use crate::messages::SerdeContextDeserializer;

pub struct StreamReader<R: AsyncReadRent> {
    read: R,
    messages: StreamMessageReader<R>,
    ctx: ViewContext,
}

impl<R: AsyncReadRent> StreamReader<BufReader<R>> {
    pub async fn try_new(read: R, ctx: &Context) -> VortexResult<Self> {
        Self::try_new_unbuffered(BufReader::new(read), ctx).await
    }
}

impl<R: AsyncReadRent> StreamReader<R> {
    pub async fn try_new_unbuffered(mut read: R, ctx: &Context) -> VortexResult<Self> {
        let mut messages = StreamMessageReader::try_new(&mut read).await?;
        match messages.peek() {
            None => vortex_bail!("IPC stream is empty"),
            Some(msg) => {
                if msg.header_as_context().is_none() {
                    vortex_bail!(InvalidSerde: "Expected IPC Context as first message in stream")
                }
            }
        }

        let view_ctx: ViewContext = SerdeContextDeserializer {
            fb: messages.next(&mut read).await?.header_as_context().unwrap(),
            ctx,
        }
        .try_into()?;

        Ok(Self {
            read,
            messages,
            ctx: view_ctx,
        })
    }

    /// Read a single array from the IPC stream.
    pub async fn read_array(&mut self) -> VortexResult<Array> {
        let mut array_reader = self
            .next()
            .await?
            .ok_or_else(|| vortex_err!(InvalidSerde: "Unexpected EOF"))?;

        let mut chunks = vec![];
        while let Some(chunk) = array_reader.next().await? {
            chunks.push(chunk.to_static());
        }

        if chunks.len() == 1 {
            Ok(chunks[0].clone())
        } else {
            ChunkedArray::try_new(chunks.into_iter().collect(), array_reader.dtype().clone())
                .map(|chunked| chunked.into_array())
        }
    }
}

#[gat]
impl<R: AsyncReadRent> FallibleLendingIterator for StreamReader<R> {
    type Error = VortexError;
    type Item<'next> = StreamArrayReader<'next, R> where Self: 'next;

    async fn next(&mut self) -> Result<Option<StreamArrayReader<'_, R>>, Self::Error> {
        if self
            .messages
            .peek()
            .and_then(|msg| msg.header_as_schema())
            .is_none()
        {
            return Ok(None);
        }

        let schema_msg = self
            .messages
            .next(&mut self.read)
            .await?
            .header_as_schema()
            .unwrap();

        let dtype = DType::read_flatbuffer(
            &schema_msg
                .dtype()
                .ok_or_else(|| vortex_err!(InvalidSerde: "Schema missing DType"))?,
        )
        .map_err(|e| vortex_err!(InvalidSerde: "Failed to parse DType: {}", e))?;

        Ok(Some(StreamArrayReader {
            ctx: &self.ctx,
            read: &mut self.read,
            messages: &mut self.messages,
            dtype,
            buffers: vec![],
            row_offset: 0,
        }))
    }
}

#[allow(dead_code)]
pub struct StreamArrayReader<'a, R: AsyncReadRent> {
    ctx: &'a ViewContext,
    read: &'a mut R,
    messages: &'a mut StreamMessageReader<R>,
    dtype: DType,
    buffers: Vec<Buffer>,
    row_offset: usize,
}

impl<'a, R: AsyncReadRent> StreamArrayReader<'a, R> {
    pub fn dtype(&self) -> &DType {
        &self.dtype
    }

    pub fn take(self, indices: &'a Array<'_>) -> VortexResult<TakeIterator<'a, R>> {
        if !indices.is_empty() {
            if !indices.statistics().compute_is_sorted()? {
                vortex_bail!("Indices must be sorted to take from IPC stream")
            }

            if indices.statistics().compute_null_count()? > 0 {
                vortex_bail!("Indices must not contain nulls")
            }

            if !indices.dtype().is_int() {
                vortex_bail!("Indices must be integers")
            }

            if indices.dtype().is_signed_int()
                && indices.statistics().compute_as_cast::<i64>(Stat::Min)? < 0
            {
                vortex_bail!("Indices must be positive")
            }
        }

        if self.row_offset != 0 {
            vortex_bail!("Stream has already been (at least partially) consumed")
        }

        Ok(TakeIterator {
            reader: self,
            indices,
            row_offset: 0,
        })
    }
}

pub struct TakeIterator<'a, R: AsyncReadRent> {
    reader: StreamArrayReader<'a, R>,
    indices: &'a Array<'a>,
    row_offset: usize,
}

/// NB: if the StreamArrayReader expires without being fully-consumed by the underlying iterator,
/// it will leave the underlying messages stream in an unreadable state and free up a mutable ref
/// to that stream, allowing someone else to try to issue a read that will inevitably fail.
/// For this reason, we force full consumption of the reader when it goes out of scope.
impl<'a, R: AsyncReadRent> Drop for StreamArrayReader<'a, R> {
    fn drop(&mut self) {
        // NB: can't catch_unwind/unwrap here because &mut self can't be made UnwindSafe
        loop {
            let next = block_on(self.next());
            match next {
                Ok(result) => {
                    if result.is_none() {
                        break;
                    }
                }
                Err(err) => {
                    error!("Error consuming StreamArrayReader in destructor: {:?}", err);
                    break;
                }
            }
        }
    }
}

/// NB: if the TakeIterator expires without being fully-consumed by the underlying iterator,
/// it will trigger the Drop impl on the StreamArrayReader, which will consume the rest of the
/// messages for that array. This is necessary to ensure that the underlying stream is in a
/// consistent state when the StreamArrayReader is dropped, but it also is not free. If users wish
/// to avoid this work happening at exipry, users can just consume the rest of the iterator
/// themselves when they see fit.
impl<'a, R: AsyncReadRent> FallibleIterator for TakeIterator<'a, R> {
    type Item = OwnedArray;
    type Error = VortexError;

    async fn next(&mut self) -> VortexResult<Option<Self::Item>> {
        if self.indices.is_empty() {
            return Ok(None);
        }
        while let Some(batch) = self.reader.next().await? {
            let curr_offset = self.row_offset;
            let left = search_sorted::<usize>(self.indices, curr_offset, SearchSortedSide::Left)?
                .to_index();
            let right = search_sorted::<usize>(
                self.indices,
                curr_offset + batch.len(),
                SearchSortedSide::Left,
            )?
            .to_index();

            self.row_offset += batch.len();

            if left == right {
                continue;
            }

            let indices_for_batch = slice(self.indices, left, right)?.flatten_primitive()?;
            let shifted_arr = match_each_integer_ptype!(indices_for_batch.ptype(), |$T| {
                subtract_scalar(&indices_for_batch.into_array(), &Scalar::from(curr_offset as $T))?
            });

            return take(&batch, &shifted_arr).map(Some);
        }
        Ok(None)
    }
}

#[gat]
impl<'iter, R: AsyncReadRent> FallibleLendingIterator for StreamArrayReader<'iter, R> {
    type Error = VortexError;
    type Item<'next> = Array<'next> where Self: 'next;

    async fn next(&mut self) -> Result<Option<Array<'_>>, Self::Error> {
        let Some(chunk_msg) = self.messages.peek().and_then(|msg| msg.header_as_chunk()) else {
            return Ok(None);
        };

        // Read all the column's buffers
        self.buffers.clear();
        let mut offset: usize = 0;
        let buffers = chunk_msg.buffers().unwrap_or_default();
        for i in 0..buffers.len() {
            let buffer = buffers.get(i);
            let next_offset = if i == buffers.len() - 1 {
                chunk_msg.buffer_size() as usize
            } else {
                buffers.get(i + 1).offset() as usize
            };
            let buf_len = buffer.length() as usize;
            let padding = next_offset - offset - buf_len;

            // TODO(ngates): read into a single buffer, then Arc::clone and slice
            let bytes = Vec::with_capacity(buf_len + padding);
            let (len_res, mut bytes_read) = self.read.read_exact(bytes).await;
            if len_res? != buf_len + padding {
                vortex_bail!("Mismatched length read from buffer");
            }
            bytes_read.truncate(buf_len);
            let arrow_buffer = ArrowBuffer::from_vec(bytes_read);
            self.buffers.push(Buffer::from(arrow_buffer));

            offset = next_offset;
        }

        // After reading the buffers we're now able to load the next message.
        let col_array = self
            .messages
            .next(self.read)
            .await?
            .header_as_chunk()
            .unwrap()
            .array()
            .unwrap();
        let view = ArrayView::try_new(self.ctx, &self.dtype, col_array, self.buffers.as_slice())?;

        // Validate it
        view.to_array().with_dyn(|_| Ok::<(), VortexError>(()))?;

        let array = view.into_array();
        self.row_offset += array.len();
        Ok(Some(array))
    }
}

struct StreamMessageReader<R: AsyncReadRent> {
    message: Vec<u8>,
    prev_message: Vec<u8>,
    finished: bool,
    phantom: PhantomData<R>,
}

impl<R: AsyncReadRent> StreamMessageReader<R> {
    pub async fn try_new(read: &mut R) -> VortexResult<Self> {
        let mut reader = Self {
            message: Vec::new(),
            prev_message: Vec::new(),
            finished: false,
            phantom: PhantomData,
        };
        reader.load_next_message(read).await?;
        Ok(reader)
    }

    pub fn peek(&self) -> Option<Message> {
        if self.finished {
            return None;
        }
        // The message has been validated by the next() call.
        Some(unsafe { root_unchecked::<Message>(&self.message) })
    }

    pub async fn next(&mut self, read: &mut R) -> VortexResult<Message> {
        if self.finished {
            panic!("StreamMessageReader is finished - should've checked peek!");
        }
        mem::swap(&mut self.prev_message, &mut self.message);
        if !self.load_next_message(read).await? {
            self.finished = true;
        }
        Ok(unsafe { root_unchecked::<Message>(&self.prev_message) })
    }

    async fn load_next_message(&mut self, read: &mut R) -> VortexResult<bool> {
        let len_buf = Vec::with_capacity(4);
        let (len_res, read_buf) = read.read_exact(len_buf).await;
        match len_res {
            Ok(_) => {}
            Err(e) => {
                return match e.kind() {
                    io::ErrorKind::UnexpectedEof => Ok(false),
                    _ => Err(e.into()),
                };
            }
        }

        let len = u32::from_le_bytes(read_buf.as_slice().try_into().unwrap());
        if len == u32::MAX {
            // Marker for no more messages.
            return Ok(false);
        }

        self.message.clear();
        self.message.reserve(len as usize);
        let (len_res, read_buf) = read
            .read_exact(SliceMut::new(mem::take(&mut self.message), 0, len as usize))
            .await;
        if len_res? != len as usize {
            vortex_bail!(InvalidSerde: "Failed to read all bytes")
        }
        self.message = read_buf.into_inner();

        std::hint::black_box(root::<Message>(&self.message)?);
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use itertools::Itertools;
    use vortex::array::chunked::{Chunked, ChunkedArray};
    use vortex::array::primitive::{Primitive, PrimitiveArray, PrimitiveEncoding};
    use vortex::encoding::{ArrayEncoding, EncodingId, EncodingRef};
    use vortex::{Array, ArrayDType, ArrayDef, IntoArray, OwnedArray};
    use vortex_alp::{ALPArray, ALPEncoding};
    use vortex_dtype::NativePType;
    use vortex_error::VortexResult;
    use vortex_fastlanes::{BitPackedArray, BitPackedEncoding};

    use crate::iter::{FallibleIterator, FallibleLendingIterator};
    use crate::reader::Context;
    use crate::reader::StreamReader;
    use crate::writer::StreamWriter;

    #[monoio::test_all]
    async fn test_read_write() {
        let ctx = Context::default().with_encodings([
            &ALPEncoding as EncodingRef,
            &BitPackedEncoding as EncodingRef,
        ]);
        let array = PrimitiveArray::from(vec![0, 1, 2]).into_array();
        let chunked_array =
            ChunkedArray::try_new(vec![array.clone(), array.clone()], array.dtype().clone())
                .unwrap()
                .into_array();

        let mut buffer = vec![];
        let mut cursor = Cursor::new(&mut buffer);
        {
            let mut writer = StreamWriter::try_new(&mut cursor, &ctx).unwrap();
            writer.write_array(&array).unwrap();
            writer.write_array(&chunked_array).unwrap();
        }
        // Push some extra bytes to test that the reader is well-behaved and doesn't read past the
        // end of the stream.
        buffer.extend_from_slice(b"hello");
        let mut read_buf = buffer.as_slice();

        {
            let mut reader = StreamReader::try_new_unbuffered(&mut read_buf, &ctx)
                .await
                .unwrap();
            let first = reader.read_array().await.unwrap();
            assert_eq!(first.encoding().id(), Primitive::ID);
            let second = reader.read_array().await.unwrap();
            assert_eq!(second.encoding().id(), Chunked::ID);
        }
        // Test our termination bytes exist
        assert_eq!(read_buf, b"hello");
    }

    #[monoio::test_all]
    async fn test_write_read_primitive() {
        // NB: the order is reversed here to ensure we aren't grabbing indexes instead of values
        let data = PrimitiveArray::from((0i32..3_000_000).rev().collect_vec()).into_array();
        test_base_case(
            &data,
            &[2999989i32, 2999988, 2999987, 2999986, 2899999, 0, 0],
            PrimitiveEncoding.id(),
        )
        .await;
    }

    #[monoio::test_all]
    async fn test_write_read_alp() {
        let pdata = PrimitiveArray::from(
            (0i32..3_000_000)
                .rev()
                .map(|v| v as f64 + 0.5)
                .collect_vec(),
        )
        .into_array();
        let alp_encoded = ALPArray::encode(pdata).unwrap();
        assert_eq!(alp_encoded.encoding().id(), ALPEncoding.id());
        test_base_case(
            &alp_encoded,
            &[
                2999989.5f64,
                2999988.5,
                2999987.5,
                2999986.5,
                2899999.5,
                0.5,
                0.5,
            ],
            ALPEncoding.id(),
        )
        .await;
    }

    #[monoio::test_all]
    async fn test_empty_index() {
        let data = PrimitiveArray::from((0i32..3_000_000).rev().collect_vec()).into_array();
        let indices: Vec<i32> = vec![];
        let indices = PrimitiveArray::from(indices).into_array();
        let mut buffer = vec![];
        {
            let mut cursor = Cursor::new(&mut buffer);
            {
                let mut writer = StreamWriter::try_new(&mut cursor, &Context::default()).unwrap();
                writer.write_array(&data).unwrap();
            }
        }

        let mut reader = StreamReader::try_new(buffer.as_slice(), &Context::default())
            .await
            .unwrap();
        let array_reader = reader.next().await.unwrap().unwrap();
        let mut result_iter = array_reader.take(&indices).unwrap();
        let result = result_iter.next().await.unwrap();
        assert!(result.is_none())
    }

    #[monoio::test_all]
    async fn test_negative_index_fails() {
        let data = PrimitiveArray::from((0i32..3_000_000).rev().collect_vec()).into_array();
        let indices =
            PrimitiveArray::from(vec![-1i32, 10, 11, 12, 13, 100_000, 2_999_999, 2_999_999])
                .into_array();
        test_read_write_single_chunk_array(&data, &indices)
            .await
            .expect_err("Expected negative index to fail");
    }

    #[monoio::test_all]
    async fn test_noninteger_index_fails() {
        let data = PrimitiveArray::from((0i32..3_000_000).rev().collect_vec()).into_array();
        let indices = PrimitiveArray::from(vec![1f32, 10.0, 11.0, 12.0]).into_array();
        test_read_write_single_chunk_array(&data, &indices)
            .await
            .expect_err("Expected float index to fail");
    }

    #[monoio::test_all]
    async fn test_rnull_index_fails() {
        let data = PrimitiveArray::from((0i32..3_000_000).rev().collect_vec()).into_array();
        let indices = PrimitiveArray::from_nullable_vec(vec![None, Some(1i32), Some(10), Some(11)])
            .into_array();
        test_read_write_single_chunk_array(&data, &indices)
            .await
            .expect_err("Expected float index to fail");
    }

    #[monoio::test_all]
    async fn test_write_read_bitpacked() {
        // NB: the order is reversed here to ensure we aren't grabbing indexes instead of values
        let uncompressed = PrimitiveArray::from((0i64..3_000).rev().collect_vec());
        let packed = BitPackedArray::encode(uncompressed.array(), 5).unwrap();

        let expected = &[2989i64, 2988, 2987, 2986];
        test_base_case(&packed.into_array(), expected, PrimitiveEncoding.id()).await;
    }

    #[monoio::test_all]
    async fn test_write_read_chunked() {
        let indices = PrimitiveArray::from(vec![
            10u32, 11, 12, 13, 100_000, 2_999_999, 2_999_999, 3_000_000,
        ])
        .into_array();

        // NB: the order is reversed here to ensure we aren't grabbing indexes instead of values
        let data = PrimitiveArray::from((0i32..3_000_000).rev().collect_vec()).into_array();
        let data2 =
            PrimitiveArray::from((3_000_000i32..6_000_000).rev().collect_vec()).into_array();
        let chunked = ChunkedArray::try_new(vec![data.clone(), data2], data.dtype().clone())
            .unwrap()
            .into_array();
        let mut buffer = vec![];
        {
            let mut cursor = Cursor::new(&mut buffer);
            {
                let mut writer = StreamWriter::try_new(&mut cursor, &Context::default()).unwrap();
                writer.write_array(&chunked).unwrap();
            }
        }

        let mut reader = StreamReader::try_new(buffer.as_slice(), &Context::default())
            .await
            .unwrap();
        let array_reader = reader.next().await.unwrap().unwrap();
        let mut take_iter = array_reader.take(&indices).unwrap();
        let next = take_iter.next().await.unwrap().unwrap();
        assert_eq!(next.encoding().id(), PrimitiveEncoding.id());

        assert_eq!(
            next.into_primitive().typed_data::<i32>(),
            vec![2999989, 2999988, 2999987, 2999986, 2899999, 0, 0]
        );
        assert_eq!(
            take_iter
                .next()
                .await
                .unwrap()
                .unwrap()
                .into_primitive()
                .typed_data::<i32>(),
            vec![5999999]
        );
    }

    /// This test ensures that our take function doesn't partially consume an array, leaving the
    /// stream in a bad state. This could happen if we:
    /// - write a chunked array with multiple chunks
    /// - write another array
    /// - stop consuming the stream after we find all the desired indices, but before we have
    ///   consumed the entire array
    /// - the next reader tries to create a new array from the stream, but can't because the stream
    ///   is in the middle of the prior array
    #[monoio::test_all]
    async fn test_write_read_does_not_compromise_stream() {
        // NB: the order is reversed here to ensure we aren't grabbing indexes instead of values
        let data = PrimitiveArray::from((0i32..3_000_000).rev().collect_vec()).into_array();
        let data2 =
            PrimitiveArray::from((3_000_000i32..6_000_000).rev().collect_vec()).into_array();
        let chunked = ChunkedArray::try_new(
            vec![data.clone(), data2.clone(), data2],
            data.dtype().clone(),
        )
        .unwrap()
        .into_array();

        let indices = PrimitiveArray::from(vec![
            10i32, 11, 12, 13, 100_000, 2_999_999, 2_999_999, 4_000_000,
        ])
        .into_array();
        let mut buffer = vec![];
        {
            let mut cursor = Cursor::new(&mut buffer);
            {
                let mut writer = StreamWriter::try_new(&mut cursor, &Context::default()).unwrap();
                writer.write_array(&chunked).unwrap();
                writer.write_array(&data).unwrap();
            }
        }

        let mut reader = StreamReader::try_new(buffer.as_slice(), &Context::default())
            .await
            .unwrap();
        let array_reader = reader.next().await.unwrap().unwrap();

        {
            let mut iter = array_reader.take(&indices).unwrap();

            // verify that for a fully-consumed iterator, the destructor does not advance the
            // underlying message stream to the next message, which would be very bad
            while iter.next().await.unwrap().is_some() {
                // Consume the iterator
            }
            // verify that if we continue to call next on the iterator, we get none and
            // nothing happens to the underlying stream
            assert!(iter.next().await.unwrap().is_none());
            assert!(iter.next().await.unwrap().is_none());
        }

        let indices = PrimitiveArray::from(vec![10i32, 11, 12, 13, 100_000, 2_999_999, 2_999_999])
            .into_array();
        let array_reader = reader.next().await.unwrap().unwrap();

        let mut take_iter = array_reader.take(&indices).unwrap();
        let array = take_iter.next().await.unwrap().unwrap();
        assert_eq!(array.encoding().id(), PrimitiveEncoding.id());

        let results = array
            .flatten_primitive()
            .unwrap()
            .typed_data::<i32>()
            .to_vec();
        assert_eq!(
            results,
            &[2999989i32, 2999988, 2999987, 2999986, 2899999, 0, 0]
        );
    }

    #[monoio::test_all]
    async fn test_take_iter() {
        let indices = PrimitiveArray::from(vec![
            10u32, 11, 12, 13, 100_000, 2_999_999, 2_999_999, 3_000_000,
        ])
        .into_array();

        // NB: the order is reversed here to ensure we aren't grabbing indexes instead of values
        let data = PrimitiveArray::from((0i32..3_000_000).rev().collect_vec()).into_array();
        let data2 =
            PrimitiveArray::from((3_000_000i32..6_000_000).rev().collect_vec()).into_array();
        let chunked = ChunkedArray::try_new(vec![data.clone(), data2], data.dtype().clone())
            .unwrap()
            .into_array();
        let mut buffer = vec![];
        {
            let mut cursor = Cursor::new(&mut buffer);
            {
                let mut writer = StreamWriter::try_new(&mut cursor, &Context::default()).unwrap();
                writer.write_array(&chunked).unwrap();
            }
        }

        let mut reader = StreamReader::try_new(buffer.as_slice(), &Context::default())
            .await
            .unwrap();
        let array_reader = reader.next().await.unwrap().unwrap();

        let mut iter = array_reader.take(&indices).unwrap();

        let chunk = iter.next().await.unwrap().unwrap();
        assert_eq!(chunk.encoding().id(), PrimitiveEncoding.id());
        assert_eq!(
            chunk.into_primitive().typed_data::<i32>(),
            vec![2999989, 2999988, 2999987, 2999986, 2899999, 0, 0]
        );
        let chunk = iter.next().await.unwrap().unwrap();
        assert_eq!(chunk.into_primitive().typed_data::<i32>(), vec![5999999]);
    }

    async fn test_base_case<T: NativePType>(
        data: &Array<'_>,
        expected: &[T],
        expected_encoding_id: EncodingId,
    ) {
        let ctx =
            Context::default().with_encodings([&ALPEncoding as EncodingRef, &BitPackedEncoding]);

        let indices = PrimitiveArray::from(vec![10i32, 11, 12, 13, 100_000, 2_999_999, 2_999_999])
            .into_array();
        let mut buffer = vec![];
        {
            let mut cursor = Cursor::new(&mut buffer);
            {
                let mut writer = StreamWriter::try_new(&mut cursor, &ctx).unwrap();
                writer.write_array(data).unwrap();
            }
        }

        let mut reader = StreamReader::try_new(buffer.as_slice(), &ctx)
            .await
            .unwrap();
        let array_reader = reader.next().await.unwrap().unwrap();
        let mut take_iter = array_reader.take(&indices).unwrap();

        let array = take_iter.next().await.unwrap().unwrap();
        assert_eq!(array.encoding().id(), expected_encoding_id);

        let results = array
            .flatten_primitive()
            .unwrap()
            .typed_data::<T>()
            .to_vec();
        assert_eq!(results, expected);
    }

    async fn test_read_write_single_chunk_array<'a>(
        data: &'a Array<'_>,
        indices: &'a Array<'_>,
    ) -> VortexResult<OwnedArray> {
        let ctx =
            Context::default().with_encodings([&ALPEncoding as EncodingRef, &BitPackedEncoding]);

        let mut buffer = vec![];
        {
            let mut cursor = Cursor::new(&mut buffer);
            {
                let mut writer = StreamWriter::try_new(&mut cursor, &ctx).unwrap();
                writer.write_array(data).unwrap();
            }
        }

        let mut reader = StreamReader::try_new(buffer.as_slice(), &ctx)
            .await
            .unwrap();
        let array_reader = reader.next().await.unwrap().unwrap();
        let mut result_iter = array_reader.take(indices)?;
        let result = result_iter.next().await.unwrap();
        assert!(result_iter.next().await.unwrap().is_none());
        Ok(result.unwrap())
    }
}
