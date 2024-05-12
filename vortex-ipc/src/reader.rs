use std::io;
use std::io::{BufReader, Read};
use std::marker::PhantomData;
use std::sync::Arc;

use arrow_buffer::Buffer as ArrowBuffer;
use fallible_iterator::FallibleIterator;
use flatbuffers::{root, root_unchecked};
use log::error;
use nougat::gat;
use vortex::array::chunked::ChunkedArray;
use vortex::compute::scalar_subtract::subtract_scalar;
use vortex::compute::search_sorted::{search_sorted, SearchSortedSide};
use vortex::compute::slice::slice;
use vortex::compute::take::take;
use vortex::stats::{ArrayStatistics, Stat};
use vortex::{Array, ArrayDType, ArrayView, Context, IntoArray, ToArray, ToStatic, ViewContext};
use vortex_buffer::Buffer;
use vortex_dtype::{match_each_integer_ptype, DType};
use vortex_error::{vortex_bail, vortex_err, VortexError, VortexResult};
use vortex_scalar::Scalar;

use crate::flatbuffers::ipc::Message;
use crate::iter::{FallibleLendingIterator, FallibleLendingIteratorඞItem};
use crate::messages::SerdeContextDeserializer;

pub struct StreamReader<R: Read> {
    read: R,
    messages: StreamMessageReader<R>,
    ctx: Arc<ViewContext>,
}

impl<R: Read> StreamReader<BufReader<R>> {
    pub fn try_new(read: R, ctx: &Context) -> VortexResult<Self> {
        Self::try_new_unbuffered(BufReader::new(read), ctx)
    }
}

impl<R: Read> StreamReader<R> {
    pub fn try_new_unbuffered(mut read: R, ctx: &Context) -> VortexResult<Self> {
        let mut messages = StreamMessageReader::try_new(&mut read)?;
        match messages.peek() {
            None => vortex_bail!("IPC stream is empty"),
            Some(msg) => {
                if msg.header_as_context().is_none() {
                    vortex_bail!(InvalidSerde: "Expected IPC Context as first message in stream")
                }
            }
        }

        let view_ctx: ViewContext = SerdeContextDeserializer {
            fb: messages.next(&mut read)?.header_as_context().unwrap(),
            ctx,
        }
        .try_into()?;

        Ok(Self {
            read,
            messages,
            ctx: view_ctx.into(),
        })
    }

    /// Read a single array from the IPC stream.
    pub fn read_array(&mut self) -> VortexResult<Array> {
        let mut array_reader = self
            .next()?
            .ok_or_else(|| vortex_err!(InvalidSerde: "Unexpected EOF"))?;

        let mut chunks = vec![];
        while let Some(chunk) = array_reader.next()? {
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
impl<R: Read> FallibleLendingIterator for StreamReader<R> {
    type Error = VortexError;
    type Item<'next> = StreamArrayReader<'next, R> where Self: 'next;

    fn next(&mut self) -> Result<Option<StreamArrayReader<'_, R>>, Self::Error> {
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
            .next(&mut self.read)?
            .header_as_schema()
            .unwrap();

        let dtype = DType::try_from(
            schema_msg
                .dtype()
                .ok_or_else(|| vortex_err!(InvalidSerde: "Schema missing DType"))?,
        )
        .map_err(|e| vortex_err!(InvalidSerde: "Failed to parse DType: {}", e))?;

        Ok(Some(StreamArrayReader {
            ctx: self.ctx.clone(),
            read: &mut self.read,
            messages: &mut self.messages,
            dtype,
            buffers: vec![],
            row_offset: 0,
        }))
    }
}

pub struct StreamArrayReader<'a, R: Read> {
    ctx: Arc<ViewContext>,
    read: &'a mut R,
    messages: &'a mut StreamMessageReader<R>,
    dtype: DType,
    buffers: Vec<Buffer>,
    row_offset: usize,
}

impl<'a, R: Read> StreamArrayReader<'a, R> {
    pub fn dtype(&self) -> &DType {
        &self.dtype
    }

    pub fn take(self, indices: &'a Array) -> VortexResult<TakeIterator<'a, R>> {
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

pub struct TakeIterator<'a, R: Read> {
    reader: StreamArrayReader<'a, R>,
    indices: &'a Array,
    row_offset: usize,
}

/// NB: if the StreamArrayReader expires without being fully-consumed by the underlying iterator,
/// it will leave the underlying messages stream in an unreadable state and free up a mutable ref
/// to that stream, allowing someone else to try to issue a read that will inevitably fail.
/// For this reason, we force full consumption of the reader when it goes out of scope.
impl<'a, R: Read> Drop for StreamArrayReader<'a, R> {
    fn drop(&mut self) {
        // NB: can't catch_unwind/unwrap here because &mut self can't be made UnwindSafe
        loop {
            let next = self.next();
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
impl<'a, R: Read> FallibleIterator for TakeIterator<'a, R> {
    type Item = Array;
    type Error = VortexError;

    fn next(&mut self) -> VortexResult<Option<Self::Item>> {
        if self.indices.is_empty() {
            return Ok(None);
        }
        while let Some(batch) = self.reader.next()? {
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
impl<'iter, R: Read> FallibleLendingIterator for StreamArrayReader<'iter, R> {
    type Error = VortexError;
    type Item<'next> = Array where Self: 'next;

    fn next(&mut self) -> Result<Option<Array>, Self::Error> {
        let Some(chunk_msg) = self.messages.peek().and_then(|msg| msg.header_as_chunk()) else {
            return Ok(None);
        };

        // Read all the column's buffers
        self.buffers.clear();
        let mut offset = 0;
        for buffer in chunk_msg.buffers().unwrap_or_default().iter() {
            let _skip = buffer.offset() - offset;
            self.read.skip(buffer.offset() - offset)?;

            // TODO(ngates): read into a single buffer, then Arc::clone and slice
            let mut bytes = Vec::with_capacity(buffer.length() as usize);
            self.read.read_into(buffer.length(), &mut bytes)?;
            let arrow_buffer = ArrowBuffer::from_vec(bytes);
            self.buffers.push(Buffer::from(arrow_buffer));

            offset = buffer.offset() + buffer.length();
        }

        // Consume any remaining padding after the final buffer.
        self.read.skip(chunk_msg.buffer_size() - offset)?;

        // After reading the buffers we're now able to load the next message.
        let flatbuffer = self.messages.next_raw(self.read)?;

        let view = ArrayView::try_new(
            self.ctx.clone(),
            self.dtype.clone(),
            flatbuffer,
            |flatbuffer| {
                root::<Message>(flatbuffer)
                    .map_err(VortexError::from)
                    .map(|msg| msg.header_as_chunk().unwrap())
                    .and_then(|chunk| chunk.array().ok_or(vortex_err!("Chunk missing Array")))
            },
            self.buffers.clone(),
        )?;

        // Validate it
        view.to_array().with_dyn(|_| Ok::<(), VortexError>(()))?;

        let array = view.into_array();
        self.row_offset += array.len();
        Ok(Some(array))
    }
}

pub trait ReadExtensions: Read {
    /// Skip n bytes in the stream.
    fn skip(&mut self, nbytes: u64) -> io::Result<()> {
        io::copy(&mut self.take(nbytes), &mut io::sink())?;
        Ok(())
    }

    /// Read exactly nbytes into the buffer.
    fn read_into(&mut self, nbytes: u64, buffer: &mut Vec<u8>) -> VortexResult<()> {
        buffer.reserve_exact(nbytes as usize);
        if self.take(nbytes).read_to_end(buffer)? != nbytes as usize {
            vortex_bail!(InvalidSerde: "Failed to read all bytes")
        }
        Ok(())
    }
}

impl<R: Read> ReadExtensions for R {}

struct StreamMessageReader<R: Read> {
    message: Vec<u8>,
    prev_message: Vec<u8>,
    finished: bool,
    phantom: PhantomData<R>,
}

impl<R: Read> StreamMessageReader<R> {
    pub fn try_new(read: &mut R) -> VortexResult<Self> {
        let mut reader = Self {
            message: Vec::new(),
            prev_message: Vec::new(),
            finished: false,
            phantom: PhantomData,
        };
        reader.load_next_message(read)?;
        Ok(reader)
    }

    pub fn peek(&self) -> Option<Message> {
        if self.finished {
            return None;
        }
        // The message has been validated by the next() call.
        Some(unsafe { root_unchecked::<Message>(&self.message) })
    }

    pub fn next_raw(&mut self, read: &mut R) -> VortexResult<Buffer> {
        if self.finished {
            panic!("StreamMessageReader is finished - should've checked peek!");
        }
        std::mem::swap(&mut self.prev_message, &mut self.message);
        if !self.load_next_message(read)? {
            self.finished = true;
        }
        Ok(Buffer::from(self.prev_message.clone()))
    }

    pub fn next(&mut self, read: &mut R) -> VortexResult<Message> {
        if self.finished {
            panic!("StreamMessageReader is finished - should've checked peek!");
        }
        std::mem::swap(&mut self.prev_message, &mut self.message);
        if !self.load_next_message(read)? {
            self.finished = true;
        }
        Ok(unsafe { root_unchecked::<Message>(&self.prev_message) })
    }

    fn load_next_message(&mut self, read: &mut R) -> VortexResult<bool> {
        let mut len_buf = [0u8; 4];
        match read.read_exact(&mut len_buf) {
            Ok(_) => {}
            Err(e) => {
                return match e.kind() {
                    io::ErrorKind::UnexpectedEof => Ok(false),
                    _ => Err(e.into()),
                };
            }
        }

        let len = u32::from_le_bytes(len_buf);
        if len == u32::MAX {
            // Marker for no more messages.
            return Ok(false);
        }

        self.message.clear();
        self.message.reserve(len as usize);
        if read.take(len as u64).read_to_end(&mut self.message)? != len as usize {
            vortex_bail!(InvalidSerde: "Failed to read all bytes")
        }

        std::hint::black_box(root::<Message>(&self.message)?);
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Read, Write};

    use fallible_iterator::FallibleIterator;
    use itertools::Itertools;
    use vortex::array::chunked::{Chunked, ChunkedArray};
    use vortex::array::primitive::{Primitive, PrimitiveArray, PrimitiveEncoding};
    use vortex::encoding::{ArrayEncoding, EncodingId, EncodingRef};
    use vortex::stats::{ArrayStatistics, Stat};
    use vortex::{Array, ArrayDType, ArrayDef, Context, IntoArray, ToStatic};
    use vortex_alp::{ALPArray, ALPEncoding};
    use vortex_dtype::NativePType;
    use vortex_error::VortexResult;
    use vortex_fastlanes::{BitPackedArray, BitPackedEncoding};

    use crate::iter::FallibleLendingIterator;
    use crate::reader::StreamReader;
    use crate::writer::StreamWriter;

    #[test]
    fn test_read_write() {
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
        let _ = cursor.write(b"hello").unwrap();

        cursor.set_position(0);
        {
            let mut reader = StreamReader::try_new_unbuffered(&mut cursor, &ctx).unwrap();
            let first = reader.read_array().unwrap();
            assert_eq!(first.encoding().id(), Primitive::ID);
            let second = reader.read_array().unwrap();
            assert_eq!(second.encoding().id(), Chunked::ID);
        }
        let _pos = cursor.position();
        // Test our termination bytes exist
        let mut terminator = [0u8; 5];
        cursor.read_exact(&mut terminator).unwrap();
        assert_eq!(&terminator, b"hello");
    }

    #[test]
    fn test_write_read_primitive() {
        // NB: the order is reversed here to ensure we aren't grabbing indexes instead of values
        let data = PrimitiveArray::from((0i32..3_000_000).rev().collect_vec()).into_array();
        test_base_case(
            &data,
            &[2999989i32, 2999988, 2999987, 2999986, 2899999, 0, 0],
            PrimitiveEncoding.id(),
        );
    }

    #[test]
    fn test_write_read_alp() {
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
        );
    }

    #[test]
    fn test_stats() {
        let data = PrimitiveArray::from((0i32..3_000_000).collect_vec()).into_array();
        // calculate stats on the input array so that the output array will also have stats
        data.statistics().compute_min::<i32>().unwrap();

        let data = round_trip(&data);
        verify_stats(&data);

        let run_count: u64 = data.statistics().get_as::<u64>(Stat::RunCount).unwrap();
        assert_eq!(run_count, 3000000);
    }

    #[test]
    fn test_stats_chunked() {
        let array = PrimitiveArray::from((0i32..1_500_000).collect_vec()).into_array();
        let array2 = PrimitiveArray::from((1_500_000i32..3_000_000).collect_vec()).into_array();

        // calculate stats on the input array so that the output array will also have stats
        array.statistics().compute_min::<i32>().unwrap();
        array2.statistics().compute_min::<i32>().unwrap();
        let chunked_array =
            ChunkedArray::try_new(vec![array.clone(), array2], array.dtype().clone())
                .unwrap()
                .into_array();

        let data = round_trip(&chunked_array);

        // NB: data is an ArrayData constructed from the result of calling read_array on an array
        // reader. compute on a ChunkedArray calls get_or_compute on the underlying chunks and
        // merges the results, while get does not. Thus we need to compute a stat and force this
        // merge computation before we can test get()
        data.statistics().compute(Stat::Min).unwrap();
        verify_stats(&data);

        // TODO(@jcasale): run_count calculation is wrong for chunked arrays, this should be 3mm
        let run_count: u64 = data.statistics().get_as::<u64>(Stat::RunCount).unwrap();
        assert_eq!(run_count, 3000001);
    }

    #[test]
    fn test_empty_index() {
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

        let mut cursor = Cursor::new(&buffer);
        let mut reader = StreamReader::try_new(&mut cursor, &Context::default()).unwrap();
        let array_reader = reader.next().unwrap().unwrap();
        let mut result_iter = array_reader.take(&indices).unwrap();
        let result = result_iter.next().unwrap();
        assert!(result.is_none())
    }

    #[test]
    fn test_negative_index_fails() {
        let data = PrimitiveArray::from((0i32..3_000_000).rev().collect_vec()).into_array();
        let indices =
            PrimitiveArray::from(vec![-1i32, 10, 11, 12, 13, 100_000, 2_999_999, 2_999_999])
                .into_array();
        test_read_write_single_chunk_array(&data, &indices)
            .expect_err("Expected negative index to fail");
    }

    #[test]
    fn test_noninteger_index_fails() {
        let data = PrimitiveArray::from((0i32..3_000_000).rev().collect_vec()).into_array();
        let indices = PrimitiveArray::from(vec![1f32, 10.0, 11.0, 12.0]).into_array();
        test_read_write_single_chunk_array(&data, &indices)
            .expect_err("Expected float index to fail");
    }

    #[test]
    fn test_null_index_fails() {
        let data = PrimitiveArray::from((0i32..3_000_000).rev().collect_vec()).into_array();
        let indices = PrimitiveArray::from_nullable_vec(vec![None, Some(1i32), Some(10), Some(11)])
            .into_array();
        test_read_write_single_chunk_array(&data, &indices)
            .expect_err("Expected float index to fail");
    }

    #[test]
    fn test_write_read_bitpacked() {
        // NB: the order is reversed here to ensure we aren't grabbing indexes instead of values
        let uncompressed = PrimitiveArray::from((0u64..3_000).rev().collect_vec());
        let packed = BitPackedArray::encode(uncompressed.array(), 5).unwrap();

        let expected = &[2989u64, 2988, 2987, 2986];
        test_base_case(&packed.into_array(), expected, PrimitiveEncoding.id());
    }

    #[test]
    fn test_write_read_chunked() {
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

        let mut cursor = Cursor::new(&buffer);
        let mut reader = StreamReader::try_new(&mut cursor, &Context::default()).unwrap();
        let array_reader = reader.next().unwrap().unwrap();
        let mut take_iter = array_reader.take(&indices).unwrap();
        let next = take_iter.next().unwrap().unwrap();
        assert_eq!(next.encoding().id(), PrimitiveEncoding.id());

        assert_eq!(
            next.into_primitive().typed_data::<i32>(),
            vec![2999989, 2999988, 2999987, 2999986, 2899999, 0, 0]
        );
        assert_eq!(
            take_iter
                .next()
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
    #[test]
    fn test_write_read_does_not_compromise_stream() {
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

        let mut cursor = Cursor::new(&buffer);
        let mut reader = StreamReader::try_new(&mut cursor, &Context::default()).unwrap();
        let array_reader = reader.next().unwrap().unwrap();

        {
            let mut iter = array_reader.take(&indices).unwrap();

            // verify that for a fully-consumed iterator, the destructor does not advance the
            // underlying message stream to the next message, which would be very bad
            while iter.next().unwrap().is_some() {
                // Consume the iterator
            }
            // verify that if we continue to call next on the iterator, we get none and
            // nothing happens to the underlying stream
            assert!(iter.next().unwrap().is_none());
            assert!(iter.next().unwrap().is_none());
        }

        let indices = PrimitiveArray::from(vec![10i32, 11, 12, 13, 100_000, 2_999_999, 2_999_999])
            .into_array();
        let array_reader = reader.next().unwrap().unwrap();

        let mut take_iter = array_reader.take(&indices).unwrap();
        let array = take_iter.next().unwrap().unwrap();
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

    #[test]
    fn test_take_iter() {
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

        let mut cursor = Cursor::new(&buffer);
        let mut reader = StreamReader::try_new(&mut cursor, &Context::default()).unwrap();
        let array_reader = reader.next().unwrap().unwrap();

        let mut iter = array_reader.take(&indices).unwrap();

        let chunk = iter.next().unwrap().unwrap();
        assert_eq!(chunk.encoding().id(), PrimitiveEncoding.id());
        assert_eq!(
            chunk.into_primitive().typed_data::<i32>(),
            vec![2999989, 2999988, 2999987, 2999986, 2899999, 0, 0]
        );
        let chunk = iter.next().unwrap().unwrap();
        assert_eq!(chunk.into_primitive().typed_data::<i32>(), vec![5999999]);
    }

    fn test_base_case<T: NativePType>(
        data: &Array,
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

        let mut cursor = Cursor::new(&buffer);
        let mut reader = StreamReader::try_new(&mut cursor, &ctx).unwrap();
        let array_reader = reader.next().unwrap().unwrap();
        let mut take_iter = array_reader.take(&indices).unwrap();

        let array = take_iter.next().unwrap().unwrap();
        assert_eq!(array.encoding().id(), expected_encoding_id);

        let results = array
            .flatten_primitive()
            .unwrap()
            .typed_data::<T>()
            .to_vec();
        assert_eq!(results, expected);
    }

    fn test_read_write_single_chunk_array<'a>(
        data: &'a Array,
        indices: &'a Array,
    ) -> VortexResult<Array> {
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

        let mut cursor = Cursor::new(&buffer);
        let mut reader = StreamReader::try_new(&mut cursor, &ctx).unwrap();
        let array_reader = reader.next().unwrap().unwrap();
        let mut result_iter = array_reader.take(indices)?;
        let result = result_iter.next().unwrap();
        assert!(result_iter.next().unwrap().is_none());
        Ok(result.unwrap())
    }

    fn verify_stats(data: &Array) {
        let min: i32 = data
            .statistics()
            .get(Stat::Min)
            .unwrap()
            .as_ref()
            .try_into()
            .unwrap();
        assert_eq!(min, 0);
        let max: i32 = data
            .statistics()
            .get(Stat::Max)
            .unwrap()
            .as_ref()
            .try_into()
            .unwrap();
        assert_eq!(max, 2_999_999);
        let is_sorted = data.statistics().get_as::<bool>(Stat::IsSorted).unwrap();
        assert!(is_sorted);
        let is_strict_sorted: bool = data
            .statistics()
            .get_as::<bool>(Stat::IsStrictSorted)
            .unwrap();
        assert!(is_strict_sorted);
        let is_constant: bool = data.statistics().get_as::<bool>(Stat::IsConstant).unwrap();
        assert!(!is_constant);

        let null_ct: u64 = data.statistics().get_as::<u64>(Stat::NullCount).unwrap();
        assert_eq!(null_ct, 0);
        let bit_width_freq = data
            .statistics()
            .get_as::<Vec<usize>>(Stat::BitWidthFreq)
            .unwrap();
        assert_eq!(
            bit_width_freq,
            vec![
                1, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768,
                65536, 131072, 262144, 524288, 1048576, 902848, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            ]
        );
        let trailing_zero_freq = data
            .statistics()
            .get_as::<Vec<usize>>(Stat::TrailingZeroFreq)
            .unwrap();
        assert_eq!(
            trailing_zero_freq,
            vec![
                1500000, 750000, 375000, 187500, 93750, 46875, 23437, 11719, 5859, 2930, 1465, 732,
                366, 183, 92, 46, 23, 11, 6, 3, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
            ]
        );
        data.statistics()
            .compute_true_count()
            .expect_err("Should not be able to calculate true count for non-boolean array");
    }

    fn round_trip(chunked_array: &Array) -> Array {
        let context = Context::default();
        let mut buffer = vec![];
        {
            let mut cursor = Cursor::new(&mut buffer);
            {
                let mut writer = StreamWriter::try_new(&mut cursor, &context).unwrap();
                writer.write_array(chunked_array).unwrap();
            }
        }

        let mut cursor = Cursor::new(&buffer);
        let mut reader = StreamReader::try_new(&mut cursor, &context).unwrap();
        let data = reader.read_array().unwrap();
        data.to_static()
    }
}
