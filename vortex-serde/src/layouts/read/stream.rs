use std::mem;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::task::{ready, Context, Poll};

use bytes::{Bytes, BytesMut};
use futures::Stream;
use futures_util::future::BoxFuture;
use futures_util::{stream, FutureExt, StreamExt, TryStreamExt};
use vortex::array::{BoolArray, StructArray};
use vortex::compute::unary::subtract_scalar;
use vortex::compute::{filter, search_sorted, slice, take, SearchSortedSide};
use vortex::validity::Validity;
use vortex::{Array, IntoArray, IntoArrayVariant};
use vortex_dtype::{match_each_integer_ptype, DType};
use vortex_error::{vortex_err, vortex_panic, VortexError, VortexResult};
use vortex_scalar::Scalar;

use crate::io::VortexReadAt;
use crate::layouts::read::cache::LayoutMessageCache;
use crate::layouts::read::schema::Schema;
use crate::layouts::read::{Layout, MessageId, ReadResult, Scan};
use crate::layouts::Projection;
use crate::stream_writer::ByteRange;

pub struct LayoutBatchStream<R> {
    reader: Option<R>,
    layout: Box<dyn Layout>,
    scan: Scan,
    messages_cache: Arc<RwLock<LayoutMessageCache>>,
    state: StreamingState<R>,
    dtype: DType,
    current_offset: usize,
    result_projection: Projection,
}

impl<R: VortexReadAt> LayoutBatchStream<R> {
    pub fn try_new(
        reader: R,
        layout: Box<dyn Layout>,
        messages_cache: Arc<RwLock<LayoutMessageCache>>,
        dtype: DType,
        scan: Scan,
        result_projection: Projection,
    ) -> VortexResult<Self> {
        Ok(LayoutBatchStream {
            reader: Some(reader),
            layout,
            scan,
            messages_cache,
            state: Default::default(),
            dtype,
            current_offset: 0,
            result_projection,
        })
    }

    pub fn schema(&self) -> Schema {
        Schema(self.dtype.clone())
    }

    // TODO(robert): Push this logic down to layouts
    fn take_batch(&mut self, batch: &Array) -> VortexResult<Array> {
        let curr_offset = self.current_offset;
        let indices = self
            .scan
            .indices
            .as_ref()
            .ok_or_else(|| vortex_err!("Missing scan indices"))?;
        let left = search_sorted(indices, curr_offset, SearchSortedSide::Left)?.to_index();
        let right =
            search_sorted(indices, curr_offset + batch.len(), SearchSortedSide::Left)?.to_index();

        self.current_offset += batch.len();

        let indices_for_batch = slice(indices, left, right)?.into_primitive()?;
        let shifted_arr = match_each_integer_ptype!(indices_for_batch.ptype(), |$T| {
            subtract_scalar(&indices_for_batch.into_array(), &Scalar::from(curr_offset as $T))?
        });

        take(batch, &shifted_arr)
    }
}

type StreamStateFuture<R> = BoxFuture<'static, VortexResult<(R, Vec<(MessageId, Bytes)>)>>;

#[derive(Default)]
enum StreamingState<R> {
    #[default]
    Init,
    Reading(StreamStateFuture<R>),
    Decoding(Array),
    Error,
}

impl<R: VortexReadAt + Unpin + Send + 'static> Stream for LayoutBatchStream<R> {
    type Item = VortexResult<Array>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.state {
                StreamingState::Init => {
                    if let Some(read) = self.layout.read()? {
                        match read {
                            ReadResult::GetMsgs(messages) => {
                                let reader = mem::take(&mut self.reader)
                                    .ok_or_else(|| vortex_err!("Invalid state transition"))?;
                                let read_future = read_ranges(reader, messages).boxed();
                                self.state = StreamingState::Reading(read_future);
                            }
                            ReadResult::Batch(a) => self.state = StreamingState::Decoding(a),
                        }
                    } else {
                        return Poll::Ready(None);
                    }
                }
                StreamingState::Decoding(arr) => {
                    let mut batch = arr.clone();
                    if self.scan.indices.is_some() {
                        batch = self.take_batch(&batch)?;
                    }

                    if let Some(row_filter) = &self.scan.filter {
                        let mask = row_filter.filter.evaluate(&batch)?;
                        let filter_array = null_as_false(mask.into_bool()?)?;
                        batch = filter(&batch, &filter_array)?;
                    }

                    batch = match &self.result_projection {
                        Projection::All => batch,
                        Projection::Flat(v) => {
                            StructArray::try_from(batch)?.project(v)?.into_array()
                        }
                    };

                    self.state = StreamingState::Init;
                    return Poll::Ready(Some(Ok(batch)));
                }
                StreamingState::Reading(f) => match ready!(f.poll_unpin(cx)) {
                    Ok((read, buffers)) => {
                        let mut write_cache =
                            self.messages_cache.write().unwrap_or_else(|poison| {
                                vortex_panic!("Failed to write to message cache: {poison}")
                            });
                        for (id, buf) in buffers {
                            write_cache.set(id, buf)
                        }
                        drop(write_cache);
                        self.reader = Some(read);
                        self.state = StreamingState::Init
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

fn null_as_false(array: BoolArray) -> VortexResult<Array> {
    match array.validity() {
        Validity::NonNullable => Ok(array.into_array()),
        Validity::AllValid => {
            Ok(BoolArray::try_new(array.boolean_buffer(), Validity::NonNullable)?.into_array())
        }
        Validity::AllInvalid => Ok(BoolArray::from(vec![false; array.len()]).into_array()),
        Validity::Array(v) => {
            let bool_buffer = &array.boolean_buffer() & &v.into_bool()?.boolean_buffer();
            Ok(BoolArray::from(bool_buffer).into_array())
        }
    }
}

async fn read_ranges<R: VortexReadAt>(
    reader: R,
    ranges: Vec<(MessageId, ByteRange)>,
) -> VortexResult<(R, Vec<(MessageId, Bytes)>)> {
    stream::iter(ranges.into_iter())
        .map(|(id, range)| {
            let mut buf = BytesMut::with_capacity(range.len());
            unsafe { buf.set_len(range.len()) }

            let read_ft = reader.read_at_into(range.begin, buf);

            read_ft.map(|result| {
                result
                    .map(|res| (id, res.freeze()))
                    .map_err(VortexError::from)
            })
        })
        .buffered(10)
        .try_collect()
        .await
        .map(|b| (reader, b))
}

#[cfg(test)]
mod tests {
    use vortex::array::BoolArray;
    use vortex::validity::Validity;
    use vortex::IntoArrayVariant;

    use crate::layouts::read::stream::null_as_false;

    #[test]
    fn coerces_nulls() {
        let bool_array = BoolArray::from_vec(
            vec![true, true, false, false],
            Validity::Array(BoolArray::from(vec![true, false, true, false]).into()),
        );
        let non_null_array = null_as_false(bool_array).unwrap().into_bool().unwrap();
        assert_eq!(
            non_null_array.boolean_buffer().iter().collect::<Vec<_>>(),
            vec![true, false, false, false]
        );
    }
}
