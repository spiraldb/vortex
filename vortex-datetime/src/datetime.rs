use std::any::Any;
use std::sync::{Arc, RwLock};

use vortex::array::{check_slice_bounds, Array, ArrayRef, ArrowIterator, Encoding, EncodingId};
use vortex::compress::EncodingCompression;
use vortex::compute::ArrayCompute;
use vortex::dtype::Nullability::NonNullable;
use vortex::dtype::{DType, Nullability};
use vortex::error::{VortexError, VortexResult};
use vortex::formatter::{ArrayDisplay, ArrayFormatter};
use vortex::ptype::PType;
use vortex::scalar::Scalar;
use vortex::serde::{ArraySerde, EncodingSerde, ReadCtx, WriteCtx};
use vortex::stats::{Stats, StatsCompute, StatsSet};

/// An array that decomposes a datetime into days, seconds, and nanoseconds.
#[derive(Debug, Clone)]
pub struct DateTimeArray {
    days: ArrayRef,
    seconds: ArrayRef,
    subsecond: ArrayRef,
    dtype: DType,
    stats: Arc<RwLock<StatsSet>>,
}

impl DateTimeArray {
    pub fn new(days: ArrayRef, seconds: ArrayRef, subsecond: ArrayRef, dtype: DType) -> Self {
        Self::try_new(days, seconds, subsecond, dtype).unwrap()
    }

    pub fn try_new(
        days: ArrayRef,
        seconds: ArrayRef,
        subsecond: ArrayRef,
        dtype: DType,
    ) -> VortexResult<Self> {
        if !matches!(days.dtype(), DType::Int(_, _, _)) {
            return Err(VortexError::InvalidDType(days.dtype().clone()));
        }
        if !matches!(seconds.dtype(), DType::Int(_, _, _)) {
            return Err(VortexError::InvalidDType(seconds.dtype().clone()));
        }
        if !matches!(subsecond.dtype(), DType::Int(_, _, _)) {
            return Err(VortexError::InvalidDType(subsecond.dtype().clone()));
        }

        Ok(Self {
            days,
            seconds,
            subsecond,
            dtype,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        })
    }

    #[inline]
    pub fn days(&self) -> &dyn Array {
        self.days.as_ref()
    }

    #[inline]
    pub fn seconds(&self) -> &dyn Array {
        self.seconds.as_ref()
    }

    #[inline]
    pub fn subsecond(&self) -> &dyn Array {
        self.subsecond.as_ref()
    }
}

impl Array for DateTimeArray {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn boxed(self) -> ArrayRef {
        Box::new(self)
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    fn len(&self) -> usize {
        self.days.len()
    }

    fn is_empty(&self) -> bool {
        self.days.is_empty()
    }

    fn dtype(&self) -> &DType {
        &DType::LocalDate(NonNullable)
    }

    fn stats(&self) -> Stats {
        Stats::new(&self.stats, self)
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        todo!()
    }

    fn slice(&self, start: usize, stop: usize) -> VortexResult<ArrayRef> {
        todo!()
    }

    fn encoding(&self) -> &'static dyn Encoding {
        &DateTimeEncoding
    }

    fn nbytes(&self) -> usize {
        self.days().nbytes() + self.seconds().nbytes() + self.subsecond().nbytes()
    }

    fn serde(&self) -> &dyn ArraySerde {
        self
    }
}

impl StatsCompute for DateTimeArray {}

impl ArrayCompute for DateTimeArray {}

impl ArraySerde for DateTimeArray {
    fn write(&self, ctx: &mut WriteCtx) -> std::io::Result<()> {
        todo!()
    }
}

impl EncodingSerde for DateTimeEncoding {
    fn read(&self, ctx: &mut ReadCtx) -> std::io::Result<ArrayRef> {
        todo!()
    }
}

impl<'arr> AsRef<(dyn Array + 'arr)> for DateTimeArray {
    fn as_ref(&self) -> &(dyn Array + 'arr) {
        self
    }
}

impl ArrayDisplay for DateTimeArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.child("days", self.days())?;
        f.child("seconds", self.seconds())?;
        f.child("subsecond", self.subsecond())
    }
}

#[derive(Debug)]
pub struct DateTimeEncoding;

pub const DATETIME_ENCODING: EncodingId = EncodingId::new("vortex.datetime");

impl Encoding for DateTimeEncoding {
    fn id(&self) -> &EncodingId {
        &DATETIME_ENCODING
    }

    fn compression(&self) -> Option<&dyn EncodingCompression> {
        Some(self)
    }

    fn serde(&self) -> Option<&dyn EncodingSerde> {
        Some(self)
    }
}
