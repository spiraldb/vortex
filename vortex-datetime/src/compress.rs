use crate::{DateTimeArray, DateTimeEncoding};
use vortex::array::downcast::DowncastArrayBuiltin;
use vortex::array::primitive::{PrimitiveArray, PrimitiveEncoding};
use vortex::array::typed::{TypedArray, TypedEncoding};
use vortex::array::{Array, ArrayRef, Encoding};
use vortex::compress::{CompressConfig, CompressCtx, EncodingCompression};
use vortex::dtype::{DType, TimeUnit};
use vortex::error::{VortexError, VortexResult};

impl EncodingCompression for DateTimeEncoding {
    fn can_compress(
        &self,
        array: &dyn Array,
        _config: &CompressConfig,
    ) -> Option<&dyn EncodingCompression> {
        if array.encoding().id() != TypedEncoding.id() {
            return None;
        }

        if array.as_typed().untyped_array().encoding().id() != PrimitiveEncoding.id() {
            return None;
        }

        if !matches!(array.dtype(), DType::ZonedDateTime(_, _)) {
            return None;
        }

        Some(self)
    }

    fn compress(
        &self,
        array: &dyn Array,
        like: Option<&dyn Array>,
        ctx: CompressCtx,
    ) -> VortexResult<ArrayRef> {
        match array.dtype() {
            DType::ZonedDateTime(unit, nullability) => {
                let tarray = array.as_any().downcast_ref::<TypedArray>().unwrap();
                let parray = tarray
                    .untyped_array()
                    .as_any()
                    .downcast_ref::<PrimitiveArray>()
                    .unwrap();
                // Eh, it's fine for now.
                let ts = parray.typed_data::<i64>();

                let ld = like.map(|l| l.as_any().downcast_ref::<DateTimeArray>().unwrap());

                match unit {
                    TimeUnit::Us => {
                        let mut days = Vec::with_capacity(ts.len());
                        let mut seconds = Vec::with_capacity(ts.len());
                        let mut subsecond = Vec::with_capacity(ts.len());
                        for &t in ts.iter() {
                            days.push(t / 86_400_000_000);
                            seconds.push((t % 86_400_000_000) / 1_000_000);
                            subsecond.push((t % 86_400_000_000) % 1_000_000);
                        }

                        let days_array = PrimitiveArray::from(days);
                        let seconds_array = PrimitiveArray::from(seconds);
                        let subsecond_array = PrimitiveArray::from(subsecond);

                        Ok(DateTimeArray::new(
                            ctx.named("days")
                                .compress(days_array.as_ref(), ld.map(|l| l.days()))?,
                            ctx.named("seconds")
                                .compress(seconds_array.as_ref(), ld.map(|l| l.seconds()))?,
                            ctx.named("subsecond")
                                .compress(subsecond_array.as_ref(), ld.map(|l| l.subsecond()))?,
                            array.dtype().clone(),
                        )
                        .boxed())
                    }
                    _ => todo!("Unit {:?}", unit),
                }
            }
            _ => Err(VortexError::InvalidDType(array.dtype().clone())),
        }
    }
}
