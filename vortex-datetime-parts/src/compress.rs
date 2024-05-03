use vortex::array::datetime::{LocalDateTimeArray, TimeUnit};
use vortex::array::primitive::PrimitiveArray;
use vortex::compress::{CompressConfig, Compressor, EncodingCompression};
use vortex::compute::cast::cast;
use vortex::{Array, ArrayTrait, IntoArray, OwnedArray};
use vortex_dtype::PType;
use vortex_error::VortexResult;

use crate::{DateTimePartsArray, DateTimePartsEncoding};

impl EncodingCompression for DateTimePartsEncoding {
    fn can_compress(
        &self,
        array: &Array,
        _config: &CompressConfig,
    ) -> Option<&dyn EncodingCompression> {
        if LocalDateTimeArray::try_from(array).is_ok() {
            return Some(self);
        }
        None
    }

    fn compress(
        &self,
        array: &Array,
        like: Option<&Array>,
        ctx: Compressor,
    ) -> VortexResult<OwnedArray> {
        compress_localdatetime(
            LocalDateTimeArray::try_from(array)?,
            like.map(|l| DateTimePartsArray::try_from(l).unwrap()),
            ctx,
        )
    }
}

fn compress_localdatetime(
    array: LocalDateTimeArray,
    like: Option<DateTimePartsArray>,
    ctx: Compressor,
) -> VortexResult<OwnedArray> {
    let timestamps = cast(&array.timestamps(), PType::I64.into())?.flatten_primitive()?;

    let divisor = match array.time_unit() {
        TimeUnit::Ns => 1_000_000_000,
        TimeUnit::Us => 1_000_000,
        TimeUnit::Ms => 1_000,
        TimeUnit::S => 1,
    };

    let length = timestamps.len();
    let mut days = Vec::with_capacity(length);
    let mut seconds = Vec::with_capacity(length);
    let mut subsecond = Vec::with_capacity(length);

    for &t in timestamps.typed_data::<i64>().iter() {
        days.push(t / (86_400 * divisor));
        seconds.push((t % (86_400 * divisor)) / divisor);
        subsecond.push((t % (86_400 * divisor)) % divisor);
    }

    Ok(DateTimePartsArray::try_new(
        array.dtype().clone(),
        ctx.named("days").compress(
            &PrimitiveArray::from_vec(days, timestamps.validity()).into_array(),
            like.as_ref().map(|l| l.days()).as_ref(),
        )?,
        ctx.named("seconds").compress(
            &PrimitiveArray::from(seconds).into_array(),
            like.as_ref().map(|l| l.seconds()).as_ref(),
        )?,
        ctx.named("subsecond").compress(
            &PrimitiveArray::from(subsecond).into_array(),
            like.as_ref().map(|l| l.subsecond()).as_ref(),
        )?,
    )?
    .into_array())
}
