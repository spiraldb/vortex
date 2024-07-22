use vortex::array::datetime::{TemporalArray, TimeUnit};
use vortex::array::primitive::PrimitiveArray;
use vortex::compute::unary::try_cast;
use vortex::{Array, IntoArray, IntoArrayVariant};
use vortex_dtype::PType;
use vortex_error::{vortex_bail, VortexResult};

/// Compress a `TemporalArray` into day, second, and subsecond components.
///
/// Splitting the components by granularity creates more small values, which enables better
/// cascading compression.
pub fn compress_temporal(array: TemporalArray) -> VortexResult<(Array, Array, Array)> {
    let timestamps = try_cast(&array.temporal_values(), PType::I64.into())?.into_primitive()?;
    let divisor = match array.temporal_metadata().time_unit() {
        TimeUnit::Ns => 1_000_000_000,
        TimeUnit::Us => 1_000_000,
        TimeUnit::Ms => 1_000,
        TimeUnit::S => 1,
        TimeUnit::D => vortex_bail!(InvalidArgument: "Cannot compress day-level data"),
    };

    let length = timestamps.len();
    let mut days = Vec::with_capacity(length);
    let mut seconds = Vec::with_capacity(length);
    let mut subsecond = Vec::with_capacity(length);

    for &t in timestamps.maybe_null_slice::<i64>().iter() {
        days.push(t / (86_400 * divisor));
        seconds.push((t % (86_400 * divisor)) / divisor);
        subsecond.push((t % (86_400 * divisor)) % divisor);
    }

    Ok((
        PrimitiveArray::from_vec(days, timestamps.validity()).into_array(),
        PrimitiveArray::from(seconds).into_array(),
        PrimitiveArray::from(subsecond).into_array(),
    ))
}
