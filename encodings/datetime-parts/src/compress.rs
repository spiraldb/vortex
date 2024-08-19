use vortex::array::{PrimitiveArray, TemporalArray};
use vortex::compute::unary::try_cast;
use vortex::{Array, IntoArray, IntoArrayVariant};
use vortex_datetime_dtype::TimeUnit;
use vortex_dtype::PType;
use vortex_error::{vortex_bail, VortexResult};

/// Compress a `TemporalArray` into day, second, and subsecond components.
///
/// Splitting the components by granularity creates more small values, which enables better
/// cascading compression.
pub fn compress_temporal(array: TemporalArray) -> VortexResult<(Array, Array, Array)> {
    // After this operation, timestamps will be PrimitiveArray<i64>
    let timestamps = try_cast(
        &array.temporal_values().into_primitive()?.into_array(),
        PType::I64.into(),
    )?;
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

    for &t in timestamps.as_primitive().maybe_null_slice::<i64>().iter() {
        days.push(t / (86_400 * divisor));
        seconds.push((t % (86_400 * divisor)) / divisor);
        subsecond.push((t % (86_400 * divisor)) % divisor);
    }

    Ok((
        PrimitiveArray::from_vec(days, timestamps.as_primitive().validity()).into_array(),
        PrimitiveArray::from(seconds).into_array(),
        PrimitiveArray::from(subsecond).into_array(),
    ))
}
