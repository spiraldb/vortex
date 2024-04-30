use vortex::array::composite::{Composite, CompositeArray};
use vortex::array::datetime::{LocalDateTimeArray, LocalDateTimeExtension, TimeUnit};
use vortex::array::primitive::PrimitiveArray;
use vortex::compress::{CompressConfig, CompressCtx, EncodingCompression};
use vortex::compute::cast::cast;
use vortex::{Array, ArrayDType, ArrayDef, ArrayTrait, IntoArray, OwnedArray};
use vortex_dtype::PType;
use vortex_error::VortexResult;

use crate::{DateTimePartsArray, DateTimePartsEncoding};

impl EncodingCompression for DateTimePartsEncoding {
    fn can_compress(
        &self,
        array: &Array,
        _config: &CompressConfig,
    ) -> Option<&dyn EncodingCompression> {
        if array.encoding().id() != Composite::ID {
            return None;
        }

        let composite = CompositeArray::try_from(array).unwrap();
        if !matches!(composite.id(), LocalDateTimeExtension::ID) {
            return None;
        }

        Some(self)
    }

    fn compress(
        &self,
        array: &Array,
        like: Option<&Array>,
        ctx: CompressCtx,
    ) -> VortexResult<OwnedArray> {
        let array = CompositeArray::try_from(array)?;
        match array.id() {
            LocalDateTimeExtension::ID => compress_localdatetime(
                array
                    .as_typed()
                    .expect("Can only compress LocalDateTimeArray"),
                like.map(|l| DateTimePartsArray::try_from(l).unwrap()),
                ctx,
            ),
            _ => panic!("Unsupported composite ID {}", array.id()),
        }
    }
}

fn compress_localdatetime(
    array: LocalDateTimeArray,
    like: Option<DateTimePartsArray>,
    ctx: CompressCtx,
) -> VortexResult<OwnedArray> {
    let underlying = cast(array.underlying(), PType::I64.into())?.flatten_primitive()?;

    let divisor = match array.underlying_metadata().time_unit() {
        TimeUnit::Ns => 1_000_000_000,
        TimeUnit::Us => 1_000_000,
        TimeUnit::Ms => 1_000,
        TimeUnit::S => 1,
    };

    let length = underlying.len();
    let mut days = Vec::with_capacity(length);
    let mut seconds = Vec::with_capacity(length);
    let mut subsecond = Vec::with_capacity(length);

    for &t in underlying.typed_data::<i64>().iter() {
        days.push(t / (86_400 * divisor));
        seconds.push((t % (86_400 * divisor)) / divisor);
        subsecond.push((t % (86_400 * divisor)) % divisor);
    }

    Ok(DateTimePartsArray::try_new(
        LocalDateTimeExtension::dtype(underlying.dtype().nullability()),
        ctx.named("days").compress(
            &PrimitiveArray::from(days).into_array(),
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
        ctx.compress_validity(underlying.validity())?,
    )?
    .into_array())
}
