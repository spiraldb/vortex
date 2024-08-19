use std::collections::HashSet;

use vortex::array::TemporalArray;
use vortex::encoding::EncodingRef;
use vortex::{Array, ArrayDType, ArrayDef, IntoArray};
use vortex_datetime_dtype::TemporalMetadata;
use vortex_datetime_parts::{
    compress_temporal, DateTimeParts, DateTimePartsArray, DateTimePartsEncoding,
};
use vortex_error::VortexResult;

use crate::compressors::{CompressedArray, CompressionTree, EncodingCompressor};
use crate::SamplingCompressor;

#[derive(Debug)]
pub struct DateTimePartsCompressor;

impl EncodingCompressor for DateTimePartsCompressor {
    fn id(&self) -> &str {
        DateTimeParts::ID.as_ref()
    }

    fn can_compress(&self, array: &Array) -> Option<&dyn EncodingCompressor> {
        if let Ok(temporal_array) = TemporalArray::try_from(array) {
            match temporal_array.temporal_metadata() {
                // We only attempt to compress Timestamp arrays.
                TemporalMetadata::Timestamp(..) => Some(self),
                _ => None,
            }
        } else {
            None
        }
    }

    fn compress<'a>(
        &'a self,
        array: &Array,
        like: Option<CompressionTree<'a>>,
        ctx: SamplingCompressor<'a>,
    ) -> VortexResult<CompressedArray<'a>> {
        let (days, seconds, subseconds) = compress_temporal(TemporalArray::try_from(array)?)?;

        let days = ctx
            .named("days")
            .compress(&days, like.as_ref().and_then(|l| l.child(0)))?;
        let seconds = ctx
            .named("seconds")
            .compress(&seconds, like.as_ref().and_then(|l| l.child(1)))?;
        let subsecond = ctx
            .named("subsecond")
            .compress(&subseconds, like.as_ref().and_then(|l| l.child(2)))?;
        Ok(CompressedArray::new(
            DateTimePartsArray::try_new(
                array.dtype().clone(),
                days.array,
                seconds.array,
                subsecond.array,
            )?
            .into_array(),
            Some(CompressionTree::new(
                self,
                vec![days.path, seconds.path, subsecond.path],
            )),
        ))
    }

    fn used_encodings(&self) -> HashSet<EncodingRef> {
        HashSet::from([&DateTimePartsEncoding as EncodingRef])
    }
}
