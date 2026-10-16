// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::fmt::Display;
use std::sync::Arc;

use vortex_error::VortexResult;
use vortex_error::vortex_bail;

use crate::dtype::extension::ExtDTypeRef;
use crate::dtype::extension::ExtVTable;
use crate::dtype::extension::Matcher;
use crate::extension::datetime::Date;
use crate::extension::datetime::Duration;
use crate::extension::datetime::Interval;
use crate::extension::datetime::Time;
use crate::extension::datetime::TimeUnit;
use crate::extension::datetime::Timestamp;

/// Matcher for temporal extension data types.
pub struct AnyTemporal;

impl Matcher for AnyTemporal {
    type Match<'a> = TemporalMetadata<'a>;

    fn try_match<'a>(item: &'a ExtDTypeRef) -> Option<Self::Match<'a>> {
        if let Some(opts) = item.metadata_opt::<Timestamp>() {
            return Some(TemporalMetadata::Timestamp(&opts.unit, &opts.tz));
        }
        if let Some(opts) = item.metadata_opt::<Date>() {
            return Some(TemporalMetadata::Date(opts));
        }
        if let Some(opts) = item.metadata_opt::<Time>() {
            return Some(TemporalMetadata::Time(opts));
        }
        if let Some(opts) = item.metadata_opt::<Duration>() {
            return Some(TemporalMetadata::Duration(opts));
        }
        if item.is::<Interval>() {
            return Some(TemporalMetadata::Interval);
        }
        None
    }
}

/// Metadata for temporal extension data types.
#[derive(Debug, PartialEq, Eq)]
pub enum TemporalMetadata<'a> {
    /// Metadata for Timestamp dtypes, a tuple of time unit and optional timezone.
    Timestamp(&'a TimeUnit, &'a Option<Arc<str>>),
    /// Metadata for Date dtypes
    Date(&'a <Date as ExtVTable>::Metadata),
    /// Metadata for Time dtypes
    Time(&'a <Time as ExtVTable>::Metadata),
    /// Metadata for Duration dtypes
    Duration(&'a <Duration as ExtVTable>::Metadata),
    /// Interval dtypes, which have no metadata
    Interval,
}

// TODO(ngates): remove this logic in favor of having an ExtScalarVTable in vortex_array::scalar.
//  Currently this is used largely to implement scalar display hacks.
impl TemporalMetadata<'_> {
    /// Get the time unit of the temporal dtype, or `None` for an Interval, which has none.
    pub fn time_unit(&self) -> Option<TimeUnit> {
        match self {
            TemporalMetadata::Time(unit) => Some(**unit),
            TemporalMetadata::Date(unit) => Some(**unit),
            TemporalMetadata::Timestamp(unit, _tz) => Some(**unit),
            TemporalMetadata::Duration(unit) => Some(**unit),
            TemporalMetadata::Interval => None,
        }
    }

    /// Convert a timestamp value to a Jiff value.
    pub fn to_jiff(&self, v: i64) -> VortexResult<TemporalJiff> {
        match self {
            TemporalMetadata::Time(unit) => Ok(TemporalJiff::Time(
                jiff::civil::Time::MIN.checked_add(unit.to_jiff_span(v)?)?,
            )),
            TemporalMetadata::Date(unit) => Ok(TemporalJiff::Date(
                jiff::civil::Date::new(1970, 1, 1)?.checked_add(unit.to_jiff_span(v)?)?,
            )),
            TemporalMetadata::Timestamp(unit, tz) => match tz {
                None => Ok(TemporalJiff::Unzoned(
                    jiff::civil::DateTime::new(1970, 1, 1, 0, 0, 0, 0)?
                        .checked_add(unit.to_jiff_span(v)?)?,
                )),
                Some(tz) => Ok(TemporalJiff::Zoned(
                    jiff::Timestamp::UNIX_EPOCH
                        .checked_add(unit.to_jiff_span(v)?)?
                        .in_tz(tz.as_ref())?,
                )),
            },
            TemporalMetadata::Duration(_) => vortex_bail!("Duration values are not points in time"),
            TemporalMetadata::Interval => vortex_bail!("Interval values are not points in time"),
        }
    }
}

/// A Jiff representation of a temporal value.
pub enum TemporalJiff {
    /// A time value.
    Time(jiff::civil::Time),
    /// A date value.
    Date(jiff::civil::Date),
    /// A zone-naive timestamp value.
    Unzoned(jiff::civil::DateTime),
    /// A zoned timestamp value.
    Zoned(jiff::Zoned),
}

impl Display for TemporalJiff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TemporalJiff::Time(t) => write!(f, "{t}"),
            TemporalJiff::Date(d) => write!(f, "{d}"),
            TemporalJiff::Unzoned(dt) => write!(f, "{dt}"),
            TemporalJiff::Zoned(z) => write!(f, "{z}"),
        }
    }
}
