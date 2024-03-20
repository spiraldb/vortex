# Vortex Datetime Composite Extensions

/// Arrow Datetime Types
/// time32/64 - time of day
/// => LocalTime
/// date32 - days since unix epoch
/// date64 - millis since unix epoch
/// => LocalDate
/// timestamp(unit, tz)
/// => Instant iff tz == UTC
/// => ZonedDateTime(Instant, tz)
/// timestamp(unit)
/// => LocalDateTime (tz is "unknown", not "UTC")
/// duration
/// => Duration