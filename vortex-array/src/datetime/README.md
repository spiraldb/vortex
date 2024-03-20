# Vortex Datetime Composite Extensions

This module provides implementations of datetime types using composite arrays.

## Arrow Conversion

| Arrow Type            | Vortex Type     |                                  |
|-----------------------|-----------------|----------------------------------|
| `time32/64`           | `LocalTime`     | Time since midnight              |
| `date32/64`           | `LocalDate`     | Julian day                       |
| `timestamp(tz=None)`  | `LocalDateTime` | Julian day + time since midnight |
| `timestamp(tz=UTC)`   | `Instant`       | Time since Unix epoch            |
| `timestamp(tz=Other)` | `ZonedDateTime` | TZ aware time since Unix epoch   |
