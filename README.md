# Vortex

[![Build Status](https://github.com/fulcrum-so/vortex/actions/workflows/rust.yml/badge.svg)](https://github.com/fulcrum-so/vortex/actions)
[![Crates.io](https://img.shields.io/crates/v/vortex-array.svg)](https://crates.io/crates/vortex-array)
[![Documentation](https://docs.rs/vortex-rs/badge.svg)](https://docs.rs/vortex-array)
[![Rust](https://img.shields.io/badge/rust-1.76.0%2B-blue.svg?maxAge=3600)](https://github.com/fulcrum-so/vortex)

An in-memory format for 1-dimensional array data.

Vortex is a maximally [Apache Arrow](https://arrow.apache.org/) compatible data format that aims to separate logical and
physical representation of data, and allow pluggable physical layout.

Array operations are separately defined in terms of their semantics, dealing only with logical types and physical layout
that defines exact ways in which values are transformed.

# Logical Types

Vortex type system only conveys semantic meaning of the array data without prescribing physical layout. When operating
over arrays you can focus on semantics of the operation. Separately you can provide low level implementation dependent
on particular physical operation.

```
Null: all null array
Bool: Single bit value
Integer: Fixed width signed/unsigned number. Supports 8, 16, 32, 64 bit widths
Float: Fixed width floating point number. Supports 16, 32, 64 bit float types
Decimal: Fixed width decimal with specified precision (total number of digits) and scale (number of digits after decimal point)
Instant: An instantaneous point on the time-line. Number of seconds/miliseconds/microseconds/nanoseconds from epoch
LocalDate: A date without a time-zone
LocalTime: A time without a time-zone
ZonedDateTime: A data and time including ISO-8601 timezone
List: Sequence of items of same type
Map: Key, value mapping
Struct: Named tuple of types
```

# Physical Encodings

Vortex calls array implementations encodings, they encode the physical layout of the data. Encodings are recurisvely
nested, i.e. encodings contain other encodings. For every array you have their value data type and the its encoding that
defines how operations will be performed. By default necessary encodings to zero copy convert to and from Apache Arrow
are included in the package.

When performing operations they're dispatched on the encodings to provide specialized implementation.

## Compression

The advantage of separating physical layout from the semantic of the data is compression. Vortex can compress data
without requiring changes to the logical operations. To support efficient data access we focus on lightweight
compression algorithms only falling back to general purpose compressors for binary data.
