# Enc

Enc is a hierarchical array library designed for holding and operating over possibly compressed data.

## Design Principles

### Data Types

Data types separate the logical type of the data from the physical representation. This allows for the same logical
type to be represented in different ways. For example, an integer can be represented as a primitive array of
`i32` or as a run-length encoded array. In either case, the logical type is `integer`.

Some data types have optional constraints. The `integer` type has optional `signedness` and `bit_width` constraints.
These are inferred from the data if not specified. If they are specified, then these constraints are enforced during
compute.

### Arrays

Modelled as an enum of array encodings. Why an enum vs dynamic trait?

### Scalars

Modelled as a dynamic trait. Why a dynamic trait vs an enum?

### Arrow Compatibility

* Support zero-copy *from* Arrow.
* Decompress and export *to* Arrow.
