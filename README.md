# Vortex

[![Build Status](https://github.com/fulcrum-so/vortex/actions/workflows/rust.yml/badge.svg)](https://github.com/fulcrum-so/vortex/actions)
[![Crates.io](https://img.shields.io/crates/v/vortex-array.svg)](https://crates.io/crates/vortex-array)
[![Documentation](https://docs.rs/vortex-rs/badge.svg)](https://docs.rs/vortex-array)
[![Rust](https://img.shields.io/badge/rust-1.76.0%2B-blue.svg?maxAge=3600)](https://github.com/fulcrum-so/vortex)

Vortex is a toolkit for working with compressed array data.

> [!CAUTION]
> This library is very much a work in progress!

The major components of Vortex are (will be!):

* **Logical Types** - a schema definition making no assertions about physical layout.
* **Encodings** - a pluggable set of physical layouts. Vortex ships with several state-of-the-art lightweight codecs
  that have the potential to support GPU decompression.
* **Compression** - recursive compression based on stratified sampling.
* **Compute** - compute kernels that can operate over compressed data.
* **Statistics** - each array carries around lazily computed summary statistics, optionally populated from disk. These
  are available to compute kernels as well as to the compressor.
* **Serde** - zero-copy serialization. Designed to work well both on-disk and over-the-wire.

At Fulcrum, we are working to build infrastructure for next-generation data processing. We believe in leaving no stone
unturned and are looking closely at every single level of the data stack.
Vortex provides the framework upon which we can experiment and develop solutions to the first level of the stack:
storage and IO.

## Overview: Logical vs Physical

One of the core principles in Vortex is separation of the logical from the physical.

A Vortex array is defined by a logical data type as well as a physical encoding. Vortex ships with several built-in
encodings, as well as several extension encodings.

The built-in encodings are designed to model the Apache Arrow in-memory format, enabling us to construct Vortex arrays
with zero-copy from Arrow arrays.
The included extension encodings are mostly designed to model compressed in-memory arrays, such as run-length or
dictionary encoding.

## Components

### Logical Types

The Vortex type-system is still in flux. The current set of logical types is:

* Null
* Bool
* Integer
* Float
* Decimal
* Binary
* UTF8
* List
* Struct
* Date/Time/DateTime/Duration: TODO
* FixedList: TODO
* Union: TODO

### Plain Encodings

Vortex includes a base set of encodings that are designed to be compatible with Apache Arrow. These are:

* Null
* Bool
* Primitive (Integer, Float)
* Struct
* VarBin
* VarBinView
* ...with more to come

### Extension Encodings

Vortex includes a set of extension encodings that are designed to model compressed in-memory arrays. These are:

* BitPacking
* Constant
* Chunked
* Dictionary
* Frame-of-Reference
* RoaringUInt
* RoaringBool
* Sparse
* ZigZag

### Compression

A compression algorithm has been built based on
the [BtrBlocks](https://www.cs.cit.tum.de/fileadmin/w00cfj/dis/papers/btrblocks.pdf) paper.

Roughly, for each chunk of data a sample is taken and a set of encodings are attempted. The best performing encoding
is then chosen to encode the entire chunk. This sounds like it would be very expensive, but given basic statistics
about a chunk, it is possible to cheaply rule out many encodings and ensure the search space does not explode in size.

### Compute

Vortex provides the ability for each encoding to provide override the implementation of a compute function to avoid
decompressing where possible. For example, filtering a dictionary-encoded UTF8 array can be more cheaply performed by
filtering the dictionary first.

### Statistics

Each array carries around lazily computed summary statistics. This allows statistics to be populated from disk formats
if they exist, and to be subsequently used by the compressor and compute kernels.

The current statistics are:

* BitWidthFreq
* TrailingZeroFreq
* IsConstant
* IsSorted
* IsStrictSorted
* Max
* Min
* RunCount
* TrueCount
* NullCount

### Serialization / Deserialization (Serde)

TODO

## Vs Apache Arrow

It is important to note that Vortex and Arrow have been designed with different goals in mind. As such, it is somewhat
unfair to make any comparison at all. But given both can be used as array libraries, it is worth noting the differences.

Vortex is designed to be maximally compatible with Apache Arrow. All Arrow arrays can be converted into Vortex arrays
with zero-copy. And a Vortex array constructed from an Arrow array can be converted back to Arrow, again with zero-copy.

Where Vortex differs from Arrow is that it explicitly separates logical types from physical encodings. This allows
Vortex to model more complex arrays while still exposing a logical interface. For example, Vortex can model a UTF8
`ChunkedArray` where the first chunk is run-length encoded and the second chunk is dictionary encoded.
In Arrow, `RunLengthArray` and `DictionaryArray` are separate logical types, and so cannot be combined in this way.

## Contributing

While we hope to turn Vortex into a community project, it is currently changing so rapidly that it would be infeasible
to take contributions without prior discussion.

## License

Licensed under the Apache License, Version 2.0 (the "License").