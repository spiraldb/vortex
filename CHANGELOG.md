# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## `vortex-zigzag` - [0.13.0](https://github.com/spiraldb/vortex/compare/vortex-zigzag-v0.12.0...vortex-zigzag-v0.13.0) - 2024-10-18

### Added
- trim metadatas (part 2) ([#1028](https://github.com/spiraldb/vortex/pull/1028))
- teach *Metadata and ScalarValue to Display ([#975](https://github.com/spiraldb/vortex/pull/975))

## `vortex-runend-bool` - [0.13.0](https://github.com/spiraldb/vortex/compare/vortex-runend-bool-v0.12.0...vortex-runend-bool-v0.13.0) - 2024-10-18

### Added
- teach *Metadata and ScalarValue to Display ([#975](https://github.com/spiraldb/vortex/pull/975))

### Other
- Faster RunEndBool decompression, plus metadata cleanup ([#981](https://github.com/spiraldb/vortex/pull/981))

## `vortex-runend` - [0.13.0](https://github.com/spiraldb/vortex/compare/vortex-runend-v0.12.0...vortex-runend-v0.13.0) - 2024-10-18

### Added
- trim metadatas (part 2) ([#1028](https://github.com/spiraldb/vortex/pull/1028))
- teach *Metadata and ScalarValue to Display ([#975](https://github.com/spiraldb/vortex/pull/975))

### Other
- RunEnd ends array max is length of the array ([#1017](https://github.com/spiraldb/vortex/pull/1017))
- Clean up runend metadata & stats ([#1011](https://github.com/spiraldb/vortex/pull/1011))

## `vortex-roaring` - [0.13.0](https://github.com/spiraldb/vortex/compare/vortex-roaring-v0.12.0...vortex-roaring-v0.13.0) - 2024-10-18

### Added
- teach *Metadata and ScalarValue to Display ([#975](https://github.com/spiraldb/vortex/pull/975))

### Fixed
- RoaringInt `can_compress` was erroneously always `None` ([#1004](https://github.com/spiraldb/vortex/pull/1004))

### Other
- RoaringBool bitmaps needs to manually append trailing false values when canonicalizing ([#988](https://github.com/spiraldb/vortex/pull/988))

## `vortex-fsst` - [0.13.0](https://github.com/spiraldb/vortex/compare/vortex-fsst-v0.12.0...vortex-fsst-v0.13.0) - 2024-10-18

### Added
- trim metadatas (part 2) ([#1028](https://github.com/spiraldb/vortex/pull/1028))
- teach *Metadata and ScalarValue to Display ([#975](https://github.com/spiraldb/vortex/pull/975))

## `vortex-dict` - [0.13.0](https://github.com/spiraldb/vortex/compare/vortex-dict-v0.12.0...vortex-dict-v0.13.0) - 2024-10-18

### Added
- increase dict decompression throughput ([#1032](https://github.com/spiraldb/vortex/pull/1032))
- trim metadatas (part 2) ([#1028](https://github.com/spiraldb/vortex/pull/1028))
- faster Dict logical validity ([#1034](https://github.com/spiraldb/vortex/pull/1034))
- improved objective function in sampling compressor ([#1000](https://github.com/spiraldb/vortex/pull/1000))
- teach *Metadata and ScalarValue to Display ([#975](https://github.com/spiraldb/vortex/pull/975))

### Other
- Use foldhash in dict encoding ([#980](https://github.com/spiraldb/vortex/pull/980))

## `vortex-datetime-parts` - [0.13.0](https://github.com/spiraldb/vortex/compare/vortex-datetime-parts-v0.12.0...vortex-datetime-parts-v0.13.0) - 2024-10-18

### Added
- teach *Metadata and ScalarValue to Display ([#975](https://github.com/spiraldb/vortex/pull/975))

## `vortex-sampling-compressor` - [0.13.0](https://github.com/spiraldb/vortex/compare/vortex-sampling-compressor-v0.12.0...vortex-sampling-compressor-v0.13.0) - 2024-10-18

### Added
- trim metadatas (part 2) ([#1028](https://github.com/spiraldb/vortex/pull/1028))
- add ChunkedCompressor which compresses chunk n+1 like chunk n ([#996](https://github.com/spiraldb/vortex/pull/996))
- improved objective function in sampling compressor ([#1000](https://github.com/spiraldb/vortex/pull/1000))

### Fixed
- disable roaring compressors ([#1076](https://github.com/spiraldb/vortex/pull/1076))
- Vortex (de)compress benchmarks read/write a Layout ([#1024](https://github.com/spiraldb/vortex/pull/1024))
- RoaringInt `can_compress` was erroneously always `None` ([#1004](https://github.com/spiraldb/vortex/pull/1004))

### Other
- Trim BitPackedMetadata to only required values ([#1046](https://github.com/spiraldb/vortex/pull/1046))

## `vortex-serde` - [0.13.0](https://github.com/spiraldb/vortex/compare/vortex-serde-v0.12.0...vortex-serde-v0.13.0) - 2024-10-18

### Added
- improved objective function in sampling compressor ([#1000](https://github.com/spiraldb/vortex/pull/1000))

### Fixed
- RelativeLayoutCache must hold disk dtype ([#1051](https://github.com/spiraldb/vortex/pull/1051))
- from_fields is fallible ([#1054](https://github.com/spiraldb/vortex/pull/1054))

### Other
- limit number of single char variables to 2 ([#1066](https://github.com/spiraldb/vortex/pull/1066))
- Error when projecting projected dtype ([#1063](https://github.com/spiraldb/vortex/pull/1063))
- Delegate dtype deserialization and projection to layouts ([#1060](https://github.com/spiraldb/vortex/pull/1060))
- Correctly resolve filter column references when reading column layouts ([#1058](https://github.com/spiraldb/vortex/pull/1058))
- Add Select expression and reorganize vortex-expr crate ([#1049](https://github.com/spiraldb/vortex/pull/1049))
- make write logic slightly more simple ([#1026](https://github.com/spiraldb/vortex/pull/1026))
- Fix writing of Chunked Struct arrays ([#1020](https://github.com/spiraldb/vortex/pull/1020))

## `vortex-expr` - [0.13.0](https://github.com/spiraldb/vortex/compare/vortex-expr-v0.12.0...vortex-expr-v0.13.0) - 2024-10-18

### Other
- Add Select expression and reorganize vortex-expr crate ([#1049](https://github.com/spiraldb/vortex/pull/1049))

## `vortex-datafusion` - [0.13.0](https://github.com/spiraldb/vortex/compare/vortex-datafusion-v0.12.0...vortex-datafusion-v0.13.0) - 2024-10-18

### Added
- teach *Metadata and ScalarValue to Display ([#975](https://github.com/spiraldb/vortex/pull/975))

### Fixed
- from_fields is fallible ([#1054](https://github.com/spiraldb/vortex/pull/1054))

### Other
- move infer_schema and infer_data_type into vortex-dtype ([#1081](https://github.com/spiraldb/vortex/pull/1081))
- Add Select expression and reorganize vortex-expr crate ([#1049](https://github.com/spiraldb/vortex/pull/1049))

## `vortex-bytebool` - [0.13.0](https://github.com/spiraldb/vortex/compare/vortex-bytebool-v0.12.0...vortex-bytebool-v0.13.0) - 2024-10-18

### Added
- trim metadatas (part 2) ([#1028](https://github.com/spiraldb/vortex/pull/1028))
- teach *Metadata and ScalarValue to Display ([#975](https://github.com/spiraldb/vortex/pull/975))

## `vortex-fastlanes` - [0.13.0](https://github.com/spiraldb/vortex/compare/vortex-fastlanes-v0.12.0...vortex-fastlanes-v0.13.0) - 2024-10-18

### Added
- canonicalize `indices` in `take` if sufficiently large ([#1036](https://github.com/spiraldb/vortex/pull/1036))
- trim metadatas (part 2) ([#1028](https://github.com/spiraldb/vortex/pull/1028))
- improved objective function in sampling compressor ([#1000](https://github.com/spiraldb/vortex/pull/1000))
- teach *Metadata and ScalarValue to Display ([#975](https://github.com/spiraldb/vortex/pull/975))

### Other
- limit number of single char variables to 2 ([#1066](https://github.com/spiraldb/vortex/pull/1066))
- Trim BitPackedMetadata to only required values ([#1046](https://github.com/spiraldb/vortex/pull/1046))

## `vortex-scalar` - [0.13.0](https://github.com/spiraldb/vortex/compare/vortex-scalar-v0.12.0...vortex-scalar-v0.13.0) - 2024-10-18

### Added
- trim metadatas (part 2) ([#1028](https://github.com/spiraldb/vortex/pull/1028))
- introduce ScalarType, the trait for scalar-y Rust types ([#1008](https://github.com/spiraldb/vortex/pull/1008))
- proto matches serde: f16 serializes as an unsigned integer ([#992](https://github.com/spiraldb/vortex/pull/992))
- teach *Metadata and ScalarValue to Display ([#975](https://github.com/spiraldb/vortex/pull/975))

### Fixed
- rely on ScalarValue Display in Scalar Display ([#978](https://github.com/spiraldb/vortex/pull/978))
- teach protobuf how to deserialize f16 ([#991](https://github.com/spiraldb/vortex/pull/991))

### Other
- StructScalar stores borrowed arcs ([#1073](https://github.com/spiraldb/vortex/pull/1073))
- limit number of single char variables to 2 ([#1066](https://github.com/spiraldb/vortex/pull/1066))

## `vortex-flatbuffers` - [0.13.0](https://github.com/spiraldb/vortex/compare/vortex-flatbuffers-v0.12.0...vortex-flatbuffers-v0.13.0) - 2024-10-18

### Other
- limit number of single char variables to 2 ([#1066](https://github.com/spiraldb/vortex/pull/1066))

## `vortex-dtype` - [0.13.0](https://github.com/spiraldb/vortex/compare/vortex-dtype-v0.12.0...vortex-dtype-v0.13.0) - 2024-10-18

### Fixed
- RelativeLayoutCache must hold disk dtype ([#1051](https://github.com/spiraldb/vortex/pull/1051))
- RoaringInt `can_compress` was erroneously always `None` ([#1004](https://github.com/spiraldb/vortex/pull/1004))

### Other
- limit number of single char variables to 2 ([#1066](https://github.com/spiraldb/vortex/pull/1066))
- Delegate dtype deserialization and projection to layouts ([#1060](https://github.com/spiraldb/vortex/pull/1060))

## `vortex-datetime-dtype` - [0.13.0](https://github.com/spiraldb/vortex/compare/vortex-datetime-dtype-v0.12.0...vortex-datetime-dtype-v0.13.0) - 2024-10-18

### Other
- limit number of single char variables to 2 ([#1066](https://github.com/spiraldb/vortex/pull/1066))

## `vortex-buffer` - [0.13.0](https://github.com/spiraldb/vortex/compare/vortex-buffer-v0.12.0...vortex-buffer-v0.13.0) - 2024-10-18

### Other
- limit number of single char variables to 2 ([#1066](https://github.com/spiraldb/vortex/pull/1066))

## `vortex-array` - [0.13.0](https://github.com/spiraldb/vortex/compare/0.12.0...0.13.0) - 2024-10-18

### Added
- BoolArray::take is faster ([#1035](https://github.com/spiraldb/vortex/pull/1035))
- better error message when an array encoding id is unknown ([#997](https://github.com/spiraldb/vortex/pull/997))
- teach *Metadata and ScalarValue to Display ([#975](https://github.com/spiraldb/vortex/pull/975))

### Fixed
- from_fields is fallible ([#1054](https://github.com/spiraldb/vortex/pull/1054))
- StructArray::try_from([]) is an error ([#1053](https://github.com/spiraldb/vortex/pull/1053))
- RoaringInt `can_compress` was erroneously always `None` ([#1004](https://github.com/spiraldb/vortex/pull/1004))
- teach pyvortex all our encodings ([#998](https://github.com/spiraldb/vortex/pull/998))
- BitWidthFreq must be u64/usize ([#974](https://github.com/spiraldb/vortex/pull/974))

### Other
- move infer_schema and infer_data_type into vortex-dtype ([#1081](https://github.com/spiraldb/vortex/pull/1081))
- limit number of single char variables to 2 ([#1066](https://github.com/spiraldb/vortex/pull/1066))
- Register ConstantArray filter function ([#1057](https://github.com/spiraldb/vortex/pull/1057))
- Use BooleanBuffer when canonicalizing Constant ([#1056](https://github.com/spiraldb/vortex/pull/1056))
- Fix infinite loop in constant array compare ([#1016](https://github.com/spiraldb/vortex/pull/1016))
- Slightly nicer unknown encodings error ([#1002](https://github.com/spiraldb/vortex/pull/1002))
- Don't clone dtype on PrimitiveArray::cast ([#972](https://github.com/spiraldb/vortex/pull/972))

## `vortex-alp` - [0.13.0](https://github.com/spiraldb/vortex/compare/vortex-alp-v0.12.0...vortex-alp-v0.13.0) - 2024-10-18

### Added
- improved objective function in sampling compressor ([#1000](https://github.com/spiraldb/vortex/pull/1000))
- teach *Metadata and ScalarValue to Display ([#975](https://github.com/spiraldb/vortex/pull/975))

### Other
- Trim BitPackedMetadata to only required values ([#1046](https://github.com/spiraldb/vortex/pull/1046))

## `vortex-runend-bool` - [0.12.0](https://github.com/spiraldb/vortex/compare/vortex-runend-bool-v0.11.0...vortex-runend-bool-v0.12.0) - 2024-10-03

### Added
- enable cargo lints ([#948](https://github.com/spiraldb/vortex/pull/948))

### Fixed
- trim array metadatas + fix validity handling (part 1) ([#966](https://github.com/spiraldb/vortex/pull/966))

### Other
- Include README in Cargo.toml ([#936](https://github.com/spiraldb/vortex/pull/936))

## `vortex-bytebool` - [0.12.0](https://github.com/spiraldb/vortex/compare/vortex-bytebool-v0.11.0...vortex-bytebool-v0.12.0) - 2024-10-03

### Added
- enable cargo lints ([#948](https://github.com/spiraldb/vortex/pull/948))

### Fixed
- trim array metadatas + fix validity handling (part 1) ([#966](https://github.com/spiraldb/vortex/pull/966))

### Other
- Include README in Cargo.toml ([#936](https://github.com/spiraldb/vortex/pull/936))

## `vortex-zigzag` - [0.12.0](https://github.com/spiraldb/vortex/compare/vortex-zigzag-v0.11.0...vortex-zigzag-v0.12.0) - 2024-10-03

### Added
- enable cargo lints ([#948](https://github.com/spiraldb/vortex/pull/948))

### Fixed
- trim array metadatas + fix validity handling (part 1) ([#966](https://github.com/spiraldb/vortex/pull/966))
- make PrimitiveArray cast properly handle validity/nullability ([#968](https://github.com/spiraldb/vortex/pull/968))

### Other
- Include README in Cargo.toml ([#936](https://github.com/spiraldb/vortex/pull/936))

## `vortex-runend` - [0.12.0](https://github.com/spiraldb/vortex/compare/vortex-runend-v0.11.0...vortex-runend-v0.12.0) - 2024-10-03

### Added
- SparseArray uses ScalarValue instead of Scalar ([#955](https://github.com/spiraldb/vortex/pull/955))
- enable cargo lints ([#948](https://github.com/spiraldb/vortex/pull/948))

### Fixed
- trim array metadatas + fix validity handling (part 1) ([#966](https://github.com/spiraldb/vortex/pull/966))
- make PrimitiveArray cast properly handle validity/nullability ([#968](https://github.com/spiraldb/vortex/pull/968))

### Other
- Include README in Cargo.toml ([#936](https://github.com/spiraldb/vortex/pull/936))

## `vortex-roaring` - [0.12.0](https://github.com/spiraldb/vortex/compare/vortex-roaring-v0.11.0...vortex-roaring-v0.12.0) - 2024-10-03

### Added
- enable cargo lints ([#948](https://github.com/spiraldb/vortex/pull/948))

### Fixed
- trim array metadatas + fix validity handling (part 1) ([#966](https://github.com/spiraldb/vortex/pull/966))

### Other
- Include README in Cargo.toml ([#936](https://github.com/spiraldb/vortex/pull/936))

## `vortex-fsst` - [0.12.0](https://github.com/spiraldb/vortex/compare/vortex-fsst-v0.11.0...vortex-fsst-v0.12.0) - 2024-10-03

### Added
- teach FSSTArray to compress the offsets of its codes ([#952](https://github.com/spiraldb/vortex/pull/952))
- enable cargo lints ([#948](https://github.com/spiraldb/vortex/pull/948))

### Fixed
- trim array metadatas + fix validity handling (part 1) ([#966](https://github.com/spiraldb/vortex/pull/966))
- make PrimitiveArray cast properly handle validity/nullability ([#968](https://github.com/spiraldb/vortex/pull/968))

### Other
- Include README in Cargo.toml ([#936](https://github.com/spiraldb/vortex/pull/936))

## `vortex-dict` - [0.12.0](https://github.com/spiraldb/vortex/compare/vortex-dict-v0.11.0...vortex-dict-v0.12.0) - 2024-10-03

### Added
- enable cargo lints ([#948](https://github.com/spiraldb/vortex/pull/948))

### Fixed
- trim array metadatas + fix validity handling (part 1) ([#966](https://github.com/spiraldb/vortex/pull/966))
- make PrimitiveArray cast properly handle validity/nullability ([#968](https://github.com/spiraldb/vortex/pull/968))

### Other
- Include README in Cargo.toml ([#936](https://github.com/spiraldb/vortex/pull/936))

## `vortex-datetime-parts` - [0.12.0](https://github.com/spiraldb/vortex/compare/vortex-datetime-parts-v0.11.0...vortex-datetime-parts-v0.12.0) - 2024-10-03

### Added
- enable cargo lints ([#948](https://github.com/spiraldb/vortex/pull/948))

### Fixed
- trim array metadatas + fix validity handling (part 1) ([#966](https://github.com/spiraldb/vortex/pull/966))
- cast error in compress_noci benchmark ([#971](https://github.com/spiraldb/vortex/pull/971))
- make PrimitiveArray cast properly handle validity/nullability ([#968](https://github.com/spiraldb/vortex/pull/968))

### Other
- Include README in Cargo.toml ([#936](https://github.com/spiraldb/vortex/pull/936))

## `vortex-sampling-compressor` - [0.12.0](https://github.com/spiraldb/vortex/compare/vortex-sampling-compressor-v0.11.0...vortex-sampling-compressor-v0.12.0) - 2024-10-03

### Added
- cost of for is ~0 ([#967](https://github.com/spiraldb/vortex/pull/967))
- implement ALP-RD compression ([#947](https://github.com/spiraldb/vortex/pull/947))
- teach FSSTArray to compress the offsets of its codes ([#952](https://github.com/spiraldb/vortex/pull/952))
- teach DeltaArray slice and scalar_at ([#927](https://github.com/spiraldb/vortex/pull/927))
- enable cargo lints ([#948](https://github.com/spiraldb/vortex/pull/948))

### Fixed
- trim array metadatas + fix validity handling (part 1) ([#966](https://github.com/spiraldb/vortex/pull/966))

### Other
- Include README in Cargo.toml ([#936](https://github.com/spiraldb/vortex/pull/936))

## `vortex-serde` - [0.12.0](https://github.com/spiraldb/vortex/compare/vortex-serde-v0.11.0...vortex-serde-v0.12.0) - 2024-10-03

### Added
- enable cargo lints ([#948](https://github.com/spiraldb/vortex/pull/948))

### Other
- increase the measurement_time of benches of serde ([#941](https://github.com/spiraldb/vortex/pull/941))
- Include README in Cargo.toml ([#936](https://github.com/spiraldb/vortex/pull/936))

## `vortex-schema` - [0.12.0](https://github.com/spiraldb/vortex/compare/vortex-schema-v0.11.0...vortex-schema-v0.12.0) - 2024-10-03

### Added
- enable cargo lints ([#948](https://github.com/spiraldb/vortex/pull/948))

### Other
- Include README in Cargo.toml ([#936](https://github.com/spiraldb/vortex/pull/936))

## `vortex-expr` - [0.12.0](https://github.com/spiraldb/vortex/compare/vortex-expr-v0.11.0...vortex-expr-v0.12.0) - 2024-10-03

### Added
- enable cargo lints ([#948](https://github.com/spiraldb/vortex/pull/948))

### Other
- Include README in Cargo.toml ([#936](https://github.com/spiraldb/vortex/pull/936))

## `vortex-datafusion` - [0.12.0](https://github.com/spiraldb/vortex/compare/vortex-datafusion-v0.11.0...vortex-datafusion-v0.12.0) - 2024-10-03

### Added
- enable cargo lints ([#948](https://github.com/spiraldb/vortex/pull/948))

### Other
- Include README in Cargo.toml ([#936](https://github.com/spiraldb/vortex/pull/936))

## `vortex-fastlanes` - [0.12.0](https://github.com/spiraldb/vortex/compare/vortex-fastlanes-v0.11.0...vortex-fastlanes-v0.12.0) - 2024-10-03

### Added
- implement ALP-RD compression ([#947](https://github.com/spiraldb/vortex/pull/947))
- SparseArray uses ScalarValue instead of Scalar ([#955](https://github.com/spiraldb/vortex/pull/955))
- teach DeltaArray slice and scalar_at ([#927](https://github.com/spiraldb/vortex/pull/927))
- enable cargo lints ([#948](https://github.com/spiraldb/vortex/pull/948))

### Fixed
- trim array metadatas + fix validity handling (part 1) ([#966](https://github.com/spiraldb/vortex/pull/966))
- make PrimitiveArray cast properly handle validity/nullability ([#968](https://github.com/spiraldb/vortex/pull/968))

### Other
- Include README in Cargo.toml ([#936](https://github.com/spiraldb/vortex/pull/936))

## `vortex-scalar` - [0.12.0](https://github.com/spiraldb/vortex/compare/vortex-scalar-v0.11.0...vortex-scalar-v0.12.0) - 2024-10-03

### Added
- teach ScalarValue and PValue is_instance_of ([#958](https://github.com/spiraldb/vortex/pull/958))
- SparseArray uses ScalarValue instead of Scalar ([#955](https://github.com/spiraldb/vortex/pull/955))
- slim down vortex-array metadata  ([#951](https://github.com/spiraldb/vortex/pull/951))
- enable cargo lints ([#948](https://github.com/spiraldb/vortex/pull/948))

### Other
- Include README in Cargo.toml ([#936](https://github.com/spiraldb/vortex/pull/936))

## `vortex-proto` - [0.12.0](https://github.com/spiraldb/vortex/compare/vortex-proto-v0.11.0...vortex-proto-v0.12.0) - 2024-10-03

### Added
- enable cargo lints ([#948](https://github.com/spiraldb/vortex/pull/948))

### Other
- Include README in Cargo.toml ([#936](https://github.com/spiraldb/vortex/pull/936))

## `vortex-flatbuffers` - [0.12.0](https://github.com/spiraldb/vortex/compare/vortex-flatbuffers-v0.11.0...vortex-flatbuffers-v0.12.0) - 2024-10-03

### Added
- enable cargo lints ([#948](https://github.com/spiraldb/vortex/pull/948))

### Other
- Include README in Cargo.toml ([#936](https://github.com/spiraldb/vortex/pull/936))

## `vortex-error` - [0.12.0](https://github.com/spiraldb/vortex/compare/vortex-error-v0.11.0...vortex-error-v0.12.0) - 2024-10-03

### Added
- enable cargo lints ([#948](https://github.com/spiraldb/vortex/pull/948))

### Other
- Include README in Cargo.toml ([#936](https://github.com/spiraldb/vortex/pull/936))

## `vortex-dtype` - [0.12.0](https://github.com/spiraldb/vortex/compare/vortex-dtype-v0.11.0...vortex-dtype-v0.12.0) - 2024-10-03

### Added
- enable cargo lints ([#948](https://github.com/spiraldb/vortex/pull/948))

### Other
- Include README in Cargo.toml ([#936](https://github.com/spiraldb/vortex/pull/936))

## `vortex-datetime-dtype` - [0.12.0](https://github.com/spiraldb/vortex/compare/vortex-datetime-dtype-v0.11.0...vortex-datetime-dtype-v0.12.0) - 2024-10-03

### Added
- enable cargo lints ([#948](https://github.com/spiraldb/vortex/pull/948))

### Other
- Include README in Cargo.toml ([#936](https://github.com/spiraldb/vortex/pull/936))

## `vortex-buffer` - [0.12.0](https://github.com/spiraldb/vortex/compare/vortex-buffer-v0.11.0...vortex-buffer-v0.12.0) - 2024-10-03

### Added
- enable cargo lints ([#948](https://github.com/spiraldb/vortex/pull/948))

### Other
- Include README in Cargo.toml ([#936](https://github.com/spiraldb/vortex/pull/936))

## `vortex-array` - [0.12.0](https://github.com/spiraldb/vortex/compare/0.11.0...0.12.0) - 2024-10-03

### Added
- implement ALP-RD compression ([#947](https://github.com/spiraldb/vortex/pull/947))
- teach ScalarValue and PValue is_instance_of ([#958](https://github.com/spiraldb/vortex/pull/958))
- SparseArray uses ScalarValue instead of Scalar ([#955](https://github.com/spiraldb/vortex/pull/955))
- BoolMetadata stores bit offset in 8 bits instead of 64 ([#956](https://github.com/spiraldb/vortex/pull/956))
- slim down vortex-array metadata  ([#951](https://github.com/spiraldb/vortex/pull/951))
- enable cargo lints ([#948](https://github.com/spiraldb/vortex/pull/948))

### Fixed
- trim array metadatas + fix validity handling (part 1) ([#966](https://github.com/spiraldb/vortex/pull/966))
- cast error in compress_noci benchmark ([#971](https://github.com/spiraldb/vortex/pull/971))
- make PrimitiveArray cast properly handle validity/nullability ([#968](https://github.com/spiraldb/vortex/pull/968))

### Other
- Include README in Cargo.toml ([#936](https://github.com/spiraldb/vortex/pull/936))
- Update README.md in vortex-array
- Include README.md files in crates and link top level readme into vortex-array crate ([#934](https://github.com/spiraldb/vortex/pull/934))
- Converting Arrow to Vortex should create Array and not ArrayData ([#931](https://github.com/spiraldb/vortex/pull/931))

## `vortex-alp` - [0.12.0](https://github.com/spiraldb/vortex/compare/vortex-alp-v0.11.0...vortex-alp-v0.12.0) - 2024-10-03

### Added
- implement ALP-RD compression ([#947](https://github.com/spiraldb/vortex/pull/947))
- SparseArray uses ScalarValue instead of Scalar ([#955](https://github.com/spiraldb/vortex/pull/955))
- enable cargo lints ([#948](https://github.com/spiraldb/vortex/pull/948))

### Fixed
- trim array metadatas + fix validity handling (part 1) ([#966](https://github.com/spiraldb/vortex/pull/966))
- make PrimitiveArray cast properly handle validity/nullability ([#968](https://github.com/spiraldb/vortex/pull/968))
- edge case in filling ALP encoded child on patches ([#939](https://github.com/spiraldb/vortex/pull/939))

### Other
- Include README in Cargo.toml ([#936](https://github.com/spiraldb/vortex/pull/936))

## `vortex-runend-bool` - [0.11.0](https://github.com/spiraldb/vortex/compare/vortex-runend-bool-v0.10.1...vortex-runend-bool-v0.11.0) - 2024-09-26

### Added
- ArrayView::child will throw if encoding not found ([#886](https://github.com/spiraldb/vortex/pull/886))

## `vortex-bytebool` - [0.11.0](https://github.com/spiraldb/vortex/compare/vortex-bytebool-v0.10.1...vortex-bytebool-v0.11.0) - 2024-09-26

### Added
- ArrayView::child will throw if encoding not found ([#886](https://github.com/spiraldb/vortex/pull/886))

## `vortex-runend` - [0.11.0](https://github.com/spiraldb/vortex/compare/vortex-runend-v0.10.1...vortex-runend-v0.11.0) - 2024-09-26

### Added
- ArrayView::child will throw if encoding not found ([#886](https://github.com/spiraldb/vortex/pull/886))

## `vortex-roaring` - [0.11.0](https://github.com/spiraldb/vortex/compare/vortex-roaring-v0.10.1...vortex-roaring-v0.11.0) - 2024-09-26

### Other
- Update croaring-sys to 4.1.4 and remove workarounds for croaring/660 ([#898](https://github.com/spiraldb/vortex/pull/898))

## `vortex-sampling-compressor` - [0.11.0](https://github.com/spiraldb/vortex/compare/vortex-sampling-compressor-v0.10.1...vortex-sampling-compressor-v0.11.0) - 2024-09-26

### Added
- sampling compressor is now seeded ([#917](https://github.com/spiraldb/vortex/pull/917))

## `vortex-fastlanes` - [0.11.0](https://github.com/spiraldb/vortex/compare/vortex-fastlanes-v0.10.1...vortex-fastlanes-v0.11.0) - 2024-09-26

### Added
- ArrayView::child will throw if encoding not found ([#886](https://github.com/spiraldb/vortex/pull/886))

### Fixed
- BitPackedArray must be unsigned ([#930](https://github.com/spiraldb/vortex/pull/930))

### Other
- Refactoring some IO-related code ([#846](https://github.com/spiraldb/vortex/pull/846))

## `vortex-serde` - [0.11.0](https://github.com/spiraldb/vortex/compare/vortex-serde-v0.10.1...vortex-serde-v0.11.0) - 2024-09-26

### Added
- update IPC format to hold buffer_index ([#903](https://github.com/spiraldb/vortex/pull/903))

### Other
- Naive interleaved filtering and data reading ([#918](https://github.com/spiraldb/vortex/pull/918))
- Refactoring some IO-related code ([#846](https://github.com/spiraldb/vortex/pull/846))

## `vortex-schema` - [0.11.0](https://github.com/spiraldb/vortex/compare/vortex-schema-v0.10.1...vortex-schema-v0.11.0) - 2024-09-26

### Other
- Refactoring some IO-related code ([#846](https://github.com/spiraldb/vortex/pull/846))

## `vortex-expr` - [0.11.0](https://github.com/spiraldb/vortex/compare/vortex-expr-v0.10.1...vortex-expr-v0.11.0) - 2024-09-26

### Other
- Refactoring some IO-related code ([#846](https://github.com/spiraldb/vortex/pull/846))

## `vortex-datafusion` - [0.11.0](https://github.com/spiraldb/vortex/compare/vortex-datafusion-v0.10.1...vortex-datafusion-v0.11.0) - 2024-09-26

### Added
- ArrayView::child will throw if encoding not found ([#886](https://github.com/spiraldb/vortex/pull/886))

### Other
- VortexScanExec stats are computed only once ([#914](https://github.com/spiraldb/vortex/pull/914))
- Refactoring some IO-related code ([#846](https://github.com/spiraldb/vortex/pull/846))
- VortexScanExec reports statistics to datafusion ([#909](https://github.com/spiraldb/vortex/pull/909))

## `vortex-scalar` - [0.11.0](https://github.com/spiraldb/vortex/compare/vortex-scalar-v0.10.1...vortex-scalar-v0.11.0) - 2024-09-26

### Other
- Teach StructTrait how to project fields ([#910](https://github.com/spiraldb/vortex/pull/910))

## `vortex-flatbuffers` - [0.11.0](https://github.com/spiraldb/vortex/compare/vortex-flatbuffers-v0.10.1...vortex-flatbuffers-v0.11.0) - 2024-09-26

### Added
- update IPC format to hold buffer_index ([#903](https://github.com/spiraldb/vortex/pull/903))

## `vortex-dtype` - [0.11.0](https://github.com/spiraldb/vortex/compare/vortex-dtype-v0.10.1...vortex-dtype-v0.11.0) - 2024-09-26

### Other
- Naive interleaved filtering and data reading ([#918](https://github.com/spiraldb/vortex/pull/918))
- Refactoring some IO-related code ([#846](https://github.com/spiraldb/vortex/pull/846))

## `vortex-array` - [0.11.0](https://github.com/spiraldb/vortex/compare/0.10.1...0.11.0) - 2024-09-26

### Added
- update IPC format to hold buffer_index ([#903](https://github.com/spiraldb/vortex/pull/903))
- ArrayView::child will throw if encoding not found ([#886](https://github.com/spiraldb/vortex/pull/886))

### Other
- Refactoring some IO-related code ([#846](https://github.com/spiraldb/vortex/pull/846))
- Teach StructTrait how to project fields ([#910](https://github.com/spiraldb/vortex/pull/910))

## `vortex-alp` - [0.11.0](https://github.com/spiraldb/vortex/compare/vortex-alp-v0.10.1...vortex-alp-v0.11.0) - 2024-09-26

### Added
- improve ALP exponent selection ([#921](https://github.com/spiraldb/vortex/pull/921))
- ArrayView::child will throw if encoding not found ([#886](https://github.com/spiraldb/vortex/pull/886))

### Other
- faster ALP encode ([#924](https://github.com/spiraldb/vortex/pull/924))

## `vortex-serde` - [0.10.1](https://github.com/spiraldb/vortex/compare/vortex-serde-v0.10.0...vortex-serde-v0.10.1) - 2024-09-20

### Added
- track compressed size & compare to parquet(zstd)? & canonical ([#882](https://github.com/spiraldb/vortex/pull/882))

## `vortex-runend-bool` - [0.10.0](https://github.com/spiraldb/vortex/compare/vortex-runend-bool-v0.9.0...vortex-runend-bool-v0.10.0) - 2024-09-20

### Fixed
- ID collision between vortex.ext and fastlanes.delta ([#878](https://github.com/spiraldb/vortex/pull/878))

### Other
- Make entry point compute functions accept generic arguments ([#861](https://github.com/spiraldb/vortex/pull/861))

## `vortex-bytebool` - [0.10.0](https://github.com/spiraldb/vortex/compare/vortex-bytebool-v0.9.0...vortex-bytebool-v0.10.0) - 2024-09-20

### Fixed
- ID collision between vortex.ext and fastlanes.delta ([#878](https://github.com/spiraldb/vortex/pull/878))

## `vortex-zigzag` - [0.10.0](https://github.com/spiraldb/vortex/compare/vortex-zigzag-v0.9.0...vortex-zigzag-v0.10.0) - 2024-09-20

### Fixed
- ID collision between vortex.ext and fastlanes.delta ([#878](https://github.com/spiraldb/vortex/pull/878))

### Other
- Make entry point compute functions accept generic arguments ([#861](https://github.com/spiraldb/vortex/pull/861))

## `vortex-runend` - [0.10.0](https://github.com/spiraldb/vortex/compare/vortex-runend-v0.9.0...vortex-runend-v0.10.0) - 2024-09-20

### Fixed
- ID collision between vortex.ext and fastlanes.delta ([#878](https://github.com/spiraldb/vortex/pull/878))

### Other
- Make entry point compute functions accept generic arguments ([#861](https://github.com/spiraldb/vortex/pull/861))
- Fix take on sliced RunEnd array ([#859](https://github.com/spiraldb/vortex/pull/859))

## `vortex-roaring` - [0.10.0](https://github.com/spiraldb/vortex/compare/vortex-roaring-v0.9.0...vortex-roaring-v0.10.0) - 2024-09-20

### Fixed
- ID collision between vortex.ext and fastlanes.delta ([#878](https://github.com/spiraldb/vortex/pull/878))

### Other
- Compute stats for RoaringBoolArray ([#874](https://github.com/spiraldb/vortex/pull/874))

## `vortex-fsst` - [0.10.0](https://github.com/spiraldb/vortex/compare/vortex-fsst-v0.9.0...vortex-fsst-v0.10.0) - 2024-09-20

### Fixed
- ID collision between vortex.ext and fastlanes.delta ([#878](https://github.com/spiraldb/vortex/pull/878))

### Other
- make miri tests fast again (take 2) ([#884](https://github.com/spiraldb/vortex/pull/884))
- also run the compress benchmarks ([#841](https://github.com/spiraldb/vortex/pull/841))
- Use sliced_bytes of VarBinArray when iterating over bytes() ([#867](https://github.com/spiraldb/vortex/pull/867))
- Make entry point compute functions accept generic arguments ([#861](https://github.com/spiraldb/vortex/pull/861))

## `vortex-dict` - [0.10.0](https://github.com/spiraldb/vortex/compare/vortex-dict-v0.9.0...vortex-dict-v0.10.0) - 2024-09-20

### Fixed
- ID collision between vortex.ext and fastlanes.delta ([#878](https://github.com/spiraldb/vortex/pull/878))

### Other
- Make entry point compute functions accept generic arguments ([#861](https://github.com/spiraldb/vortex/pull/861))

## `vortex-datetime-parts` - [0.10.0](https://github.com/spiraldb/vortex/compare/vortex-datetime-parts-v0.9.0...vortex-datetime-parts-v0.10.0) - 2024-09-20

### Fixed
- ID collision between vortex.ext and fastlanes.delta ([#878](https://github.com/spiraldb/vortex/pull/878))

### Other
- Make entry point compute functions accept generic arguments ([#861](https://github.com/spiraldb/vortex/pull/861))

## `vortex-sampling-compressor` - [0.10.0](https://github.com/spiraldb/vortex/compare/vortex-sampling-compressor-v0.9.0...vortex-sampling-compressor-v0.10.0) - 2024-09-20

### Added
- use Buffer for BitPackedArray ([#862](https://github.com/spiraldb/vortex/pull/862))

## `vortex-fastlanes` - [0.10.0](https://github.com/spiraldb/vortex/compare/vortex-fastlanes-v0.9.0...vortex-fastlanes-v0.10.0) - 2024-09-20

### Added
- add back ptype check for BitPackedArray ([#872](https://github.com/spiraldb/vortex/pull/872))
- use Buffer for BitPackedArray ([#862](https://github.com/spiraldb/vortex/pull/862))

### Fixed
- ID collision between vortex.ext and fastlanes.delta ([#878](https://github.com/spiraldb/vortex/pull/878))

### Other
- Make entry point compute functions accept generic arguments ([#861](https://github.com/spiraldb/vortex/pull/861))

## `vortex-datafusion` - [0.10.0](https://github.com/spiraldb/vortex/compare/vortex-datafusion-v0.9.0...vortex-datafusion-v0.10.0) - 2024-09-20

### Other
- Make entry point compute functions accept generic arguments ([#861](https://github.com/spiraldb/vortex/pull/861))

## `vortex-buffer` - [0.10.0](https://github.com/spiraldb/vortex/compare/vortex-buffer-v0.9.0...vortex-buffer-v0.10.0) - 2024-09-20

### Added
- use Buffer for BitPackedArray ([#862](https://github.com/spiraldb/vortex/pull/862))

## `vortex-array` - [0.10.0](https://github.com/spiraldb/vortex/compare/0.9.0...0.10.0) - 2024-09-20

### Fixed
- ID collision between vortex.ext and fastlanes.delta ([#878](https://github.com/spiraldb/vortex/pull/878))
- teach compute_as_cast and get_as_cast to handle null-only arrays ([#881](https://github.com/spiraldb/vortex/pull/881))

### Other
- Remove clone when creating ArrayData to run validation ([#888](https://github.com/spiraldb/vortex/pull/888))
- Don't validate offset buffers when converting them to arrow ([#887](https://github.com/spiraldb/vortex/pull/887))
- Add doc to bytes and sliced_bytes methods of VarBinArray ([#869](https://github.com/spiraldb/vortex/pull/869))
- Use sliced_bytes of VarBinArray when iterating over bytes() ([#867](https://github.com/spiraldb/vortex/pull/867))
- Make entry point compute functions accept generic arguments ([#861](https://github.com/spiraldb/vortex/pull/861))

## `vortex-alp` - [0.10.0](https://github.com/spiraldb/vortex/compare/vortex-alp-v0.9.0...vortex-alp-v0.10.0) - 2024-09-20

### Fixed
- ID collision between vortex.ext and fastlanes.delta ([#878](https://github.com/spiraldb/vortex/pull/878))

### Other
- Make entry point compute functions accept generic arguments ([#861](https://github.com/spiraldb/vortex/pull/861))

## `vortex-serde` - [0.9.0](https://github.com/spiraldb/vortex/compare/vortex-serde-v0.8.0...vortex-serde-v0.9.0) - 2024-09-17

### Added
- more Results, fewer panics, always have backtraces ([#761](https://github.com/spiraldb/vortex/pull/761))

### Fixed
- vortex-serde benchmarks depend on the ipc feature in arrow ([#849](https://github.com/spiraldb/vortex/pull/849))

### Other
- Simplify/idiomize the way arrays return `&Array` ([#826](https://github.com/spiraldb/vortex/pull/826))
- Reorder row filters ([#825](https://github.com/spiraldb/vortex/pull/825))
- Introduce a new `vortex-schema` crate ([#819](https://github.com/spiraldb/vortex/pull/819))
- Convert pruning filters to express whether the block should be pruned and not whether it should stay ([#800](https://github.com/spiraldb/vortex/pull/800))
- Fix ChunkedArray find_chunk_idx for empty chunks ([#802](https://github.com/spiraldb/vortex/pull/802))
- More explicit API for converting search sorted results into indices ([#777](https://github.com/spiraldb/vortex/pull/777))
- overload the name 'Footer' a bit less ([#773](https://github.com/spiraldb/vortex/pull/773))

## `vortex-expr` - [0.9.0](https://github.com/spiraldb/vortex/compare/vortex-expr-v0.8.0...vortex-expr-v0.9.0) - 2024-09-17

### Added
- more Results, fewer panics, always have backtraces ([#761](https://github.com/spiraldb/vortex/pull/761))

### Other
- Reorder row filters ([#825](https://github.com/spiraldb/vortex/pull/825))

## `vortex-scalar` - [0.9.0](https://github.com/spiraldb/vortex/compare/vortex-scalar-v0.8.0...vortex-scalar-v0.9.0) - 2024-09-17

### Added
- more Results, fewer panics, always have backtraces ([#761](https://github.com/spiraldb/vortex/pull/761))

### Other
- Define consistent float ordering ([#808](https://github.com/spiraldb/vortex/pull/808))
- Actually fuzz Struct and Chunked Arrays ([#805](https://github.com/spiraldb/vortex/pull/805))
- Fuzz Chunked and Struct arrays ([#801](https://github.com/spiraldb/vortex/pull/801))
- Fuzzer performs multiple operations on the underlying array instead of just one ([#766](https://github.com/spiraldb/vortex/pull/766))

## `vortex-flatbuffers` - [0.9.0](https://github.com/spiraldb/vortex/compare/vortex-flatbuffers-v0.8.0...vortex-flatbuffers-v0.9.0) - 2024-09-17

### Added
- more Results, fewer panics, always have backtraces ([#761](https://github.com/spiraldb/vortex/pull/761))

## `vortex-error` - [0.9.0](https://github.com/spiraldb/vortex/compare/vortex-error-v0.8.0...vortex-error-v0.9.0) - 2024-09-17

### Added
- more Results, fewer panics, always have backtraces ([#761](https://github.com/spiraldb/vortex/pull/761))

## `vortex-dtype` - [0.9.0](https://github.com/spiraldb/vortex/compare/vortex-dtype-v0.8.0...vortex-dtype-v0.9.0) - 2024-09-17

### Added
- more Results, fewer panics, always have backtraces ([#761](https://github.com/spiraldb/vortex/pull/761))

### Other
- Add description to new `vortex-schema` crate ([#829](https://github.com/spiraldb/vortex/pull/829))
- Define consistent float ordering ([#808](https://github.com/spiraldb/vortex/pull/808))
- Actually fuzz Struct and Chunked Arrays ([#805](https://github.com/spiraldb/vortex/pull/805))

## `vortex-datetime-dtype` - [0.9.0](https://github.com/spiraldb/vortex/compare/vortex-datetime-dtype-v0.8.0...vortex-datetime-dtype-v0.9.0) - 2024-09-17

### Added
- more Results, fewer panics, always have backtraces ([#761](https://github.com/spiraldb/vortex/pull/761))

### Other
- release to Test PyPI on each push to version tags ([#760](https://github.com/spiraldb/vortex/pull/760))

## `vortex-array` - [0.9.0](https://github.com/spiraldb/vortex/compare/0.8.0...0.9.0) - 2024-09-17

### Added
- implement search_sorted_many ([#840](https://github.com/spiraldb/vortex/pull/840))
- more Results, fewer panics, always have backtraces ([#761](https://github.com/spiraldb/vortex/pull/761))

### Other
- Update to rust 1.81 binary_search algorithm ([#851](https://github.com/spiraldb/vortex/pull/851))
- Fix chunked filter handling of set slices spanning multiple chunks ([#842](https://github.com/spiraldb/vortex/pull/842))
- Handle empty filters when filtering empty structs ([#834](https://github.com/spiraldb/vortex/pull/834))
- Handle filtering empty struct arrays ([#827](https://github.com/spiraldb/vortex/pull/827))
- Simplify/idiomize the way arrays return `&Array` ([#826](https://github.com/spiraldb/vortex/pull/826))
- Define consistent float ordering ([#808](https://github.com/spiraldb/vortex/pull/808))
- Actually fuzz Struct and Chunked Arrays ([#805](https://github.com/spiraldb/vortex/pull/805))
- Add is_encoding to array and fix cases of redundant encoding id checks ([#796](https://github.com/spiraldb/vortex/pull/796))
- implement FilterFn for ChunkedArray ([#794](https://github.com/spiraldb/vortex/pull/794))
- Fix ChunkedArray find_chunk_idx for empty chunks ([#802](https://github.com/spiraldb/vortex/pull/802))
- Fuzz Chunked and Struct arrays ([#801](https://github.com/spiraldb/vortex/pull/801))
- implement FilterFn for SparseArray ([#799](https://github.com/spiraldb/vortex/pull/799))
- Better scalar compare using collect_bool ([#792](https://github.com/spiraldb/vortex/pull/792))
- greedily combine chunks before compressing ([#783](https://github.com/spiraldb/vortex/pull/783))
- Introduce MaybeCompareFn trait to allow for partial compare specializations ([#768](https://github.com/spiraldb/vortex/pull/768))
- More explicit API for converting search sorted results into indices ([#777](https://github.com/spiraldb/vortex/pull/777))
- Fix slicing already sliced SparseArray ([#780](https://github.com/spiraldb/vortex/pull/780))
- Fix SearchSorted for SparseArray when searching from Right ([#770](https://github.com/spiraldb/vortex/pull/770))
- Fix StructArray::filter length calculation ([#769](https://github.com/spiraldb/vortex/pull/769))
- Fuzzer performs multiple operations on the underlying array instead of just one ([#766](https://github.com/spiraldb/vortex/pull/766))
- Filter struct arrays ([#767](https://github.com/spiraldb/vortex/pull/767))
- Fix unary/binary fn on `PrimitiveArray` ([#764](https://github.com/spiraldb/vortex/pull/764))
- Fix benchmarks ([#762](https://github.com/spiraldb/vortex/pull/762))
- Better implementation for `Validity::and` ([#758](https://github.com/spiraldb/vortex/pull/758))

## `vortex-alp` - [0.9.0](https://github.com/spiraldb/vortex/compare/vortex-alp-v0.8.0...vortex-alp-v0.9.0) - 2024-09-17

### Added
- more Results, fewer panics, always have backtraces ([#761](https://github.com/spiraldb/vortex/pull/761))

### Other
- Simplify/idiomize the way arrays return `&Array` ([#826](https://github.com/spiraldb/vortex/pull/826))
- Introduce MaybeCompareFn trait to allow for partial compare specializations ([#768](https://github.com/spiraldb/vortex/pull/768))

## `vortex-runend-bool` - [0.8.0](https://github.com/spiraldb/vortex/compare/vortex-runend-bool-v0.7.0...vortex-runend-bool-v0.8.0) - 2024-09-05

### Other
- Add `scalar_at_unchecked` ([#666](https://github.com/spiraldb/vortex/pull/666))
- Fix RunEnd take and scalar_at compute functions ([#588](https://github.com/spiraldb/vortex/pull/588))

## `vortex-bytebool` - [0.8.0](https://github.com/spiraldb/vortex/compare/vortex-bytebool-v0.7.0...vortex-bytebool-v0.8.0) - 2024-09-05

### Other
- Fix issues discovered by fuzzer ([#707](https://github.com/spiraldb/vortex/pull/707))
- Add `scalar_at_unchecked` ([#666](https://github.com/spiraldb/vortex/pull/666))
- Move expression filters out of datafusion ([#638](https://github.com/spiraldb/vortex/pull/638))
- Clippy deny `unwrap` & `panic` in functions that return `Result` ([#578](https://github.com/spiraldb/vortex/pull/578))

## `vortex-zigzag` - [0.8.0](https://github.com/spiraldb/vortex/compare/vortex-zigzag-v0.7.0...vortex-zigzag-v0.8.0) - 2024-09-05

### Other
- Add `scalar_at_unchecked` ([#666](https://github.com/spiraldb/vortex/pull/666))
- Clippy deny `unwrap` & `panic` in functions that return `Result` ([#578](https://github.com/spiraldb/vortex/pull/578))

## `vortex-runend` - [0.8.0](https://github.com/spiraldb/vortex/compare/vortex-runend-v0.7.0...vortex-runend-v0.8.0) - 2024-09-05

### Other
- Teach RunEnd take to respect its own validity ([#691](https://github.com/spiraldb/vortex/pull/691))
- Add `scalar_at_unchecked` ([#666](https://github.com/spiraldb/vortex/pull/666))
- Assert expected row count in tpch_benchmark binary ([#620](https://github.com/spiraldb/vortex/pull/620))
- RunEnd array scalar_at respects validity ([#608](https://github.com/spiraldb/vortex/pull/608))
- Fix RunEnd take and scalar_at compute functions ([#588](https://github.com/spiraldb/vortex/pull/588))

## `vortex-roaring` - [0.8.0](https://github.com/spiraldb/vortex/compare/vortex-roaring-v0.7.0...vortex-roaring-v0.8.0) - 2024-09-05

### Other
- Add `scalar_at_unchecked` ([#666](https://github.com/spiraldb/vortex/pull/666))
- Clippy deny `unwrap` & `panic` in functions that return `Result` ([#578](https://github.com/spiraldb/vortex/pull/578))

## `vortex-fsst` - [0.8.0](https://github.com/spiraldb/vortex/compare/vortex-fsst-v0.7.0...vortex-fsst-v0.8.0) - 2024-09-05

### Other
- FSSTCompressor ([#664](https://github.com/spiraldb/vortex/pull/664))
- Fix issues discovered by fuzzer ([#707](https://github.com/spiraldb/vortex/pull/707))
- Add `scalar_at_unchecked` ([#666](https://github.com/spiraldb/vortex/pull/666))

## `vortex-dict` - [0.8.0](https://github.com/spiraldb/vortex/compare/vortex-dict-v0.7.0...vortex-dict-v0.8.0) - 2024-09-05

### Other
- Add `scalar_at_unchecked` ([#666](https://github.com/spiraldb/vortex/pull/666))
- Clippy deny `unwrap` & `panic` in functions that return `Result` ([#578](https://github.com/spiraldb/vortex/pull/578))

## `vortex-datetime-parts` - [0.8.0](https://github.com/spiraldb/vortex/compare/vortex-datetime-parts-v0.7.0...vortex-datetime-parts-v0.8.0) - 2024-09-05

### Other
- Add `scalar_at_unchecked` ([#666](https://github.com/spiraldb/vortex/pull/666))
- Move expression filters out of datafusion ([#638](https://github.com/spiraldb/vortex/pull/638))
- Clippy deny `unwrap` & `panic` in functions that return `Result` ([#578](https://github.com/spiraldb/vortex/pull/578))

## `vortex-sampling-compressor` - [0.8.0](https://github.com/spiraldb/vortex/compare/vortex-sampling-compressor-v0.7.0...vortex-sampling-compressor-v0.8.0) - 2024-09-05

### Other
- Add fuzzing for Take and SearchSorted functions ([#724](https://github.com/spiraldb/vortex/pull/724))
- FSSTCompressor ([#664](https://github.com/spiraldb/vortex/pull/664))
- Move expression filters out of datafusion ([#638](https://github.com/spiraldb/vortex/pull/638))
- FoR compressor handles nullable arrays ([#617](https://github.com/spiraldb/vortex/pull/617))
- Use then vs then_some for values that have to be lazy ([#599](https://github.com/spiraldb/vortex/pull/599))
- Clippy deny `unwrap` & `panic` in functions that return `Result` ([#578](https://github.com/spiraldb/vortex/pull/578))

## `vortex-fastlanes` - [0.8.0](https://github.com/spiraldb/vortex/compare/vortex-fastlanes-v0.7.0...vortex-fastlanes-v0.8.0) - 2024-09-05

### Other
- Fix search_sorted for FoRArray, BitPacked and PrimitiveArray ([#732](https://github.com/spiraldb/vortex/pull/732))
- Fix issues discovered by fuzzer ([#707](https://github.com/spiraldb/vortex/pull/707))
- FoR decompression happens in place if possible ([#699](https://github.com/spiraldb/vortex/pull/699))
- Remove length of patches from ALP and BitPacked array ([#688](https://github.com/spiraldb/vortex/pull/688))
- Add `scalar_at_unchecked` ([#666](https://github.com/spiraldb/vortex/pull/666))
- Bitpacking validity is checked first when getting a scalar ([#630](https://github.com/spiraldb/vortex/pull/630))
- Fix FoRArray decompression with non 0 shift ([#625](https://github.com/spiraldb/vortex/pull/625))
- FoR compressor handles nullable arrays ([#617](https://github.com/spiraldb/vortex/pull/617))
- Basic fuzzing for compression and slicing functions ([#600](https://github.com/spiraldb/vortex/pull/600))
- Use then vs then_some for values that have to be lazy ([#599](https://github.com/spiraldb/vortex/pull/599))
- Clippy deny `unwrap` & `panic` in functions that return `Result` ([#578](https://github.com/spiraldb/vortex/pull/578))

## `vortex-serde` - [0.8.0](https://github.com/spiraldb/vortex/compare/vortex-serde-v0.7.0...vortex-serde-v0.8.0) - 2024-09-05

### Other
- Upgrade rust nightly toolchain & MSRV ([#745](https://github.com/spiraldb/vortex/pull/745))
- directly implement VortexReadAt on File ([#738](https://github.com/spiraldb/vortex/pull/738))
- Teach schema dtype() and into_dtype() ([#714](https://github.com/spiraldb/vortex/pull/714))
- Add method for converting VortexExpr into equivalent pruning expression ([#701](https://github.com/spiraldb/vortex/pull/701))
- Primitive and Bool array roundtrip serialization ([#704](https://github.com/spiraldb/vortex/pull/704))
- Move flatbuffer schema project functions around ([#680](https://github.com/spiraldb/vortex/pull/680))
- Deduplicate filter projection with result projection ([#668](https://github.com/spiraldb/vortex/pull/668))
- Push filter schema manipulation into layout reader and reuse ipc message writer in file writer ([#651](https://github.com/spiraldb/vortex/pull/651))
- Bring back ability to convert ArrayView to ArrayData ([#626](https://github.com/spiraldb/vortex/pull/626))
- Move expression filters out of datafusion ([#638](https://github.com/spiraldb/vortex/pull/638))
- Assert expected row count in tpch_benchmark binary ([#620](https://github.com/spiraldb/vortex/pull/620))
- ArrayBufferReader assumes flatbuffers are validated ([#610](https://github.com/spiraldb/vortex/pull/610))
- Clippy deny `unwrap` & `panic` in functions that return `Result` ([#578](https://github.com/spiraldb/vortex/pull/578))

## `vortex-expr` - [0.8.0](https://github.com/spiraldb/vortex/compare/vortex-expr-v0.7.0...vortex-expr-v0.8.0) - 2024-09-05

### Other
- Add method for converting VortexExpr into equivalent pruning expression ([#701](https://github.com/spiraldb/vortex/pull/701))
- Fix Operator::swap ([#672](https://github.com/spiraldb/vortex/pull/672))
- Push filter schema manipulation into layout reader and reuse ipc message writer in file writer ([#651](https://github.com/spiraldb/vortex/pull/651))
- Support Temporal scalar conversion between datafusion and arrow ([#657](https://github.com/spiraldb/vortex/pull/657))
- cargo-sort related maintenance  ([#650](https://github.com/spiraldb/vortex/pull/650))
- Move expression filters out of datafusion ([#638](https://github.com/spiraldb/vortex/pull/638))
- Generate more structured inputs for fuzzing ([#635](https://github.com/spiraldb/vortex/pull/635))
- Fix bug where operations were negated instead of swapped when lhs/rhs were flipped ([#619](https://github.com/spiraldb/vortex/pull/619))
- Clippy deny `unwrap` & `panic` in functions that return `Result` ([#578](https://github.com/spiraldb/vortex/pull/578))

## `vortex-datafusion` - [0.8.0](https://github.com/spiraldb/vortex/compare/vortex-datafusion-v0.7.0...vortex-datafusion-v0.8.0) - 2024-09-05

### Other
- Push filter schema manipulation into layout reader and reuse ipc message writer in file writer ([#651](https://github.com/spiraldb/vortex/pull/651))
- Support Temporal scalar conversion between datafusion and arrow ([#657](https://github.com/spiraldb/vortex/pull/657))
- Move expression filters out of datafusion ([#638](https://github.com/spiraldb/vortex/pull/638))
- Remove dead code after disk and in memory table provider unification ([#633](https://github.com/spiraldb/vortex/pull/633))
- Unify expression evaluation for both Table Providers ([#632](https://github.com/spiraldb/vortex/pull/632))
- `Exact` support for more expressions  ([#628](https://github.com/spiraldb/vortex/pull/628))
- Assert expected row count in tpch_benchmark binary ([#620](https://github.com/spiraldb/vortex/pull/620))
- Fix a bug in vortex in-memory predicate pushdown ([#618](https://github.com/spiraldb/vortex/pull/618))
- Nulls as false respects original array nullability ([#606](https://github.com/spiraldb/vortex/pull/606))
- Fix a bug in the handling the conversion physical expression ([#601](https://github.com/spiraldb/vortex/pull/601))
- Vortex physical expressions support for on-disk data ([#581](https://github.com/spiraldb/vortex/pull/581))
- *(deps)* update datafusion to v41 (major) ([#595](https://github.com/spiraldb/vortex/pull/595))
- Clippy deny `unwrap` & `panic` in functions that return `Result` ([#578](https://github.com/spiraldb/vortex/pull/578))

## `vortex-scalar` - [0.8.0](https://github.com/spiraldb/vortex/compare/vortex-scalar-v0.7.0...vortex-scalar-v0.8.0) - 2024-09-05

### Other
- Fix search_sorted for FoRArray, BitPacked and PrimitiveArray ([#732](https://github.com/spiraldb/vortex/pull/732))
- Add fuzzing for Take and SearchSorted functions ([#724](https://github.com/spiraldb/vortex/pull/724))
- impl Display for Time, Date, and Timestamp ([#683](https://github.com/spiraldb/vortex/pull/683))
- impl Display for StructValue ([#682](https://github.com/spiraldb/vortex/pull/682))
- impl Display for Utf8Scalar and BinaryScalar ([#678](https://github.com/spiraldb/vortex/pull/678))
- Add doc to pvalue typed accessor methods ([#658](https://github.com/spiraldb/vortex/pull/658))
- Support Temporal scalar conversion between datafusion and arrow ([#657](https://github.com/spiraldb/vortex/pull/657))
- Move expression filters out of datafusion ([#638](https://github.com/spiraldb/vortex/pull/638))
- Clippy deny `unwrap` & `panic` in functions that return `Result` ([#578](https://github.com/spiraldb/vortex/pull/578))

## `vortex-proto` - [0.8.0](https://github.com/spiraldb/vortex/compare/vortex-proto-v0.7.0...vortex-proto-v0.8.0) - 2024-09-05

### Other
- Push filter schema manipulation into layout reader and reuse ipc message writer in file writer ([#651](https://github.com/spiraldb/vortex/pull/651))
- cargo-sort related maintenance  ([#650](https://github.com/spiraldb/vortex/pull/650))

## `vortex-flatbuffers` - [0.8.0](https://github.com/spiraldb/vortex/compare/vortex-flatbuffers-v0.7.0...vortex-flatbuffers-v0.8.0) - 2024-09-05

### Other
- Push filter schema manipulation into layout reader and reuse ipc message writer in file writer ([#651](https://github.com/spiraldb/vortex/pull/651))
- Clippy deny `unwrap` & `panic` in functions that return `Result` ([#578](https://github.com/spiraldb/vortex/pull/578))

## `vortex-error` - [0.8.0](https://github.com/spiraldb/vortex/compare/vortex-error-v0.7.0...vortex-error-v0.8.0) - 2024-09-05

### Other
- impl Display for Time, Date, and Timestamp ([#683](https://github.com/spiraldb/vortex/pull/683))

## `vortex-dtype` - [0.8.0](https://github.com/spiraldb/vortex/compare/vortex-dtype-v0.7.0...vortex-dtype-v0.8.0) - 2024-09-05

### Other
- Move flatbuffer schema project functions around ([#680](https://github.com/spiraldb/vortex/pull/680))
- DType serde project requires flatbuffers feature ([#679](https://github.com/spiraldb/vortex/pull/679))
- Push filter schema manipulation into layout reader and reuse ipc message writer in file writer ([#651](https://github.com/spiraldb/vortex/pull/651))
- Support Temporal scalar conversion between datafusion and arrow ([#657](https://github.com/spiraldb/vortex/pull/657))
- Get beyond the immediate fuzzing failures ([#611](https://github.com/spiraldb/vortex/pull/611))
- Clippy deny `unwrap` & `panic` in functions that return `Result` ([#578](https://github.com/spiraldb/vortex/pull/578))

## `vortex-datetime-dtype` - [0.8.0](https://github.com/spiraldb/vortex/compare/vortex-datetime-dtype-v0.7.0...vortex-datetime-dtype-v0.8.0) - 2024-09-05

### Other
- impl Display for Time, Date, and Timestamp ([#683](https://github.com/spiraldb/vortex/pull/683))
- Support Temporal scalar conversion between datafusion and arrow ([#657](https://github.com/spiraldb/vortex/pull/657))
- cargo-sort related maintenance  ([#650](https://github.com/spiraldb/vortex/pull/650))

## `vortex-buffer` - [0.8.0](https://github.com/spiraldb/vortex/compare/vortex-buffer-v0.7.0...vortex-buffer-v0.8.0) - 2024-09-05

### Other
- Primitive Iterator API ([#689](https://github.com/spiraldb/vortex/pull/689))
- Support Temporal scalar conversion between datafusion and arrow ([#657](https://github.com/spiraldb/vortex/pull/657))
- Clippy deny `unwrap` & `panic` in functions that return `Result` ([#578](https://github.com/spiraldb/vortex/pull/578))

## `vortex-array` - [0.8.0](https://github.com/spiraldb/vortex/compare/vortex-array-v0.7.0...vortex-array-v0.8.0) - 2024-09-05

### Other
- PyVortex ([#729](https://github.com/spiraldb/vortex/pull/729))
- Upgrade rust nightly toolchain & MSRV ([#745](https://github.com/spiraldb/vortex/pull/745))
- Fix search_sorted for FoRArray, BitPacked and PrimitiveArray ([#732](https://github.com/spiraldb/vortex/pull/732))
- Unary and Binary functions trait ([#726](https://github.com/spiraldb/vortex/pull/726))
- Add fuzzing for Take and SearchSorted functions ([#724](https://github.com/spiraldb/vortex/pull/724))
- Fix issues discovered by fuzzer ([#707](https://github.com/spiraldb/vortex/pull/707))
- Primitive Iterator API ([#689](https://github.com/spiraldb/vortex/pull/689))
- StructArray roundtrips arrow conversion ([#705](https://github.com/spiraldb/vortex/pull/705))
- Primitive and Bool array roundtrip serialization ([#704](https://github.com/spiraldb/vortex/pull/704))
- Fix pack_varbin ([#674](https://github.com/spiraldb/vortex/pull/674))
- Slightly faster iter of `LogicalValidity` to `Validity` ([#673](https://github.com/spiraldb/vortex/pull/673))
- Fix Operator::swap ([#672](https://github.com/spiraldb/vortex/pull/672))
- Add `scalar_at_unchecked` ([#666](https://github.com/spiraldb/vortex/pull/666))
- Push filter schema manipulation into layout reader and reuse ipc message writer in file writer ([#651](https://github.com/spiraldb/vortex/pull/651))
- Faster canonicalization ([#663](https://github.com/spiraldb/vortex/pull/663))
- Fix slicing of ChunkedArray if end index == array length ([#660](https://github.com/spiraldb/vortex/pull/660))
- Implement LogicalValidity for ChunkedArray ([#661](https://github.com/spiraldb/vortex/pull/661))
- Support Temporal scalar conversion between datafusion and arrow ([#657](https://github.com/spiraldb/vortex/pull/657))
- Bring back ability to convert ArrayView to ArrayData ([#626](https://github.com/spiraldb/vortex/pull/626))
- Improve Primitive array comparison ([#644](https://github.com/spiraldb/vortex/pull/644))
- Let chunked arrays use specialized `compare` implementations ([#640](https://github.com/spiraldb/vortex/pull/640))
- Expand fuzzing space ([#639](https://github.com/spiraldb/vortex/pull/639))
- Move expression filters out of datafusion ([#638](https://github.com/spiraldb/vortex/pull/638))
- `Exact` support for more expressions  ([#628](https://github.com/spiraldb/vortex/pull/628))
- Fix bug where operations were negated instead of swapped when lhs/rhs were flipped ([#619](https://github.com/spiraldb/vortex/pull/619))
- Get beyond the immediate fuzzing failures ([#611](https://github.com/spiraldb/vortex/pull/611))
- Basic fuzzing for compression and slicing functions ([#600](https://github.com/spiraldb/vortex/pull/600))
- Vortex physical expressions support for on-disk data ([#581](https://github.com/spiraldb/vortex/pull/581))
- Use then vs then_some for values that have to be lazy ([#599](https://github.com/spiraldb/vortex/pull/599))
- Child assert includes index and encoding id ([#598](https://github.com/spiraldb/vortex/pull/598))
- Clippy deny `unwrap` & `panic` in functions that return `Result` ([#578](https://github.com/spiraldb/vortex/pull/578))
- Add tests to sparse array slicing + extra length validation ([#590](https://github.com/spiraldb/vortex/pull/590))

## `vortex-alp` - [0.8.0](https://github.com/spiraldb/vortex/compare/vortex-alp-v0.7.0...vortex-alp-v0.8.0) - 2024-09-05

### Other
- ALP compressor is better at roundtripping values ([#736](https://github.com/spiraldb/vortex/pull/736))
- Fix issues discovered by fuzzer ([#707](https://github.com/spiraldb/vortex/pull/707))
- Primitive Iterator API ([#689](https://github.com/spiraldb/vortex/pull/689))
- ALP decompress in place ([#700](https://github.com/spiraldb/vortex/pull/700))
- Remove length of patches from ALP and BitPacked array ([#688](https://github.com/spiraldb/vortex/pull/688))
- Add `scalar_at_unchecked` ([#666](https://github.com/spiraldb/vortex/pull/666))
- Fix alp null handling ([#623](https://github.com/spiraldb/vortex/pull/623))
- Clippy deny `unwrap` & `panic` in functions that return `Result` ([#578](https://github.com/spiraldb/vortex/pull/578))

## `vortex-array` - [0.7.0](https://github.com/spiraldb/vortex/compare/vortex-array-v0.6.0...vortex-array-v0.7.0) - 2024-08-09

### Other
- Fix REE slicing with end being equal to array len ([#586](https://github.com/spiraldb/vortex/pull/586))
- Fix vortex compressed benchmarks ([#577](https://github.com/spiraldb/vortex/pull/577))

## `vortex-serde` - [0.6.0](https://github.com/spiraldb/vortex/compare/vortex-serde-v0.5.0...vortex-serde-v0.6.0) - 2024-08-09

### Other
- Only deserialize the required dtypes by projection from the footer ([#569](https://github.com/spiraldb/vortex/pull/569))

## `vortex-buffer` - [0.6.0](https://github.com/spiraldb/vortex/compare/vortex-buffer-v0.5.0...vortex-buffer-v0.6.0) - 2024-08-09

### Other
- enforce docstrings in vortex-buffer ([#575](https://github.com/spiraldb/vortex/pull/575))

## `vortex-array` - [0.6.0](https://github.com/spiraldb/vortex/compare/vortex-array-v0.5.0...vortex-array-v0.6.0) - 2024-08-09

### Other
- Remove to_present_null_buffer from LogicalValidity ([#579](https://github.com/spiraldb/vortex/pull/579))

## `vortex-runend-bool` - [0.5.0](https://github.com/spiraldb/vortex/compare/vortex-runend-bool-v0.4.12...vortex-runend-bool-v0.5.0) - 2024-08-08

### Other
- Re-import array types ([#559](https://github.com/spiraldb/vortex/pull/559))
- Use versioned workspace deps ([#551](https://github.com/spiraldb/vortex/pull/551))
- Run cargo-sort on the whole workspace ([#550](https://github.com/spiraldb/vortex/pull/550))
- Added bool iterators index and slice and filtering across some array types ([#505](https://github.com/spiraldb/vortex/pull/505))
- Fix out ouf bounds when taking from run end arrays ([#501](https://github.com/spiraldb/vortex/pull/501))
- Change codes for runendbool so it doesn't conflict with datetimeparts ([#498](https://github.com/spiraldb/vortex/pull/498))
- Make unary functions nicer to `use` ([#493](https://github.com/spiraldb/vortex/pull/493))
- use FQDNs in impl_encoding macro ([#490](https://github.com/spiraldb/vortex/pull/490))

## `vortex-bytebool` - [0.5.0](https://github.com/spiraldb/vortex/compare/vortex-bytebool-v0.4.12...vortex-bytebool-v0.5.0) - 2024-08-08

### Other
- Refactor specialized conversion traits into `From` and `Into` ([#560](https://github.com/spiraldb/vortex/pull/560))
- Re-import array types ([#559](https://github.com/spiraldb/vortex/pull/559))
- Use versioned workspace deps ([#551](https://github.com/spiraldb/vortex/pull/551))
- Run cargo-sort on the whole workspace ([#550](https://github.com/spiraldb/vortex/pull/550))
- Simpler ByteBool slice ([#527](https://github.com/spiraldb/vortex/pull/527))
- Added bool iterators index and slice and filtering across some array types ([#505](https://github.com/spiraldb/vortex/pull/505))
- Make unary functions nicer to `use` ([#493](https://github.com/spiraldb/vortex/pull/493))
- use FQDNs in impl_encoding macro ([#490](https://github.com/spiraldb/vortex/pull/490))

## `vortex-serde` - [0.5.0](https://github.com/spiraldb/vortex/compare/vortex-serde-v0.4.12...vortex-serde-v0.5.0) - 2024-08-08

### Other
- Push column projections down to the file IO layer ([#568](https://github.com/spiraldb/vortex/pull/568))
- Lots of things to try and get publishing working ([#557](https://github.com/spiraldb/vortex/pull/557))
- Support dynamic layouts with io batching ([#533](https://github.com/spiraldb/vortex/pull/533))
- Re-import array types ([#559](https://github.com/spiraldb/vortex/pull/559))
- File-based table provider for Datafusion ([#546](https://github.com/spiraldb/vortex/pull/546))
- build-vortex -> vortex-build ([#552](https://github.com/spiraldb/vortex/pull/552))
- Use versioned workspace deps ([#551](https://github.com/spiraldb/vortex/pull/551))
- Add identity projection to the file reader ([#532](https://github.com/spiraldb/vortex/pull/532))
- Support reading unaligned chunks across columns ([#531](https://github.com/spiraldb/vortex/pull/531))
- Initial version of simple FileReader/Writer ([#516](https://github.com/spiraldb/vortex/pull/516))

## `vortex-datafusion` - [0.5.0](https://github.com/spiraldb/vortex/compare/vortex-datafusion-v0.4.12...vortex-datafusion-v0.5.0) - 2024-08-08

### Other
- Hook on-disk vortex files into benchmarking ([#565](https://github.com/spiraldb/vortex/pull/565))

## `vortex-error` - [0.5.0](https://github.com/spiraldb/vortex/compare/vortex-error-v0.4.12...vortex-error-v0.5.0) - 2024-08-08

### Other
- Lots of things to try and get publishing working ([#557](https://github.com/spiraldb/vortex/pull/557))

## `vortex-array` - [0.5.0](https://github.com/spiraldb/vortex/compare/vortex-array-v0.4.12...vortex-array-v0.5.0) - 2024-08-08

### Other
- Lots of things to try and get publishing working ([#557](https://github.com/spiraldb/vortex/pull/557))
- Hook on-disk vortex files into benchmarking ([#565](https://github.com/spiraldb/vortex/pull/565))

## `vortex-runend-bool` - [0.2.0](https://github.com/spiraldb/vortex/releases/tag/vortex-runend-bool-v0.2.0) - 2024-08-05

### Other
- Use versioned workspace deps ([#551](https://github.com/spiraldb/vortex/pull/551))
- Run cargo-sort on the whole workspace ([#550](https://github.com/spiraldb/vortex/pull/550))
- Added bool iterators index and slice and filtering across some array types ([#505](https://github.com/spiraldb/vortex/pull/505))
- Fix out ouf bounds when taking from run end arrays ([#501](https://github.com/spiraldb/vortex/pull/501))
- Change codes for runendbool so it doesn't conflict with datetimeparts ([#498](https://github.com/spiraldb/vortex/pull/498))
- Make unary functions nicer to `use` ([#493](https://github.com/spiraldb/vortex/pull/493))
- use FQDNs in impl_encoding macro ([#490](https://github.com/spiraldb/vortex/pull/490))

## `vortex-bytebool` - [0.2.0](https://github.com/spiraldb/vortex/releases/tag/vortex-bytebool-v0.2.0) - 2024-08-05

### Other
- Use versioned workspace deps ([#551](https://github.com/spiraldb/vortex/pull/551))
- Run cargo-sort on the whole workspace ([#550](https://github.com/spiraldb/vortex/pull/550))
- Simpler ByteBool slice ([#527](https://github.com/spiraldb/vortex/pull/527))
- Added bool iterators index and slice and filtering across some array types ([#505](https://github.com/spiraldb/vortex/pull/505))
- Make unary functions nicer to `use` ([#493](https://github.com/spiraldb/vortex/pull/493))
- use FQDNs in impl_encoding macro ([#490](https://github.com/spiraldb/vortex/pull/490))

## `vortex-serde` - [0.2.0](https://github.com/spiraldb/vortex/releases/tag/vortex-serde-v0.2.0) - 2024-08-05

### Other
- build-vortex -> vortex-build ([#552](https://github.com/spiraldb/vortex/pull/552))
- Use versioned workspace deps ([#551](https://github.com/spiraldb/vortex/pull/551))
- Add identity projection to the file reader ([#532](https://github.com/spiraldb/vortex/pull/532))
- Support reading unaligned chunks across columns ([#531](https://github.com/spiraldb/vortex/pull/531))
- Initial version of simple FileReader/Writer ([#516](https://github.com/spiraldb/vortex/pull/516))

## `vortex-sampling-compressor` - [0.2.0](https://github.com/spiraldb/vortex/releases/tag/vortex-sampling-compressor-v0.2.0) - 2024-08-05

### Fixed
- fix UB and run tests with miri ([#517](https://github.com/spiraldb/vortex/pull/517))

### Other
- Use versioned workspace deps ([#551](https://github.com/spiraldb/vortex/pull/551))
- Run cargo-sort on the whole workspace ([#550](https://github.com/spiraldb/vortex/pull/550))
- FoR will compress signed array when min == 0 now ([#511](https://github.com/spiraldb/vortex/pull/511))
- Smoketest for SamplingCompressor, fix bug in varbin stats ([#510](https://github.com/spiraldb/vortex/pull/510))
- Make unary functions nicer to `use` ([#493](https://github.com/spiraldb/vortex/pull/493))
- Remove LocalDateTimeArray, introduce TemporalArray ([#480](https://github.com/spiraldb/vortex/pull/480))
- DType variant traits ([#473](https://github.com/spiraldb/vortex/pull/473))
- Slightly nicer use statements for compute functions ([#466](https://github.com/spiraldb/vortex/pull/466))
- Array Length ([#445](https://github.com/spiraldb/vortex/pull/445))
- Split compression from encodings ([#422](https://github.com/spiraldb/vortex/pull/422))

## `vortex-runend` - [0.2.0](https://github.com/spiraldb/vortex/releases/tag/vortex-runend-v0.2.0) - 2024-08-05

### Other
- Use versioned workspace deps ([#551](https://github.com/spiraldb/vortex/pull/551))
- Run cargo-sort on the whole workspace ([#550](https://github.com/spiraldb/vortex/pull/550))
- Fix out ouf bounds when taking from run end arrays ([#501](https://github.com/spiraldb/vortex/pull/501))
- Make unary functions nicer to `use` ([#493](https://github.com/spiraldb/vortex/pull/493))
- use FQDNs in impl_encoding macro ([#490](https://github.com/spiraldb/vortex/pull/490))
- demo module level imports granularity ([#485](https://github.com/spiraldb/vortex/pull/485))
- DType variant traits ([#473](https://github.com/spiraldb/vortex/pull/473))
- Slightly nicer use statements for compute functions ([#466](https://github.com/spiraldb/vortex/pull/466))
- Use shorthand canonicalize methods ([#460](https://github.com/spiraldb/vortex/pull/460))
- Array Length ([#445](https://github.com/spiraldb/vortex/pull/445))
- Remove ViewContext and assign stable ids to encodings ([#433](https://github.com/spiraldb/vortex/pull/433))
- Split compression from encodings ([#422](https://github.com/spiraldb/vortex/pull/422))
- Rename flatten -> canonicalize + bugfix + a secret third thing ([#402](https://github.com/spiraldb/vortex/pull/402))
- ArrayData can contain child Arrays instead of just ArrayData ([#391](https://github.com/spiraldb/vortex/pull/391))
- Rename typed_data to maybe_null_slice ([#386](https://github.com/spiraldb/vortex/pull/386))
- Move encodings into directory ([#379](https://github.com/spiraldb/vortex/pull/379))

## `vortex-datafusion` - [0.2.0](https://github.com/spiraldb/vortex/releases/tag/vortex-datafusion-v0.2.0) - 2024-08-05

### Fixed
- fix UB and run tests with miri ([#517](https://github.com/spiraldb/vortex/pull/517))

### Other
- Use versioned workspace deps ([#551](https://github.com/spiraldb/vortex/pull/551))
- Run cargo-sort on the whole workspace ([#550](https://github.com/spiraldb/vortex/pull/550))
- Follow up for 537 ([#538](https://github.com/spiraldb/vortex/pull/538))
- Rename the pushdown config into a positive boolean value ([#537](https://github.com/spiraldb/vortex/pull/537))
- Ignore tests that miri can't run ([#514](https://github.com/spiraldb/vortex/pull/514))
- Add and/or compute functions ([#481](https://github.com/spiraldb/vortex/pull/481))
- demo module level imports granularity ([#485](https://github.com/spiraldb/vortex/pull/485))
- Remove LocalDateTimeArray, introduce TemporalArray ([#480](https://github.com/spiraldb/vortex/pull/480))
- Expand pushdown support with more comparison and logical operations ([#478](https://github.com/spiraldb/vortex/pull/478))
- Debug compilation caching ([#475](https://github.com/spiraldb/vortex/pull/475))
- Basic predicate pushdown support for Datafusion ([#472](https://github.com/spiraldb/vortex/pull/472))
- Slightly nicer use statements for compute functions ([#466](https://github.com/spiraldb/vortex/pull/466))
- Run tpch_benchmark queries single-threaded in rayon pool ([#463](https://github.com/spiraldb/vortex/pull/463))
- Update datafusion to v40 (major) ([#455](https://github.com/spiraldb/vortex/pull/455))
- Make into_arrow truly zero-copy, rewrite DataFusion operators ([#451](https://github.com/spiraldb/vortex/pull/451))
- Setup TPC-H benchmark infra ([#444](https://github.com/spiraldb/vortex/pull/444))
- v0 Datafusion with late materialization ([#414](https://github.com/spiraldb/vortex/pull/414))
- Rename flatten -> canonicalize + bugfix + a secret third thing ([#402](https://github.com/spiraldb/vortex/pull/402))
- DataFusion TableProvider for memory arrays ([#384](https://github.com/spiraldb/vortex/pull/384))

## `vortex-scalar` - [0.2.0](https://github.com/spiraldb/vortex/releases/tag/vortex-scalar-v0.2.0) - 2024-08-05

### Other
- build-vortex -> vortex-build ([#552](https://github.com/spiraldb/vortex/pull/552))
- Use versioned workspace deps ([#551](https://github.com/spiraldb/vortex/pull/551))
- Initial version of simple FileReader/Writer ([#516](https://github.com/spiraldb/vortex/pull/516))
- More specialized compare functions ([#488](https://github.com/spiraldb/vortex/pull/488))
- demo module level imports granularity ([#485](https://github.com/spiraldb/vortex/pull/485))
- Basic predicate pushdown support for Datafusion ([#472](https://github.com/spiraldb/vortex/pull/472))
- DType variant traits ([#473](https://github.com/spiraldb/vortex/pull/473))
- FoR encoding doesn't panic if array min > i64::MAX ([#406](https://github.com/spiraldb/vortex/pull/406))
- Add `ByteBoolArray` type and fixe a bug in `BoolArray` ([#383](https://github.com/spiraldb/vortex/pull/383))
- FoR array holds encoded values as unsinged ([#401](https://github.com/spiraldb/vortex/pull/401))
- DataFusion expr conversion ([#349](https://github.com/spiraldb/vortex/pull/349))
- Fix FOR bug, also fix bench to compile ([#341](https://github.com/spiraldb/vortex/pull/341))
- Implement StructValue proto serde without google.protobuf.Value ([#343](https://github.com/spiraldb/vortex/pull/343))
- Random access benchmarks are runnable again ([#330](https://github.com/spiraldb/vortex/pull/330))
- define ScalarValue in VortexScalar protobuf ([#323](https://github.com/spiraldb/vortex/pull/323))
- Proto Refactor ([#325](https://github.com/spiraldb/vortex/pull/325))
- IPC Bench ([#319](https://github.com/spiraldb/vortex/pull/319))
- Static ArrayView ([#310](https://github.com/spiraldb/vortex/pull/310))
- StatsView2 ([#305](https://github.com/spiraldb/vortex/pull/305))
- Add ScalarView ([#301](https://github.com/spiraldb/vortex/pull/301))
- DType Serialization ([#298](https://github.com/spiraldb/vortex/pull/298))
- OwnedBuffer ([#300](https://github.com/spiraldb/vortex/pull/300))
- Add validity to Struct arrays ([#289](https://github.com/spiraldb/vortex/pull/289))
- Extension Array ([#287](https://github.com/spiraldb/vortex/pull/287))
- Remove composite and decimal ([#285](https://github.com/spiraldb/vortex/pull/285))
- Add convenience stats retrieval functions and avoid needless copy when unwrapping stat value ([#279](https://github.com/spiraldb/vortex/pull/279))
- Scalar subtraction ([#270](https://github.com/spiraldb/vortex/pull/270))
- Add ExtDType ([#281](https://github.com/spiraldb/vortex/pull/281))
- Refactor for DType::Primitive ([#276](https://github.com/spiraldb/vortex/pull/276))
- Extract a vortex-scalar crate ([#275](https://github.com/spiraldb/vortex/pull/275))

## `vortex-runend-bool` - [0.2.0](https://github.com/spiraldb/vortex/compare/vortex-runend-bool-v0.1.0...vortex-runend-bool-v0.2.0) - 2024-08-05

### Other
- Use versioned workspace deps ([#551](https://github.com/spiraldb/vortex/pull/551))
- Run cargo-sort on the whole workspace ([#550](https://github.com/spiraldb/vortex/pull/550))
- Added bool iterators index and slice and filtering across some array types ([#505](https://github.com/spiraldb/vortex/pull/505))
- Fix out ouf bounds when taking from run end arrays ([#501](https://github.com/spiraldb/vortex/pull/501))
- Change codes for runendbool so it doesn't conflict with datetimeparts ([#498](https://github.com/spiraldb/vortex/pull/498))
- Make unary functions nicer to `use` ([#493](https://github.com/spiraldb/vortex/pull/493))
- use FQDNs in impl_encoding macro ([#490](https://github.com/spiraldb/vortex/pull/490))

## `vortex-bytebool` - [0.2.0](https://github.com/spiraldb/vortex/compare/vortex-bytebool-v0.1.0...vortex-bytebool-v0.2.0) - 2024-08-05

### Other
- Use versioned workspace deps ([#551](https://github.com/spiraldb/vortex/pull/551))
- Run cargo-sort on the whole workspace ([#550](https://github.com/spiraldb/vortex/pull/550))
- Simpler ByteBool slice ([#527](https://github.com/spiraldb/vortex/pull/527))
- Added bool iterators index and slice and filtering across some array types ([#505](https://github.com/spiraldb/vortex/pull/505))
- Make unary functions nicer to `use` ([#493](https://github.com/spiraldb/vortex/pull/493))
- use FQDNs in impl_encoding macro ([#490](https://github.com/spiraldb/vortex/pull/490))

## `vortex-serde` - [0.2.0](https://github.com/spiraldb/vortex/compare/vortex-serde-v0.1.0...vortex-serde-v0.2.0) - 2024-08-05

### Other
- build-vortex -> vortex-build ([#552](https://github.com/spiraldb/vortex/pull/552))
- Use versioned workspace deps ([#551](https://github.com/spiraldb/vortex/pull/551))
- Add identity projection to the file reader ([#532](https://github.com/spiraldb/vortex/pull/532))
- Support reading unaligned chunks across columns ([#531](https://github.com/spiraldb/vortex/pull/531))
- Initial version of simple FileReader/Writer ([#516](https://github.com/spiraldb/vortex/pull/516))

## `vortex-zigzag` - [0.2.0](https://github.com/spiraldb/vortex/compare/vortex-zigzag-v0.1.0...vortex-zigzag-v0.2.0) - 2024-08-05

### Other
- Use versioned workspace deps ([#551](https://github.com/spiraldb/vortex/pull/551))
- Run cargo-sort on the whole workspace ([#550](https://github.com/spiraldb/vortex/pull/550))
- Make unary functions nicer to `use` ([#493](https://github.com/spiraldb/vortex/pull/493))
- use FQDNs in impl_encoding macro ([#490](https://github.com/spiraldb/vortex/pull/490))
- DType variant traits ([#473](https://github.com/spiraldb/vortex/pull/473))
- Slightly nicer use statements for compute functions ([#466](https://github.com/spiraldb/vortex/pull/466))
- Array Length ([#445](https://github.com/spiraldb/vortex/pull/445))
- Remove ViewContext and assign stable ids to encodings ([#433](https://github.com/spiraldb/vortex/pull/433))
- Split compression from encodings ([#422](https://github.com/spiraldb/vortex/pull/422))
- Rename flatten -> canonicalize + bugfix + a secret third thing ([#402](https://github.com/spiraldb/vortex/pull/402))
- ArrayData can contain child Arrays instead of just ArrayData ([#391](https://github.com/spiraldb/vortex/pull/391))
- Rename typed_data to maybe_null_slice ([#386](https://github.com/spiraldb/vortex/pull/386))
- Fastlanez -> Fastlanes ([#381](https://github.com/spiraldb/vortex/pull/381))
- Move encodings into directory ([#379](https://github.com/spiraldb/vortex/pull/379))

## `vortex-sampling-compressor` - [0.2.0](https://github.com/spiraldb/vortex/compare/vortex-sampling-compressor-v0.1.0...vortex-sampling-compressor-v0.2.0) - 2024-08-05

### Fixed
- fix UB and run tests with miri ([#517](https://github.com/spiraldb/vortex/pull/517))

### Other
- Use versioned workspace deps ([#551](https://github.com/spiraldb/vortex/pull/551))
- Run cargo-sort on the whole workspace ([#550](https://github.com/spiraldb/vortex/pull/550))
- FoR will compress signed array when min == 0 now ([#511](https://github.com/spiraldb/vortex/pull/511))
- Smoketest for SamplingCompressor, fix bug in varbin stats ([#510](https://github.com/spiraldb/vortex/pull/510))
- Make unary functions nicer to `use` ([#493](https://github.com/spiraldb/vortex/pull/493))
- Remove LocalDateTimeArray, introduce TemporalArray ([#480](https://github.com/spiraldb/vortex/pull/480))
- DType variant traits ([#473](https://github.com/spiraldb/vortex/pull/473))
- Slightly nicer use statements for compute functions ([#466](https://github.com/spiraldb/vortex/pull/466))
- Array Length ([#445](https://github.com/spiraldb/vortex/pull/445))
- Split compression from encodings ([#422](https://github.com/spiraldb/vortex/pull/422))

## `vortex-runend` - [0.2.0](https://github.com/spiraldb/vortex/compare/vortex-runend-v0.1.0...vortex-runend-v0.2.0) - 2024-08-05

### Other
- Use versioned workspace deps ([#551](https://github.com/spiraldb/vortex/pull/551))
- Run cargo-sort on the whole workspace ([#550](https://github.com/spiraldb/vortex/pull/550))
- Fix out ouf bounds when taking from run end arrays ([#501](https://github.com/spiraldb/vortex/pull/501))
- Make unary functions nicer to `use` ([#493](https://github.com/spiraldb/vortex/pull/493))
- use FQDNs in impl_encoding macro ([#490](https://github.com/spiraldb/vortex/pull/490))
- demo module level imports granularity ([#485](https://github.com/spiraldb/vortex/pull/485))
- DType variant traits ([#473](https://github.com/spiraldb/vortex/pull/473))
- Slightly nicer use statements for compute functions ([#466](https://github.com/spiraldb/vortex/pull/466))
- Use shorthand canonicalize methods ([#460](https://github.com/spiraldb/vortex/pull/460))
- Array Length ([#445](https://github.com/spiraldb/vortex/pull/445))
- Remove ViewContext and assign stable ids to encodings ([#433](https://github.com/spiraldb/vortex/pull/433))
- Split compression from encodings ([#422](https://github.com/spiraldb/vortex/pull/422))
- Rename flatten -> canonicalize + bugfix + a secret third thing ([#402](https://github.com/spiraldb/vortex/pull/402))
- ArrayData can contain child Arrays instead of just ArrayData ([#391](https://github.com/spiraldb/vortex/pull/391))
- Rename typed_data to maybe_null_slice ([#386](https://github.com/spiraldb/vortex/pull/386))
- Move encodings into directory ([#379](https://github.com/spiraldb/vortex/pull/379))

## `vortex-roaring` - [0.2.0](https://github.com/spiraldb/vortex/compare/vortex-roaring-v0.1.0...vortex-roaring-v0.2.0) - 2024-08-05

### Other
- Use versioned workspace deps ([#551](https://github.com/spiraldb/vortex/pull/551))
- Run cargo-sort on the whole workspace ([#550](https://github.com/spiraldb/vortex/pull/550))
- Ignore tests that miri can't run ([#514](https://github.com/spiraldb/vortex/pull/514))
- Added bool iterators index and slice and filtering across some array types ([#505](https://github.com/spiraldb/vortex/pull/505))
- Make unary functions nicer to `use` ([#493](https://github.com/spiraldb/vortex/pull/493))
- use FQDNs in impl_encoding macro ([#490](https://github.com/spiraldb/vortex/pull/490))
- demo module level imports granularity ([#485](https://github.com/spiraldb/vortex/pull/485))
- DType variant traits ([#473](https://github.com/spiraldb/vortex/pull/473))
- Slightly nicer use statements for compute functions ([#466](https://github.com/spiraldb/vortex/pull/466))
- Array Length ([#445](https://github.com/spiraldb/vortex/pull/445))
- Remove ViewContext and assign stable ids to encodings ([#433](https://github.com/spiraldb/vortex/pull/433))
- Split compression from encodings ([#422](https://github.com/spiraldb/vortex/pull/422))
- Rename flatten -> canonicalize + bugfix + a secret third thing ([#402](https://github.com/spiraldb/vortex/pull/402))
- Rename typed_data to maybe_null_slice ([#386](https://github.com/spiraldb/vortex/pull/386))
- Move encodings into directory ([#379](https://github.com/spiraldb/vortex/pull/379))

## `vortex-fastlanes` - [0.2.0](https://github.com/spiraldb/vortex/compare/vortex-fastlanes-v0.1.0...vortex-fastlanes-v0.2.0) - 2024-08-05

### Fixed
- fix UB and run tests with miri ([#517](https://github.com/spiraldb/vortex/pull/517))

### Other
- Use versioned workspace deps ([#551](https://github.com/spiraldb/vortex/pull/551))
- Run cargo-sort on the whole workspace ([#550](https://github.com/spiraldb/vortex/pull/550))
- Make unary functions nicer to `use` ([#493](https://github.com/spiraldb/vortex/pull/493))
- use FQDNs in impl_encoding macro ([#490](https://github.com/spiraldb/vortex/pull/490))
- demo module level imports granularity ([#485](https://github.com/spiraldb/vortex/pull/485))
- DType variant traits ([#473](https://github.com/spiraldb/vortex/pull/473))
- Slightly nicer use statements for compute functions ([#466](https://github.com/spiraldb/vortex/pull/466))
- Use shorthand canonicalize methods ([#460](https://github.com/spiraldb/vortex/pull/460))
- Array Length ([#445](https://github.com/spiraldb/vortex/pull/445))
- Remove ViewContext and assign stable ids to encodings ([#433](https://github.com/spiraldb/vortex/pull/433))
- Split compression from encodings ([#422](https://github.com/spiraldb/vortex/pull/422))
- Fix semantic conflict between searching and slicing sparse and bitpacked arrays ([#412](https://github.com/spiraldb/vortex/pull/412))
- Fix Slice and SearchSorted for BitPackedArray ([#410](https://github.com/spiraldb/vortex/pull/410))
- FoR encoding doesn't panic if array min > i64::MAX ([#406](https://github.com/spiraldb/vortex/pull/406))
- Add search_sorted for FOR, Bitpacked and Sparse arrays ([#398](https://github.com/spiraldb/vortex/pull/398))
- FoR array holds encoded values as unsinged ([#401](https://github.com/spiraldb/vortex/pull/401))
- Rename flatten -> canonicalize + bugfix + a secret third thing ([#402](https://github.com/spiraldb/vortex/pull/402))
- ArrayData can contain child Arrays instead of just ArrayData ([#391](https://github.com/spiraldb/vortex/pull/391))
- Rename typed_data to maybe_null_slice ([#386](https://github.com/spiraldb/vortex/pull/386))
- Fastlanez -> Fastlanes ([#381](https://github.com/spiraldb/vortex/pull/381))
- Move encodings into directory ([#379](https://github.com/spiraldb/vortex/pull/379))

## `vortex-dict` - [0.2.0](https://github.com/spiraldb/vortex/compare/vortex-dict-v0.1.0...vortex-dict-v0.2.0) - 2024-08-05

### Other
- Use versioned workspace deps ([#551](https://github.com/spiraldb/vortex/pull/551))
- Run cargo-sort on the whole workspace ([#550](https://github.com/spiraldb/vortex/pull/550))
- Make unary functions nicer to `use` ([#493](https://github.com/spiraldb/vortex/pull/493))
- use FQDNs in impl_encoding macro ([#490](https://github.com/spiraldb/vortex/pull/490))
- demo module level imports granularity ([#485](https://github.com/spiraldb/vortex/pull/485))
- Basic predicate pushdown support for Datafusion ([#472](https://github.com/spiraldb/vortex/pull/472))
- DType variant traits ([#473](https://github.com/spiraldb/vortex/pull/473))
- Slightly nicer use statements for compute functions ([#466](https://github.com/spiraldb/vortex/pull/466))
- Run `cargo doc` at CI time ([#469](https://github.com/spiraldb/vortex/pull/469))
- Use shorthand canonicalize methods ([#460](https://github.com/spiraldb/vortex/pull/460))
- Array Length ([#445](https://github.com/spiraldb/vortex/pull/445))
- Remove ViewContext and assign stable ids to encodings ([#433](https://github.com/spiraldb/vortex/pull/433))
- Split compression from encodings ([#422](https://github.com/spiraldb/vortex/pull/422))
- Rename flatten -> canonicalize + bugfix + a secret third thing ([#402](https://github.com/spiraldb/vortex/pull/402))
- ArrayData can contain child Arrays instead of just ArrayData ([#391](https://github.com/spiraldb/vortex/pull/391))
- Rename typed_data to maybe_null_slice ([#386](https://github.com/spiraldb/vortex/pull/386))
- Move encodings into directory ([#379](https://github.com/spiraldb/vortex/pull/379))

## `vortex-datetime-parts` - [0.2.0](https://github.com/spiraldb/vortex/compare/vortex-datetime-parts-v0.1.0...vortex-datetime-parts-v0.2.0) - 2024-08-05

### Fixed
- canonicalization of chunked ExtensionArray ([#499](https://github.com/spiraldb/vortex/pull/499))

### Other
- Use versioned workspace deps ([#551](https://github.com/spiraldb/vortex/pull/551))
- Run cargo-sort on the whole workspace ([#550](https://github.com/spiraldb/vortex/pull/550))
- Add license check to CI ([#518](https://github.com/spiraldb/vortex/pull/518))
- Fix SparseArray validity logic and give DateTimeParts unique code ([#495](https://github.com/spiraldb/vortex/pull/495))
- Make unary functions nicer to `use` ([#493](https://github.com/spiraldb/vortex/pull/493))
- use FQDNs in impl_encoding macro ([#490](https://github.com/spiraldb/vortex/pull/490))
- demo module level imports granularity ([#485](https://github.com/spiraldb/vortex/pull/485))
- Remove LocalDateTimeArray, introduce TemporalArray ([#480](https://github.com/spiraldb/vortex/pull/480))
- DType variant traits ([#473](https://github.com/spiraldb/vortex/pull/473))
- Slightly nicer use statements for compute functions ([#466](https://github.com/spiraldb/vortex/pull/466))
- Use shorthand canonicalize methods ([#460](https://github.com/spiraldb/vortex/pull/460))
- Array Length ([#445](https://github.com/spiraldb/vortex/pull/445))
- Remove ViewContext and assign stable ids to encodings ([#433](https://github.com/spiraldb/vortex/pull/433))
- Split compression from encodings ([#422](https://github.com/spiraldb/vortex/pull/422))
- Rename flatten -> canonicalize + bugfix + a secret third thing ([#402](https://github.com/spiraldb/vortex/pull/402))
- ArrayData can contain child Arrays instead of just ArrayData ([#391](https://github.com/spiraldb/vortex/pull/391))
- Rename typed_data to maybe_null_slice ([#386](https://github.com/spiraldb/vortex/pull/386))
- Move encodings into directory ([#379](https://github.com/spiraldb/vortex/pull/379))

## `vortex-datafusion` - [0.2.0](https://github.com/spiraldb/vortex/compare/vortex-datafusion-v0.1.0...vortex-datafusion-v0.2.0) - 2024-08-05

### Fixed
- fix UB and run tests with miri ([#517](https://github.com/spiraldb/vortex/pull/517))

### Other
- Use versioned workspace deps ([#551](https://github.com/spiraldb/vortex/pull/551))
- Run cargo-sort on the whole workspace ([#550](https://github.com/spiraldb/vortex/pull/550))
- Follow up for 537 ([#538](https://github.com/spiraldb/vortex/pull/538))
- Rename the pushdown config into a positive boolean value ([#537](https://github.com/spiraldb/vortex/pull/537))
- Ignore tests that miri can't run ([#514](https://github.com/spiraldb/vortex/pull/514))
- Add and/or compute functions ([#481](https://github.com/spiraldb/vortex/pull/481))
- demo module level imports granularity ([#485](https://github.com/spiraldb/vortex/pull/485))
- Remove LocalDateTimeArray, introduce TemporalArray ([#480](https://github.com/spiraldb/vortex/pull/480))
- Expand pushdown support with more comparison and logical operations ([#478](https://github.com/spiraldb/vortex/pull/478))
- Debug compilation caching ([#475](https://github.com/spiraldb/vortex/pull/475))
- Basic predicate pushdown support for Datafusion ([#472](https://github.com/spiraldb/vortex/pull/472))
- Slightly nicer use statements for compute functions ([#466](https://github.com/spiraldb/vortex/pull/466))
- Run tpch_benchmark queries single-threaded in rayon pool ([#463](https://github.com/spiraldb/vortex/pull/463))
- Update datafusion to v40 (major) ([#455](https://github.com/spiraldb/vortex/pull/455))
- Make into_arrow truly zero-copy, rewrite DataFusion operators ([#451](https://github.com/spiraldb/vortex/pull/451))
- Setup TPC-H benchmark infra ([#444](https://github.com/spiraldb/vortex/pull/444))
- v0 Datafusion with late materialization ([#414](https://github.com/spiraldb/vortex/pull/414))
- Rename flatten -> canonicalize + bugfix + a secret third thing ([#402](https://github.com/spiraldb/vortex/pull/402))
- DataFusion TableProvider for memory arrays ([#384](https://github.com/spiraldb/vortex/pull/384))

## `vortex-scalar` - [0.2.0](https://github.com/spiraldb/vortex/compare/vortex-scalar-v0.1.0...vortex-scalar-v0.2.0) - 2024-08-05

### Other
- build-vortex -> vortex-build ([#552](https://github.com/spiraldb/vortex/pull/552))
- Use versioned workspace deps ([#551](https://github.com/spiraldb/vortex/pull/551))
- Initial version of simple FileReader/Writer ([#516](https://github.com/spiraldb/vortex/pull/516))
- More specialized compare functions ([#488](https://github.com/spiraldb/vortex/pull/488))
- demo module level imports granularity ([#485](https://github.com/spiraldb/vortex/pull/485))
- Basic predicate pushdown support for Datafusion ([#472](https://github.com/spiraldb/vortex/pull/472))
- DType variant traits ([#473](https://github.com/spiraldb/vortex/pull/473))
- FoR encoding doesn't panic if array min > i64::MAX ([#406](https://github.com/spiraldb/vortex/pull/406))
- Add `ByteBoolArray` type and fixe a bug in `BoolArray` ([#383](https://github.com/spiraldb/vortex/pull/383))
- FoR array holds encoded values as unsinged ([#401](https://github.com/spiraldb/vortex/pull/401))
- DataFusion expr conversion ([#349](https://github.com/spiraldb/vortex/pull/349))
- Fix FOR bug, also fix bench to compile ([#341](https://github.com/spiraldb/vortex/pull/341))
- Implement StructValue proto serde without google.protobuf.Value ([#343](https://github.com/spiraldb/vortex/pull/343))
- Random access benchmarks are runnable again ([#330](https://github.com/spiraldb/vortex/pull/330))
- define ScalarValue in VortexScalar protobuf ([#323](https://github.com/spiraldb/vortex/pull/323))
- Proto Refactor ([#325](https://github.com/spiraldb/vortex/pull/325))
- IPC Bench ([#319](https://github.com/spiraldb/vortex/pull/319))
- Static ArrayView ([#310](https://github.com/spiraldb/vortex/pull/310))
- StatsView2 ([#305](https://github.com/spiraldb/vortex/pull/305))
- Add ScalarView ([#301](https://github.com/spiraldb/vortex/pull/301))
- DType Serialization ([#298](https://github.com/spiraldb/vortex/pull/298))
- OwnedBuffer ([#300](https://github.com/spiraldb/vortex/pull/300))
- Add validity to Struct arrays ([#289](https://github.com/spiraldb/vortex/pull/289))
- Extension Array ([#287](https://github.com/spiraldb/vortex/pull/287))
- Remove composite and decimal ([#285](https://github.com/spiraldb/vortex/pull/285))
- Add convenience stats retrieval functions and avoid needless copy when unwrapping stat value ([#279](https://github.com/spiraldb/vortex/pull/279))
- Scalar subtraction ([#270](https://github.com/spiraldb/vortex/pull/270))
- Add ExtDType ([#281](https://github.com/spiraldb/vortex/pull/281))
- Refactor for DType::Primitive ([#276](https://github.com/spiraldb/vortex/pull/276))
- Extract a vortex-scalar crate ([#275](https://github.com/spiraldb/vortex/pull/275))

## `vortex-expr` - [0.2.0](https://github.com/spiraldb/vortex/compare/vortex-expr-v0.1.0...vortex-expr-v0.2.0) - 2024-08-05

### Other
- build-vortex -> vortex-build ([#552](https://github.com/spiraldb/vortex/pull/552))
- Use versioned workspace deps ([#551](https://github.com/spiraldb/vortex/pull/551))
- Run cargo-sort on the whole workspace ([#550](https://github.com/spiraldb/vortex/pull/550))
- Clean up fields / field paths ([#353](https://github.com/spiraldb/vortex/pull/353))
- Expression proto serde ([#351](https://github.com/spiraldb/vortex/pull/351))
- DataFusion expr conversion ([#349](https://github.com/spiraldb/vortex/pull/349))
- FilterIndices compute function ([#326](https://github.com/spiraldb/vortex/pull/326))
- Introduce FieldPath abstraction, restrict predicates to Field, Op, (Field|Scalar) ([#324](https://github.com/spiraldb/vortex/pull/324))
- Minimal expressions API for vortex ([#318](https://github.com/spiraldb/vortex/pull/318))

## `vortex-flatbuffers` - [0.2.0](https://github.com/spiraldb/vortex/compare/vortex-flatbuffers-v0.1.0...vortex-flatbuffers-v0.2.0) - 2024-08-05

### Other
- Run cargo-sort on the whole workspace ([#550](https://github.com/spiraldb/vortex/pull/550))
- Add ScalarView ([#301](https://github.com/spiraldb/vortex/pull/301))
- Remove unused dependencies (and bump lance) ([#286](https://github.com/spiraldb/vortex/pull/286))
- Add ExtDType ([#281](https://github.com/spiraldb/vortex/pull/281))
- IPC Terminator ([#267](https://github.com/spiraldb/vortex/pull/267))
- Refactor ([#237](https://github.com/spiraldb/vortex/pull/237))
- Constant ([#230](https://github.com/spiraldb/vortex/pull/230))
- Format imports ([#184](https://github.com/spiraldb/vortex/pull/184))
- IPC Prototype ([#181](https://github.com/spiraldb/vortex/pull/181))

## `vortex-error` - [0.2.0](https://github.com/spiraldb/vortex/compare/vortex-error-v0.1.0...vortex-error-v0.2.0) - 2024-08-05

### Other
- Run cargo-sort on the whole workspace ([#550](https://github.com/spiraldb/vortex/pull/550))
- Minimal support for reading vortex with object_store ([#427](https://github.com/spiraldb/vortex/pull/427))
- IPC Bench ([#319](https://github.com/spiraldb/vortex/pull/319))
- More Async IPC ([#313](https://github.com/spiraldb/vortex/pull/313))
- Add ScalarView ([#301](https://github.com/spiraldb/vortex/pull/301))
- Extension Array ([#283](https://github.com/spiraldb/vortex/pull/283))
- Struct Array ([#217](https://github.com/spiraldb/vortex/pull/217))
- Array Data + View ([#210](https://github.com/spiraldb/vortex/pull/210))
- IPC Prototype ([#181](https://github.com/spiraldb/vortex/pull/181))
- Reduce number of distinct error types and capture tracebacks ([#175](https://github.com/spiraldb/vortex/pull/175))
- Random Access Benchmark ([#149](https://github.com/spiraldb/vortex/pull/149))
- Vortex Error ([#133](https://github.com/spiraldb/vortex/pull/133))

## `vortex-dtype` - [0.2.0](https://github.com/spiraldb/vortex/compare/vortex-dtype-v0.1.0...vortex-dtype-v0.2.0) - 2024-08-05

### Other
- build-vortex -> vortex-build ([#552](https://github.com/spiraldb/vortex/pull/552))
- Use versioned workspace deps ([#551](https://github.com/spiraldb/vortex/pull/551))
- Run cargo-sort on the whole workspace ([#550](https://github.com/spiraldb/vortex/pull/550))
- Initial version of simple FileReader/Writer ([#516](https://github.com/spiraldb/vortex/pull/516))
- Add and/or compute functions ([#481](https://github.com/spiraldb/vortex/pull/481))
- demo module level imports granularity ([#485](https://github.com/spiraldb/vortex/pull/485))
- Avoid dtype comparison failure in `take` -- upcast indices in `take_strict_sorted`  ([#464](https://github.com/spiraldb/vortex/pull/464))
- Rename flatten -> canonicalize + bugfix + a secret third thing ([#402](https://github.com/spiraldb/vortex/pull/402))
- DataFusion TableProvider for memory arrays ([#384](https://github.com/spiraldb/vortex/pull/384))
- Clean up fields / field paths ([#353](https://github.com/spiraldb/vortex/pull/353))
- Expression proto serde ([#351](https://github.com/spiraldb/vortex/pull/351))
- DataFusion expr conversion ([#349](https://github.com/spiraldb/vortex/pull/349))
- FilterIndices compute function ([#326](https://github.com/spiraldb/vortex/pull/326))
- Proto Refactor ([#325](https://github.com/spiraldb/vortex/pull/325))
- Introduce FieldPath abstraction, restrict predicates to Field, Op, (Field|Scalar) ([#324](https://github.com/spiraldb/vortex/pull/324))
- Minimal expressions API for vortex ([#318](https://github.com/spiraldb/vortex/pull/318))
- IPC Bench ([#319](https://github.com/spiraldb/vortex/pull/319))
- Remove buffer -> dtype dependency ([#309](https://github.com/spiraldb/vortex/pull/309))
- Add ScalarView ([#301](https://github.com/spiraldb/vortex/pull/301))
- DType Serialization ([#298](https://github.com/spiraldb/vortex/pull/298))
- Add validity to Struct arrays ([#289](https://github.com/spiraldb/vortex/pull/289))
- Remove unused dependencies (and bump lance) ([#286](https://github.com/spiraldb/vortex/pull/286))
- Remove composite and decimal ([#285](https://github.com/spiraldb/vortex/pull/285))
- Extension Array ([#283](https://github.com/spiraldb/vortex/pull/283))
- Add convenience stats retrieval functions and avoid needless copy when unwrapping stat value ([#279](https://github.com/spiraldb/vortex/pull/279))
- Scalar subtraction ([#270](https://github.com/spiraldb/vortex/pull/270))
- Add ExtDType ([#281](https://github.com/spiraldb/vortex/pull/281))
- Refactor for DType::Primitive ([#276](https://github.com/spiraldb/vortex/pull/276))
- Move PType into vortex-dtype ([#274](https://github.com/spiraldb/vortex/pull/274))
- Vortex Schema -> Vortex DType ([#273](https://github.com/spiraldb/vortex/pull/273))

## `vortex-buffer` - [0.2.0](https://github.com/spiraldb/vortex/compare/vortex-buffer-v0.1.0...vortex-buffer-v0.2.0) - 2024-08-05

### Other
- Use versioned workspace deps ([#551](https://github.com/spiraldb/vortex/pull/551))
- Run cargo-sort on the whole workspace ([#550](https://github.com/spiraldb/vortex/pull/550))
- setup automated releases with release-plz ([#547](https://github.com/spiraldb/vortex/pull/547))
- Make into_arrow truly zero-copy, rewrite DataFusion operators ([#451](https://github.com/spiraldb/vortex/pull/451))
- DataFusion TableProvider for memory arrays ([#384](https://github.com/spiraldb/vortex/pull/384))
- Buffer into_vec respects value alignment ([#387](https://github.com/spiraldb/vortex/pull/387))
- More IPC Refactorings ([#329](https://github.com/spiraldb/vortex/pull/329))
- IPC Bench ([#319](https://github.com/spiraldb/vortex/pull/319))
- More Async IPC ([#313](https://github.com/spiraldb/vortex/pull/313))
- Async IPC ([#307](https://github.com/spiraldb/vortex/pull/307))
- Remove buffer -> dtype dependency ([#309](https://github.com/spiraldb/vortex/pull/309))
- Add ScalarView ([#301](https://github.com/spiraldb/vortex/pull/301))
- OwnedBuffer ([#300](https://github.com/spiraldb/vortex/pull/300))
- Vortex Buffer Crate ([#299](https://github.com/spiraldb/vortex/pull/299))

## `vortex-array` - [0.2.0](https://github.com/spiraldb/vortex/compare/vortex-array-v0.1.0...vortex-array-v0.2.0) - 2024-08-05

### Fixed
- fix UB and run tests with miri ([#517](https://github.com/spiraldb/vortex/pull/517))
- canonicalization of chunked ExtensionArray ([#499](https://github.com/spiraldb/vortex/pull/499))
- fix comment on TemporalArray::new_time ([#482](https://github.com/spiraldb/vortex/pull/482))

### Other
- build-vortex -> vortex-build ([#552](https://github.com/spiraldb/vortex/pull/552))
- Use versioned workspace deps ([#551](https://github.com/spiraldb/vortex/pull/551))
- Run cargo-sort on the whole workspace ([#550](https://github.com/spiraldb/vortex/pull/550))
- setup automated releases with release-plz ([#547](https://github.com/spiraldb/vortex/pull/547))
- Initial version of simple FileReader/Writer ([#516](https://github.com/spiraldb/vortex/pull/516))
- Use Arrow's varbin builder ([#519](https://github.com/spiraldb/vortex/pull/519))
- Smoketest for SamplingCompressor, fix bug in varbin stats ([#510](https://github.com/spiraldb/vortex/pull/510))
- Added bool iterators index and slice and filtering across some array types ([#505](https://github.com/spiraldb/vortex/pull/505))
- Fix remaining copies ([#504](https://github.com/spiraldb/vortex/pull/504))
- Remove some vortex mem allocations in "zero-copy" memory transformations ([#503](https://github.com/spiraldb/vortex/pull/503))
- Lazy deserialize metadata from ArrayData and ArrayView ([#502](https://github.com/spiraldb/vortex/pull/502))
- Fix out ouf bounds when taking from run end arrays ([#501](https://github.com/spiraldb/vortex/pull/501))
- More specialized compare functions ([#488](https://github.com/spiraldb/vortex/pull/488))
- Fix SparseArray validity logic and give DateTimeParts unique code ([#495](https://github.com/spiraldb/vortex/pull/495))
- Add and/or compute functions ([#481](https://github.com/spiraldb/vortex/pull/481))
- Implement CastFn for chunkedarray ([#497](https://github.com/spiraldb/vortex/pull/497))
- Make unary functions nicer to `use` ([#493](https://github.com/spiraldb/vortex/pull/493))
- use FQDNs in impl_encoding macro ([#490](https://github.com/spiraldb/vortex/pull/490))
- demo module level imports granularity ([#485](https://github.com/spiraldb/vortex/pull/485))
- Remove LocalDateTimeArray, introduce TemporalArray ([#480](https://github.com/spiraldb/vortex/pull/480))
- Debug compilation caching ([#475](https://github.com/spiraldb/vortex/pull/475))
- Basic predicate pushdown support for Datafusion ([#472](https://github.com/spiraldb/vortex/pull/472))
- DType variant traits ([#473](https://github.com/spiraldb/vortex/pull/473))
- Slightly nicer use statements for compute functions ([#466](https://github.com/spiraldb/vortex/pull/466))
- Avoid dtype comparison failure in `take` -- upcast indices in `take_strict_sorted`  ([#464](https://github.com/spiraldb/vortex/pull/464))
- Use shorthand canonicalize methods ([#460](https://github.com/spiraldb/vortex/pull/460))
- Add FilterFn trait + default implementation ([#458](https://github.com/spiraldb/vortex/pull/458))
- Make into_arrow truly zero-copy, rewrite DataFusion operators ([#451](https://github.com/spiraldb/vortex/pull/451))
- Completely remove scalar_buffer method ([#448](https://github.com/spiraldb/vortex/pull/448))
- Chunked take ([#447](https://github.com/spiraldb/vortex/pull/447))
- Array Length ([#445](https://github.com/spiraldb/vortex/pull/445))
- Remove ViewContext and assign stable ids to encodings ([#433](https://github.com/spiraldb/vortex/pull/433))
- Split compression from encodings ([#422](https://github.com/spiraldb/vortex/pull/422))
- Buffer chunks to read when taking rows ([#419](https://github.com/spiraldb/vortex/pull/419))
- v0 Datafusion with late materialization ([#414](https://github.com/spiraldb/vortex/pull/414))
- Add SearchSortedFn for ConstantArray ([#417](https://github.com/spiraldb/vortex/pull/417))
- Add SliceFn implementation for ConstantArray ([#416](https://github.com/spiraldb/vortex/pull/416))
- Fix Slice and SearchSorted for BitPackedArray ([#410](https://github.com/spiraldb/vortex/pull/410))
- Fix SearchSorted on sliced sparse array ([#411](https://github.com/spiraldb/vortex/pull/411))
- Add `ByteBoolArray` type and fixe a bug in `BoolArray` ([#383](https://github.com/spiraldb/vortex/pull/383))
- Add search_sorted for FOR, Bitpacked and Sparse arrays ([#398](https://github.com/spiraldb/vortex/pull/398))
- Rename flatten -> canonicalize + bugfix + a secret third thing ([#402](https://github.com/spiraldb/vortex/pull/402))
- DataFusion TableProvider for memory arrays ([#384](https://github.com/spiraldb/vortex/pull/384))
- Use ChunkedArrayReader in random access benchmark ([#393](https://github.com/spiraldb/vortex/pull/393))
- ArrayData can contain child Arrays instead of just ArrayData ([#391](https://github.com/spiraldb/vortex/pull/391))
- Buffer into_vec respects value alignment ([#387](https://github.com/spiraldb/vortex/pull/387))
- Rename typed_data to maybe_null_slice ([#386](https://github.com/spiraldb/vortex/pull/386))
- Fastlanez -> Fastlanes ([#381](https://github.com/spiraldb/vortex/pull/381))
- Use IntoArrayData when we have owned arrays ([#376](https://github.com/spiraldb/vortex/pull/376))
- Clean up fields / field paths ([#353](https://github.com/spiraldb/vortex/pull/353))
- Use new search-sorted for finding chunk index ([#342](https://github.com/spiraldb/vortex/pull/342))
- NullArray + statsset cleanup ([#350](https://github.com/spiraldb/vortex/pull/350))
- Expression proto serde ([#351](https://github.com/spiraldb/vortex/pull/351))
- DataFusion expr conversion ([#349](https://github.com/spiraldb/vortex/pull/349))
- Fix FOR bug, also fix bench to compile ([#341](https://github.com/spiraldb/vortex/pull/341))
- Array comparison compute function ([#336](https://github.com/spiraldb/vortex/pull/336))
- FilterIndices compute function ([#326](https://github.com/spiraldb/vortex/pull/326))
- Take Rows Chunked Array ([#331](https://github.com/spiraldb/vortex/pull/331))
- Random access benchmarks are runnable again ([#330](https://github.com/spiraldb/vortex/pull/330))
- ChunkedArray is not a flat encoding ([#332](https://github.com/spiraldb/vortex/pull/332))
- More IPC Refactorings ([#329](https://github.com/spiraldb/vortex/pull/329))
- Add ArrayIterator and ArrayStream ([#327](https://github.com/spiraldb/vortex/pull/327))
- Stats don't allocate errors on missing stats ([#320](https://github.com/spiraldb/vortex/pull/320))
- IPC Bench ([#319](https://github.com/spiraldb/vortex/pull/319))
- Remove flatbuffers build.rs ([#316](https://github.com/spiraldb/vortex/pull/316))
- BoolArray stats respect nulls ([#314](https://github.com/spiraldb/vortex/pull/314))
- Remove array lifetimes ([#312](https://github.com/spiraldb/vortex/pull/312))
- Static ArrayView ([#310](https://github.com/spiraldb/vortex/pull/310))
- Async IPC ([#307](https://github.com/spiraldb/vortex/pull/307))
- Remove buffer -> dtype dependency ([#309](https://github.com/spiraldb/vortex/pull/309))
- Fix chunked array stat merging ([#303](https://github.com/spiraldb/vortex/pull/303))
- Include stats in IPC messages ([#302](https://github.com/spiraldb/vortex/pull/302))
- StatsView2 ([#305](https://github.com/spiraldb/vortex/pull/305))
- Add ScalarView ([#301](https://github.com/spiraldb/vortex/pull/301))
- DType Serialization ([#298](https://github.com/spiraldb/vortex/pull/298))
- OwnedBuffer ([#300](https://github.com/spiraldb/vortex/pull/300))
- Vortex Buffer Crate ([#299](https://github.com/spiraldb/vortex/pull/299))
- Support WASM ([#297](https://github.com/spiraldb/vortex/pull/297))
- Add Context and remove linkme ([#295](https://github.com/spiraldb/vortex/pull/295))
- Add validity to Struct arrays ([#289](https://github.com/spiraldb/vortex/pull/289))
- IPC take returns an iterator instead of ChunkedArray ([#271](https://github.com/spiraldb/vortex/pull/271))
- Extension Array ([#287](https://github.com/spiraldb/vortex/pull/287))
- Remove unused dependencies (and bump lance) ([#286](https://github.com/spiraldb/vortex/pull/286))
- Remove composite and decimal ([#285](https://github.com/spiraldb/vortex/pull/285))
- DateTimeParts ([#284](https://github.com/spiraldb/vortex/pull/284))
- Extension Array ([#283](https://github.com/spiraldb/vortex/pull/283))
- Add convenience stats retrieval functions and avoid needless copy when unwrapping stat value ([#279](https://github.com/spiraldb/vortex/pull/279))
- Scalar subtraction ([#270](https://github.com/spiraldb/vortex/pull/270))
- Add ExtDType ([#281](https://github.com/spiraldb/vortex/pull/281))
- Bring back slice for ChunkedArray ([#280](https://github.com/spiraldb/vortex/pull/280))
- Refactor for DType::Primitive ([#276](https://github.com/spiraldb/vortex/pull/276))
- Extract a vortex-scalar crate ([#275](https://github.com/spiraldb/vortex/pull/275))
- Move PType into vortex-dtype ([#274](https://github.com/spiraldb/vortex/pull/274))
- Vortex Schema -> Vortex DType ([#273](https://github.com/spiraldb/vortex/pull/273))
- Implement take for StreamArrayReader ([#266](https://github.com/spiraldb/vortex/pull/266))
- Don't skip first element in stats calculation ([#268](https://github.com/spiraldb/vortex/pull/268))
- Enable sparse compression ([#262](https://github.com/spiraldb/vortex/pull/262))
- Logical validity from stats ([#264](https://github.com/spiraldb/vortex/pull/264))
- Refactor ([#237](https://github.com/spiraldb/vortex/pull/237))
- Comparison artifacts & analysis ([#247](https://github.com/spiraldb/vortex/pull/247))
- Fix binary stats for arrays containing null bytes and match stats behaviour between varbin and primitive arrays ([#233](https://github.com/spiraldb/vortex/pull/233))
- Address comments from varbin enhancement pr ([#231](https://github.com/spiraldb/vortex/pull/231))
- SearchSorted can return whether search resulted in exact match ([#226](https://github.com/spiraldb/vortex/pull/226))
- Convert slice to compute function ([#227](https://github.com/spiraldb/vortex/pull/227))
- Constant ([#230](https://github.com/spiraldb/vortex/pull/230))
- Array2 compute ([#224](https://github.com/spiraldb/vortex/pull/224))
- Better iterators for VarBin/VarBinView that don't always copy ([#221](https://github.com/spiraldb/vortex/pull/221))
- Try to inline WithCompute calls ([#223](https://github.com/spiraldb/vortex/pull/223))
- Struct Array ([#217](https://github.com/spiraldb/vortex/pull/217))
- Optimize bitpacked `take` ([#192](https://github.com/spiraldb/vortex/pull/192))
- SparseArray TakeFn returns results in the requested order ([#212](https://github.com/spiraldb/vortex/pull/212))
- Add TakeFn for SparseArray ([#206](https://github.com/spiraldb/vortex/pull/206))
- Slightly simplified SparseArray FlattenFn ([#205](https://github.com/spiraldb/vortex/pull/205))
- Don't zero memory when reading a buffer ([#208](https://github.com/spiraldb/vortex/pull/208))
- Move validity into a trait ([#198](https://github.com/spiraldb/vortex/pull/198))
- Patching Bitpacked and ALP arrays doesn't require multiple copies ([#189](https://github.com/spiraldb/vortex/pull/189))
- Implement Validity for SparseArray, make scalar_at for bitpacked array respect patches ([#194](https://github.com/spiraldb/vortex/pull/194))
- Simplify chunk end searching in ChunkedArray ([#199](https://github.com/spiraldb/vortex/pull/199))
- Compute with a primitive trait ([#191](https://github.com/spiraldb/vortex/pull/191))
- Skip codecs where can_compress on sample is null ([#188](https://github.com/spiraldb/vortex/pull/188))
- Accessor lifetime ([#186](https://github.com/spiraldb/vortex/pull/186))
- Validity array ([#185](https://github.com/spiraldb/vortex/pull/185))
- Format imports ([#184](https://github.com/spiraldb/vortex/pull/184))
- IPC Prototype ([#181](https://github.com/spiraldb/vortex/pull/181))
- Use wrapping arithmetic for Frame of Reference ([#178](https://github.com/spiraldb/vortex/pull/178))
- Reduce number of distinct error types and capture tracebacks ([#175](https://github.com/spiraldb/vortex/pull/175))
- Implement generic search sorted using scalar_at ([#167](https://github.com/spiraldb/vortex/pull/167))
- Add Take for Bitpacked array ([#161](https://github.com/spiraldb/vortex/pull/161))
- Scalar_at for FORArray ([#159](https://github.com/spiraldb/vortex/pull/159))
- Random Access Benchmark ([#149](https://github.com/spiraldb/vortex/pull/149))
- Remove unknown ([#156](https://github.com/spiraldb/vortex/pull/156))
- Nullable scalars ([#152](https://github.com/spiraldb/vortex/pull/152))
- Implement Flatten for DictArray ([#143](https://github.com/spiraldb/vortex/pull/143))
- Implement take for BoolArray ([#146](https://github.com/spiraldb/vortex/pull/146))
- Chunked Take ([#141](https://github.com/spiraldb/vortex/pull/141))
- Fix dict encoding validity ([#138](https://github.com/spiraldb/vortex/pull/138))
- Add Validity enum ([#136](https://github.com/spiraldb/vortex/pull/136))
- Vortex Error ([#133](https://github.com/spiraldb/vortex/pull/133))
- Fastlanes delta ([#57](https://github.com/spiraldb/vortex/pull/57))
- Fix encoding discovery ([#132](https://github.com/spiraldb/vortex/pull/132))
- Upgrade arrow-rs to 51.0.0 and extract common dependencies to top level ([#127](https://github.com/spiraldb/vortex/pull/127))
- Make EncodingID Copy ([#131](https://github.com/spiraldb/vortex/pull/131))
- Noah's Arc ([#130](https://github.com/spiraldb/vortex/pull/130))
- Use flatbuffers to serialize dtypes  ([#126](https://github.com/spiraldb/vortex/pull/126))
- DateTime encoding ([#90](https://github.com/spiraldb/vortex/pull/90))
- Split vortex-schema from main crate ([#124](https://github.com/spiraldb/vortex/pull/124))
- flatten ALP arrays ([#123](https://github.com/spiraldb/vortex/pull/123))
- Composite Arrays ([#122](https://github.com/spiraldb/vortex/pull/122))
- Rename Typed to Composite ([#120](https://github.com/spiraldb/vortex/pull/120))
- Replace iter arrow with flatten ([#109](https://github.com/spiraldb/vortex/pull/109))
- Decompress to Arrow ([#106](https://github.com/spiraldb/vortex/pull/106))
- Add ability to define composite dtypes, i.e. dtypes redefining meaning ([#103](https://github.com/spiraldb/vortex/pull/103))
- Serde errors ([#105](https://github.com/spiraldb/vortex/pull/105))
- Trim down arrow dependency ([#98](https://github.com/spiraldb/vortex/pull/98))
- Add bit shifting to FOR ([#89](https://github.com/spiraldb/vortex/pull/89))
- remove dead polars code ([#95](https://github.com/spiraldb/vortex/pull/95))
- Add sizeof tests ([#94](https://github.com/spiraldb/vortex/pull/94))
- Scalars are an enum ([#93](https://github.com/spiraldb/vortex/pull/93))
- Search sorted ([#91](https://github.com/spiraldb/vortex/pull/91))
- More Compression ([#87](https://github.com/spiraldb/vortex/pull/87))
- Cleanup Dict encoding ([#82](https://github.com/spiraldb/vortex/pull/82))
- Compression Updates ([#84](https://github.com/spiraldb/vortex/pull/84))
- Array display ([#83](https://github.com/spiraldb/vortex/pull/83))
- Compressor recursion ([#73](https://github.com/spiraldb/vortex/pull/73))
- Rust ALP ([#72](https://github.com/spiraldb/vortex/pull/72))
- Remove PrimitiveArray::from_vec in favour of PrimitiveArray::from ([#70](https://github.com/spiraldb/vortex/pull/70))
- Fill forward compute function ([#69](https://github.com/spiraldb/vortex/pull/69))
- Root project is vortex-array ([#67](https://github.com/spiraldb/vortex/pull/67))

## `vortex-alp` - [0.2.0](https://github.com/spiraldb/vortex/compare/vortex-alp-v0.1.0...vortex-alp-v0.2.0) - 2024-08-05

### Other
- Use versioned workspace deps ([#551](https://github.com/spiraldb/vortex/pull/551))
- Run cargo-sort on the whole workspace ([#550](https://github.com/spiraldb/vortex/pull/550))
- setup automated releases with release-plz ([#547](https://github.com/spiraldb/vortex/pull/547))
- Make unary functions nicer to `use` ([#493](https://github.com/spiraldb/vortex/pull/493))
- use FQDNs in impl_encoding macro ([#490](https://github.com/spiraldb/vortex/pull/490))
- demo module level imports granularity ([#485](https://github.com/spiraldb/vortex/pull/485))
- DType variant traits ([#473](https://github.com/spiraldb/vortex/pull/473))
- Slightly nicer use statements for compute functions ([#466](https://github.com/spiraldb/vortex/pull/466))
- Array Length ([#445](https://github.com/spiraldb/vortex/pull/445))
- Remove ViewContext and assign stable ids to encodings ([#433](https://github.com/spiraldb/vortex/pull/433))
- Split compression from encodings ([#422](https://github.com/spiraldb/vortex/pull/422))
- Add search_sorted for FOR, Bitpacked and Sparse arrays ([#398](https://github.com/spiraldb/vortex/pull/398))
- Rename flatten -> canonicalize + bugfix + a secret third thing ([#402](https://github.com/spiraldb/vortex/pull/402))
- ArrayData can contain child Arrays instead of just ArrayData ([#391](https://github.com/spiraldb/vortex/pull/391))
- Rename typed_data to maybe_null_slice ([#386](https://github.com/spiraldb/vortex/pull/386))
- Fastlanez -> Fastlanes ([#381](https://github.com/spiraldb/vortex/pull/381))
- Move encodings into directory ([#379](https://github.com/spiraldb/vortex/pull/379))
