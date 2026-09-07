# Vortex C++ bindings

A C++20 API for reading and writing Vortex files. See the
[C++ quickstart](../../docs/getting-started/cpp.rst) for API examples.

## Build

Requirements: CMake 3.25+, native C/C++ compilers, and Cargo/rustc 1.95+ on `PATH`.
Supported platforms are GNU/Linux x86_64 and aarch64, and macOS arm64 for standalone development.
Use a complete Vortex checkout and run commands from its root:

```sh
cmake -S lang/cpp -B build/cpp -DCMAKE_BUILD_TYPE=Release
cmake --build build/cpp --parallel
```

CMake builds the Rust FFI through Cargo; no separate `cargo build` is needed. Use a single-config
CMake generator such as Ninja or Unix Makefiles. Cross-compilation, Apple universal binaries,
Windows, musl, multi-config generators, and shared Vortex targets are not supported.

## Embed in a CMake project

Vendor or fetch a pinned, complete checkout, then add the repository root once:

```cmake
add_subdirectory(path/to/vortex vortex)
target_link_libraries(my_cpp_target PRIVATE Vortex::cpp_static)
target_link_libraries(my_c_target PRIVATE Vortex::ffi_static)
```

`Vortex::cpp_static` includes the FFI archive, headers, and native link dependencies. Link the target,
not `libvortex_cxx.a` alone. You can also add `lang/cpp` or `vortex-ffi` directly. There are no Vortex
installation rules or `find_package(Vortex)` package.

Vortex keeps its compile/link options target-scoped and leaves the parent's build type, language
standards, flags, and `BUILD_SHARED_LIBS` unchanged. The static archives are position-independent.
When linking them into a shared library, keep Vortex calls private and exclude `vx_*` and other
implementation symbols from the public ABI. Use the parent's export policy, such as an ELF version
script with `--exclude-libs,ALL`, or a macOS exported-symbol allowlist.

## Build options

Set options before `add_subdirectory`, or pass them with `-D`. Options apply to both the C and C++
layers unless noted.

| Option                      | Default                         | Effect                                                                                               |
| --------------------------- | ------------------------------- | ---------------------------------------------------------------------------------------------------- |
| `VORTEX_BUILD_TESTING`      | `OFF`                           | Build C API tests and C++23 wrapper tests. Embedded builds need the parent to enable CTest.          |
| `VORTEX_BUILD_EXAMPLES`     | `OFF`                           | Build C and C++ examples.                                                                            |
| `VORTEX_ENABLE_CUDA`        | `OFF`                           | Select the Linux-only CUDA FFI archive; see [CUDA](#cuda).                                           |
| `VORTEX_CARGO_PROFILE`      | Inferred                        | Override the Cargo profile; see below.                                                               |
| `VORTEX_WARNINGS_AS_ERRORS` | `ON` standalone, `OFF` embedded | Treat warnings as errors in the C++ wrapper and C API tests/examples, not dependencies or consumers. |
| `VORTEX_SANITIZER`          | Empty                           | Select sanitizers, e.g. `asan,ubsan`; see [Sanitizers](#sanitizers).                                 |
| `VORTEX_SANITIZE_RUST_STD`  | `OFF`                           | Also instrument Rust's standard library; requires nightly `rust-src` and a Rust sanitizer.           |

Standalone builds default to `Debug`. Unless `VORTEX_CARGO_PROFILE` overrides it:

| `CMAKE_BUILD_TYPE` | Cargo profile         |
| ------------------ | --------------------- |
| `Debug` or empty   | `dev`                 |
| `Release`          | `release`             |
| `RelWithDebInfo`   | `release_debug`       |
| `MinSizeRel`       | `release_size`        |
| Other              | `dev`, with a warning |

Custom Cargo profiles use same-named artifact directories. Cargo's `test` and `bench` profiles are
not supported.

### Toolchain and build behavior

- CMake discovers Cargo and rustc with `find_program`. Override them with
  `VORTEX_CARGO_EXECUTABLE` and `VORTEX_RUSTC_EXECUTABLE`. The rustc host determines the native target.
- `VORTEX_RUSTUP_TOOLCHAIN` caches the Rust toolchain override across reconfiguration. It defaults
  to `RUSTUP_TOOLCHAIN`, or `nightly` for Rust sanitizers; empty uses the workspace's
  `rust-toolchain.toml`. Change it with `-DVORTEX_RUSTUP_TOOLCHAIN=<toolchain>`, or clear it with
  `-DVORTEX_RUSTUP_TOOLCHAIN=`. When enabling Rust sanitizers in an existing tree, select a nightly
  explicitly if its cached toolchain is stable.
- Cargo uses the lockfile, with optional features such as `mimalloc` disabled. CMake supplies the
  complete Rust flags, overriding flags from the environment and Cargo configuration.
- Cargo-built native dependencies use CMake's compilers, archiver, and C/C++ flags, except
  warning-as-error flags. Host build dependencies also omit sanitizer instrumentation.
- Cargo checks for changes whenever a target depending on Vortex is built. Its cache lives under
  the FFI binary directory: `ffi/cargo-target` in root and C++ builds. The CMake `clean` target
  removes this cache too.
- After Cargo runs, CMake stages headers from the checkout into the build directory so changes
  trigger recompilation immediately. Nightly builds without Rust sanitizers may regenerate `vortex.h`
  with cbindgen and `clang-format`; stable and Rust-sanitized builds leave it unchanged.
  Nightly and CUDA builds need a writable checkout because they can generate source files there.
- Cargo may download locked dependencies. Enabling tests/examples may also download Nanoarrow,
  Catch2, and magic_enum during CMake configure.

## Development

### Tests and examples

C++ tests require a C++23 compiler. To build both test suites and all examples:

```sh
cmake -S lang/cpp -B build/cpp-dev \
    -DVORTEX_BUILD_TESTING=ON -DVORTEX_BUILD_EXAMPLES=ON
cmake --build build/cpp-dev --parallel
ctest --test-dir build/cpp-dev --output-on-failure
```

C++ examples (`reader`, `writer`, `dtype`, `scan`, `scan_to_arrow`) are in
`build/cpp-dev/examples/`; C examples are in `build/cpp-dev/ffi/examples/`.

To run build-system regression tests without compiling the Rust archive:

```sh
python3 -m unittest discover -s vortex-ffi/cmake/tests
```

### Sanitizers

`VORTEX_SANITIZER` accepts a comma-separated list of `asan`, `lsan`, `ubsan`, and `tsan`.
It requires `Debug` and Clang. Flags instrument Vortex's C/C++ code, Cargo-built native
target dependencies, and targets linking Vortex, but not Cargo's host build tools or their
native dependencies. All but `ubsan` also instrument Rust, which has no UBSan.
Rust instrumentation defaults to rustup's `nightly` unless `RUSTUP_TOOLCHAIN` is set.

```sh
rustup toolchain install nightly
cmake -S lang/cpp -B build/cpp-asan \
    -DCMAKE_BUILD_TYPE=Debug \
    -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ \
    -DVORTEX_SANITIZER=asan,ubsan -DVORTEX_BUILD_TESTING=ON
cmake --build build/cpp-asan --parallel
ctest --test-dir build/cpp-asan --output-on-failure
```

Rust sanitizer builds require upstream LLVM Clang; CMake accepts AppleClang only for `ubsan`.
AppleClang's runtime cannot link Rust's ASan instrumentation. To instrument Rust's standard
library, install `rust-src` for the selected nightly and add `-DVORTEX_SANITIZE_RUST_STD=ON`.
CUDA device code, the CUB helper, and nvCOMP are not sanitizer-instrumented.

### Coverage

The helper reports C++ coverage only. It needs a compatible gcov/LCOV toolchain and `genhtml` for
HTML output. Omit `html` to write only `coverage.info`:

```sh
cd lang/cpp
./gcov-report.sh html
```

## CUDA

`VORTEX_ENABLE_CUDA=ON` selects `vortex-cuda-ffi` and adds `vortex_cuda.h` to the existing targets;
there is no separate CUDA CMake target. It requires Linux, a CUDA toolkit discoverable through
`find_package(CUDAToolkit)` (set `CUDAToolkit_ROOT` if needed), and libclang for bindgen. The build
may download the pinned CUDA 12 nvCOMP SDK from NVIDIA. End-to-end cuDF integration and
GCC 14, Conda compiler-wrapper, and glibc 2.28 compatibility remain unvalidated.

CUDA output is **not a self-contained, relocatable deployment artifact**:

- Build scripts use NVCC's `-arch=native`; `CMAKE_CUDA_ARCHITECTURES` has no effect.
- Kernel sources and PTX are generated in the checkout; PTX is embedded in the Rust archive.
- CMake does not stage `libvortex_cub.so` or `libnvcomp.so`. The CUB helper must stay at its Cargo
  build path or sit beside the host executable; nvCOMP is loaded from its original Cargo build path.
- CUDA operations need a compatible NVIDIA driver and an accessible GPU.
