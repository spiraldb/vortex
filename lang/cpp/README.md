# Vortex C++ bindings

A C++20 API for reading and writing Vortex files. See the
[C++ quickstart](../../docs/getting-started/cpp.rst) for API examples.

## Build

Requires CMake 3.25+, C/C++ compilers, and Cargo/rustc 1.95+ on `PATH`.
Use a single-config generator: Ninja or Unix Makefiles.

From a complete Vortex checkout:

```sh
cmake -S lang/cpp -B build/cpp -DCMAKE_BUILD_TYPE=Release
cmake --build build/cpp --parallel
```

CMake runs Cargo for you. Configure/build may download dependencies.

**Native builds only:** GNU/Linux x86_64 and aarch64, plus macOS arm64 for standalone development.
Cross-compilation, universal binaries, Windows, musl, and shared Vortex targets are unsupported.

## Embed in a CMake project

Vendor or fetch a pinned, complete checkout:

```cmake
add_subdirectory(path/to/vortex vortex)
target_link_libraries(my_cpp_target PRIVATE Vortex::cpp_static)
target_link_libraries(my_c_target PRIVATE Vortex::ffi_static)
```

Link the targets, not raw archives: they carry headers and native dependencies.
You can also add `lang/cpp` or `vortex-ffi` directly. There are no installation rules or
`find_package(Vortex)` package.

Vortex leaves parent build settings unchanged. Its archives are position-independent.
When embedding them in a shared library, hide Vortex symbols (including `vx_*`) with your
export policy, such as an ELF version script and `--exclude-libs,ALL`, or a macOS export allowlist.

## Build options

Pass `-D<OPTION>=<VALUE>` or set options before `add_subdirectory`. Both bindings share these
options. Defaults below are for standalone builds.

| Option                      | Default  | Purpose                                                     |
| --------------------------- | -------- | ----------------------------------------------------------- |
| `VORTEX_BUILD_TESTING`      | `OFF`    | C API and C++23 wrapper tests.                              |
| `VORTEX_BUILD_EXAMPLES`     | `OFF`    | C/C++ examples.                                             |
| `VORTEX_WARNINGS_AS_ERRORS` | `ON`     | Warnings as errors for Vortex targets only.                 |
| `VORTEX_CARGO_PROFILE`      | Inferred | Override the mapping below.                                 |
| `VORTEX_RUSTUP_TOOLCHAIN`   | Inferred | [Rust toolchain override](#toolchain-and-build-behavior).   |
| `VORTEX_SANITIZER`          | Empty    | [Sanitizers](#sanitizers): `asan`, `lsan`, `ubsan`, `tsan`. |
| `VORTEX_SANITIZE_RUST_STD`  | `OFF`    | Also instrument Rust's standard library.                    |
| `VORTEX_ENABLE_CUDA`        | `OFF`    | Linux-only [CUDA build](#cuda).                             |

Embedded builds default `VORTEX_WARNINGS_AS_ERRORS` to `OFF`. Parents must call
`enable_testing()` to register tests.

### Cargo profiles

Standalone builds default to `Debug`. Unless overridden, Cargo uses:

| `CMAKE_BUILD_TYPE` | Cargo profile         |
| ------------------ | --------------------- |
| `Debug` or empty   | `dev`                 |
| `Release`          | `release`             |
| `RelWithDebInfo`   | `release_debug`       |
| `MinSizeRel`       | `release_size`        |
| Other              | `dev`, with a warning |

Cargo's `test` and `bench` profiles are unsupported.

### Toolchain and build behavior

- `VORTEX_RUSTUP_TOOLCHAIN` is cached. Initially it uses `RUSTUP_TOOLCHAIN`, otherwise `nightly`
  for Rust sanitizers or the workspace `rust-toolchain.toml`. Set it explicitly to change toolchains.
  An empty value selects the workspace toolchain.
- Cargo/rustc are found on `PATH`. Override them with `VORTEX_CARGO_EXECUTABLE` and
  `VORTEX_RUSTC_EXECUTABLE`. CMake replaces environment/configuration Rust flags and builds with
  the lockfile and no optional FFI features.
- Cargo's native dependencies use CMake's compilers, archiver, SDK, and C/C++ flags, except
  warning-as-error flags. Global flags can override dependency choices, including language standards.
  Prefer target-scoped flags or Vortex options.
- Cargo caches under the FFI build directory (`ffi/cargo-target` in C++/root builds).
  `clean` removes this cache and staged headers, not checkout headers.
- Nightly builds without Rust sanitizers may regenerate `vortex.h`.
  Nightly and CUDA builds need a writable checkout for generated sources.

## Development

### Tests and examples

```sh
cmake -S lang/cpp -B build/cpp-dev \
    -DVORTEX_BUILD_TESTING=ON -DVORTEX_BUILD_EXAMPLES=ON
cmake --build build/cpp-dev --parallel
ctest --test-dir build/cpp-dev --output-on-failure
```

Examples appear in `build/cpp-dev/examples/` (C++) and `build/cpp-dev/ffi/examples/` (C).

Build-system regressions need Python 3.11+, but do not build Vortex:

```sh
cargo fetch --locked
python3 vortex-ffi/cmake/tests/run_tests.py
```

Add `--ci` to reject unexpected skips.

### Sanitizers

Use `Debug`. `asan`, `lsan`, and `tsan` require nightly Rust and upstream LLVM Clang.
`ubsan` is native-only and also supports stable Rust and AppleClang.

```sh
rustup toolchain install nightly

cmake -S lang/cpp -B build/cpp-asan \
    -DCMAKE_BUILD_TYPE=Debug -DVORTEX_RUSTUP_TOOLCHAIN=nightly \
    -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ \
    -DVORTEX_SANITIZER=asan,ubsan -DVORTEX_BUILD_TESTING=ON
cmake --build build/cpp-asan --parallel
ctest --test-dir build/cpp-asan --output-on-failure
```

Set the nightly explicitly when reusing a stable build tree. To instrument Rust's standard library,
install `rust-src` for that nightly and add `-DVORTEX_SANITIZE_RUST_STD=ON` alongside a Rust sanitizer.

Flags instrument Vortex, targets linking it, and Cargo's native library dependencies—not Cargo's
build tools or FetchContent dependencies. CUDA device code, the CUB helper, and nvCOMP are excluded.

### Coverage

Requires gcov/LCOV and, for HTML output, `genhtml`. Reports C++ coverage only.

```sh
cd lang/cpp
./gcov-report.sh html
```

Omit `html` for `coverage.info` only. `CMAKE_BUILD_PARALLEL_LEVEL` overrides the online CPU count.
Coverage instrumentation stays off Cargo's build tools.

## CUDA

`VORTEX_ENABLE_CUDA=ON` selects `vortex-cuda-ffi` and adds `vortex_cuda.h` to the existing targets.
Requires Linux, a CUDA toolkit (`CUDAToolkit_ROOT` if needed), and libclang.
The build may download the pinned CUDA 12 nvCOMP SDK.

**Not a relocatable deployment artifact:**

- NVCC uses `-arch=native`. `CMAKE_CUDA_ARCHITECTURES` has no effect.
- Kernel sources and PTX are generated in the checkout. PTX is embedded in the archive.
- CMake does not stage shared libraries. `libvortex_cub.so` must remain at its Cargo build path
  or beside the executable. `libnvcomp.so` is loaded from its original Cargo build path.
- CUDA operations require a compatible NVIDIA driver and an accessible GPU.

End-to-end cuDF integration, GCC 14, Conda compiler wrappers, and glibc 2.28 remain unvalidated.
