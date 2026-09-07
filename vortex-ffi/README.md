# Vortex C bindings

## Use from CMake

CMake builds the Rust archive through Cargo; no separate `cargo build` is needed. Add a complete
Vortex checkout and link the C target:

```cmake
add_subdirectory(path/to/vortex vortex)
target_link_libraries(my_target PRIVATE Vortex::ffi_static)
```

You can also add `vortex-ffi` alone. The target supplies the archive, headers, and native link
libraries. See the [C++ README](../lang/cpp/README.md) for requirements, build options, and deployment
constraints shared by both bindings.

CMake stages headers under `vortex-artifacts/include` in the FFI binary directory after Cargo
runs, so header changes trigger recompilation in the same build. `clean` removes these copies,
not the headers in the checkout.

### Examples and tests

Run from the repository root. Tests use Catch2 and require a C++ compiler.

```sh
cmake -S vortex-ffi -B build/ffi \
    -DVORTEX_BUILD_EXAMPLES=ON -DVORTEX_BUILD_TESTING=ON
cmake --build build/ffi --parallel
ctest --test-dir build/ffi --output-on-failure

./build/ffi/examples/write_sample sample.vortex
./build/ffi/examples/dtype sample.vortex
./build/ffi/examples/scan sample.vortex
./build/ffi/examples/scan_to_arrow sample.vortex
```

## Runtime threading

By default, host threads executing FFI calls drive a shared runtime; Vortex creates no worker
threads. Concurrent FFI calls can drive runtime work in parallel.

`vx_runtime_set_worker_threads(n)` adds Vortex-owned background workers so a single FFI call can
make progress on multiple threads. This setting is process-global and shared by all FFI sessions.
Setting it to zero signals the workers to stop and restores host-thread-only execution. Leave it
at zero if your application already provides concurrency, to avoid oversubscription.

## Update the C header

To regenerate `vortex-ffi/cinclude/vortex.h`:

```sh
cargo +nightly build -p vortex-ffi
```

## Sanitizer tests

### Rust and C/C++ together

Use CMake to instrument the Rust archive, its native dependencies, and the C API tests. See
[Sanitizers](../lang/cpp/README.md#sanitizers) for toolchain requirements and supported sanitizers.
From the repository root:

```sh
rustup toolchain install nightly --component rust-src
cmake -S vortex-ffi -B build/ffi-asan \
    -DCMAKE_BUILD_TYPE=Debug \
    -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ \
    -DVORTEX_SANITIZER=asan,ubsan -DVORTEX_SANITIZE_RUST_STD=ON \
    -DVORTEX_BUILD_TESTING=ON
cmake --build build/ffi-asan --parallel
ctest --test-dir build/ffi-asan --output-on-failure
```

For ThreadSanitizer, use `tsan` and set `TSAN_OPTIONS` to
`suppressions=/absolute/path/to/vortex/vortex-ffi/tsan_suppressions.txt`.
For Rust-demangled output, run `build/ffi-asan/test/vortex_ffi_test` directly and pipe its output
through `rustfilt -i-`.

### Rust only

Use nightly `cargo test`, with `rust-src` installed. This example targets Linux x86_64; replace the
triple with your native target:

```sh
RUSTFLAGS="-Zsanitizer=address -Cunsafe-allow-abi-mismatch=sanitizer" \
cargo +nightly test -p vortex-ffi -Zbuild-std \
    --target x86_64-unknown-linux-gnu --tests -- --no-capture
```

Use `-Zsanitizer=memory` for MemorySanitizer or `-Zsanitizer=thread` for ThreadSanitizer, with the
suppression file above. Keep these flags and tools in mind:

- `-Zbuild-std` instruments the standard library, avoiding false positives with memory/thread
  sanitizers.
- `-Cunsafe-allow-abi-mismatch=sanitizer` permits dependencies such as `compiler_builtins` to opt
  out of instrumentation deliberately.
- `--tests` skips doctests: rustdoc ignores `RUSTFLAGS`, causing sanitizer mismatches.
- Use `cargo test`, not nextest, to catch more leaks; install `llvm-symbolizer` for stack traces.
