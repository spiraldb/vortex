# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

# Selects the Rust tools and the native Rust target for the Vortex C++ build.

include_guard(GLOBAL)

# Sets VORTEX_RUST_TARGET and VORTEX_APPLE_DEPLOYMENT_TARGET in the caller's
# scope. Cache entries hold the selected Cargo/rustc tools and rustup override.
function(_vortex_resolve_rust_toolchain workspace_root sanitizer_rustflags)
    # Initialize once: automatic reconfiguration may run without the original environment.
    if(NOT DEFINED VORTEX_RUSTUP_TOOLCHAIN)
        set(VORTEX_RUSTUP_TOOLCHAIN "$ENV{RUSTUP_TOOLCHAIN}")
        if(sanitizer_rustflags AND VORTEX_RUSTUP_TOOLCHAIN STREQUAL "")
            set(VORTEX_RUSTUP_TOOLCHAIN nightly)
        endif()
    endif()
    set(VORTEX_RUSTUP_TOOLCHAIN "${VORTEX_RUSTUP_TOOLCHAIN}" CACHE STRING
        "Rustup toolchain override (empty uses the workspace rust-toolchain.toml)")

    if(VORTEX_RUSTUP_TOOLCHAIN)
        set(_rustup_environment "RUSTUP_TOOLCHAIN=${VORTEX_RUSTUP_TOOLCHAIN}")
        message(STATUS "Vortex Rust toolchain override: ${VORTEX_RUSTUP_TOOLCHAIN}")
    else()
        set(_rustup_environment --unset=RUSTUP_TOOLCHAIN)
        message(STATUS "Vortex Rust toolchain: workspace rust-toolchain.toml (no override)")
    endif()

    find_program(VORTEX_CARGO_EXECUTABLE NAMES cargo REQUIRED)
    find_program(VORTEX_RUSTC_EXECUTABLE NAMES rustc REQUIRED)
    execute_process(
        COMMAND "${CMAKE_COMMAND}" -E env "${_rustup_environment}"
            "${VORTEX_RUSTC_EXECUTABLE}" -vV
        WORKING_DIRECTORY "${workspace_root}"
        OUTPUT_VARIABLE _rustc_verbose
        COMMAND_ERROR_IS_FATAL ANY)
    string(REGEX MATCH "host: ([^\r\n]+)" _match "${_rustc_verbose}")
    set(VORTEX_RUST_TARGET "${CMAKE_MATCH_1}" PARENT_SCOPE)

    # Without an explicit deployment target the cc crate uses the SDK version,
    # which can exceed CMake's link target and makes ld64 warn about every
    # Cargo-built C object; 11.0 is rustc's minimum for aarch64-apple-darwin.
    if(APPLE AND NOT CMAKE_OSX_DEPLOYMENT_TARGET)
        set(VORTEX_APPLE_DEPLOYMENT_TARGET "11.0" PARENT_SCOPE)
    else()
        set(VORTEX_APPLE_DEPLOYMENT_TARGET "${CMAKE_OSX_DEPLOYMENT_TARGET}" PARENT_SCOPE)
    endif()
endfunction()
