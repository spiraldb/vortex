# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

# Translate CMake's architecture policy for Cargo-owned NVCC invocations.

include_guard(GLOBAL)

function(_vortex_resolve_cuda_architectures output)
    # Keep the default local: embedding projects own the variable and its cache.
    if(NOT DEFINED CMAKE_CUDA_ARCHITECTURES)
        set(${output} "-arch=native" PARENT_SCOPE)
        return()
    elseif(CMAKE_CUDA_ARCHITECTURES STREQUAL "")
        message(FATAL_ERROR
            "CMAKE_CUDA_ARCHITECTURES must be non-empty if set; "
            "use OFF to omit architecture flags")
    elseif(NOT CMAKE_CUDA_ARCHITECTURES)
        set(${output} "" PARENT_SCOPE)
        return()
    elseif(CMAKE_CUDA_ARCHITECTURES MATCHES "^(native|all|all-major)$")
        set(${output} "-arch=${CMAKE_CUDA_ARCHITECTURES}" PARENT_SCOPE)
        return()
    endif()

    # Validate syntax, not GPU/toolkit support. NVCC checks capabilities, including
    # architecture-specific (a) and family-specific (f) variants, without a version table here.
    set(_entry "[0-9]+[af]?(-real|-virtual)?")
    if(NOT CMAKE_CUDA_ARCHITECTURES MATCHES "^${_entry}(;${_entry})*$")
        message(FATAL_ERROR
            "Invalid CMAKE_CUDA_ARCHITECTURES='${CMAKE_CUDA_ARCHITECTURES}': "
            "expected a semicolon-separated list of numeric capabilities with optional "
            "a/f and -real/-virtual suffixes, or a CMake false constant to omit flags. "
            "The keywords native, all, and all-major must be used alone")
    endif()

    set(_flags "")
    foreach(_architecture IN LISTS CMAKE_CUDA_ARCHITECTURES)
        string(REGEX REPLACE "-(real|virtual)$" "" _capability "${_architecture}")
        if(_architecture MATCHES "-real$")
            set(_code "sm_${_capability}")
        elseif(_architecture MATCHES "-virtual$")
            set(_code "compute_${_capability}")
        else()
            set(_code "compute_${_capability},sm_${_capability}")
        endif()
        list(APPEND _flags "--generate-code=arch=compute_${_capability},code=[${_code}]")
    endforeach()
    set(${output} "${_flags}" PARENT_SCOPE)
endfunction()
