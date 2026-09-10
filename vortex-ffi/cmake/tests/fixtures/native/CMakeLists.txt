# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

cmake_minimum_required(VERSION 3.25)
project(NativeFixture LANGUAGES C CXX)
# Supply quoted mandatory arguments after CMake's compiler detection/cache step.
foreach(language IN ITEMS C CXX)
    set(CMAKE_${language}_COMPILER_ARG1 "${FIXTURE_${language}_ARG1}")
endforeach()
add_subdirectory(vortex-ffi ffi)
add_library(parent_native STATIC native-helper/native.c native-helper/native.cpp)
add_executable(header_consumer header_consumer.c)
target_link_libraries(header_consumer PRIVATE Vortex::ffi_static)
