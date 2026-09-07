# SPDX-License-Identifier: CC-BY-4.0
# SPDX-FileCopyrightText: Copyright the Vortex contributors

# Prefix targets and keep executables local so a parent's shared output
# directory cannot merge C and C++ examples with the same name.
foreach(_name scan scan_to_arrow dtype write_sample)
    add_executable(ffi_${_name} ${_name}.c)

    set_target_properties(ffi_${_name} PROPERTIES
        OUTPUT_NAME ${_name}
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}")
    target_compile_features(ffi_${_name} PRIVATE c_std_17)
    target_compile_options(ffi_${_name} PRIVATE -Wall -Wextra -Wpedantic)
    if(VORTEX_WARNINGS_AS_ERRORS)
        target_compile_options(ffi_${_name} PRIVATE -Werror)
    endif()
    target_link_libraries(ffi_${_name} PRIVATE Vortex::ffi_static)
endforeach()
target_link_libraries(ffi_scan_to_arrow PRIVATE nanoarrow_shared)
