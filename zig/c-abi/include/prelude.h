#ifndef SPIRAL_ZIG_PRELUDE_H
#define SPIRAL_ZIG_PRELUDE_H

#include "stdint.h"
#include "float.h"
#include "assert.h"

#ifndef SPIRAL_ALIGNMENT
#define SPIRAL_ALIGNMENT 128
#endif // SPIRAL_ALIGNMENT

#ifndef FL_MIN_ALIGNMENT
#define FL_MIN_ALIGNMENT 64
#endif // FL_MIN_ALIGNMENT

#if defined(__cplusplus)
extern "C" {
#endif

static_assert(sizeof(float) == 4, "float type must have 32 bits");
static_assert(sizeof(double) == 8, "double type must have 64 bits");
static_assert(sizeof(uintptr_t) == sizeof(uint64_t), "uintptr_t must be 64 bits");
typedef uint64_t expected_zig_usize_t; // for a comptime check in zig

// 
#if defined(__cplusplus)
} // extern "C"
#endif

#endif // SPIRAL_ZIG_PRELUDE_H