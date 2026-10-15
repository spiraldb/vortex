// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

#include <cuda_runtime.h>
#include <stdint.h>
#include <type_traits>

#include "compare.h"

// Keep these dimensions and layouts in sync with exact_filter.rs.
constexpr uint32_t FILTER_THREADS = 256;
constexpr uint32_t FILTER_TILE_ROWS = 2048;
constexpr uint32_t FILTER_MAX_PREDICATES = 16;

struct FilterComparison {
    uint64_t literal;
    uint32_t op;
    uint32_t reserved;
};
struct FilterPredicates {
    FilterComparison comparisons[FILTER_MAX_PREDICATES];
};
static_assert(sizeof(FilterComparison) == 16);
static_assert(sizeof(FilterPredicates) == 256);

template <typename T>
__device__ bool filter_compare(T value, T literal, uint32_t op) {
    // Preserve the superset contract for both IEEE consumers and Vortex total ordering.
    if constexpr (std::is_floating_point_v<T>) {
        if (isnan(value) || isnan(literal) || (value == 0 && literal == 0)) {
            return true;
        }
    }
    switch (op) {
    case COMPARE_EQ: return value == literal;
    case COMPARE_NOT_EQ: return value != literal;
    case COMPARE_GT: return value > literal;
    case COMPARE_GTE: return value >= literal;
    case COMPARE_LT: return value < literal;
    case COMPARE_LTE: return value <= literal;
    default: return true;
    }
}

template <typename T>
__device__ T filter_literal(uint64_t bits) {
    return static_cast<T>(bits);
}

template <>
__device__ float filter_literal<float>(uint64_t bits) {
    return __uint_as_float(static_cast<uint32_t>(bits));
}

template <>
__device__ double filter_literal<double>(uint64_t bits) {
    return __longlong_as_double(static_cast<long long>(bits));
}

// The host dispatches only compatible same-column, same-type conjunctions here.
// Count and selection remain the existing count_valid and per-column CUB operations.
template <typename T>
__device__ __forceinline__ void filter_typed_mask(const T *input,
                                                const FilterPredicates &predicates,
                                                uint32_t num_predicates,
                                                uint64_t len,
                                                uint32_t *words) {
    const uint64_t tile_start = static_cast<uint64_t>(blockIdx.x) * FILTER_TILE_ROWS;
    const uint32_t lane = threadIdx.x % 32;
    for (uint32_t offset = threadIdx.x; offset < FILTER_TILE_ROWS; offset += FILTER_THREADS) {
        const uint64_t row = tile_start + offset;
        bool keep = row < len;
        if (keep) {
            const T value = input[row];
            for (uint32_t index = 0; index < num_predicates; ++index) {
                const FilterComparison &comparison = predicates.comparisons[index];
                keep = keep && filter_compare(value, filter_literal<T>(comparison.literal), comparison.op);
            }
        }
        const uint32_t bits = __ballot_sync(0xffffffff, keep);
        if (lane == 0 && row < len) {
            words[row / 32] = bits;
        }
    }
}

#define GENERATE_TYPED_FILTER_MASK(suffix, Type)                                                             \
    extern "C" __global__ void exact_filter_mask_##suffix(const Type *__restrict input,                       \
                                                          FilterPredicates predicates,                       \
                                                          uint32_t num_predicates,                           \
                                                          uint64_t len,                                      \
                                                          uint32_t *__restrict words) {                      \
        filter_typed_mask<Type>(input, predicates, num_predicates, len, words);                               \
    }

GENERATE_TYPED_FILTER_MASK(u8, uint8_t)
GENERATE_TYPED_FILTER_MASK(u16, uint16_t)
GENERATE_TYPED_FILTER_MASK(u32, uint32_t)
GENERATE_TYPED_FILTER_MASK(u64, uint64_t)
GENERATE_TYPED_FILTER_MASK(i8, int8_t)
GENERATE_TYPED_FILTER_MASK(i16, int16_t)
GENERATE_TYPED_FILTER_MASK(i32, int32_t)
GENERATE_TYPED_FILTER_MASK(i64, int64_t)
GENERATE_TYPED_FILTER_MASK(f32, float)
GENERATE_TYPED_FILTER_MASK(f64, double)
#undef GENERATE_TYPED_FILTER_MASK
