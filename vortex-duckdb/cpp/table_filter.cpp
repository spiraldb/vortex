// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

#include "table_filter.h"
#include <cstdlib>
#include <cstring>
#include <mutex>

#if defined(__x86_64__) && (defined(__GNUC__) || defined(__clang__)) && !defined(_MSC_VER)
#include <immintrin.h>
#define VX_DUCKDB_VECTOR_BLOOM
#endif

#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/dynamic_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/struct_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/filter/bloom_filter.hpp"
#include "duckdb/planner/filter/selectivity_optional_filter.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace duckdb;

extern "C" idx_t duckdb_vx_table_filter_set_get(duckdb_vx_table_filter_set ffi_filter_set,
                                                size_t index,
                                                duckdb_vx_table_filter *table_filter_out) {
    auto &filters = reinterpret_cast<TableFilterSet *>(ffi_filter_set)->filters;
    if (filters.size() <= index) {
        *table_filter_out = nullptr;
        return 0;
    }
    auto &[column_idx, filter] = *std::next(filters.begin(), index);
    *table_filter_out = reinterpret_cast<duckdb_vx_table_filter>(filter.get());
    return column_idx;
}

extern "C" idx_t duckdb_vx_table_filter_set_size(duckdb_vx_table_filter_set ffi_filter_set) {
    auto filter_set = reinterpret_cast<TableFilterSet *>(ffi_filter_set);
    return filter_set->filters.size();
}

extern "C" duckdb_vx_table_filter_type duckdb_vx_table_filter_get_type(duckdb_vx_table_filter ffi_filter) {
    auto filter = reinterpret_cast<TableFilter *>(ffi_filter);
    return static_cast<duckdb_vx_table_filter_type>(filter->filter_type);
}

extern "C" const char *duckdb_vx_table_filter_to_debug_string(duckdb_vx_table_filter ffi_filter) {
    auto filter = reinterpret_cast<TableFilter *>(ffi_filter);
    auto str = filter->DebugToString();
    auto result = static_cast<char *>(duckdb_malloc(str.size() + 1));
    memcpy(result, str.c_str(), str.size() + 1);
    return result;
}

extern "C" void duckdb_vx_table_filter_get_constant(duckdb_vx_table_filter ffi_filter,
                                                    duckdb_vx_table_filter_constant *out) {
    if (!ffi_filter || !out) {
        return;
    }
    auto &filter = reinterpret_cast<TableFilter *>(ffi_filter)->Cast<ConstantFilter>();
    out->value = reinterpret_cast<duckdb_value>(&filter.constant);
    out->comparison_type = static_cast<duckdb_vx_expr_type>(filter.comparison_type);
}

extern "C" void duckdb_vx_table_filter_get_conjunction_or(duckdb_vx_table_filter ffi_filter,
                                                          duckdb_vx_table_filter_conjunction *out) {
    if (!ffi_filter || !out) {
        return;
    }
    auto &filter = reinterpret_cast<TableFilter *>(ffi_filter)->Cast<ConjunctionOrFilter>();
    out->children = reinterpret_cast<duckdb_vx_table_filter *>(filter.child_filters.data());
    out->children_count = filter.child_filters.size();
}

extern "C" void duckdb_vx_table_filter_get_conjunction_and(duckdb_vx_table_filter ffi_filter,
                                                           duckdb_vx_table_filter_conjunction *out) {
    if (!ffi_filter || !out) {
        return;
    }
    auto &filter = reinterpret_cast<TableFilter *>(ffi_filter)->Cast<ConjunctionAndFilter>();
    out->children = reinterpret_cast<duckdb_vx_table_filter *>(filter.child_filters.data());
    out->children_count = filter.child_filters.size();
}

// Wrapper to hold the shared pointer for dynamic filter data.
namespace {
struct DynamicFilterDataWrapper {
    shared_ptr<DynamicFilterData> data;

    explicit DynamicFilterDataWrapper(shared_ptr<DynamicFilterData> d) : data(std::move(d)) {
    }
};
} // namespace

extern "C" void duckdb_vx_table_filter_get_dynamic(duckdb_vx_table_filter ffi_filter,
                                                   duckdb_vx_table_filter_dynamic *out) {
    if (!ffi_filter || !out) {
        return;
    }
    auto &filter = reinterpret_cast<TableFilter *>(ffi_filter)->Cast<DynamicFilter>();

    // Hold the lock while accessing the filter data.
    std::lock_guard<std::mutex> lock(filter.filter_data->lock);

    auto data_wrapper = make_uniq<DynamicFilterDataWrapper>(filter.filter_data);
    out->data = reinterpret_cast<duckdb_vx_dynamic_filter_data>(data_wrapper.release());
    out->comparison_type = static_cast<duckdb_vx_expr_type>(filter.filter_data->filter->comparison_type);
}

extern "C" void duckdb_vx_dynamic_filter_data_free(duckdb_vx_dynamic_filter_data *ffi_data) {
    if (!ffi_data || !*ffi_data) {
        return;
    }
    delete reinterpret_cast<DynamicFilterDataWrapper *>(*ffi_data);
    *ffi_data = nullptr;
}

extern "C" duckdb_value duckdb_vx_dynamic_filter_data_get_value(duckdb_vx_dynamic_filter_data ffi_data) {
    if (!ffi_data) {
        return nullptr;
    }
    auto data_wrapper = reinterpret_cast<DynamicFilterDataWrapper *>(ffi_data);
    if (!data_wrapper->data) {
        return nullptr;
    }

    // Hold the lock while accessing the filter data.
    std::lock_guard<std::mutex> lock(data_wrapper->data->lock);

    if (!data_wrapper->data->filter || !data_wrapper->data->initialized) {
        return nullptr;
    }

    // Return a heap allocated copy of the value.
    return reinterpret_cast<duckdb_value>(new Value(data_wrapper->data->filter->constant));
}

extern "C" duckdb_vx_table_filter duckdb_vx_table_filter_get_optional(duckdb_vx_table_filter ffi_filter) {
    if (!ffi_filter) {
        return nullptr;
    }
    auto &filter = reinterpret_cast<TableFilter *>(ffi_filter)->Cast<OptionalFilter>();
    return reinterpret_cast<duckdb_vx_table_filter>(filter.child_filter.get());
}

extern "C" bool duckdb_vx_table_filter_get_optional_policy(duckdb_vx_table_filter ffi_filter,
                                                           float *threshold,
                                                           uint64_t *sample_rows) {
    const auto &filter = reinterpret_cast<TableFilter *>(ffi_filter)->Cast<OptionalFilter>();
    const auto *adaptive = dynamic_cast<const SelectivityOptionalFilter *>(&filter);
    if (!adaptive) {
        return false;
    }
    *threshold = adaptive->selectivity_threshold;
    *sample_rows = adaptive->n_vectors_to_check * STANDARD_VECTOR_SIZE;
    return true;
}

namespace {
struct BloomSnapshot {
    const vector<uint64_t> bits;
    const LogicalType key_type;
    const bool filters_nulls;
    const bool vector_probe;

    explicit BloomSnapshot(const BFTableFilter &filter)
        : bits(filter.GetBloomFilter().CopyBits()), key_type(filter.GetKeyType()),
          filters_nulls(filter.FiltersNullValues()), vector_probe(VectorProbeEnabled()) {
    }

private:
    static bool VectorProbeEnabled() {
#ifdef VX_DUCKDB_VECTOR_BLOOM
        const auto *enabled = std::getenv("VORTEX_DUCKDB_BLOOM_VECTOR");
        return (!enabled || std::strcmp(enabled, "0") != 0) && __builtin_cpu_supports("avx512f");
#else
        return false;
#endif
    }
};

template <bool ALL_VALID>
void ProbeBloomBits(const BloomSnapshot &snapshot,
                    const hash_t *hashes,
                    const UnifiedVectorFormat &values,
                    idx_t count,
                    uint8_t *output) {
    const auto sectors = snapshot.bits.data();
    const auto sector_mask = snapshot.bits.size() - 1;
    for (idx_t offset = 0; offset < count; offset += 8) {
        uint8_t matches_byte = 0;
        const auto width = MinValue<idx_t>(8, count - offset);
        for (idx_t lane = 0; lane < width; lane++) {
            const auto index = offset + lane;
            bool matches;
            if (!ALL_VALID && !values.validity.RowIsValid(values.sel->get_index(index))) {
                matches = !snapshot.filters_nulls;
            } else {
                const auto hash = hashes[index];
                const auto shifts = reinterpret_cast<const uint8_t *>(&hash);
                uint64_t mask = 0;
                for (idx_t j = 4; j < 8; j++) {
                    mask |= uint64_t(1) << (shifts[j] & 63);
                }
                matches = (sectors[hash & sector_mask] & mask) == mask;
            }
            matches_byte |= uint8_t(matches) << lane;
        }
        output[offset / 8] = matches_byte;
    }
}
#ifdef VX_DUCKDB_VECTOR_BLOOM
// One lane per row makes the comparison mask the packed output byte, without a selection vector.
__attribute__((target("avx512f"))) void ProbeBloomBitsVector(const BloomSnapshot &snapshot,
                                                             const hash_t *hashes,
                                                             const UnifiedVectorFormat &values, idx_t count,
                                                             uint8_t *output) {
    const auto sectors = snapshot.bits.data();
    const auto sector_mask = _mm512_set1_epi64(snapshot.bits.size() - 1);
    const auto bit_mask = _mm512_set1_epi64(63);
    const auto one = _mm512_set1_epi64(1);
    idx_t offset = 0;
    for (; offset + 8 <= count; offset += 8) {
        const auto hash = _mm512_loadu_si512(hashes + offset);
        const auto indexes = _mm512_and_si512(hash, sector_mask);
        const auto words = _mm512_mask_i64gather_epi64(_mm512_setzero_si512(), 0xff, indexes, sectors, 8);
        const auto mask0 = _mm512_maskz_sllv_epi64(
            0xff, one, _mm512_and_si512(_mm512_maskz_srli_epi64(0xff, hash, 32), bit_mask));
        const auto mask1 = _mm512_maskz_sllv_epi64(
            0xff, one, _mm512_and_si512(_mm512_maskz_srli_epi64(0xff, hash, 40), bit_mask));
        const auto mask2 = _mm512_maskz_sllv_epi64(
            0xff, one, _mm512_and_si512(_mm512_maskz_srli_epi64(0xff, hash, 48), bit_mask));
        const auto mask3 = _mm512_maskz_sllv_epi64(
            0xff, one, _mm512_and_si512(_mm512_maskz_srli_epi64(0xff, hash, 56), bit_mask));
        const auto masks = _mm512_or_si512(_mm512_or_si512(mask0, mask1), _mm512_or_si512(mask2, mask3));
        output[offset / 8] = _mm512_cmpeq_epi64_mask(_mm512_and_si512(words, masks), masks);
    }
    if (offset < count) {
        ProbeBloomBits<true>(snapshot, hashes + offset, values, count - offset, output + offset / 8);
    }
}
#endif
} // namespace

extern "C" duckdb_vx_bloom_snapshot duckdb_vx_table_filter_copy_bloom(duckdb_vx_table_filter ffi_filter) {
    const auto &filter = reinterpret_cast<TableFilter *>(ffi_filter)->Cast<BFTableFilter>();
    auto snapshot = make_uniq<BloomSnapshot>(filter);
    if (snapshot->bits.empty()) {
        return nullptr;
    }
    return reinterpret_cast<duckdb_vx_bloom_snapshot>(snapshot.release());
}

extern "C" void duckdb_vx_bloom_snapshot_free(duckdb_vx_bloom_snapshot *snapshot) {
    delete reinterpret_cast<BloomSnapshot *>(*snapshot);
    *snapshot = nullptr;
}

extern "C" duckdb_logical_type duckdb_vx_bloom_snapshot_key_type(duckdb_vx_bloom_snapshot snapshot) {
    return reinterpret_cast<duckdb_logical_type>(
        new LogicalType(reinterpret_cast<BloomSnapshot *>(snapshot)->key_type));
}

extern "C" void duckdb_vx_bloom_snapshot_apply(duckdb_vx_bloom_snapshot ffi_snapshot,
                                               duckdb_vector ffi_keys,
                                               duckdb_vector ffi_hashes,
                                               idx_t count,
                                               uint8_t *output) {
    const auto &snapshot = *reinterpret_cast<const BloomSnapshot *>(ffi_snapshot);
    auto &keys = *reinterpret_cast<Vector *>(ffi_keys);
    auto &hashes = *reinterpret_cast<Vector *>(ffi_hashes);
    D_ASSERT(count <= STANDARD_VECTOR_SIZE);
    D_ASSERT(keys.GetType() == snapshot.key_type);
    D_ASSERT(hashes.GetType() == LogicalType::HASH);
    VectorOperations::Hash(keys, hashes, count);
    hashes.Flatten(count);
    const auto hash_data = FlatVector::GetData<hash_t>(hashes);
    UnifiedVectorFormat values;
    keys.ToUnifiedFormat(count, values);
    if (values.validity.AllValid()) {
#ifdef VX_DUCKDB_VECTOR_BLOOM
        if (snapshot.vector_probe && count >= 8) {
            ProbeBloomBitsVector(snapshot, hash_data, values, count, output);
            return;
        }
#endif
        ProbeBloomBits<true>(snapshot, hash_data, values, count, output);
    } else {
        ProbeBloomBits<false>(snapshot, hash_data, values, count, output);
    }
}

extern "C" duckdb_vx_expr duckdb_vx_table_filter_get_expression(duckdb_vx_table_filter ffi_filter) {
    if (!ffi_filter) {
        return nullptr;
    }
    auto &filter = reinterpret_cast<TableFilter *>(ffi_filter)->Cast<ExpressionFilter>();
    return reinterpret_cast<duckdb_vx_expr>(filter.expr.get());
}

extern "C" void duckdb_vx_table_filter_get_struct_extract(duckdb_vx_table_filter ffi_filter,
                                                          duckdb_vx_table_filter_struct_extract *out) {
    if (!ffi_filter || !out) {
        return;
    }
    auto &filter = reinterpret_cast<TableFilter *>(ffi_filter)->Cast<StructFilter>();

    out->child_filter = reinterpret_cast<duckdb_vx_table_filter>(filter.child_filter.get());
    out->child_name_len = filter.child_name.size();
    out->child_name = static_cast<char *>(filter.child_name.data());
}

extern "C" void duckdb_vx_table_filter_get_in_filter(duckdb_vx_table_filter ffi_filter,
                                                     duckdb_vx_table_filter_in_filter *out) {
    if (!ffi_filter || !out) {
        return;
    }
    auto &filter = reinterpret_cast<TableFilter *>(ffi_filter)->Cast<InFilter>();

    out->values_count = filter.values.size();
    out->values = reinterpret_cast<duckdb_vx_values_vec>(&filter.values);
}

extern "C" duckdb_value duckdb_vx_values_vec_get(duckdb_vx_values_vec ffi_vec, size_t idx) {
    if (!ffi_vec) {
        return nullptr;
    }
    auto vec = reinterpret_cast<vector<Value> *>(ffi_vec);
    if (idx >= vec->size()) {
        return nullptr;
    }
    return reinterpret_cast<duckdb_value>(&(*vec)[idx]);
}
