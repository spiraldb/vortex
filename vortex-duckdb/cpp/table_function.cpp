// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

#include "data.hpp"
#include "error.hpp"
#include "table_function.hpp"
#include "multi_file_reader.hpp"
#include "expr.h"
#include "vortex_duckdb.h"
#include "table_function.h"
#include "vortex.h"

#include "duckdb.h"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/default/default_functions.hpp"
#include "duckdb/common/insertion_order_preserving_map.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/function/partition_stats.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/storage/storage_index.hpp"
#include "duckdb/storage/table/row_group_reorderer.hpp"
#include <map>
#include <cstdlib>
#include <cstring>

using namespace std::string_literals;
constexpr column_t COLUMN_IDENTIFIER_FILE_INDEX = MultiFileReader::COLUMN_IDENTIFIER_FILE_INDEX;
constexpr column_t COLUMN_IDENTIFIER_FILE_ROW_NUMBER = MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER;

// This is a flaw of Duckdb API which doesn't allow passing non-const
// expressions. We never modify the value on Rust side.
static duckdb_vx_expr get_ffi_expr(const Expression &expr) {
    return reinterpret_cast<duckdb_vx_expr>(const_cast<Expression *>(&expr));
}

static void *get_ffi_bind(const FunctionData *bind_data) {
    return bind_data->Cast<MultiFileBindData>().bind_data->Cast<VortexBindData>().ffi_bind_data->DataPtr();
}

bool projection_expression_pushdown(ClientContext &, const TableFunctionProjectionExpressionInput &input) {
    duckdb_vx_expr ffi_expr = get_ffi_expr(input.expression);
    void *const ffi_bind = get_ffi_bind(input.get.bind_data.get());
    duckdb_vx_error error_out = nullptr;

    const bool ret = duckdb_table_function_pushdown_projection_expression( //
        ffi_bind,
        ffi_expr,
        input.projection_idx,
        &error_out);
    if (error_out) {
        throw BinderException(IntoErrString(error_out));
    }
    return ret;
}

extern "C" {
idx_t duckdb_vx_aggregate_len(duckdb_vx_agg_input ffi_input) {
    return reinterpret_cast<const TableFunctionUngroupedAggregateInput *>(ffi_input)->projections.size();
}

duckdb_vx_expr duckdb_vx_aggregate_at(duckdb_vx_agg_input ffi_input, idx_t i, idx_t *proj_idx) {
    const auto &input = *reinterpret_cast<const TableFunctionUngroupedAggregateInput *>(ffi_input);
    const auto &[scan_index, expr] = input.projections[i];
    *proj_idx = scan_index == COUNT_STAR_PROJ_IDX ? scan_index
                                                  : input.get.GetColumnIds()[scan_index].GetPrimaryIndex();
    return get_ffi_expr(expr);
}
}

bool aggregate_pushdown(ClientContext &, const TableFunctionUngroupedAggregateInput &input) {
    void *const ffi_bind = get_ffi_bind(input.get.bind_data.get());
    duckdb_vx_error error_out = nullptr;
    const auto ffi_input =
        reinterpret_cast<duckdb_vx_agg_input>(const_cast<TableFunctionUngroupedAggregateInput *>(&input));
    const bool res = duckdb_table_function_pushdown_projection_aggregates(ffi_bind, ffi_input, &error_out);
    if (error_out) {
        throw BinderException(IntoErrString(error_out));
    }
    return res;
}

using FilterVec = vector<unique_ptr<Expression>>;

void pushdown_complex_filter(const FunctionData &bind_data, FilterVec &filters) {
    void *const ffi_bind = get_ffi_bind(&bind_data);
    duckdb_vx_error error_out = nullptr;

    for (auto iter = filters.begin(); iter != filters.end();) {
        duckdb_vx_expr ffi_expr = reinterpret_cast<duckdb_vx_expr>(iter->get());

        const bool pushed = duckdb_table_function_pushdown_complex_filter(ffi_bind, ffi_expr, &error_out);
        if (error_out) {
            throw BinderException(IntoErrString(error_out));
        }
        iter = pushed ? filters.erase(iter) : std::next(iter);
    }
}

extern "C" void duckdb_vx_tfunc_bind_result_add_column(duckdb_bind_result ffi_result,
                                                       const char *name_str,
                                                       size_t name_len,
                                                       duckdb_logical_type ffi_type) {
    D_ASSERT(ffi_result);
    D_ASSERT(name_str);
    D_ASSERT(ffi_type);
    VortexBindResult &result = *reinterpret_cast<VortexBindResult *>(ffi_result);
    const LogicalType logical_type = *reinterpret_cast<LogicalType *>(ffi_type);

    result.names.emplace_back(name_str, name_len);
    result.return_types.emplace_back(logical_type);
}

extern "C" void duckdb_vx_string_map_insert(duckdb_vx_string_map map, const char *key, const char *value) {
    D_ASSERT(map);
    D_ASSERT(key);
    D_ASSERT(value);
    reinterpret_cast<InsertionOrderPreservingMap<string> *>(map)->insert(key, value);
}

InsertionOrderPreservingMap<string> to_string(TableFunctionToStringInput &input) {
    InsertionOrderPreservingMap<string> result;
    duckdb_vx_string_map ffi_map = reinterpret_cast<duckdb_vx_string_map>(&result);
    const void *const ffi_bind = get_ffi_bind(input.bind_data.get());
    duckdb_table_function_to_string(ffi_bind, ffi_map);
    return result;
}

bool is_vortex_scan(const TableFunction &function) {
    return function.bind == MultiFileFunction<VortexReaderInterface>::MultiFileBind;
}

static bool RestoreConsumedFilterHints(LogicalOperator &op, bool require_reduced_estimate) {
    if (op.type == LogicalOperatorType::LOGICAL_GET) {
        auto &get = op.Cast<LogicalGet>();
        if (is_vortex_scan(get.function) && get.bind_data) {
            const auto *bind = get_ffi_bind(get.bind_data.get());
            if (!require_reduced_estimate) {
                return duckdb_table_function_has_consumed_filter(bind, 1, 0);
            }
            if (!get.has_estimated_cardinality) {
                return false;
            }
            const auto &data = get.bind_data->Cast<MultiFileBindData>();
            const auto count = data.file_list->GetFileCount();
            if (count.type != FileExpansionType::ALL_FILES_EXPANDED) {
                return false;
            }
            return duckdb_table_function_has_consumed_filter(bind, count.count, get.estimated_cardinality);
        }
    }

    LogicalComparisonJoin *join = nullptr;
    idx_t build_index = DConstants::INVALID_INDEX;
    if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
        join = &op.Cast<LogicalComparisonJoin>();
        if (join->filter_pushdown && op.children.size() == 2) {
            build_index = op.children[1]->type == LogicalOperatorType::LOGICAL_DELIM_GET ? 0 : 1;
        }
    }
    bool has_consumed_filter = false;
    for (idx_t index = 0; index < op.children.size(); ++index) {
        const bool child_has_filter = RestoreConsumedFilterHints(*op.children[index], require_reduced_estimate);
        has_consumed_filter |= child_has_filter;
        if (child_has_filter && index == build_index) {
            // DuckDB's IsFiltering cannot see consumed predicates. Requiring a reduced scan
            // estimate excludes late join bounds that trim little and create costly Bloom filters.
            join->filter_pushdown->build_side_has_filter = true;
        }
    }
    return has_consumed_filter;
}

void restore_join_filter_hints(LogicalOperator &plan) {
    const auto *enabled = std::getenv("VORTEX_DUCKDB_JOIN_FILTER_HINT");
    if (!enabled || std::strcmp(enabled, "0") != 0) {
        RestoreConsumedFilterHints(plan, !enabled || std::strcmp(enabled, "1") != 0);
    }
}

unique_ptr<MultiFileReader> get_multi_file_reader(const TableFunction &) {
    return make_uniq<VortexMultiFileReader>();
}

unique_ptr<BaseStatistics> VortexRowGroup::GetColumnStatistics(const StorageIndex &storage_index) {
    if (storage_index.HasChildren()) {
        return {};
    }
    duckdb_column_statistics statistics = {};
    const idx_t idx = storage_index.GetPrimaryIndex();
    const void *const ffi_footer_ptr = ffi_footer->DataPtr();
    if (!duckdb_footer_get_statistics(ffi_footer_ptr, idx, &statistics)) {
        return {};
    }
    return to_duckdb_statistics(statistics);
}

static vector<PartitionStatistics> partition_stats(const MultiFileBindData &bind_data) {
    VortexBindData &bind = bind_data.bind_data->Cast<VortexBindData>();
    void *const ffi_bind = bind.ffi_bind_data->DataPtr();

    if (!duckdb_table_function_can_get_partition_stats(ffi_bind)) {
        return {};
    }

    vector<OpenFileInfo> files = bind_data.file_list->GetAllFiles();
    vector<PartitionStatistics> result(files.size());
    idx_t row_start = 0;
    uint64_t count = 0;
    duckdb_vx_error error = nullptr;
    for (size_t i = 0; i < files.size(); ++i) {
        const std::string_view path = files[i].path;
        duckdb_vx_data raw = duckdb_footer_get_cached(ffi_bind, path.data(), path.size(), &count, &error);
        unique_ptr<CData> cdata(reinterpret_cast<CData *>(raw));
        if (error) {
            throw BinderException(IntoErrString(error));
        }
        if (!cdata) {
            return {};
        }

        PartitionStatistics &stats = result[i];
        stats.row_start = row_start;
        stats.count = count;
        stats.count_type = CountType::COUNT_EXACT;
        row_start += count;
        stats.partition_row_group = make_shared_ptr<VortexRowGroup>(std::move(cdata));
    }
    return result;
}

static vector<PartitionStatistics> get_partition_stats(ClientContext &, GetPartitionStatsInput &input) {
    return partition_stats(input.bind_data->Cast<MultiFileBindData>());
}

static void set_scan_order(unique_ptr<RowGroupOrderOptions> options, optional_ptr<FunctionData> bind_data) {
    const auto &multi_bind = bind_data->Cast<MultiFileBindData>();
    auto &bind = multi_bind.bind_data->Cast<VortexBindData>();
    bind.offset_pruned_files.clear();
    if (options->row_group_offset || options->leading_null_group_offset) {
        // DuckDB has already reduced the query's OFFSET. Skip the corresponding original files,
        // retaining their file IDs for the second scan used by late materialization.
        auto partitions = partition_stats(multi_bind);
        std::multimap<Value, idx_t> ordered;
        vector<idx_t> null_only;
        for (idx_t i = 0; i < partitions.size(); i++) {
            auto stats = partitions[i].partition_row_group->GetColumnStatistics(options->column_idx);
            if (!stats) {
                throw InternalException("Missing Vortex statistics after offset pruning");
            }
            if (!stats->CanHaveNoNull()) {
                null_only.push_back(i);
                continue;
            }
            auto value = RowGroupReorderer::RetrieveStat(*stats, options->order_by, options->column_type);
            if (!value.IsNull()) {
                ordered.emplace(std::move(value), i);
            }
        }
        if (null_only.size() < options->leading_null_group_offset ||
            ordered.size() < options->row_group_offset) {
            throw InternalException("Invalid Vortex offset-pruning groups");
        }
        bind.offset_pruned_files.insert(bind.offset_pruned_files.end(),
                                        null_only.begin(),
                                        null_only.begin() + options->leading_null_group_offset);
        if (options->order_type == OrderType::ASCENDING) {
            auto it = ordered.begin();
            for (idx_t i = 0; i < options->row_group_offset; i++, ++it) {
                bind.offset_pruned_files.push_back(it->second);
            }
        } else {
            auto it = ordered.rbegin();
            for (idx_t i = 0; i < options->row_group_offset; i++, ++it) {
                bind.offset_pruned_files.push_back(it->second);
            }
        }
    }
    std::sort(bind.offset_pruned_files.begin(), bind.offset_pruned_files.end());
    const auto column_idx =
        options->column_idx.HasChildren() ? DConstants::INVALID_INDEX : options->column_idx.GetPrimaryIndex();
    duckdb_table_function_set_scan_order(bind.ffi_bind_data->DataPtr(),
                                         column_idx,
                                         options->order_type == OrderType::DESCENDING,
                                         options->null_order == OrderByNullType::NULLS_FIRST);
}

duckdb_state register_table_function(DatabaseInstance &db, LogicalType parameter, const std::string &name) {
    MultiFileFunction<VortexReaderInterface> fn(name);
    fn.arguments[0] = parameter;
    fn.named_parameters = {{"filename", LogicalType::ANY},
                           {"allow_empty", LogicalType::BOOLEAN},
                           {"hive_partitioning", LogicalType::BOOLEAN}};

    fn.filter_pushdown = true;
    fn.filter_prune = true;
    // Pushed-down filters, projections and aggregates live in the FFI bind data, which has no
    // serializer. DuckDB's common-subplan optimizer keys scans on their serialized form, so
    // without this two scans of the same file with different pushed-down filters look identical
    // and are merged into one shared CTE, returning the wrong rows for one of them.
    fn.verify_serialization = false;

    fn.pushdown_expression = [](auto &, const auto &, Expression &expression) {
        return duckdb_table_function_pushdown_expression(reinterpret_cast<duckdb_vx_expr>(&expression));
    };
    fn.pushdown_complex_filter = [](auto &, auto &, FunctionData *bind_data, FilterVec &filters) {
        pushdown_complex_filter(*bind_data, filters);
    };
    fn.to_string = to_string;

    fn.late_materialization = true;
    // Columns that uniquely identify a row for deferred re-fetch in a multi
    // file scan: (file index, row number in file).
    fn.get_row_id_columns = [](auto &, auto) -> vector<column_t> {
        return {COLUMN_IDENTIFIER_FILE_INDEX, COLUMN_IDENTIFIER_FILE_ROW_NUMBER};
    };

    fn.statistics = MultiFileFunction<VortexReaderInterface>::MultiFileScanStats;
    fn.get_partition_stats = get_partition_stats;
    fn.set_scan_order = set_scan_order;
    fn.get_multi_file_reader = get_multi_file_reader;

    /**
     * duckdb's serialization is broken. If you don't set serialize/deserialize
     * callbacks, duckdb serializes only the internal state which doesn't work
     * for Vortex if you have filters pushed down. Worse, duckdb uses this
     * information for CommonSubplanOptimizer which then merges different
     * Vortex scans (with different filters pushed down) into one scan in tpcds.
     *
     * However, this is a regression on q15 and such where we do have
     * completely equal scans which can't be merged. This is a reasonable price
     * for correctness.
     *
     * Very unexpectedly verify_serialization doesn't do any verification but
     * disables serialization at all.
     */
    fn.verify_serialization = false;
    fn.serialize = [](auto &, auto, auto &) {
        throw NotImplementedException("Can't serialize Vortex state");
    };
    fn.deserialize = [](auto &, auto &) -> unique_ptr<FunctionData> {
        throw NotImplementedException("Can't deserialize Vortex state");
    };

    try {
        auto &system_catalog = Catalog::GetSystemCatalog(db);
        auto data = CatalogTransaction::GetSystemTransaction(db);
        CreateTableFunctionInfo tf_info(fn);
        tf_info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
        system_catalog.CreateFunction(data, tf_info);
    } catch (const std::exception &e) {
        ErrorData data(e);
        DUCKDB_LOG_ERROR(db, "Failed to create Vortex table function:\t" + data.Message());
        return DuckDBError;
    }
    return DuckDBSuccess;
}

extern "C" duckdb_state duckdb_vx_register_version_function(duckdb_database ffi_db, const char *version) {
    D_ASSERT(ffi_db);
    D_ASSERT(version);
    const DatabaseWrapper &wrapper = *reinterpret_cast<DatabaseWrapper *>(ffi_db);
    DatabaseInstance &db = *wrapper.database->instance;

    const string quoted = KeywordHelper::WriteQuoted(version);

    const DefaultMacro macro {DEFAULT_SCHEMA,
                              "vortex_version",
                              {nullptr},
                              {{nullptr, nullptr}},
                              quoted.c_str()};
    try {
        auto info = DefaultFunctionGenerator::CreateInternalMacroInfo(macro);
        auto &system_catalog = Catalog::GetSystemCatalog(db);
        auto data = CatalogTransaction::GetSystemTransaction(db);
        system_catalog.CreateFunction(data, *info);
    } catch (const std::exception &e) {
        ErrorData data(e);
        DUCKDB_LOG_ERROR(db, "Failed to create the vortex_version macro:\t" + data.Message());
        return DuckDBError;
    }
    return DuckDBSuccess;
}

extern "C" duckdb_state duckdb_vx_register_table_functions(duckdb_database ffi_db) {
    D_ASSERT(ffi_db);
    const DatabaseWrapper &wrapper = *reinterpret_cast<DatabaseWrapper *>(ffi_db);
    DatabaseInstance &db = *wrapper.database->instance;

    for (LogicalType type : {LogicalType(LogicalType::VARCHAR), LogicalType::LIST(LogicalType::VARCHAR)}) {
        for (const std::string &name : {"read_vortex"s, "vortex_scan"s}) {
            if (register_table_function(db, type, name) == DuckDBError) {
                return DuckDBError;
            }
        }
    }
    return DuckDBSuccess;
}
