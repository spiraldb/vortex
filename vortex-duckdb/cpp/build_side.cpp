// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

#include "build_side.hpp"
#include "table_function.hpp"

#include "duckdb/optimizer/column_lifetime_analyzer.hpp"
#include "duckdb/optimizer/join_filter_pushdown_optimizer.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_order.hpp"

#include <algorithm>
#include <cstdlib>
#include <cstring>

using namespace duckdb;

namespace {

bool SupportedPlan(const LogicalOperator &op) {
    switch (op.type) {
    case LogicalOperatorType::LOGICAL_GET:
        return op.children.empty() && is_vortex_scan(op.Cast<LogicalGet>().function);
    case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
        const auto &join = op.Cast<LogicalComparisonJoin>();
        if (join.join_type != JoinType::INNER || join.conditions.empty())
            return false;
        for (const auto &condition : join.conditions) {
            if (condition.comparison != ExpressionType::COMPARE_EQUAL)
                return false;
        }
        break;
    }
    case LogicalOperatorType::LOGICAL_PROJECTION:
    case LogicalOperatorType::LOGICAL_FILTER:
    case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
    case LogicalOperatorType::LOGICAL_ORDER_BY:
    case LogicalOperatorType::LOGICAL_TOP_N:
    case LogicalOperatorType::LOGICAL_LIMIT:
        break;
    default:
        return false;
    }
    for (const auto &child : op.children) {
        if (!SupportedPlan(*child))
            return false;
    }
    return true;
}

idx_t PreferSmallerBuilds(LogicalOperator &op, bool &changed, bool cap_estimates) {
    if (op.type == LogicalOperatorType::LOGICAL_GET) {
        return op.has_estimated_cardinality ? op.estimated_cardinality : 0;
    }
    idx_t maximum_scan_rows = 0;
    idx_t child_scan_rows[2] = {0, 0};
    for (idx_t i = 0; i < op.children.size(); ++i) {
        const auto rows = PreferSmallerBuilds(*op.children[i], changed, cap_estimates);
        maximum_scan_rows = std::max(maximum_scan_rows, rows);
        if (i < 2)
            child_scan_rows[i] = rows;
    }
    if (op.type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN)
        return maximum_scan_rows;
    const auto &left = *op.children[0];
    const auto &right = *op.children[1];
    if (!left.has_estimated_cardinality || !right.has_estimated_cardinality || right.types.size() <= 1) {
        return maximum_scan_rows;
    }
    auto left_rows = left.estimated_cardinality;
    const auto right_rows = right.estimated_cardinality;
    // Preserve width costing when the current build's estimate is inflated. When only the
    // probe estimate is inflated, a source-sized cap can expose a selective compound build.
    if (right_rows > child_scan_rows[1])
        return maximum_scan_rows;
    if (cap_estimates) {
        if (child_scan_rows[0] == 0)
            return maximum_scan_rows;
        left_rows = std::min(left_rows, child_scan_rows[0]);
    } else if (left_rows > child_scan_rows[0]) {
        return maximum_scan_rows;
    }
    if (left_rows >= right_rows)
        return maximum_scan_rows;
    bool variable_payload = false;
    for (const auto &type : right.types) {
        variable_payload |= type.InternalType() == PhysicalType::VARCHAR || type.IsNested();
    }
    if (!variable_payload)
        return maximum_scan_rows;

    auto &join = op.Cast<LogicalComparisonJoin>();
    std::swap(op.children[0], op.children[1]);
    for (auto &condition : join.conditions)
        std::swap(condition.left, condition.right);
    std::swap(join.left_projection_map, join.right_projection_map);
    changed = true;
    return maximum_scan_rows;
}

void ClearProjectionMapsAndFilters(LogicalOperator &op) {
    switch (op.type) {
    case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
        auto &join = op.Cast<LogicalComparisonJoin>();
        join.left_projection_map.clear();
        join.right_projection_map.clear();
        join.filter_pushdown.reset();
        break;
    }
    case LogicalOperatorType::LOGICAL_FILTER:
        op.Cast<LogicalFilter>().projection_map.clear();
        break;
    case LogicalOperatorType::LOGICAL_ORDER_BY:
        op.Cast<LogicalOrder>().projection_map.clear();
        break;
    default:
        break;
    }
    for (auto &child : op.children)
        ClearProjectionMapsAndFilters(*child);
}

} // namespace

void optimize_vortex_join_build_sides(Optimizer &optimizer, LogicalOperator &plan) {
    const auto *enabled = std::getenv("VORTEX_DUCKDB_BUILD_ROWS");
    if ((enabled && std::strcmp(enabled, "0") == 0) || !SupportedPlan(plan))
        return;
    plan.ResolveOperatorTypes();
    bool changed = false;
    const auto *cap = std::getenv("VORTEX_DUCKDB_BUILD_CAP");
    PreferSmallerBuilds(plan, changed, !cap || std::strcmp(cap, "0") != 0);
    if (!changed)
        return;
    // Projection maps index child output positions, which the swaps change. Join filters also
    // need new probe targets and build-side aggregates before physical planning.
    ClearProjectionMapsAndFilters(plan);
    ColumnLifetimeAnalyzer(optimizer, plan, true).VisitOperator(plan);
    plan.ResolveOperatorTypes();
    JoinFilterPushdownOptimizer(optimizer).VisitOperator(plan);
}
