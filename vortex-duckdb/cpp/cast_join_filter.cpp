// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

#include "cast_join_filter.hpp"
#include "table_function.hpp"

#include "duckdb/function/aggregate/distributive_function_utils.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/optimizer/join_filter_pushdown_optimizer.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/filter/dynamic_filter.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#include <cstdlib>
#include <cstring>

using namespace duckdb;

namespace {

bool ExactDoubleInput(const LogicalType &type) {
    switch (type.id()) {
    case LogicalTypeId::TINYINT:
    case LogicalTypeId::SMALLINT:
    case LogicalTypeId::INTEGER:
    case LogicalTypeId::UTINYINT:
    case LogicalTypeId::USMALLINT:
    case LogicalTypeId::UINTEGER:
        return true;
    default:
        return false;
    }
}

void AddCastFilter(Optimizer &optimizer, LogicalComparisonJoin &join) {
    if (join.join_type != JoinType::INNER || join.conditions.size() != 1 || join.filter_pushdown)
        return;
    auto &condition = join.conditions[0];
    if (condition.comparison != ExpressionType::COMPARE_EQUAL ||
        condition.left->GetExpressionClass() != ExpressionClass::BOUND_CAST)
        return;
    const auto &cast = condition.left->Cast<BoundCastExpression>();
    if (cast.return_type.id() != LogicalTypeId::DOUBLE || condition.right->return_type != cast.return_type ||
        !ExactDoubleInput(cast.child->return_type) ||
        cast.child->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF)
        return;
    const auto &column = cast.child->Cast<BoundColumnRefExpression>();
    JoinFilterPushdownColumn filter;
    filter.probe_column_index = column.binding;
    vector<JoinFilterPushdownColumn> columns;
    columns.push_back(std::move(filter));
    vector<PushdownFilterTarget> targets;
    JoinFilterPushdownOptimizer::GetPushdownFilterTargets(*join.children[0], std::move(columns), targets);
    vector<idx_t> eligible;
    for (idx_t i = 0; i < targets.size(); ++i) {
        const auto &target = targets[i];
        if (is_vortex_scan(target.get.function) && target.columns.size() == 1 &&
            target.columns[0].storage_type == cast.child->return_type)
            eligible.push_back(i);
    }
    if (eligible.empty())
        return;
    auto info = make_uniq<JoinFilterPushdownInfo>();
    info->join_condition.push_back(0);
    vector<AggregateFunction> aggregates;
    aggregates.push_back(MinFunction::GetFunction());
    aggregates.push_back(MaxFunction::GetFunction());
    for (const auto &aggregate : aggregates) {
        FunctionBinder binder(optimizer.GetContext());
        vector<unique_ptr<Expression>> arguments;
        arguments.push_back(condition.right->Copy());
        auto expression = binder.BindAggregateFunction(aggregate, std::move(arguments), nullptr,
                                                       AggregateType::NON_DISTINCT);
        if (expression->children.size() != 1)
            return;
        info->min_max_aggregates.push_back(std::move(expression));
    }
    for (const auto index : eligible) {
        auto &target = targets[index];
        auto &get = target.get;
        if (!get.dynamic_filters)
            get.dynamic_filters = make_shared_ptr<DynamicTableFilterSet>();
        // Vortex evaluates the exact cast before applying filters in the join's key type.
        target.columns[0].storage_type = cast.return_type;
        JoinFilterPushdownFilter probe;
        probe.dynamic_filters = get.dynamic_filters;
        probe.columns = std::move(target.columns);
        info->probe_info.push_back(std::move(probe));
    }
    info->build_side_has_filter = JoinFilterPushdownOptimizer::IsFiltering(join.children[1]);
    join.filter_pushdown = std::move(info);
}

void VisitCastJoins(Optimizer &optimizer, LogicalOperator &op) {
    if (op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN)
        return;
    if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
        AddCastFilter(optimizer, op.Cast<LogicalComparisonJoin>());
    }
    for (auto &child : op.children)
        VisitCastJoins(optimizer, *child);
}

} // namespace

void add_exact_cast_join_filters(Optimizer &optimizer, LogicalOperator &plan) {
    const auto *enabled = std::getenv("VORTEX_DUCKDB_DOUBLE_CAST_FILTERS");
    if (!enabled || std::strcmp(enabled, "0") != 0)
        VisitCastJoins(optimizer, plan);
}
