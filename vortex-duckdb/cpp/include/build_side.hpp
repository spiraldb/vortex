// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors
#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {
class Optimizer;
}

/// Prefer smaller estimated builds carrying variable-size payloads in Vortex inner equality joins.
/// Preserve compact and inflated build estimates, cap inflated probe costs at their source estimates,
/// and rebuild affected optimizer metadata.
void optimize_vortex_join_build_sides(duckdb::Optimizer &optimizer, duckdb::LogicalOperator &plan);
