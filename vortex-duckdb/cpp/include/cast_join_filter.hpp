// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors
#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {
class Optimizer;
}

/// Add runtime join-filter targets for exact integer-to-DOUBLE casts over Vortex scans.
void add_exact_cast_join_filters(duckdb::Optimizer &optimizer, duckdb::LogicalOperator &plan);
