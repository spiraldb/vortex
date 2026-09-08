// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Rendering of DuckDB `EXPLAIN (FORMAT json)` plans as DataFusion-style text.
//!
//! DuckDB's JSON plans are complete but verbose: a TPC-H plan runs to hundreds of lines, most
//! of them braces. [`render_plan`] flattens a plan into the form DataFusion's sqllogictest engine
//! uses for its own plans: one operator per line, numbered, indented with `--` per level of
//! nesting, with the operator's `extra_info` inlined as `key=value` pairs:
//!
//! ```text
//! 01)ORDER_BY: order_by=[lineitem.l_returnflag, lineitem.l_linestatus]
//! 02)--PROJECTION: expressions=[l_returnflag, l_linestatus, sum_qty], estimated_cardinality=6
//! 03)----READ_VORTEX: filters=($.l_shipdate <= 1998-09-02), function=Vortex Scan
//! ```
//!
//! The rendering is deliberately lossy compared to the JSON: keys are snake-cased, empty entries
//! are dropped, and lists lose their quoting. It keeps what plan assertions care about, the
//! operator tree and what each operator does, and drops the syntax around it.
//!
//! [`render_explain_rows`] applies the rendering to the rows of an `EXPLAIN` query so that
//! expected output in `.slt` files reads like the DataFusion plans next to it.

use std::fmt;

use serde::Deserialize;
use serde::Deserializer;
use serde::de::MapAccess;
use serde::de::Visitor;

/// One operator of a DuckDB plan, as emitted by `EXPLAIN (FORMAT json)`.
#[derive(Debug, Deserialize)]
pub struct PlanNode {
    name: String,
    #[serde(default)]
    children: Vec<PlanNode>,
    #[serde(default)]
    extra_info: ExtraInfo,
}

/// The `extra_info` entries of an operator, in the order DuckDB emitted them.
///
/// A plain map would sort or hash the keys; the emitted order groups related entries
/// (a scan's filters next to its projections) and is what plan assertions get to see.
#[derive(Debug, Default)]
struct ExtraInfo(Vec<(String, ExtraValue)>);

/// A single `extra_info` value.
///
/// DuckDB emits a string, or a list of strings for multi-valued entries such as projections.
/// Anything else is kept as raw JSON rather than failing the whole plan.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ExtraValue {
    Text(String),
    List(Vec<String>),
    Other(serde_json::Value),
}

impl<'de> Deserialize<'de> for ExtraInfo {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ExtraInfoVisitor;

        impl<'de> Visitor<'de> for ExtraInfoVisitor {
            type Value = ExtraInfo;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a map of extra_info entries")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut entries = Vec::with_capacity(map.size_hint().unwrap_or_default());
                while let Some(entry) = map.next_entry()? {
                    entries.push(entry);
                }
                Ok(ExtraInfo(entries))
            }
        }

        deserializer.deserialize_map(ExtraInfoVisitor)
    }
}

/// Parses the JSON that DuckDB's `EXPLAIN (FORMAT json)` emits for one plan.
///
/// The document is an array of root operators, almost always exactly one.
pub fn parse_plan(json: &str) -> serde_json::Result<Vec<PlanNode>> {
    serde_json::from_str(json)
}

/// Renders a parsed plan as numbered, `--`-indented lines, one per operator, depth first
/// with each operator's children in DuckDB's order.
pub fn render_plan(roots: &[PlanNode]) -> Vec<String> {
    let mut lines = Vec::new();
    for root in roots {
        render_node(root, 0, &mut lines);
    }
    lines
}

fn render_node(node: &PlanNode, depth: usize, lines: &mut Vec<String>) {
    let mut line = format!("{:02}){}{}", lines.len() + 1, "--".repeat(depth), node.name);
    let entries = node
        .extra_info
        .0
        .iter()
        .filter_map(|(key, value)| {
            render_value(value).map(|value| format!("{}={value}", snake_case(key)))
        })
        .collect::<Vec<_>>();
    if !entries.is_empty() {
        line.push_str(": ");
        line.push_str(&entries.join(", "));
    }
    lines.push(line);
    for child in &node.children {
        render_node(child, depth + 1, lines);
    }
}

/// Renders one `extra_info` value, or `None` when it carries nothing worth a line's space.
fn render_value(value: &ExtraValue) -> Option<String> {
    match value {
        ExtraValue::Text(text) if text.is_empty() => None,
        ExtraValue::Text(text) => Some(text.clone()),
        ExtraValue::List(items) if items.is_empty() => None,
        ExtraValue::List(items) => Some(format!("[{}]", items.join(", "))),
        ExtraValue::Other(other) => Some(other.to_string()),
    }
}

/// Turns DuckDB's `Estimated Cardinality`-style keys into `estimated_cardinality`.
fn snake_case(key: &str) -> String {
    key.split_whitespace()
        .map(str::to_lowercase)
        .collect::<Vec<_>>()
        .join("_")
}

/// Whether `sql` is an `EXPLAIN` statement, so its rows may carry JSON plans.
pub fn is_explain(sql: &str) -> bool {
    sql.trim_start()
        .get(..7)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("explain"))
}

/// Rewrites the rows of an `EXPLAIN` query so each JSON plan is rendered as text.
///
/// DuckDB returns one row per plan, `(kind, plan)`, where `kind` is `logical_plan`,
/// `logical_opt` or `physical_plan`. Each such row becomes a row holding just `kind` followed
/// by one row per rendered line, mirroring how DataFusion's engine splits its multi-line
/// plans, so both engines' expected output has the same shape:
///
/// ```text
/// physical_plan
/// 01)READ_VORTEX: function=Vortex Scan, projections=str, estimated_cardinality=3
/// ```
///
/// Rows whose last cell is not a JSON plan (for example DuckDB's default tree-drawing
/// `EXPLAIN` output) are passed through unchanged.
pub fn render_explain_rows(rows: Vec<Vec<String>>) -> Vec<Vec<String>> {
    rows.into_iter()
        .flat_map(|mut row| {
            let Some(plan) = row.last().and_then(|cell| parse_plan(cell).ok()) else {
                return vec![row];
            };
            row.pop();
            std::iter::once(row)
                .chain(render_plan(&plan).into_iter().map(|line| vec![line]))
                .collect()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::is_explain;
    use super::parse_plan;
    use super::render_explain_rows;
    use super::render_plan;
    use super::snake_case;

    const JOIN_PLAN: &str = r#"[
        {
            "name": "HASH_JOIN",
            "children": [
                {
                    "name": "READ_VORTEX",
                    "children": [],
                    "extra_info": {
                        "Function": "Vortex Scan",
                        "Filters": "($.o_orderdate < 1995-03-15)",
                        "Projections": ["o_orderkey", "o_custkey"],
                        "Estimated Cardinality": "30000"
                    }
                },
                {
                    "name": "PROJECTION",
                    "children": [
                        {
                            "name": "READ_VORTEX",
                            "children": [],
                            "extra_info": {
                                "Function": "Vortex Scan",
                                "Filters": "",
                                "Projections": "c_custkey"
                            }
                        }
                    ],
                    "extra_info": {
                        "Expressions": []
                    }
                }
            ],
            "extra_info": {
                "Join Type": "INNER",
                "Conditions": "(o_custkey = c_custkey)"
            }
        }
    ]"#;

    #[test]
    fn renders_operators_depth_first_with_inlined_extra_info() -> serde_json::Result<()> {
        let plan = parse_plan(JOIN_PLAN)?;
        assert_eq!(
            render_plan(&plan),
            [
                "01)HASH_JOIN: join_type=INNER, conditions=(o_custkey = c_custkey)",
                "02)--READ_VORTEX: function=Vortex Scan, filters=($.o_orderdate < 1995-03-15), \
                 projections=[o_orderkey, o_custkey], estimated_cardinality=30000",
                // Empty entries are dropped, as is the `: ` when nothing is left.
                "03)--PROJECTION",
                "04)----READ_VORTEX: function=Vortex Scan, projections=c_custkey",
            ]
        );
        Ok(())
    }

    #[test]
    fn keeps_extra_info_in_emitted_order() -> serde_json::Result<()> {
        let plan = parse_plan(
            r#"[{"name": "N", "children": [], "extra_info": {"Zeta": "1", "Alpha": "2"}}]"#,
        )?;
        assert_eq!(render_plan(&plan), ["01)N: zeta=1, alpha=2"]);
        Ok(())
    }

    #[test]
    fn numbers_multiple_roots_continuously() -> serde_json::Result<()> {
        let plan = parse_plan(r#"[{"name": "A"}, {"name": "B", "children": [{"name": "C"}]}]"#)?;
        assert_eq!(render_plan(&plan), ["01)A", "02)B", "03)--C"]);
        Ok(())
    }

    #[test]
    fn keeps_unexpected_values_as_json() -> serde_json::Result<()> {
        let plan = parse_plan(r#"[{"name": "N", "extra_info": {"Nested": {"k": 1}}}]"#)?;
        assert_eq!(render_plan(&plan), [r#"01)N: nested={"k":1}"#]);
        Ok(())
    }

    #[rstest]
    #[case("Estimated Cardinality", "estimated_cardinality")]
    #[case("SELECT projections", "select_projections")]
    #[case("Filters", "filters")]
    #[case("Join Type", "join_type")]
    fn snake_cases_keys(#[case] key: &str, #[case] expected: &str) {
        assert_eq!(snake_case(key), expected);
    }

    #[rstest]
    #[case("EXPLAIN SELECT 1", true)]
    #[case("  explain (FORMAT json) SELECT 1", true)]
    #[case("SELECT 'EXPLAIN'", false)]
    #[case("", false)]
    fn detects_explain_statements(#[case] sql: &str, #[case] expected: bool) {
        assert_eq!(is_explain(sql), expected);
    }

    #[test]
    fn splits_each_plan_row_into_kind_and_lines() {
        let scan = r#"[{"name": "READ_VORTEX", "extra_info": {"Projections": "str"}}]"#;
        let rows = vec![
            vec!["logical_plan".to_string(), scan.to_string()],
            vec!["physical_plan".to_string(), scan.to_string()],
        ];
        assert_eq!(
            render_explain_rows(rows),
            [
                vec!["logical_plan".to_string()],
                vec!["01)READ_VORTEX: projections=str".to_string()],
                vec!["physical_plan".to_string()],
                vec!["01)READ_VORTEX: projections=str".to_string()],
            ]
        );
    }

    #[test]
    fn passes_non_json_rows_through() {
        let rows = vec![vec![
            "physical_plan".to_string(),
            "┌───────────┐\n│ READ_VORTEX │\n└───────────┘".to_string(),
        ]];
        assert_eq!(render_explain_rows(rows.clone()), rows);
    }
}
