# Vortex Expressions

Expressions for querying vortex arrays, designed to express a minimal
superset of the row predicates that can be pushed down to vortex metadata.

Takes inspiration from postgres https://www.postgresql.org/docs/current/sql-expressions.html
and datafusion https://github.com/apache/datafusion/tree/5fac581efbaffd0e6a9edf931182517524526afd/datafusion/expr
