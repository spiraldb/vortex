# Vortex Expressions

Expressions for querying vortex arrays. The query algebra is designed to express a minimal
superset of linear operations that can be pushed down to vortex metadata. Conversely, not all
operations that can be expressed in this algebra can be pushed down to metadata.

Takes inspiration from postgres https://www.postgresql.org/docs/current/sql-expressions.html
and datafusion https://github.com/apache/datafusion/tree/5fac581efbaffd0e6a9edf931182517524526afd/datafusion/expr
