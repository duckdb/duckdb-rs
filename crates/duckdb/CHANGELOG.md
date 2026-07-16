# Changelog

All notable changes to the `duckdb` crate are documented here.

## [Unreleased]

- `Connection::register_arrow` filter pushdown now covers every filter kind DuckDB v1.5.4
  pushes to an arrow scan: STRUCT_EXTRACT (struct-field) predicates via column paths, plus
  scalar breadth (Date/Time/Timestamp/Decimal128/Blob + float NaN total-order) on top of the
  existing constant-comparison / IS [NOT] NULL / AND·OR / IN coverage. OPTIONAL/DYNAMIC/BLOOM
  are safely skipped; any unhandled required filter fails loud.
