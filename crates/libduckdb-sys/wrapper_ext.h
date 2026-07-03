// FIXME: remove this once C EXTENSION API is stable (expected for DuckDB v1.2.0 release)
#define DUCKDB_EXTENSION_API_VERSION_DEV 1

// Matches build.rs bindgen setup for currently ungated unstable extension API symbols.
#define DUCKDB_EXTENSION_API_VERSION_UNSTABLE
#include "duckdb/duckdb_extension.h"
