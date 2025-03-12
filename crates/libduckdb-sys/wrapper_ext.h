// FIXME: remove this once C EXTENSION API is stable (expected for DuckDB v1.2.0 release)
#define DUCKDB_EXTENSION_API_VERSION_DEV 1

// We need to allow unstable API for now to get the deprecated arrow functions
#define DUCKDB_EXTENSION_API_VERSION_UNSTABLE
#include "../../../duckdb/src/include/duckdb_extension.h"
