//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/similar_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {
class SchemaCatalogEntry;

//! Return value of SimilarEntryInSchemas
struct SimilarCatalogEntry {
	//! The entry name. Empty if absent
	string name;
	//! The distance to the given name.
	idx_t distance = idx_t(-1);
	//! The schema of the entry.
	SchemaCatalogEntry *schema = nullptr;

	DUCKDB_API bool Found() const {
		return !name.empty();
	}

	DUCKDB_API string GetQualifiedName(bool qualify_catalog, bool qualify_schema) const;
};

} // namespace duckdb
