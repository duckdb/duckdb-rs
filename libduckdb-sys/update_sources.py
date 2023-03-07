import json

# path to package_build.py
scripts_dir = '../../duckdb/scripts'
# path to target
target_dir = '/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb'
# list of extensions to bundle
extensions = ['parquet', 'json', 'httpfs']
# extensions = []

import sys
sys.path.append(scripts_dir)
import package_build

(source_list, include_list, original_sources) = package_build.build_package(target_dir, extensions, False)

# the list of all source files (.cpp files) that have been copied into the `duckdb_source_copy` directory
print("SOURCES")
print(json.dumps(source_list))
# the list of all include files
print("HEADERS")
print(json.dumps(include_list))
