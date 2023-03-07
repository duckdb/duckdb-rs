#!/usr/bin/env python3

import json
import os
import shutil
import subprocess

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

# Path to package_build.py
DUCKDB_SCRIPTS_DIR = os.path.join(SCRIPT_DIR, "duckdb-sources", "scripts")
# Path to target
TARGET_DIR = os.path.join(SCRIPT_DIR, "duckdb")

# List of extensions' sources to grab. Technically, these sources will be compiled
# but not included in the final build unless they're explicitly enabled.
EXTENSIONS = ["parquet", "json", "httpfs"]

# copy in the https_config.py file that allows https to be included in the cmopiled sources
shutil.copyfile(
    os.path.join(SCRIPT_DIR, "extras", "httpfs_config.py"),
    os.path.join(
        SCRIPT_DIR, "duckdb-sources", "extension", "httpfs", "httpfs_config.py"
    ),
)

# Clear the duckdb directory
shutil.rmtree(os.path.join(TARGET_DIR))
os.mkdir(TARGET_DIR)

import sys

sys.path.append(DUCKDB_SCRIPTS_DIR)
import package_build

(source_list, include_list, original_sources) = package_build.build_package(
    TARGET_DIR, EXTENSIONS, False
)

# Remove the absolute prefix on the files (some get generated with it)
source_list = [
    x[len(SCRIPT_DIR) + 1 :] if x.startswith(SCRIPT_DIR) else x for x in source_list
]

with open(os.path.join(TARGET_DIR, "manifest.json"), "w") as f:
    json.dump({"cpp_files": source_list, "include_dirs": include_list}, f, indent=2)


subprocess.check_call(
    'find "' + SCRIPT_DIR + '/../target" -type f -name bindgen.rs -exec rm {} \;',
    shell=True,
)

subprocess.check_call(
    'env LIBDUCKDB_SYS_BUNDLING=1 cargo test --features "bundled buildtime_bindgen"',
    shell=True,
)
print(
    'find "'
    + SCRIPT_DIR
    + '/../target" -type f -name "bindgen.rs" -exec cp {} "'
    + TARGET_DIR
    + '/bindgen_bundled_version.rs" \;'
)
subprocess.check_call(
    'find "'
    + SCRIPT_DIR
    + '/../target" -type f -name "libduckdb-sys.*bindgen.rs" -exec cp {} "'
    + TARGET_DIR
    + '/bindgen_bundled_version.rs" \;',
    shell=True,
)

# Remove the extra patch file
os.remove(
    os.path.join(
        SCRIPT_DIR, "duckdb-sources", "extension", "httpfs", "httpfs_config.py"
    )
)
