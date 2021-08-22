.EXPORT_ALL_VARIABLES:

# In order to use buildtime_bindgen
# you need to build duckdb locally and export the envs
LD_LIBRARY_PATH = /Users/wangfenjin/duckdb:$LD_LIBRARY_PATH
DUCKDB_LIB_DIR = /Users/wangfenjin/duckdb
DUCKDB_INCLUDE_DIR = /Users/wangfenjin/duckdb

all:
	cargo test --features buildtime_bindgen --features modern-full -- --nocapture
	cargo clippy --all-targets --workspace --features buildtime_bindgen --features modern-full -- -D warnings -A clippy::redundant-closure
