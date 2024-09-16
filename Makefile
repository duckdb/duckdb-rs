.EXPORT_ALL_VARIABLES:

# In order to use buildtime_bindgen
# you need to build duckdb locally and export the envs
LD_LIBRARY_PATH = /Users/wangfenjin/duckdb:$LD_LIBRARY_PATH
DUCKDB_LIB_DIR = /Users/wangfenjin/duckdb
DUCKDB_INCLUDE_DIR = /Users/wangfenjin/duckdb

all:
	cargo test --features buildtime_bindgen --features modern-full -- --nocapture
	cargo clippy --all-targets --workspace --features buildtime_bindgen --features modern-full -- -D warnings -A clippy::redundant-closure

test:
	cargo test --features bundled --features modern-full -- --nocapture

# Build the rust-based CAPI extension demo, change the target arch below to
EXAMPLE_TARGET_ARCH = osx_arm64
examples-capi-demo:
	cargo build --example hello-ext-capi --features="vtab-loadable,loadable_extension" --release
	python3 scripts/append_extension_metadata.py -l target/release/examples/libhello_ext_capi.dylib -n rusty_quack -dv v0.0.1 -ev v0.0.1 -p $EXAMPLE_TARGET_ARCH