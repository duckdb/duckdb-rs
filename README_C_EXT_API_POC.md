# POC C Extension API
This branch contains a POC for the C API Extensions. To demo this, follow these steps:

I used duckdb version `75682dc704067f53b3d9ea1aa7452281af75d2f1`.

First go into duckdb and add the deprecated functions from `<your_desired_duckdb_version>/src/include/duckdb/main/capi/header_generation/apis/v0/exclusion_list.json`
to the `<your_desired_duckdb_version>/src/include/duckdb/main/capi/header_generation/apis/v0/dev/dev.json` then run `python3 scripts/generate_c_api.py`

Next:
```shell
cp <your_desired_duckdb_version>/src/include/duckdb.h crates/libduckdb-sys/duckdb/duckdb.h 
cp <your_desired_duckdb_version>/src/include/duckdb.h crates/libduckdb-sys/duckdb/duckdb_extension.h
cargo build --example hello-ext-capi --features="vtab-loadable,loadable_extension,buildtime_bindgen"
cp target/debug/examples/libhello_ext_capi.dylib rusty_quack.duckdb_extension
./<your_desired_duckdb_version>/build/debug/duckdb -unsigned
```

```SQL
D set allow_extensions_metadata_mismatch=true;
D load 'rusty_quack.duckdb_extension';
D from hello("from the rusty side");
┌───────────────────────────┐
│          column0          │
│          varchar          │
├───────────────────────────┤
│ Hello from the rusty side │
└───────────────────────────┘
```

The thing works! Note that this extension is not linked to duckdb meaning that it is portable across duckdb versions.