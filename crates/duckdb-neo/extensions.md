# Extensions

_Experimental._ `duckdb-neo` can also be used to write DuckDB extensions. Add `duckdb-loadable-macros` with its `neo`
feature, mark the entrypoint with `#[duckdb_entrypoint_c_api_v2]`, and register functionality through the `Extension`
it receives:

```rust
use duckdb_loadable_macros::duckdb_entrypoint_c_api_v2;
use duckdb_neo::extension::Extension;

#[duckdb_entrypoint_c_api_v2(extension_name = "my_ext", api_version = "v2.0.0")]
fn entrypoint(extension: &Extension) -> duckdb_neo::Result<()> {
    // Register scalar, aggregate, table, ... functions with `.register(extension)`.
    Ok(())
}
```

Returning an error fails the load with its message. A panic is reported as an internal error instead, which
invalidates the database: after the failed `LOAD`, every later query on it errors too. A static extension that fails
either way makes every instance fail to open. Enable exactly one of these features:

- `build-extension` - A loadable extension, loaded with `LOAD`.
- `build-extension-static` - An extension linked into a binary.

Both are pinned to the exact DuckDB build `duckdb-neo` was built against, the `DUCKDB_RELEASE_COMMIT` in
`crates/libduckdb-sys/.duckdb-release`.

## Loadable extension

1. Build a `cdylib`:

   ```toml
   [lib]
   crate-type = ["cdylib"]

   [dependencies]
   duckdb-neo = { version = "...", features = ["build-extension"] }
   duckdb-loadable-macros = { version = "...", features = ["neo"] }
   ```

   ```shell
   cargo build --release
   ```

2. Append the extension metadata with
   [`append_extension_metadata.py`](https://github.com/duckdb/extension-ci-tools/blob/main/scripts/append_extension_metadata.py).
   The file must be named `<extension_name>.duckdb_extension`, and the platform and version must match the CLI:

   ```shell
   platform=$(duckdb -noheader -list -c "PRAGMA platform")
   version=$(duckdb -noheader -list -c "SELECT CASE WHEN library_version LIKE '%-dev%' THEN source_id ELSE library_version END FROM pragma_version()")
   python3 append_extension_metadata.py -l target/release/libmy_ext.dylib -n my_ext -o my_ext.duckdb_extension \
       --abi-type C_STRUCT_UNSTABLE -dv "$version" --duckdb-platform "$platform" --extension-version 0.0.1
   ```

3. Load it into an unsigned CLI session, using an absolute path:

   ```shell
   duckdb -unsigned -c "LOAD '$PWD/my_ext.duckdb_extension'"
   ```

## Static extension in your own program

TBD
