# duckdb-rs

[![Build Status](https://github.com/wangfenjin/duckdb-rs/workflows/CI/badge.svg)](https://github.com/wangfenjin/duckdb-rs/actions)
[![dependency status](https://deps.rs/repo/github/wangfenjin/duckdb-rs/status.svg)](https://deps.rs/repo/github/wangfenjin/duckdb-rs)
[![codecov](https://codecov.io/gh/wangfenjin/duckdb-rs/branch/main/graph/badge.svg?token=0xV88q8KU0)](https://codecov.io/gh/wangfenjin/duckdb-rs)
[![Latest Version](https://img.shields.io/crates/v/duckdb.svg)](https://crates.io/crates/duckdb)
[![Docs](https://docs.rs/duckdb/badge.svg)](https://docs.rs/duckdb)

duckdb-rs is an ergonomic wrapper for using [duckdb](https://github.com/duckdb/duckdb) from Rust. It attempts to expose
an interface similar to [rusqlite](https://github.com/rusqlite/rusqlite). Acctually the initial code and even this README is
forked from rusqlite as duckdb also tries to expose a sqlite3 compatible API.

```rust
use duckdb::{params, Connection, Result};

#[derive(Debug)]
struct Person {
    id: i32,
    name: String,
    data: Option<Vec<u8>>,
}

fn main() -> Result<()> {
    let conn = Connection::open_in_memory()?;

    conn.execute_batch(
        r"CREATE SEQUENCE seq;
          CREATE TABLE person (
                  id              INTEGER PRIMARY KEY DEFAULT NEXTVAL('seq'),
                  name            TEXT NOT NULL,
                  data            BLOB
                  );
        ")?;

    let me = Person {
        id: 0,
        name: "Steven".to_string(),
        data: None,
    };
    conn.execute(
        "INSERT INTO person (name, data) VALUES (?, ?)",
        params![me.name, me.data],
    )?;

    let mut stmt = conn.prepare("SELECT id, name, data FROM person")?;
    let person_iter = stmt.query_map([], |row| {
        Ok(Person {
            id: row.get(0)?,
            name: row.get(1)?,
            data: row.get(2)?,
        })
    })?;

    for person in person_iter {
        println!("Found person {:?}", person.unwrap());
    }
    Ok(())
}
```

## Notes on building duckdb and libduckdb-sys

`libduckdb-sys` is a separate crate from `duckdb-rs` that provides the Rust
declarations for DuckDB's C API. By default, `libduckdb-sys` attempts to find a DuckDB library that already exists on your system using pkg-config, or a
[Vcpkg](https://github.com/Microsoft/vcpkg) installation for MSVC ABI builds.

You can adjust this behavior in a number of ways:

* If you use the `bundled` feature, `libduckdb-sys` will use the
  [cc](https://crates.io/crates/cc) crate to compile DuckDB from source and
  link against that. This source is embedded in the `libduckdb-sys` crate and
  as we are still in development, we will update it regularly. After we are more stable,
  we will use the stable released version from [duckdb](https://github.com/duckdb/duckdb/releases).
  This is probably the simplest solution to any build problems. You can enable this by adding the following in your `Cargo.toml` file:
  ```toml
  [dependencies.duckdb]
  version = "0.1"
  features = ["bundled"]
  ```
* When linking against a DuckDB library already on the system (so *not* using any of the `bundled` features), you can set the `DUCKDB_LIB_DIR` environment variable to point to a directory containing the library. You can also set the `DUCKDB_INCLUDE_DIR` variable to point to the directory containing `duckdb.h`.
* Installing the duckdb development packages will usually be all that is required, but
  the build helpers for [pkg-config](https://github.com/alexcrichton/pkg-config-rs)
  and [vcpkg](https://github.com/mcgoo/vcpkg-rs) have some additional configuration
  options. The default when using vcpkg is to dynamically link,
  which must be enabled by setting `VCPKGRS_DYNAMIC=1` environment variable before build.


### Binding generation

We use [bindgen](https://crates.io/crates/bindgen) to generate the Rust
declarations from DuckDB's C header file. `bindgen`
[recommends](https://github.com/servo/rust-bindgen#library-usage-with-buildrs)
running this as part of the build process of libraries that used this. We tried
this briefly (`duckdb` 0.10.0, specifically), but it had some annoyances:

* The build time for `libduckdb-sys` (and therefore `duckdb`) increased
  dramatically.
* Running `bindgen` requires a relatively-recent version of Clang, which many
  systems do not have installed by default.
* Running `bindgen` also requires the DuckDB header file to be present.

So we try to avoid running `bindgen` at build-time by shipping
pregenerated bindings for DuckDB.

If you use the `bundled` features, you will get pregenerated bindings for the
bundled version of DuckDB. If you want to run `bindgen` at buildtime to
produce your own bindings, use the `buildtime_bindgen` Cargo feature.

## Contributing

If running bindgen is problematic for you, `--features bundled` enables
bundled and all features which don't require binding generation, and can be used
instead.

### Checklist

- Run `cargo fmt` to ensure your Rust code is correctly formatted.
- Ensure `cargo clippy --all-targets --workspace --features bundled` passes without warnings.
- Ensure `cargo test --all-targets --workspace --features bundled` reports no failures.

### TODOs

- [x] Refactor the ErrorCode part, it's borrowed from rusqlite, we should have our own
- [ ] Support more type
- [x] Update duckdb.h
- [x] Adjust the code examples and documentation
- [x] Delete unused code / functions
- [x] Add CI
- [x] Publish to crate

## License

DuckDB and libduckdb-sys are available under the MIT license. See the LICENSE file for more info.
