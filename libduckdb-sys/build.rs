use std::env;
use std::path::Path;

/// Tells whether we're building for Windows. This is more suitable than a plain
/// `cfg!(windows)`, since the latter does not properly handle cross-compilation
///
/// Note that there is no way to know at compile-time which system we'll be
/// targetting, and this test must be made at run-time (of the build script) See
/// https://doc.rust-lang.org/cargo/reference/environment-variables.html#environment-variables-cargo-sets-for-build-scripts
#[allow(dead_code)]
fn win_target() -> bool {
    std::env::var("CARGO_CFG_WINDOWS").is_ok()
}

/// Tells whether a given compiler will be used `compiler_name` is compared to
/// the content of `CARGO_CFG_TARGET_ENV` (and is always lowercase)
///
/// See [`win_target`]
#[allow(dead_code)]
fn is_compiler(compiler_name: &str) -> bool {
    std::env::var("CARGO_CFG_TARGET_ENV").map_or(false, |v| v == compiler_name)
}

fn main() {
    let out_dir = env::var("OUT_DIR").unwrap();
    let out_path = Path::new(&out_dir).join("bindgen.rs");
    #[cfg(feature = "bundled")]
    {
        build_bundled::main(&out_dir, &out_path);
    }
    #[cfg(not(feature = "bundled"))]
    {
        build_linked::main(&out_dir, &out_path)
    }
}

#[cfg(feature = "bundled")]
mod build_bundled {
    use std::path::Path;

    use crate::win_target;

    pub fn main(out_dir: &str, out_path: &Path) {
        let lib_name = super::lib_name();

        if !cfg!(feature = "bundled") {
            // This is just a sanity check, the top level `main` should ensure this.
            panic!("This module should not be used: bundled feature has not been enabled");
        }

        #[cfg(feature = "buildtime_bindgen")]
        {
            use super::{bindings, HeaderLocation};
            let header = HeaderLocation::FromPath(format!("{}/src/include/duckdb.h", lib_name));
            bindings::write_to_out_dir(header, out_path);
        }
        #[cfg(not(feature = "buildtime_bindgen"))]
        {
            use std::fs;
            fs::copy(format!("{}/bindgen_bundled_version.rs", lib_name), out_path)
                .expect("Could not copy bindings to output directory");
        }

        let cpp_files = [
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_catalog.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_catalog_catalog_entry.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_catalog_default.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_common.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_common_arrow.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_common_crypto.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_common_enums.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_common_operator.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_common_progress_bar.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_common_row_operations.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_common_serializer.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_common_sort.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_common_types.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_common_value_operations.cpp",
            "duckdb/src/common/vector_operations/boolean_operators.cpp",
            "duckdb/src/common/vector_operations/comparison_operators.cpp",
            "duckdb/src/common/vector_operations/generators.cpp",
            "duckdb/src/common/vector_operations/is_distinct_from.cpp",
            "duckdb/src/common/vector_operations/null_operations.cpp",
            "duckdb/src/common/vector_operations/numeric_inplace_operators.cpp",
            "duckdb/src/common/vector_operations/vector_cast.cpp",
            "duckdb/src/common/vector_operations/vector_copy.cpp",
            "duckdb/src/common/vector_operations/vector_hash.cpp",
            "duckdb/src/common/vector_operations/vector_storage.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_execution.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_execution_expression_executor.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_execution_index_art.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_execution_nested_loop_join.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_execution_operator_aggregate.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_execution_operator_filter.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_execution_operator_helper.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_execution_operator_join.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_execution_operator_order.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_execution_operator_persistent.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_execution_operator_projection.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_execution_operator_scan.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_execution_operator_schema.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_execution_operator_set.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_execution_physical_plan.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_function_aggregate_algebraic.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_function_aggregate.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_function_aggregate_distributive.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_function_aggregate_holistic.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_function_aggregate_nested.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_function_aggregate_regression.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_function.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_function_cast.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_function_pragma.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_function_scalar_blob.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_function_scalar_date.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_function_scalar.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_function_scalar_enum.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_function_scalar_generic.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_function_scalar_list.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_function_scalar_map.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_function_scalar_math.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_function_scalar_operators.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_function_scalar_sequence.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_function_scalar_string.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_function_scalar_struct.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_function_scalar_system.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_function_scalar_union.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_function_table.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_function_table_system.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_function_table_version.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_main.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_main_capi.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_main_capi_cast.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_main_extension.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_main_relation.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_main_settings.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_optimizer.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_optimizer_join_order.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_optimizer_matcher.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_optimizer_pullup.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_optimizer_pushdown.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_optimizer_rule.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_optimizer_statistics_expression.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_optimizer_statistics_operator.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_parallel.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_parser.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_parser_constraints.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_parser_expression.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_parser_parsed_data.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_parser_query_node.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_parser_statement.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_parser_tableref.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_parser_transform_constraint.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_parser_transform_expression.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_parser_transform_helpers.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_parser_transform_statement.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_parser_transform_tableref.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_planner.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_planner_binder_expression.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_planner_binder_query_node.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_planner_binder_statement.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_planner_binder_tableref.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_planner_expression.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_planner_expression_binder.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_planner_filter.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_planner_operator.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_planner_parsed_data.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_planner_subquery.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_storage.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_storage_buffer.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_storage_checkpoint.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_storage_compression.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_storage_compression_chimp.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_storage_statistics.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_storage_table.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_src_transaction.cpp",
            "duckdb/src/verification/copied_statement_verifier.cpp",
            "duckdb/src/verification/deserialized_statement_verifier.cpp",
            "duckdb/src/verification/external_statement_verifier.cpp",
            "duckdb/src/verification/parsed_statement_verifier.cpp",
            "duckdb/src/verification/prepared_statement_verifier.cpp",
            "duckdb/src/verification/statement_verifier.cpp",
            "duckdb/src/verification/unoptimized_statement_verifier.cpp",
            "duckdb/third_party/fmt/format.cc",
            "duckdb/third_party/fsst/fsst_avx512.cpp",
            "duckdb/third_party/fsst/libfsst.cpp",
            "duckdb/third_party/miniz/miniz.cpp",
            "duckdb/third_party/re2/re2/bitstate.cc",
            "duckdb/third_party/re2/re2/compile.cc",
            "duckdb/third_party/re2/re2/dfa.cc",
            "duckdb/third_party/re2/re2/filtered_re2.cc",
            "duckdb/third_party/re2/re2/mimics_pcre.cc",
            "duckdb/third_party/re2/re2/nfa.cc",
            "duckdb/third_party/re2/re2/onepass.cc",
            "duckdb/third_party/re2/re2/parse.cc",
            "duckdb/third_party/re2/re2/perl_groups.cc",
            "duckdb/third_party/re2/re2/prefilter.cc",
            "duckdb/third_party/re2/re2/prefilter_tree.cc",
            "duckdb/third_party/re2/re2/prog.cc",
            "duckdb/third_party/re2/re2/re2.cc",
            "duckdb/third_party/re2/re2/regexp.cc",
            "duckdb/third_party/re2/re2/set.cc",
            "duckdb/third_party/re2/re2/simplify.cc",
            "duckdb/third_party/re2/re2/stringpiece.cc",
            "duckdb/third_party/re2/re2/tostring.cc",
            "duckdb/third_party/re2/re2/unicode_casefold.cc",
            "duckdb/third_party/re2/re2/unicode_groups.cc",
            "duckdb/third_party/re2/util/rune.cc",
            "duckdb/third_party/re2/util/strutil.cc",
            "duckdb/third_party/hyperloglog/hyperloglog.cpp",
            "duckdb/third_party/hyperloglog/sds.cpp",
            "duckdb/third_party/fastpforlib/bitpacking.cpp",
            "duckdb/third_party/utf8proc/utf8proc.cpp",
            "duckdb/third_party/utf8proc/utf8proc_wrapper.cpp",
            "duckdb/third_party/libpg_query/pg_functions.cpp",
            "duckdb/third_party/libpg_query/postgres_parser.cpp",
            "duckdb/third_party/libpg_query/src_backend_nodes_list.cpp",
            "duckdb/third_party/libpg_query/src_backend_nodes_makefuncs.cpp",
            "duckdb/third_party/libpg_query/src_backend_nodes_value.cpp",
            "duckdb/third_party/libpg_query/src_backend_parser_gram.cpp",
            "duckdb/third_party/libpg_query/src_backend_parser_parser.cpp",
            "duckdb/third_party/libpg_query/src_backend_parser_scan.cpp",
            "duckdb/third_party/libpg_query/src_backend_parser_scansup.cpp",
            "duckdb/third_party/libpg_query/src_common_keywords.cpp",
            "duckdb/third_party/mbedtls/library/asn1parse.cpp",
            "duckdb/third_party/mbedtls/library/base64.cpp",
            "duckdb/third_party/mbedtls/library/bignum.cpp",
            "duckdb/third_party/mbedtls/library/constant_time.cpp",
            "duckdb/third_party/mbedtls/library/md.cpp",
            "duckdb/third_party/mbedtls/library/oid.cpp",
            "duckdb/third_party/mbedtls/library/pem.cpp",
            "duckdb/third_party/mbedtls/library/pk.cpp",
            "duckdb/third_party/mbedtls/library/pk_wrap.cpp",
            "duckdb/third_party/mbedtls/library/pkparse.cpp",
            "duckdb/third_party/mbedtls/library/platform_util.cpp",
            "duckdb/third_party/mbedtls/library/rsa.cpp",
            "duckdb/third_party/mbedtls/library/rsa_alt_helpers.cpp",
            "duckdb/third_party/mbedtls/library/sha1.cpp",
            "duckdb/third_party/mbedtls/library/sha256.cpp",
            "duckdb/third_party/mbedtls/library/sha512.cpp",
            "duckdb/third_party/mbedtls/mbedtls_wrapper.cpp",
            "duckdb/extension/parquet/parquet-extension.cpp",
            "duckdb/extension/parquet/column_writer.cpp",
            "duckdb/extension/parquet/parquet_reader.cpp",
            "duckdb/extension/parquet/parquet_timestamp.cpp",
            "duckdb/extension/parquet/parquet_writer.cpp",
            "duckdb/extension/parquet/column_reader.cpp",
            "duckdb/extension/parquet/parquet_statistics.cpp",
            "duckdb/extension/parquet/parquet_metadata.cpp",
            "duckdb/extension/parquet/zstd_file_system.cpp",
            "duckdb/third_party/parquet/parquet_constants.cpp",
            "duckdb/third_party/parquet/parquet_types.cpp",
            "duckdb/third_party/thrift/thrift/protocol/TProtocol.cpp",
            "duckdb/third_party/thrift/thrift/transport/TTransportException.cpp",
            "duckdb/third_party/thrift/thrift/transport/TBufferTransports.cpp",
            "duckdb/third_party/snappy/snappy.cc",
            "duckdb/third_party/snappy/snappy-sinksource.cc",
            "duckdb/third_party/zstd/decompress/zstd_ddict.cpp",
            "duckdb/third_party/zstd/decompress/huf_decompress.cpp",
            "duckdb/third_party/zstd/decompress/zstd_decompress.cpp",
            "duckdb/third_party/zstd/decompress/zstd_decompress_block.cpp",
            "duckdb/third_party/zstd/common/entropy_common.cpp",
            "duckdb/third_party/zstd/common/fse_decompress.cpp",
            "duckdb/third_party/zstd/common/zstd_common.cpp",
            "duckdb/third_party/zstd/common/error_private.cpp",
            "duckdb/third_party/zstd/common/xxhash.cpp",
            "duckdb/third_party/zstd/compress/fse_compress.cpp",
            "duckdb/third_party/zstd/compress/hist.cpp",
            "duckdb/third_party/zstd/compress/huf_compress.cpp",
            "duckdb/third_party/zstd/compress/zstd_compress.cpp",
            "duckdb/third_party/zstd/compress/zstd_compress_literals.cpp",
            "duckdb/third_party/zstd/compress/zstd_compress_sequences.cpp",
            "duckdb/third_party/zstd/compress/zstd_compress_superblock.cpp",
            "duckdb/third_party/zstd/compress/zstd_double_fast.cpp",
            "duckdb/third_party/zstd/compress/zstd_fast.cpp",
            "duckdb/third_party/zstd/compress/zstd_lazy.cpp",
            "duckdb/third_party/zstd/compress/zstd_ldm.cpp",
            "duckdb/third_party/zstd/compress/zstd_opt.cpp",
            "duckdb/extension/json/buffered_json_reader.cpp",
            "duckdb/extension/json/json-extension.cpp",
            "duckdb/extension/json/json_common.cpp",
            "duckdb/extension/json/json_functions.cpp",
            "duckdb/extension/json/json_scan.cpp",
            "/Users/agoyal/projects/duckdb-rs/libduckdb-sys/duckdb/ub_extension_json_json_functions.cpp",
            "duckdb/extension/json/yyjson/yyjson.cpp",
            "duckdb/extension/httpfs/httpfs-extension.cpp",
            "duckdb/extension/httpfs/httpfs.cpp",
            "duckdb/extension/httpfs/s3fs.cpp",
            "duckdb/extension/httpfs/crypto.cpp",
        ];

        let include_dirs = [
            "src/include",
            "third_party/fmt/include",
            "third_party/fsst",
            "third_party/re2",
            "third_party/miniz",
            "third_party/utf8proc/include",
            "third_party/utf8proc",
            "third_party/hyperloglog",
            "third_party/fastpforlib",
            "third_party/tdigest",
            "third_party/libpg_query/include",
            "third_party/libpg_query",
            "third_party/concurrentqueue",
            "third_party/pcg",
            "third_party/httplib",
            "third_party/fast_float",
            "third_party/mbedtls",
            "third_party/mbedtls/include",
            "third_party/mbedtls/library",
            "third_party/jaro_winkler",
            "third_party/jaro_winkler/details",
            "extension/parquet/include",
            "third_party/parquet",
            "third_party/snappy",
            "third_party/thrift",
            "third_party/zstd/include",
            "extension/json/include",
            "extension/json/yyjson/include",
            "extension/httpfs/include",
            "third_party/httplib",
            "extension/parquet/include",
        ];

        println!("cargo:lib_dir={out_dir}");
        println!("cargo:rustc-cfg=const_fn");
        println!("cargo:rustc-cfg=openssl");
        println!("cargo:rerun-if-env-changed=AARCH64_APPLE_DARWIN_OPENSSL_LIB_DIR");
        println!("AARCH64_APPLE_DARWIN_OPENSSL_LIB_DIR unset");
        println!("cargo:rerun-if-env-changed=OPENSSL_LIB_DIR");
        println!("OPENSSL_LIB_DIR unset");
        println!("cargo:rerun-if-env-changed=AARCH64_APPLE_DARWIN_OPENSSL_INCLUDE_DIR");
        println!("AARCH64_APPLE_DARWIN_OPENSSL_INCLUDE_DIR unset");
        println!("cargo:rerun-if-env-changed=OPENSSL_INCLUDE_DIR");
        println!("OPENSSL_INCLUDE_DIR unset");
        println!("cargo:rerun-if-env-changed=AARCH64_APPLE_DARWIN_OPENSSL_DIR");
        println!("AARCH64_APPLE_DARWIN_OPENSSL_DIR unset");
        println!("cargo:rerun-if-env-changed=OPENSSL_DIR");
        println!("OPENSSL_DIR unset");
        println!("cargo:rustc-link-search=native=/opt/homebrew/opt/openssl@3/lib");
        println!("cargo:include=/opt/homebrew/opt/openssl@3/include");
        println!("cargo:rerun-if-changed=build/expando.c");
        println!("cargo:rerun-if-env-changed=CC_aarch64-apple-darwin");
        println!("CC_aarch64-apple-darwin = None");
        println!("cargo:rerun-if-env-changed=CC_aarch64_apple_darwin");
        println!("CC_aarch64_apple_darwin = None");
        println!("cargo:rerun-if-env-changed=HOST_CC");
        println!("HOST_CC = None");
        println!("cargo:rerun-if-env-changed=CC");
        println!("CC = None");
        println!("cargo:rerun-if-env-changed=CFLAGS_aarch64-apple-darwin");
        println!("CFLAGS_aarch64-apple-darwin = None");
        println!("cargo:rerun-if-env-changed=CFLAGS_aarch64_apple_darwin");
        println!("CFLAGS_aarch64_apple_darwin = None");
        println!("cargo:rerun-if-env-changed=HOST_CFLAGS");
        println!("HOST_CFLAGS = None");
        println!("cargo:rerun-if-env-changed=CFLAGS");
        println!("CFLAGS = None");
        println!("cargo:rerun-if-env-changed=CRATE_CC_NO_DEFAULTS");
        println!("CRATE_CC_NO_DEFAULTS = None");
        println!("version: 3_0_8");
        println!("cargo:rustc-cfg=osslconf=\"OPENSSL_NO_SSL3_METHOD\"");
        println!("cargo:conf=OPENSSL_NO_SSL3_METHOD");
        println!("cargo:rustc-cfg=ossl300");
        println!("cargo:rustc-cfg=ossl101");
        println!("cargo:rustc-cfg=ossl102");
        println!("cargo:rustc-cfg=ossl102f");
        println!("cargo:rustc-cfg=ossl102h");
        println!("cargo:rustc-cfg=ossl110");
        println!("cargo:rustc-cfg=ossl110f");
        println!("cargo:rustc-cfg=ossl110g");
        println!("cargo:rustc-cfg=ossl110h");
        println!("cargo:rustc-cfg=ossl111");
        println!("cargo:rustc-cfg=ossl111b");
        println!("cargo:rustc-cfg=ossl111c");
        println!("cargo:version_number=30000080");
        println!("cargo:rerun-if-env-changed=AARCH64_APPLE_DARWIN_OPENSSL_LIBS");
        println!("AARCH64_APPLE_DARWIN_OPENSSL_LIBS unset");
        println!("cargo:rerun-if-env-changed=OPENSSL_LIBS");
        println!("OPENSSL_LIBS unset");
        println!("cargo:rerun-if-env-changed=AARCH64_APPLE_DARWIN_OPENSSL_STATIC");
        println!("AARCH64_APPLE_DARWIN_OPENSSL_STATIC unset");
        println!("cargo:rerun-if-env-changed=OPENSSL_STATIC");
        println!("OPENSSL_STATIC unset");
        println!("cargo:rustc-link-lib=dylib=ssl");
        println!("cargo:rustc-link-lib=dylib=crypto");

        // XXX FIX
        /*
        for f in header_files.iter().chain(cpp_files.iter()) {
            println!("cargo:rerun-if-changed={}/{}", lib_name, f);
        }
        */
        let mut cfg = cc::Build::new();

        cfg.include(lib_name);
        cfg.include("/opt/homebrew/opt/openssl@3/include"); // Needs to be derived somehow?

        cfg.includes(include_dirs.iter().map(|x| format!("{}/{}", lib_name, x)));

        for f in cpp_files {
            cfg.file(format!("{}", f));
        }

        cfg.cpp(true)
            .flag_if_supported("-std=c++11")
            .flag_if_supported("-stdlib=libc++")
            .flag_if_supported("-stdlib=libstdc++")
            .flag_if_supported("/bigobj")
            .warnings(false);

        if win_target() {
            cfg.define("DUCKDB_BUILD_LIBRARY", None);
        }

        cfg.compile(lib_name);
    }
}

fn env_prefix() -> &'static str {
    "DUCKDB"
}

fn lib_name() -> &'static str {
    "duckdb"
}

pub enum HeaderLocation {
    FromEnvironment,
    Wrapper,
    FromPath(String),
}

impl From<HeaderLocation> for String {
    fn from(header: HeaderLocation) -> String {
        match header {
            HeaderLocation::FromEnvironment => {
                let prefix = env_prefix();
                let mut header = env::var(format!("{prefix}_INCLUDE_DIR"))
                    .unwrap_or_else(|_| env::var(format!("{}_LIB_DIR", env_prefix())).unwrap());
                header.push_str("/duckdb.h");
                header
            }
            HeaderLocation::Wrapper => "wrapper.h".into(),
            HeaderLocation::FromPath(path) => path,
        }
    }
}

#[cfg(not(feature = "bundled"))]
mod build_linked {
    #[cfg(feature = "vcpkg")]
    extern crate vcpkg;

    #[cfg(feature = "buildtime_bindgen")]
    use super::bindings;

    use super::{env_prefix, is_compiler, lib_name, win_target, HeaderLocation};
    use std::env;
    use std::path::Path;

    pub fn main(_out_dir: &str, out_path: &Path) {
        // We need this to config the LD_LIBRARY_PATH
        #[allow(unused_variables)]
        let header = find_duckdb();

        if !cfg!(feature = "buildtime_bindgen") {
            std::fs::copy(format!("{}/bindgen_bundled_version.rs", lib_name()), out_path)
                .expect("Could not copy bindings to output directory");
        } else {
            #[cfg(feature = "buildtime_bindgen")]
            {
                bindings::write_to_out_dir(header, out_path);
            }
        }
    }

    fn find_link_mode() -> &'static str {
        // If the user specifies DUCKDB_STATIC, do static
        // linking, unless it's explicitly set to 0.
        match &env::var(format!("{}_STATIC", env_prefix())) {
            Ok(v) if v != "0" => "static",
            _ => "dylib",
        }
    }
    // Prints the necessary cargo link commands and returns the path to the header.
    fn find_duckdb() -> HeaderLocation {
        let link_lib = lib_name();

        println!("cargo:rerun-if-env-changed={}_INCLUDE_DIR", env_prefix());
        println!("cargo:rerun-if-env-changed={}_LIB_DIR", env_prefix());
        println!("cargo:rerun-if-env-changed={}_STATIC", env_prefix());
        if cfg!(feature = "vcpkg") && is_compiler("msvc") {
            println!("cargo:rerun-if-env-changed=VCPKGRS_DYNAMIC");
        }

        // dependents can access `DEP_DUCKDB_LINK_TARGET` (`duckdb` being the
        // `links=` value in our Cargo.toml) to get this value. This might be
        // useful if you need to ensure whatever crypto library sqlcipher relies
        // on is available, for example.
        println!("cargo:link-target={link_lib}");

        if win_target() && cfg!(feature = "winduckdb") {
            println!("cargo:rustc-link-lib=dylib={link_lib}");
            return HeaderLocation::Wrapper;
        }

        // Allow users to specify where to find DuckDB.
        if let Ok(dir) = env::var(format!("{}_LIB_DIR", env_prefix())) {
            println!("cargo:rustc-env=LD_LIBRARY_PATH={dir}");
            // Try to use pkg-config to determine link commands
            let pkgconfig_path = Path::new(&dir).join("pkgconfig");
            env::set_var("PKG_CONFIG_PATH", pkgconfig_path);
            if pkg_config::Config::new().probe(link_lib).is_err() {
                // Otherwise just emit the bare minimum link commands.
                println!("cargo:rustc-link-lib={}={}", find_link_mode(), link_lib);
                println!("cargo:rustc-link-search={dir}");
            }
            return HeaderLocation::FromEnvironment;
        }

        if let Some(header) = try_vcpkg() {
            return header;
        }

        // See if pkg-config can do everything for us.
        match pkg_config::Config::new().print_system_libs(false).probe(link_lib) {
            Ok(mut lib) => {
                if let Some(mut header) = lib.include_paths.pop() {
                    header.push("duckdb.h");
                    HeaderLocation::FromPath(header.to_string_lossy().into())
                } else {
                    HeaderLocation::Wrapper
                }
            }
            Err(_) => {
                // No env var set and pkg-config couldn't help; just output the link-lib
                // request and hope that the library exists on the system paths. We used to
                // output /usr/lib explicitly, but that can introduce other linking problems;
                // see https://github.com/rusqlite/rusqlite/issues/207.
                println!("cargo:rustc-link-lib={}={}", find_link_mode(), link_lib);
                HeaderLocation::Wrapper
            }
        }
    }

    fn try_vcpkg() -> Option<HeaderLocation> {
        if cfg!(feature = "vcpkg") && is_compiler("msvc") {
            // See if vcpkg can find it.
            if let Ok(mut lib) = vcpkg::Config::new().probe(lib_name()) {
                if let Some(mut header) = lib.include_paths.pop() {
                    header.push("duckdb.h");
                    return Some(HeaderLocation::FromPath(header.to_string_lossy().into()));
                }
            }
            None
        } else {
            None
        }
    }
}

#[cfg(feature = "buildtime_bindgen")]
mod bindings {
    use super::HeaderLocation;

    use std::fs::OpenOptions;
    use std::io::Write;
    use std::path::Path;

    pub fn write_to_out_dir(header: HeaderLocation, out_path: &Path) {
        let header: String = header.into();
        let mut output = Vec::new();
        bindgen::builder()
            .trust_clang_mangling(false)
            .header(header.clone())
            .parse_callbacks(Box::new(bindgen::CargoCallbacks))
            .rustfmt_bindings(true)
            .generate()
            .unwrap_or_else(|_| panic!("could not run bindgen on header {header}"))
            .write(Box::new(&mut output))
            .expect("could not write output of bindgen");
        let output = String::from_utf8(output).expect("bindgen output was not UTF-8?!");
        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(out_path)
            .unwrap_or_else(|_| panic!("Could not write to {out_path:?}"));

        file.write_all(output.as_bytes())
            .unwrap_or_else(|_| panic!("Could not write to {out_path:?}"));
    }
}
