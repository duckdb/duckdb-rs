//! Replacing unresolved table references with table-function calls.

use crate::{
    Context, Result,
    builder_helpers::{OpaqueHandle, get_user_data, handle_unwind},
    check_api_call,
    database::Database,
    ffi,
    value::Value,
};

/// Callback-scoped controls for claiming an unresolved table reference.
///
/// Calling [`Self::set_function_name`] claims the reference. Parameters added
/// before returning are passed to that table function.
pub struct ReplacementHandle<'a> {
    info: &'a ffi::duckdb_v2_replacement_scan_info_handle,
}

impl<'a> ReplacementHandle<'a> {
    /// Append a positional table-function parameter.
    pub fn add_parameter(&self, value: Value) -> Result<()> {
        check_api_call!(ffi::duckdb_v2_replacement_scan_add_parameter, *self.info, value.handle,)
    }

    /// Add a named table-function parameter.
    pub fn add_parameter_with_name(&self, name: &str, value: Value) -> Result<()> {
        check_api_call!(
            ffi::duckdb_v2_replacement_scan_add_named_parameter,
            *self.info,
            name.into(),
            value.handle,
        )
    }

    /// Claim the reference with a table function.
    pub fn set_function_name(&self, name: &str) -> Result<()> {
        check_api_call!(
            ffi::duckdb_v2_replacement_scan_set_function_name,
            *self.info,
            name.into(),
        )
    }
}

unsafe extern "C" fn replacement_callback<T: ReplacementScanCallbacks>(
    info: ffi::duckdb_v2_replacement_scan_info_handle,
    context: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = get_user_data!(ffi::duckdb_v2_replacement_scan_get_user_data, info);

            let catalog = check_api_call!(ffi::duckdb_v2_replacement_scan_get_catalog_name, info, RET)?;

            let schema = check_api_call!(ffi::duckdb_v2_replacement_scan_get_schema_name, info, RET)?;

            let table = check_api_call!(ffi::duckdb_v2_replacement_scan_get_table_name, info, RET)?;

            dbg!(
                "replacement_callback called with catalog: {}, schema: {}, table: {}",
                catalog,
                schema,
                table
            );

            T::scan(
                user_data,
                Context(context),
                if catalog.ptr.is_null() {
                    None
                } else {
                    Some(catalog.into())
                },
                if schema.ptr.is_null() {
                    None
                } else {
                    Some(schema.into())
                },
                table.into(),
                ReplacementHandle { info: &info },
            )
        },
        err,
    );
}

/// Registers a replacement scan callback.
///
/// Registered callbacks are consulted in order whenever binding cannot resolve
/// a table name. Registration lasts until the database closes.
pub struct ReplacementScanBuilder<T> {
    implementation: OpaqueHandle<T>,
}

impl<T: ReplacementScanCallbacks> ReplacementScanBuilder<T> {
    /// Create a builder from its callback implementation.
    pub fn new(implementation: T) -> Self {
        Self {
            implementation: OpaqueHandle::new(implementation),
        }
    }

    /// Register the callback on the context's database.
    pub fn register_with_context(self, context: &Context) -> Result<()> {
        check_api_call!(
            ffi::duckdb_v2_replacement_scan_register_with_context,
            **context,
            Some(replacement_callback::<T>),
            self.implementation.to_handle(),
        )?;
        Ok(())
    }

    /// Register the callback on a database.
    pub fn register_with_database(self, database: &Database) -> Result<()> {
        check_api_call!(
            ffi::duckdb_v2_replacement_scan_register_with_database,
            database.handle.lock().unwrap().handle,
            Some(replacement_callback::<T>),
            self.implementation.to_handle(),
        )?;
        Ok(())
    }
}

/// Binding callback for unresolved table references.
pub trait ReplacementScanCallbacks: Send + Sync + 'static {
    /// **Bind:** claim, decline, or reject an unresolved table reference.
    ///
    /// Set a function name to claim it, return without doing so to let the next
    /// replacement scan try, or return an error to reject the query.
    fn scan(
        &self,
        context: Context,
        catalog: Option<&str>,
        schema: Option<&str>,
        table: &str,
        parameters: ReplacementHandle,
    ) -> Result<()>;
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use libduckdb_sys::DUCKDB_V2_LOGICAL_TYPE_ID::{
        DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT, DUCKDB_V2_LOGICAL_TYPE_ID_BOOLEAN,
    };

    use crate::{
        Context, Environment, Parameters, Result, StorageLocation, ToValue,
        replacement_scan::{ReplacementHandle, ReplacementScanBuilder, ReplacementScanCallbacks},
    };

    struct CustomReplacementScan {
        count: i32,
    }

    impl ReplacementScanCallbacks for CustomReplacementScan {
        fn scan(
            &self,
            context: Context,
            catalog: Option<&str>,
            schema: Option<&str>,
            table: &str,
            replacement: ReplacementHandle,
        ) -> Result<()> {
            if table.starts_with("num") {
                assert!(catalog == Some("test"));
                assert!(schema == Some("main"));

                let split = table
                    .replace("num_", "")
                    .replace('\'', "")
                    .split('_')
                    .map(|x| x.parse::<i32>().unwrap())
                    .collect::<Vec<_>>();

                dbg!(&split);

                replacement.set_function_name("range")?;
                replacement.add_parameter(split[0].value(&context)?)?;
                replacement.add_parameter((split[1] + self.count).value(&context)?)?;
            }

            Ok(())
        }
    }

    struct CustomNamedParameters {}

    impl ReplacementScanCallbacks for CustomNamedParameters {
        fn scan(
            &self,
            context: Context,
            catalog: Option<&str>,
            schema: Option<&str>,
            table: &str,
            replacement: ReplacementHandle,
        ) -> Result<()> {
            if table.starts_with("alltypes") {
                assert!(catalog.is_none());
                assert!(schema.is_none());

                replacement.set_function_name("test_all_types")?;
                replacement.add_parameter_with_name("use_large_bignum", true.value(&context)?)?;
                replacement.add_parameter_with_name("use_large_enum", false.value(&context)?)?;
            }

            Ok(())
        }
    }

    #[test]
    fn test_replacement_scan() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        ReplacementScanBuilder::new(CustomReplacementScan { count: 42 }).register_with_database(&db)?;

        ReplacementScanBuilder::new(CustomNamedParameters {}).register_with_database(&db)?;

        let mut query = conn.query("SELECT * FROM test.main.num_10_20", Parameters::None)?;

        let chunk = query.next().unwrap()?;

        assert!(chunk.get_vector_at::<i64>(0)?.logical_type().type_id() == DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT);

        assert_eq!(chunk.row_count()?, 10 + 42);

        assert!(query.next().is_none());

        let mut query = conn.query("SELECT * FROM alltypes", Parameters::None)?;

        let chunk = query.next().unwrap()?;

        assert!(chunk.get_vector_at::<bool>(0)?.logical_type().type_id() == DUCKDB_V2_LOGICAL_TYPE_ID_BOOLEAN);

        assert_eq!(chunk.vectors_count()?, 59);

        Ok(())
    }
}
