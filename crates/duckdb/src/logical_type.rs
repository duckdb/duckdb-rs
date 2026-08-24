//! Construct and inspect DuckDB logical types.

use std::ops::Deref;

use libduckdb_sys::{self as ffi, DUCKDB_V2_LOGICAL_TYPE_ID};

use crate::{
    Parameters, Result,
    builder_helpers::context_and_connection_fn,
    check_api_call, check_api_call_no_err,
    connection::FFILink,
    error::{DuckDBError, Error},
    value::Value,
};

/// DuckDB's logical type identifier.
pub type LogicalTypeID = DUCKDB_V2_LOGICAL_TYPE_ID;

/// An owned DuckDB logical type.
///
/// Types can be created from primitive IDs, parsed SQL, or a type name with
/// value parameters. Parameter inspection exposes the information needed to
/// reconstruct parameterized types such as `DECIMAL`, `MAP`, and `STRUCT`.
///
/// # Example
/// ```
/// use duckdb_rs::{environment::Environment, environment::StorageLocation};
/// use duckdb_rs::logical_type::{LogicalType, LogicalTypeID};
///
/// # fn main() -> duckdb_rs::Result<()> {
/// let env = Environment::new()?;
/// let db = env.open(StorageLocation::InMemory)?;
/// let conn = db.connect()?;
/// let logical_type = LogicalType::from_text(&conn, "MAP(VARCHAR, INTEGER)")?;
///
/// assert_eq!(
///     logical_type.type_id(),
///     LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_MAP
/// );
/// assert_eq!(logical_type.param_count()?, 2);
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct LogicalType {
    /// The owned DuckDB logical-type handle.
    pub handle: ffi::duckdb_v2_logical_type_handle,
}

impl LogicalType {
    context_and_connection_fn! {
        /// Construct a logical type from a primitive ID and optional parameters.
        pub fn create_from_id_with_[context, connection](
            type_id: LogicalTypeID,
            parameters: Parameters<'_>,
        ) -> Result<Self>
        {
            context_fn: ffi::duckdb_v2_context_create_type_from_id,
            connection_fn: ffi::duckdb_v2_connection_create_type_from_id,
        }
        let (names, values) = parameters.into_values(api_arg!())?;
        let names = names.map(|names| {
            names
                .iter()
                .map(|name| (*name).into())
                .collect::<Vec<ffi::duckdb_v2_str>>()
        });
        let values = values
            .iter()
            .map(|value| value.as_value().handle)
            .collect::<Vec<_>>();
        let handle = check_api_call!(
            api_fn!(),
            **api_arg!(),
            type_id,
            names
                .as_ref()
                .map_or(std::ptr::null(), |names| names.as_ptr()),
            values.as_ptr(),
            values.len() as u64,
            RET
        )?;

        Ok(LogicalType { handle })
    }

    context_and_connection_fn! {
        pub(crate) fn from_text_with_[context, connection](text: &str) -> Result<Self>
        {
            context_fn: ffi::duckdb_v2_context_create_type_from_text,
            connection_fn: ffi::duckdb_v2_connection_create_type_from_text,
        }
        let handle = check_api_call!(
            api_fn!(),
            **api_arg!(),
            text.into(),
            RET
        )?;

        Ok(LogicalType { handle })
    }

    context_and_connection_fn! {
        /// Return an alias with the same representation.
        pub fn to_alias_with_[context, connection](&self, alias: &str) -> Result<Self>
        {
            context_fn: ffi::duckdb_v2_context_create_type_with_alias,
            connection_fn: ffi::duckdb_v2_connection_create_type_with_alias,
        }
        let handle = check_api_call!(
            api_fn!(),
            **api_arg!(),
            self.handle,
            alias.into(),
            RET
        )?;

        Ok(LogicalType { handle })
    }

    /// Parse a SQL type using a connection or callback context.
    pub fn from_text<C: FFILink>(handle: &C, text: &str) -> Result<Self> {
        handle.logical_type_from_text(text)
    }

    context_and_connection_fn! {
        /// Construct a logical type from a name and optional parameters.
        pub fn create_with_[context, connection](
            name: &str,
            parameters: Parameters<'_>,
        ) -> Result<Self>
        {
            context_fn: ffi::duckdb_v2_context_create_type_from_name,
            connection_fn: ffi::duckdb_v2_connection_create_type_from_name,
        }
        let (names, values) = parameters.into_values(api_arg!())?;
        let names = names.map(|names| {
            names
                .iter()
                .map(|name| (*name).into())
                .collect::<Vec<ffi::duckdb_v2_str>>()
        });
        let values = values
            .iter()
            .map(|value| value.as_value().handle)
            .collect::<Vec<_>>();

        let handle = check_api_call!(
            api_fn!(),
            **api_arg!(),
            name.into(),
            names
                .as_ref()
                .map_or(std::ptr::null(), |names| names.as_ptr()),
            values.as_ptr(),
            values.len() as u64,
            RET
        )?;

        Ok(LogicalType { handle })
    }

    /// Return the logical type ID.
    pub fn type_id(&self) -> DUCKDB_V2_LOGICAL_TYPE_ID {
        let type_id: DUCKDB_V2_LOGICAL_TYPE_ID =
            check_api_call!(ffi::duckdb_v2_logical_type_get_id, self.handle, RET)
                .expect("Failed to get logical type id");
        type_id
    }

    /// Return the alias or canonical type name.
    pub fn name(&self) -> Result<&str> {
        let name: ffi::duckdb_v2_str =
            check_api_call!(ffi::duckdb_v2_logical_type_get_name, self.handle, RET)?;

        Ok(name.into())
    }

    /// Render the type as SQL text.
    pub fn to_string(&self) -> Result<String> {
        let capacity = check_api_call!(
            ffi::duckdb_v2_logical_type_to_text,
            self.handle,
            std::ptr::null::<i8>() as *mut i8,
            0,
            RET
        )?;

        let buffer_capacity = capacity + 1;
        let mut text: Vec<u8> = Vec::with_capacity(buffer_capacity as usize);

        let length = check_api_call!(
            ffi::duckdb_v2_logical_type_to_text,
            self.handle,
            text.as_mut_ptr() as *mut i8,
            buffer_capacity,
            RET
        )?;
        unsafe {
            text.set_len(length as usize);
        }

        String::from_utf8(text).map_err(|_| Error {
            code: DuckDBError::DUCKDB_V2_ERROR_API,
            message: "Failed to convert logical type text to UTF-8".to_string(),
        })
    }

    /// Return the number of value parameters carried by the type.
    pub fn param_count(&self) -> Result<usize> {
        let count: u64 = check_api_call!(
            ffi::duckdb_v2_logical_type_get_param_count,
            self.handle,
            RET
        )?;

        Ok(count as usize)
    }

    /// Return a parameter's name, empty when positional, and owned value.
    ///
    /// An out-of-range index returns an error.
    pub fn get_param(&self, index: usize) -> Result<(&str, Value)> {
        let mut value = std::ptr::null_mut();

        let name: ffi::duckdb_v2_str = check_api_call!(
            ffi::duckdb_v2_logical_type_get_param,
            self.handle,
            index as u64,
            RET,
            &mut value
        )?;

        let name = if name.ptr.is_null() || name.len == 0 {
            ""
        } else {
            name.into()
        };

        Ok((name, Value { handle: value }))
    }

    /// Return all parameter names and values in declaration order.
    pub fn get_params(&self) -> Result<Vec<(String, Value)>> {
        let count = self.param_count()?;
        let mut params = Vec::with_capacity(count);

        for i in 0..count {
            let (name, value) = self.get_param(i)?;
            params.push((name.to_string(), value));
        }

        Ok(params)
    }
}

impl PartialEq for LogicalType {
    fn eq(&self, other: &Self) -> bool {
        let result: bool = check_api_call!(
            ffi::duckdb_v2_logical_type_is_equal,
            self.handle,
            other.handle,
            RET
        )
        .expect("Failed to compare logical types");

        result
    }
}

impl Clone for LogicalType {
    fn clone(&self) -> Self {
        let handle = check_api_call!(ffi::duckdb_v2_logical_type_copy, self.handle, RET)
            .expect("Failed to clone logical type");

        LogicalType { handle }
    }
}

impl Drop for LogicalType {
    fn drop(&mut self) {
        check_api_call_no_err!(ffi::duckdb_v2_logical_type_destroy, &mut self.handle).unwrap();
    }
}

impl Deref for LogicalType {
    type Target = ffi::duckdb_v2_logical_type_handle;
    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod test {
    use crate::{
        DuckDBType, environment::Environment, environment::StorageLocation, types::MapValue,
    };

    use super::*;

    #[test]
    fn test_custom_logical_type() -> crate::Result<()> {
        let env = Environment::new().expect("Failed to create environment");
        let db = env
            .open(StorageLocation::InMemory)
            .expect("Failed to open in-memory database");
        let conn = db.connect().expect("Failed to connect to database");

        let key_type = Value::from_logical_type(&conn, &i32::logical_type(&conn)?)?;
        let value_type = Value::from_logical_type(&conn, &String::logical_type(&conn)?)?;
        let ltype = LogicalType::create_with_connection(
            &conn,
            "map",
            Parameters::positional(&[&key_type, &value_type]),
        )?;

        assert_eq!(ltype.name()?, "MAP");
        assert_eq!(ltype.param_count()?, 2);
        assert_eq!(
            ltype.get_param(0)?.1.logical_type()?.type_id(),
            LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER
        );
        assert_eq!(
            ltype.get_param(1)?.1.logical_type()?.type_id(),
            LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR
        );

        Ok(())
    }

    #[test]
    fn test_logical_type_alias() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let ltype = MapValue::<i32, String>::logical_type(&conn)?;

        let alias_ltype = ltype.to_alias_with_connection(&conn, "my_map")?;

        assert_eq!(alias_ltype.name()?, "my_map");
        assert_eq!(alias_ltype.param_count()?, 2);
        assert_eq!(
            alias_ltype.type_id(),
            LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_MAP
        );

        assert_eq!(
            alias_ltype.get_param(0)?.1.logical_type()?.type_id(),
            LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER
        );
        assert_eq!(
            alias_ltype.get_param(1)?.1.logical_type()?.type_id(),
            LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR
        );

        Ok(())
    }

    #[test]
    fn test_logical_type_get_from_text() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let lt = LogicalType::from_text(&conn, "UNION(text VARCHAR, num INTEGER, delim TINYINT)")?;

        assert_eq!(lt.name()?, "UNION");
        assert_eq!(lt.param_count()?, 3);
        assert_eq!(
            lt.get_param(0)?.1.logical_type()?.type_id(),
            LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR
        );
        assert_eq!(
            lt.get_param(1)?.1.logical_type()?.type_id(),
            LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER
        );
        assert_eq!(
            lt.get_param(2)?.1.logical_type()?.type_id(),
            LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_TINYINT
        );

        Ok(())
    }
}
