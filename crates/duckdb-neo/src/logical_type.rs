//! Construct and inspect DuckDB logical types.

use crate::ffi::DUCKDB_V2_LOGICAL_TYPE_ID;

use crate::connection::HasFactory;
use crate::ffi_str::DuckDBStr;
use crate::{
    AsRaw, DropHandle, FromRaw, Parameters, Result, check_api_call, check_api_call_string, connection::FFILink, ffi,
    qualified_name::QualifiedName, raw::RawExt, value::Value,
};

/// DuckDB's logical type identifier.
pub type LogicalTypeID = DUCKDB_V2_LOGICAL_TYPE_ID;

/// An owned DuckDB logical type.
///
/// Types can be created from primitive IDs, parsed SQL, or a type name with
/// value parameters. Parameter inspection exposes the information needed to
/// reconstruct parameterized types such as `DECIMAL`, `MAP`, and `STRUCT`.
///
/// A type resolved from the catalog, such as an ENUM made with `CREATE TYPE`,
/// shares instance-owned storage and holds no reference to its instance.
///
/// # Example
/// ```
/// use duckdb_neo::{environment::Environment, environment::StorageLocation};
/// use duckdb_neo::logical_type::{LogicalType, LogicalTypeID};
///
/// # fn main() -> duckdb_neo::Result<()> {
/// let env = Environment::new()?;
/// let instance = env.open(StorageLocation::InMemory)?;
/// let conn = instance.connect()?;
/// let logical_type = LogicalType::from_text(&conn, "MAP(VARCHAR, INTEGER)")?;
///
/// assert_eq!(
///     logical_type.id()?,
///     LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_MAP
/// );
/// assert_eq!(logical_type.param_count()?, 2);
/// # Ok(())
/// # }
/// ```
#[derive(AsRaw, FromRaw, DropHandle)]
#[drop_handle(ffi::duckdb_v2_logical_type_destroy)]
pub struct LogicalType {
    pub(crate) handle: ffi::duckdb_v2_logical_type_handle,
}

// SAFETY: an owned C++ `LogicalType` that `&self` methods only read; shared parts are atomically
// refcounted. A catalog-resolved type shares instance storage, so it must not outlive the instance.
unsafe impl Send for LogicalType {}
unsafe impl Sync for LogicalType {}

fn with_type_parameters<C: FFILink + ?Sized, T>(
    link: &C,
    parameters: Parameters<'_>,
    call: impl FnOnce(*const ffi::duckdb_v2_str, *const ffi::duckdb_v2_value_handle, u64) -> Result<T>,
) -> Result<T> {
    let (names, parameter_values) = parameters.into_values(&link)?;
    let names = names.map(|names| names.into_iter().map(Into::into).collect::<Vec<_>>());
    let values = parameter_values
        .iter()
        .map(|value| value.as_value().handle)
        .collect::<Vec<_>>();
    let names_ptr = names
        .as_ref()
        .filter(|names| !names.is_empty())
        .map_or(std::ptr::null(), |names| names.as_ptr());
    let values_ptr = if values.is_empty() {
        std::ptr::null()
    } else {
        values.as_ptr()
    };
    call(names_ptr, values_ptr, values.len() as u64)
}

impl LogicalType {
    /// Construct a primitive or parameterized type by ID; empty parameter names mean positional arguments.
    /// Child types must be TYPE values from [`Value::from_logical_type`]; catalog types borrow instance storage.
    pub fn create_from_id<C: FFILink + ?Sized>(
        link: &C,
        type_id: LogicalTypeID,
        parameters: Parameters<'_>,
    ) -> Result<Self> {
        with_type_parameters(link, parameters, |names, values, count| {
            check_api_call!(
                ffi::duckdb_v2_factory_create_type_from_id,
                link.factory()?.raw(),
                type_id,
                names,
                values,
                count,
                RET
            )
            .map(|handle| LogicalType { handle })
        })
    }

    /// Return a copy of this type named `alias`.
    pub fn with_alias<C: HasFactory + ?Sized>(&self, link: &C, alias: &str) -> Result<Self> {
        check_api_call!(
            ffi::duckdb_v2_factory_create_type_with_alias,
            link.factory()?.raw(),
            self.raw(),
            &alias.into(),
            RET
        )
        .map(|handle| LogicalType { handle })
    }

    /// Parse a SQL type using a connection or callback context.
    pub fn from_text<C: HasFactory + ?Sized>(link: &C, text: &str) -> Result<Self> {
        check_api_call!(
            ffi::duckdb_v2_factory_create_type_from_text,
            link.factory()?.raw(),
            &text.into(),
            RET
        )
        .map(|handle| Self { handle })
    }

    /// Resolve a SQL-qualified or quoted type `name`; empty parameter names mean positional arguments.
    /// Child types must be TYPE values from [`Value::from_logical_type`]; catalog types borrow instance storage.
    pub fn create<C: FFILink + ?Sized>(link: &C, name: &str, parameters: Parameters<'_>) -> Result<Self> {
        with_type_parameters(link, parameters, |names, values, count| {
            let qname = QualifiedName::from_sql(name)?;
            check_api_call!(
                ffi::duckdb_v2_factory_create_type_from_name,
                link.factory()?.raw(),
                qname.raw(),
                names,
                values,
                count,
                RET
            )
            .map(|handle| LogicalType { handle })
        })
    }

    /// Return the logical type ID.
    pub fn id(&self) -> Result<LogicalTypeID> {
        check_api_call!(ffi::duckdb_v2_logical_type_get_id, self.handle, RET)
    }

    /// Return the alias or canonical type name.
    pub fn name(&self) -> Result<&str> {
        check_api_call!(ffi::duckdb_v2_logical_type_get_name, self.handle, RET)
            .map(|name| DuckDBStr::from_raw(name).as_str().unwrap_or_default())
    }

    /// Render the type as SQL text.
    pub fn to_text(&self) -> Result<String> {
        check_api_call_string!(ffi::duckdb_v2_logical_type_to_text, self.handle)
    }

    /// Return the number of value parameters carried by the type.
    pub fn param_count(&self) -> Result<usize> {
        check_api_call!(ffi::duckdb_v2_logical_type_get_param_count, self.handle, RET).map(|count| count as usize)
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

        let name = DuckDBStr::from_raw(name).as_str().unwrap_or_default();

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

    pub(crate) fn copy_handle(handle: &mut ffi::duckdb_v2_logical_type_handle) -> Result<LogicalType> {
        check_api_call!(ffi::duckdb_v2_logical_type_copy, *handle, RET).map(|handle| LogicalType { handle })
    }
}

impl crate::ToValue for LogicalType {
    fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
        Value::from_logical_type(link, self)
    }
}

impl std::fmt::Display for LogicalType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_text().map_err(|_| std::fmt::Error)?)
    }
}

impl std::fmt::Debug for LogicalType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let text = self.to_text().unwrap_or_else(|error| format!("<{error}>"));
        f.debug_tuple("LogicalType").field(&text).finish()
    }
}

impl Eq for LogicalType {}

impl PartialEq for LogicalType {
    fn eq(&self, other: &Self) -> bool {
        check_api_call!(ffi::duckdb_v2_logical_type_is_equal, self.handle, other.handle, RET)
            .expect("Failed to compare logical types")
    }
}

impl Clone for LogicalType {
    fn clone(&self) -> Self {
        let handle =
            check_api_call!(ffi::duckdb_v2_logical_type_copy, self.handle, RET).expect("Failed to clone logical type");

        LogicalType { handle }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod test {
    use crate::{DuckDBType, environment::Environment, environment::StorageLocation, types::MapValue};

    use super::*;

    #[test]
    fn test_logical_type_as_parameter() -> crate::Result<()> {
        let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
        let child = i32::logical_type(&conn)?;
        let list = LogicalType::create(&conn, "LIST", Parameters::positional(&[&child]))?;
        assert_eq!(list.to_text()?, "INTEGER[]");
        assert_eq!(list.get_param(0)?.1.get::<LogicalType>()?, Some(child));
        Ok(())
    }

    #[test]
    fn test_logical_type_formatting_and_equality() -> crate::Result<()> {
        fn assert_eq_type<T: Eq>() {}
        assert_eq_type::<LogicalType>();
        let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
        for text in ["INTEGER", "MAP(VARCHAR, INTEGER)"] {
            let logical_type = LogicalType::from_text(&conn, text)?;
            assert_eq!(logical_type.to_text()?, text);
            assert_eq!(format!("{logical_type}"), text);
            assert_eq!(format!("{logical_type:?}"), format!("LogicalType({text:?})"));
            assert_eq!(logical_type, logical_type.clone());
        }
        let alias = i32::logical_type(&conn)?.with_alias(&conn, "temperature")?;
        assert_eq!(format!("{alias}"), "temperature");
        assert_eq!(format!("{alias:?}"), "LogicalType(\"temperature\")");
        assert_ne!(alias, i32::logical_type(&conn)?);
        Ok(())
    }

    #[test]
    fn test_empty_type_parameters_use_null_buffers() -> crate::Result<()> {
        let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
        for parameters in [Parameters::None, Parameters::positional(&[]), Parameters::named(&[])] {
            with_type_parameters(&conn, parameters, |names, values, count| {
                assert!(names.is_null());
                assert!(values.is_null());
                assert_eq!(count, 0);
                Ok(())
            })?;
        }
        assert_eq!(
            LogicalType::create(&conn, "INTEGER", Parameters::None)?.name()?,
            "INTEGER"
        );
        assert_eq!(
            LogicalType::create_from_id(
                &conn,
                LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER,
                Parameters::None
            )?
            .name()?,
            "INTEGER"
        );
        Ok(())
    }

    #[test]
    fn test_create_map_type() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;

        let key_type = Value::from_logical_type(&conn, &i32::logical_type(&conn)?)?;
        let value_type = Value::from_logical_type(&conn, &String::logical_type(&conn)?)?;
        let ltype = LogicalType::create(&conn, "map", Parameters::positional(&[&key_type, &value_type]))?;

        assert_eq!(ltype.name()?, "MAP");
        assert_eq!(ltype.param_count()?, 2);
        assert_eq!(
            ltype.get_param(0)?.1.type_value_get_inner()?.id()?,
            LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER
        );
        assert_eq!(
            ltype.get_param(1)?.1.type_value_get_inner()?.id()?,
            LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR
        );

        Ok(())
    }

    #[test]
    fn test_create_from_id_named() -> crate::Result<()> {
        let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;

        let int_type = Value::from_logical_type(&conn, &i32::logical_type(&conn)?)?;
        let text_type = Value::from_logical_type(&conn, &String::logical_type(&conn)?)?;
        let ltype = LogicalType::create_from_id(
            &conn,
            LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_STRUCT,
            Parameters::named(&[("id", &int_type), ("label", &text_type)]),
        )?;

        assert_eq!(ltype.id()?, LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_STRUCT);
        let params = ltype.get_params()?;
        assert_eq!(
            params.iter().map(|(name, _)| name.as_str()).collect::<Vec<_>>(),
            ["id", "label"]
        );
        assert_eq!(
            params[1].1.type_value_get_inner()?.id()?,
            LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR
        );

        Ok(())
    }

    #[test]
    fn test_logical_type_alias() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;

        let ltype = MapValue::<i32, String>::logical_type(&conn)?;

        let alias_ltype = ltype.with_alias(&conn, "my_map")?;

        assert_eq!(alias_ltype.name()?, "my_map");
        assert_eq!(alias_ltype.param_count()?, 2);
        assert_eq!(alias_ltype.id()?, LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_MAP);

        assert_eq!(
            alias_ltype.get_param(0)?.1.type_value_get_inner()?.id()?,
            LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER
        );
        assert_eq!(
            alias_ltype.get_param(1)?.1.type_value_get_inner()?.id()?,
            LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR
        );

        Ok(())
    }

    #[test]
    fn test_logical_type_get_from_text() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;

        let lt = LogicalType::from_text(&conn, "UNION(text VARCHAR, num INTEGER, delim TINYINT)")?;

        assert_eq!(lt.name()?, "UNION");
        assert_eq!(lt.param_count()?, 3);
        assert_eq!(
            lt.get_param(0)?.1.type_value_get_inner()?.id()?,
            LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR
        );
        assert_eq!(
            lt.get_param(1)?.1.type_value_get_inner()?.id()?,
            LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER
        );
        assert_eq!(
            lt.get_param(2)?.1.type_value_get_inner()?.id()?,
            LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_TINYINT
        );

        Ok(())
    }
}
